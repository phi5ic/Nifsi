use bytes::{Buf, BufMut, BytesMut};
use chacha20poly1305::{
    aead::{Aead, KeyInit},
    ChaCha20Poly1305, Key, Nonce,
};
use nifsi_core::protocol::{
    SliceAnnouncement, SliceTransferRequest, SliceTransferResponse,
};
use std::convert::TryFrom;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

/// Hardware Cluster Master Key (256-bit / 32 bytes)
/// Cryptographically aligned with the Renesas RA4M1 Arduino Uno R4 HSM Flash
/// MADE WITH AI
pub const HARDWARE_CLUSTER_KEY: [u8; 32] = [
    0x7f, 0x8e, 0x9a, 0x2b, 0x4c, 0x10, 0xd3, 0xe4,
    0xf5, 0x06, 0x17, 0x28, 0x39, 0x4a, 0x5b, 0x6c,
    0x81, 0x92, 0xa3, 0xb4, 0xc5, 0xd6, 0xe7, 0xf8,
    0x09, 0x1a, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70,
];

pub const FRAME_MAGIC: &[u8; 4] = b"NIFS";
pub const MAGIC_LEN: usize = 4;
pub const NONCE_LEN: usize = 12;
pub const LENGTH_FIELD_LEN: usize = 4;
pub const TAG_LEN: usize = 16;
pub const WIRE_HEADER_SIZE: usize = MAGIC_LEN + NONCE_LEN + LENGTH_FIELD_LEN; // 20

use std::sync::OnceLock;

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);
static SESSION_SALT: OnceLock<[u8; 4]> = OnceLock::new();

/// Initialises or retrieves the 4-byte session salt.
///
/// Incorporates process ID and epoch nanoseconds to guarantee distinct
/// salt values across process lifecycles and nodes, preventing nonce collisions
/// when encrypting under the same 256-bit cluster master key.
fn get_session_salt() -> [u8; 4] {
    *SESSION_SALT.get_or_init(|| {
        use std::time::SystemTime;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();
        let nanos = now.as_nanos();
        let pid = std::process::id() as u128;
        let mixed = (nanos ^ (pid << 16) ^ 0x6e696673) as u32; // "nifs" xor
        // Also prime NONCE_COUNTER from epoch microseconds if uninitialised
        let micros = now.as_micros() as u64;
        let _ = NONCE_COUNTER.compare_exchange(0, micros, Ordering::SeqCst, Ordering::Relaxed);
        mixed.to_be_bytes()
    })
}

pub fn seed_nonce_session(node_id: &str) {
    let mut hash = 0x811c9dc5u32;
    for b in node_id.bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    let _ = SESSION_SALT.set(hash.to_be_bytes());
}

fn generate_nonce() -> [u8; 12] {
    let salt = get_session_salt();
    let mut nonce = [0u8; 12];
    nonce[0..4].copy_from_slice(&salt);
    let count = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    nonce[4..12].copy_from_slice(&count.to_be_bytes());
    nonce
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Incomplete message header")]
    IncompleteHeader,
    #[error("Incomplete payload")]
    IncompletePayload,
    #[error("ChaCha20-Poly1305 AEAD decryption or authentication failed")]
    AeadAuthenticationFailed,
    #[error("Invalid frame magic prefix")]
    InvalidMagic,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Heartbeat = 0x01,
    FsRead = 0x02,
    FsWrite = 0x03,
    FsList = 0x04,
    FsMetadata = 0x05,
    ProcSpawn = 0x06,
    ProcSignal = 0x07,
    ProcStream = 0x08,
    RaftVote = 0x09,
    RaftAppend = 0x0A,
    RaftResponse = 0x0B,
    ChunkTransfer = 0x0C,
    ChunkRequest = 0x0D,
    NodeJoin = 0x0E,
    NodeLeave = 0x0F,
    SwarmGossip = 0x10,
    SliceTransfer = 0x11,
    SliceRequest = 0x12,
    SliceAnnouncement = 0x13,
}

impl TryFrom<u8> for MessageType {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(MessageType::Heartbeat),
            0x02 => Ok(MessageType::FsRead),
            0x03 => Ok(MessageType::FsWrite),
            0x04 => Ok(MessageType::FsList),
            0x05 => Ok(MessageType::FsMetadata),
            0x06 => Ok(MessageType::ProcSpawn),
            0x07 => Ok(MessageType::ProcSignal),
            0x08 => Ok(MessageType::ProcStream),
            0x09 => Ok(MessageType::RaftVote),
            0x0A => Ok(MessageType::RaftAppend),
            0x0B => Ok(MessageType::RaftResponse),
            0x0C => Ok(MessageType::ChunkTransfer),
            0x0D => Ok(MessageType::ChunkRequest),
            0x0E => Ok(MessageType::NodeJoin),
            0x0F => Ok(MessageType::NodeLeave),
            0x10 => Ok(MessageType::SwarmGossip),
            0x11 => Ok(MessageType::SliceTransfer),
            0x12 => Ok(MessageType::SliceRequest),
            0x13 => Ok(MessageType::SliceAnnouncement),
            _ => Err(ProtocolError::InvalidMessageType(value)),
        }
    }
}

pub const HEADER_SIZE: usize = 13;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub msg_type: MessageType,
    pub size: u32,
    pub req_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(msg_type: MessageType, req_id: u64, payload: Vec<u8>) -> Self {
        Self {
            header: MessageHeader {
                msg_type,
                size: payload.len() as u32,
                req_id,
            },
            payload,
        }
    }
    pub fn new_slice_request(req_id: u64, req: &SliceTransferRequest) -> Result<Self, bincode::Error> {
        let payload = bincode::serialize(req)?;
        Ok(Self::new(MessageType::SliceRequest, req_id, payload))
    }
    pub fn new_slice_transfer(req_id: u64, resp: &SliceTransferResponse) -> Result<Self, bincode::Error> {
        let payload = bincode::serialize(resp)?;
        Ok(Self::new(MessageType::SliceTransfer, req_id, payload))
    }
    pub fn new_slice_announcement(req_id: u64, ann: &SliceAnnouncement) -> Result<Self, bincode::Error> {
        let payload = bincode::serialize(ann)?;
        Ok(Self::new(MessageType::SliceAnnouncement, req_id, payload))
    }
    pub fn parse_slice_request(&self) -> Result<SliceTransferRequest, bincode::Error> {
        bincode::deserialize(&self.payload)
    }
    pub fn parse_slice_transfer(&self) -> Result<SliceTransferResponse, bincode::Error> {
        bincode::deserialize(&self.payload)
    }
    pub fn parse_slice_announcement(&self) -> Result<SliceAnnouncement, bincode::Error> {
        bincode::deserialize(&self.payload)
    }
    /// Encrypts and frames the message using ChaCha20-Poly1305 AEAD
    pub fn encode(&self, dst: &mut BytesMut) {
        let mut plaintext = Vec::with_capacity(9 + self.payload.len());
        plaintext.push(self.header.msg_type as u8);
        plaintext.extend_from_slice(&self.header.req_id.to_be_bytes());
        plaintext.extend_from_slice(&self.payload);

        let nonce_bytes = generate_nonce();
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&HARDWARE_CLUSTER_KEY));
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext.as_ref())
            .expect("ChaCha20Poly1305 encryption should not fail");

        let frame_len = WIRE_HEADER_SIZE + ciphertext.len();
        dst.reserve(frame_len);
        dst.put_slice(FRAME_MAGIC);
        dst.put_slice(&nonce_bytes);
        dst.put_u32(ciphertext.len() as u32);
        dst.put_slice(&ciphertext);
    }

    /// Decrypts and authenticates a ChaCha20-Poly1305 AEAD frame
    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ProtocolError> {
        if src.len() < WIRE_HEADER_SIZE {
            return Ok(None);
        }

        if &src[0..4] != FRAME_MAGIC {
            return Err(ProtocolError::InvalidMagic);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&src[16..20]);
        let ciphertext_len = u32::from_be_bytes(len_bytes) as usize;

        if src.len() < WIRE_HEADER_SIZE + ciphertext_len {
            return Ok(None);
        }

        // Full encrypted frame available in buffer
        src.advance(4); // consume magic
        let mut nonce_bytes = [0u8; 12];
        src.copy_to_slice(&mut nonce_bytes); // consume 12 bytes nonce
        let _len = src.get_u32(); // consume 4 bytes len

        let mut ciphertext = vec![0u8; ciphertext_len];
        src.copy_to_slice(&mut ciphertext);

        let cipher = ChaCha20Poly1305::new(Key::from_slice(&HARDWARE_CLUSTER_KEY));
        let nonce = Nonce::from_slice(&nonce_bytes);
        let plaintext = cipher
            .decrypt(nonce, ciphertext.as_ref())
            .map_err(|_| ProtocolError::AeadAuthenticationFailed)?;

        if plaintext.len() < 9 {
            return Err(ProtocolError::IncompleteHeader);
        }

        let msg_type = MessageType::try_from(plaintext[0])?;
        let mut req_id_bytes = [0u8; 8];
        req_id_bytes.copy_from_slice(&plaintext[1..9]);
        let req_id = u64::from_be_bytes(req_id_bytes);
        let payload = plaintext[9..].to_vec();

        Ok(Some(Message {
            header: MessageHeader {
                msg_type,
                size: payload.len() as u32,
                req_id,
            },
            payload,
        }))
    }
}

pub struct NifsiCodec;

impl Decoder for NifsiCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        Message::decode(src)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
    }
}

impl Encoder<Message> for NifsiCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aead_encode_decode() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF, 0x42, 0x13, 0x37];
        let msg = Message::new(MessageType::Heartbeat, 12345, payload.clone());

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        // Header (20) + Plaintext (1 + 8 + 7 = 16) + Poly1305 tag (16) = 52 bytes
        assert_eq!(buf.len(), WIRE_HEADER_SIZE + 16 + 16);

        let decoded = Message::decode(&mut buf).unwrap().expect("Decoded message");
        assert_eq!(decoded.header.msg_type, MessageType::Heartbeat);
        assert_eq!(decoded.header.size, payload.len() as u32);
        assert_eq!(decoded.header.req_id, 12345);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn test_incomplete_decode() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let msg = Message::new(MessageType::FsRead, 999, payload);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        // Remove one byte from the end
        buf.truncate(buf.len() - 1);

        let decoded = Message::decode(&mut buf).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = BytesMut::new();
        buf.put_slice(b"BAD!");
        buf.put_slice(&[0u8; 12]);
        buf.put_u32(16);
        buf.put_slice(&[0u8; 16]);

        let result = Message::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::InvalidMagic)));
    }

    #[test]
    fn test_tamper_detection() {
        let payload = vec![1, 2, 3, 4, 5];
        let msg = Message::new(MessageType::FsWrite, 42, payload);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        // Flip 1 bit in the ciphertext
        let last_idx = buf.len() - 1;
        buf[last_idx] ^= 0x01;

        let result = Message::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::AeadAuthenticationFailed)));
    }

    #[test]
    fn test_slice_request_encode_decode() {
        let req = SliceTransferRequest {
            file_id: [1u8; 16],
            chunk_id: [2u8; 32],
            chunk_sequence_idx: 0,
            slice_idx: 3,
            kind: nifsi_core::protocol::SliceParityKind::Data,
        };
        let msg = Message::new_slice_request(777, &req).unwrap();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let decoded = Message::decode(&mut buf).unwrap().expect("Decoded message");
        assert_eq!(decoded.header.msg_type, MessageType::SliceRequest);
        assert_eq!(decoded.header.req_id, 777);
        let parsed = decoded.parse_slice_request().unwrap();
        assert_eq!(parsed, req);
    }

    #[test]
    fn test_slice_transfer_4kb_wire_framing() {
        use nifsi_core::protocol::BiologicalSliceWire;
        let slice = BiologicalSliceWire {
            slice_id: [0xaa; 32],
            chunk_id: [0xbb; 32],
            file_id: [0xcc; 16],
            chunk_sequence_idx: 1,
            slice_idx: 5,
            total_slices: 16,
            kind: nifsi_core::protocol::SliceParityKind::Data,
            left_arm: None,
            right_arm: None,
            cohort_id: [0xdd; 16],
            data: vec![0x42; 4096],
        };
        let resp = SliceTransferResponse {
            file_id: [0xcc; 16],
            chunk_id: [0xbb; 32],
            chunk_sequence_idx: 1,
            slice_idx: 5,
            kind: nifsi_core::protocol::SliceParityKind::Data,
            slice: Some(slice.clone()),
            error: None,
        };
        let msg = Message::new_slice_transfer(888, &resp).unwrap();
        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let decoded = Message::decode(&mut buf).unwrap().expect("Decoded slice message");
        assert_eq!(decoded.header.msg_type, MessageType::SliceTransfer);
        let parsed = decoded.parse_slice_transfer().unwrap();
        assert_eq!(parsed.slice.unwrap().data.len(), 4096);
    }
}
