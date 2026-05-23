use bytes::{Buf, BufMut, BytesMut};
use std::convert::TryFrom;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};


pub mod config {

}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Invalid message type: {0}")]
    InvalidMessageType(u8),
    #[error("Incomplete message header")]
    IncompleteHeader,
    #[error("Incomplete payload")]
    IncompletePayload,
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
            0x0f => Ok(MessageType::NodeLeave),
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

    pub fn encode(&self, dst: &mut BytesMut) {
        dst.reserve(HEADER_SIZE + self.payload.len());
        dst.put_u8(self.header.msg_type as u8);
        dst.put_u32(self.header.size);
        dst.put_u64(self.header.req_id);
        dst.put_slice(&self.payload);
    }

    pub fn decode(src: &mut BytesMut) -> Result<Option<Self>, ProtocolError> {
        if src.len() <HEADER_SIZE {
            return Ok(None);
        }

        let msg_type_byte=src[0];
        let msg_type=MessageType::try_from(msg_type_byte)?;

        //peek size (bytes 1..5)
        let mut size_bytes = [0u8; 4];
        size_bytes.copy_from_slice(&src[1..5]);
        let size = u32::from_be_bytes(size_bytes);

        if src.len() < HEADER_SIZE + size as usize {
            return Ok(None);
        }

        src.advance(1);
        let _size = src.get_u32();
        let req_id = src.get_u64();

        let mut payload = vec![0u8; size as usize];
        if size > 0 {
            src.copy_to_slice(&mut payload);
        }

        Ok(Some(Message {
            header: MessageHeader {
                msg_type,
                size,
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
        Message::decode(src).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
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
    fn test_encode_decode() {
        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let msg = Message::new(MessageType::FsRead, 999, payload);

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        buf.truncate(buf.len() - 1);

        let decoded = Message::decode(&mut buf).unwrap();
        assert!(decoded.is_none());
    }

    #[test]
    fn test_invalid_type() {
        let mut buf = BytesMut::new();
        buf.put_u8(0xFF);
        buf.put_u32(0);
        buf.put_u64(0);

        let result = Message::decode(&mut buf);
        assert!(matches!(result, Err(ProtocolError::InvalidMessageType(0xFF))));
    }

    #[test]
    fn test_full_roundtrip() {
        let payload = vec![0xDE, 0xAD, OxBE, OxEF];
        let msg = Message::new(MessageType::FsRead, 99, payload.clone());

        let mut buf = BytesMut::new();
        msg.encode(&mut buf);

        let decoded = Message::decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.msg_type, MessageType::FsRead);
        assert_eq!(decoded.header.req_id, 999);
        assert_eq!(decoded.payload, payload);
    }
}
