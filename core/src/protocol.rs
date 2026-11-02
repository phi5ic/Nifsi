
use serde::{Deserialize, Serialize};

pub const DEFAULT_SOCKET_PATH: &str = "/tmp/nifsid.sock";
pub const DEFAULT_SLICE_SIZE: usize = 4096;
pub const SLICE_SIZE: usize = 4096; 
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; 
pub const DEFAULT_COHORT_SIZE: usize = 4;

// Cryptographic and protocol type aliases
pub type ChunkId = [u8; 32];
pub type SliceId = [u8; 32];
pub type HomologyArm = [u8; 32];
pub type FileId = [u8; 16];
pub type CohortId = [u8; 16];

// Sub-Packet Sliced Wire Types

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceParityKind {
    Data,
    HorizontalParity,
    DiagonalParity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiologicalSliceWire {
    pub slice_id: SliceId,
    pub chunk_id: ChunkId,
    pub file_id: FileId,
    pub chunk_sequence_idx: u32,
    pub slice_idx: u16,
    pub total_slices: u16,
    pub kind: SliceParityKind,
    pub left_arm: Option<HomologyArm>,
    pub right_arm: Option<HomologyArm>,
    pub cohort_id: CohortId,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceAnnouncement {
    pub node_id: String,
    pub file_id: FileId,
    pub chunk_id: ChunkId,
    pub chunk_sequence_idx: u32,
    pub slice_idx: u16,
    pub slice_id: SliceId,
    pub cohort_id: CohortId,
    pub kind: SliceParityKind,
    pub left_arm: Option<HomologyArm>,
    pub right_arm: Option<HomologyArm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAnnouncement {
    pub node_id: String,
    pub chunk_id: ChunkId,
    pub file_id: FileId,
    pub sequence_idx: u32,
    pub left_arm: Option<HomologyArm>,
    pub right_arm: Option<HomologyArm>,
    pub cohort_id: CohortId,
    pub available_slices: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceTransferRequest {
    pub file_id: FileId,
    pub chunk_id: ChunkId,
    pub chunk_sequence_idx: u32,
    pub slice_idx: u16,
    pub kind: SliceParityKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceTransferResponse {
    pub file_id: FileId,
    pub chunk_id: ChunkId,
    pub chunk_sequence_idx: u32,
    pub slice_idx: u16,
    pub kind: SliceParityKind,
    pub slice: Option<BiologicalSliceWire>,
    pub error: Option<String>,
}

// Partial Write Journaling & Delta Types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceDelta {
    pub file_id: FileId,
    pub cohort_id: CohortId,
    pub sequence_idx: u32,
    pub slice_idx: u16,
    pub cohort_idx: u16,
    pub offset_in_slice: u16,
    pub delta_bytes: Vec<u8>,
    pub new_slice_hash: SliceId,
}

/// Distributed filesystem mutation event replicated across cluster peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FsMutation {
    WriteSlice {
        version: u64,
        file_id: FileId,
        chunk_sequence_idx: u32,
        slice_idx: u16,
        slice: BiologicalSliceWire,
    },
    SliceDeltaJournal {
        version: u64,
        delta: SliceDelta,
    },
    WriteFile {
        version: u64,
        name: String,
        data: Vec<u8>,
    },
    DeleteFile {
        version: u64,
        name: String,
    },
}

impl FsMutation {
    pub fn version(&self) -> u64 {
        match self {
            FsMutation::WriteSlice { version, .. } => *version,
            FsMutation::SliceDeltaJournal { version, .. } => *version,
            FsMutation::WriteFile { version, .. } => *version,
            FsMutation::DeleteFile { version, .. } => *version,
        }
    }

    pub fn file_name(&self) -> Option<&str> {
        match self {
            FsMutation::WriteFile { name, .. } => Some(name.as_str()),
            FsMutation::DeleteFile { name, .. } => Some(name.as_str()),
            _ => None,
        }
    }
}

// Local IPC Protocol (Daemon <-> CLI / Web)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IpcRequest {
    Status,
    InspectFile { name: String },
    ListSlices { file: String, chunk_idx: u32 },
    RepairSlice { file: String, chunk_idx: u32, slice_idx: u16 },
    Join { address: String },
    Leave,
    Evict { node: String },
    Mount { mountpoint: String },
    ListStrands,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub id: String,
    pub address: String,
    pub is_leader: bool,
    pub rtt_ms: f64,
    pub disk_usage: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClusterStatusResponse {
    pub node_id: String,
    pub term: u64,
    pub role: String,
    pub leader_id: Option<String>,
    pub peers: Vec<ClusterNodeInfo>,
    pub active_chunks: usize,
    pub detected_breaks: usize,
    pub repaired_chunks: usize,
    pub mountpoint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceInfo {
    pub slice_idx: u16,
    pub slice_id: String,
    pub kind: SliceParityKind,
    pub size: usize,
    pub left_arm: Option<String>,
    pub right_arm: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileInspectionResponse {
    pub file_name: String,
    pub file_id: String,
    pub file_size: usize,
    pub total_chunks: u32,
    pub total_slices: usize,
    pub cohorts: usize,
    pub healthy_slices: usize,
    pub degraded_slices: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IpcResponse {
    Status(ClusterStatusResponse),
    FileInspection(FileInspectionResponse),
    SliceList(Vec<SliceInfo>),
    Success(String),
    Error(String),
}
