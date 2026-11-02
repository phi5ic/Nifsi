//! Homology-Arm Distributed Repair (HADR)
//!
//! A biomimetic self-healing storage protocol inspired by *Deinococcus radiodurans*
//! Extended Synthesis-Dependent Strand Annealing (ESDSA) and CRISPR-Cas guide-directed repair.
//!
//! # Architecture
//!
//! 1. **The Biological Strand (`Strand`):**
//!    Files are sliced into linked [`BiologicalChunk`] chains and sub-packet [`BiologicalSlice`]
//!    arrays. Every chunk and slice carries overlapping cryptographic [`HomologyArm`] signatures
//!    linking it to adjacent neighbors, plus a cohort parity spacer token.
//!
//! 2. **Sub-Packet Sliced Array Coding (HADR-v2):**
//!    Chunks are partitioned into addressable 4 KB sub-packet slices (`BiologicalSlice`).
//!    Cohorts maintain independent horizontal slice parity ($P_h[s] = \bigoplus D_i[s]$)
//!    and diagonal array parity ($P_d[s] = \bigoplus D_i[(s + i) \bmod M]$).
//!
//! 3. **Degraded Reads & Repair (`CasRepairEngine`):**
//!    Degraded reads at a specific slice offset retrieve only $k \times 4\text{ KB}$ across
//!    the network instead of $k \times \text{ChunkSize}$, reducing network bandwidth by 16x–256x.
//!
//! 4. **Strand & Slice Break Detection (`StrandDetector`):**
//!    Autonomous decentralized break detection operates at both chunk granularity (`StrandBreak`)
//!    and sub-packet slice granularity (`SliceBreak`).
//!
//! 5. **Partial Write Optimization (`SliceDeltaJournal`):**
//!    Modifications generate bitwise XOR deltas ($\Delta = D_{\text{old}} \oplus D_{\text{new}}$),
//!    incrementally updating parities ($P^{\text{new}} = P^{\text{old}} \oplus \Delta$) without
//!    whole-chunk read-modify-write.
//!    
//!    THIS IS A MODEL CODE CREATED BY AN AI.... ONLY FOR TESTING PURPOSES

use nifsi_core::protocol::SliceDelta;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Type definitions & Constants
// ---------------------------------------------------------------------------

pub const SLICE_SIZE: usize = 4096; // 4 KB sub-packet slice
pub const DEFAULT_SLICE_SIZE: usize = 4096;
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64 KB chunk
pub const DEFAULT_COHORT_SIZE: usize = 4; // 4 data chunks per cohort

pub type ChunkId = [u8; 32];
pub type SliceId = [u8; 32];
pub type HomologyArm = [u8; 32];
pub type FileId = [u8; 16];
pub type CohortId = [u8; 16];

/// Error types encountered during HADR encoding, break detection, or repair.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum HadrError {
    #[error("Empty data cannot be encoded into a strand")]
    EmptyData,

    #[error("Chunk size must be greater than 0")]
    InvalidChunkSize,

    #[error("Cohort size must be greater than 0")]
    InvalidCohortSize,

    #[error("Slice size must be greater than 0 and align with chunk boundaries")]
    InvalidSliceSize,

    #[error("Missing chunk at sequence index {0}")]
    MissingSequenceIndex(u32),

    #[error("Missing slice {slice_idx} in chunk at sequence index {chunk_seq}")]
    MissingSliceIndex { chunk_seq: u32, slice_idx: u16 },

    #[error("Strand continuity violation between chunk {0} and chunk {1}")]
    ContinuityViolation(u32, u32),

    #[error("Slice continuity violation in chunk {chunk_seq} between slice {slice_a} and slice {slice_b}")]
    SliceContinuityViolation { chunk_seq: u32, slice_a: u16, slice_b: u16 },

    #[error("Cohort mismatch: expected cohort {expected:?}, found {found:?}")]
    CohortMismatch { expected: CohortId, found: CohortId },

    #[error("Insufficient surviving chunks to perform repair (found {found}, need {needed})")]
    InsufficientChunks { found: usize, needed: usize },

    #[error("Insufficient surviving slices to perform repair (found {found}, need {needed})")]
    InsufficientSlices { found: usize, needed: usize },

    #[error("Homology arm verification failed: reconstructed payload does not match expected hash")]
    HomologyMismatch,

    #[error("Slice homology verification failed at slice {slice_idx}")]
    SliceHomologyMismatch { slice_idx: u16 },

    #[error("Target chunk is not part of the provided cohort")]
    TargetNotInCohort,

    #[error("Target slice is not part of the provided chunk")]
    TargetNotInChunk,

    #[error("Slice delta offset {offset} out of bounds for slice length {slice_len}")]
    DeltaOffsetOutOfBounds { offset: usize, slice_len: usize },
}

// ---------------------------------------------------------------------------
// Cryptographic Hash Helper
// ---------------------------------------------------------------------------

/// Compute the SHA-256 digest of an arbitrary byte slice.
pub fn hash_sha256(data: &[u8]) -> [u8; 32] {
    let mut context = Context::new(&SHA256);
    context.update(data);
    let digest = context.finish();
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

// ---------------------------------------------------------------------------
// Core Data Structures
// ---------------------------------------------------------------------------

/// A 4 KB sub-packet storage slice equipped with intra-chunk homology arms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiologicalSlice {
    /// 0-indexed position of this slice within its parent chunk (0..M-1).
    pub slice_idx: u16,

    /// Content hash (SHA-256) of parent chunk.
    pub chunk_id: ChunkId,

    /// Unique identifier of the file/stream this slice belongs to.
    pub file_id: FileId,

    /// 0-indexed sequence index of parent chunk within parent strand.
    pub sequence_idx: u32,

    /// Byte offset within the parent chunk payload.
    pub offset: usize,

    /// Payload data bytes of this slice (up to 4096 bytes).
    pub data: Vec<u8>,

    /// Cryptographic content hash (SHA-256) of this slice's data.
    pub slice_hash: SliceId,

    /// Intra-chunk left homology arm: Hash of slice (s - 1), or None if head slice.
    pub left_arm: Option<HomologyArm>,

    /// Intra-chunk right homology arm: Hash of slice (s + 1), or None if tail slice.
    pub right_arm: Option<HomologyArm>,

    /// Parity cohort identifier this slice participates in.
    pub cohort_id: CohortId,
}

/// A biological storage chunk equipped with inter-chunk homology arms and sub-packet slices.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BiologicalChunk {
    /// Content hash (SHA-256) of this chunk's payload data.
    pub chunk_id: ChunkId,

    /// Unique identifier of the file/stream this chunk belongs to.
    pub file_id: FileId,

    /// 0-indexed position within the parent file strand.
    pub sequence_idx: u32,

    /// Total number of chunks in this strand.
    pub total_chunks: u32,

    /// Homology Arm Left: Hash of chunk (N - 1), or None if this is the head chunk.
    pub left_arm: Option<HomologyArm>,

    /// Homology Arm Right: Hash of chunk (N + 1), or None if this is the tail chunk.
    pub right_arm: Option<HomologyArm>,

    /// Primary payload data bytes (preserved for backward compatibility).
    pub data: Vec<u8>,

    /// Addressable sub-packet slices comprising this chunk (HADR-v2).
    pub slices: Vec<BiologicalSlice>,

    /// Sub-packet slice size in bytes (e.g., 4096).
    pub slice_size: usize,

    /// Parity cohort identifier this chunk participates in.
    pub cohort_id: CohortId,

    /// Index of this chunk inside its cohort (0..cohort_size - 1).
    pub cohort_idx: u16,

    /// Size of this chunk's cohort (number of data chunks covered by the spacer).
    pub cohort_size: u16,
}

impl BiologicalChunk {
    /// Retrieve a slice by its intra-chunk index.
    pub fn get_slice(&self, slice_idx: u16) -> Option<&BiologicalSlice> {
        self.slices.get(slice_idx as usize)
    }

    /// Retrieve the slice covering a given offset within the chunk.
    pub fn slice_for_offset(&self, offset_in_chunk: usize) -> Option<&BiologicalSlice> {
        if self.slice_size == 0 {
            return None;
        }
        let idx = offset_in_chunk / self.slice_size;
        self.slices.get(idx)
    }
}

/// A CRISPR-like parity spacer generated for a cohort of chunks with dual horizontal and diagonal array parity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CohortSpacer {
    /// Identifier of the cohort this spacer protects.
    pub cohort_id: CohortId,

    /// Identifier of the file this cohort belongs to.
    pub file_id: FileId,

    /// Number of data chunks in this cohort.
    pub cohort_size: u16,

    /// Linear XOR combination of all data chunk payloads in this cohort.
    /// Preserved for backward compatibility with monolithic callers.
    pub parity_data: Vec<u8>,

    /// Horizontal parity slices P_h[s] for s in 0..M-1.
    pub horizontal_parity_slices: Vec<Vec<u8>>,

    /// Diagonal array parity slices P_d[s] for s in 0..M-1.
    pub diagonal_parity_slices: Vec<Vec<u8>>,

    /// Ordered list of chunk IDs that comprise this cohort.
    pub member_chunk_ids: Vec<ChunkId>,

    /// Lengths of each member's data payload (needed for variable-length last chunks).
    pub member_lengths: Vec<usize>,
}

/// A complete file encoded as a self-annealing biological strand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Strand {
    pub file_id: FileId,
    pub total_chunks: u32,
    pub original_file_size: usize,
    pub chunk_size: usize,
    pub chunks: Vec<BiologicalChunk>,
    pub spacers: Vec<CohortSpacer>,
}

// ---------------------------------------------------------------------------
// Strand Break & Decentralized Detection
// ---------------------------------------------------------------------------

/// Lightweight announcement gossiped by a node about a chunk it currently stores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkAnnouncement {
    pub node_id: String,
    pub chunk_id: ChunkId,
    pub file_id: FileId,
    pub sequence_idx: u32,
    pub left_arm: Option<HomologyArm>,
    pub right_arm: Option<HomologyArm>,
    pub cohort_id: CohortId,
}

/// A detected discontinuity in a file strand across the swarm.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrandBreak {
    pub file_id: FileId,
    pub missing_chunk_id: ChunkId,
    pub missing_sequence_idx: u32,
    pub left_neighbor: Option<(String, ChunkId)>,
    pub right_neighbor: Option<(String, ChunkId)>,
    pub cohort_id: CohortId,
}

/// Guide RNA (gRNA) targeting probe generated to locate and synthesize a missing chunk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GuideRNA {
    pub file_id: FileId,
    pub target_chunk_id: ChunkId,
    pub sequence_idx: u32,
    pub cohort_id: CohortId,
    pub expected_left_arm: Option<HomologyArm>,
    pub expected_right_arm: Option<HomologyArm>,
}

impl GuideRNA {
    /// Synthesize a GuideRNA probe from a detected strand break.
    pub fn from_break(brk: &StrandBreak) -> Self {
        Self {
            file_id: brk.file_id,
            target_chunk_id: brk.missing_chunk_id,
            sequence_idx: brk.missing_sequence_idx,
            cohort_id: brk.cohort_id,
            expected_left_arm: brk.left_neighbor.as_ref().map(|(_, id)| *id),
            expected_right_arm: brk.right_neighbor.as_ref().map(|(_, id)| *id),
        }
    }
}

// ---------------------------------------------------------------------------
// Micro-Tier: Slice Break Detection & GuideRNA Probes
// ---------------------------------------------------------------------------

/// Reason why a slice break was flagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SliceBreakReason {
    /// Slice is completely missing at the given index.
    Missing,
    /// Slice data does not match its cryptographic content hash.
    CorruptedHash,
    /// Intra-chunk homology arm linkage is broken.
    ContinuityViolation,
}

/// A detected discontinuity or corruption in a chunk at the sub-packet slice level.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceBreak {
    pub file_id: FileId,
    pub chunk_id: ChunkId,
    pub chunk_sequence_idx: u32,
    pub slice_idx: u16,
    pub missing_slice_idx: u16,
    pub cohort_id: CohortId,
    pub cohort_idx: u16,
    pub expected_hash: Option<SliceId>,
    pub left_neighbor_hash: Option<SliceId>,
    pub right_neighbor_hash: Option<SliceId>,
    pub reason: SliceBreakReason,
}

/// Guide RNA (gRNA) targeting probe generated to locate, verify, and synthesize a single 4 KB slice.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SliceGuideRNA {
    pub file_id: FileId,
    pub target_chunk_id: ChunkId,
    pub chunk_id: ChunkId,
    pub sequence_idx: u32,
    pub slice_idx: u16,
    pub cohort_id: CohortId,
    pub cohort_idx: u16,
    pub expected_slice_hash: Option<SliceId>,
    pub expected_left_arm: Option<HomologyArm>,
    pub expected_right_arm: Option<HomologyArm>,
    pub slice_len: usize,
}

impl SliceGuideRNA {
    /// Synthesize a SliceGuideRNA probe from a detected slice break.
    pub fn from_break(brk: &SliceBreak) -> Self {
        Self {
            file_id: brk.file_id,
            target_chunk_id: brk.chunk_id,
            chunk_id: brk.chunk_id,
            sequence_idx: brk.chunk_sequence_idx,
            slice_idx: brk.slice_idx,
            cohort_id: brk.cohort_id,
            cohort_idx: brk.cohort_idx,
            expected_slice_hash: brk.expected_hash,
            expected_left_arm: brk.left_neighbor_hash,
            expected_right_arm: brk.right_neighbor_hash,
            slice_len: SLICE_SIZE,
        }
    }

    /// Construct a targeting probe for an arbitrary slice in a known chunk.
    pub fn for_slice(
        chunk: &BiologicalChunk,
        slice_idx: u16,
        expected_hash: Option<SliceId>,
        expected_len: usize,
    ) -> Self {
        let left_arm = if slice_idx > 0 && (slice_idx as usize) <= chunk.slices.len() {
            chunk.slices.get(slice_idx as usize - 1).map(|s| s.slice_hash)
        } else {
            None
        };
        let right_arm = chunk.slices.get(slice_idx as usize + 1).map(|s| s.slice_hash);

        Self {
            file_id: chunk.file_id,
            target_chunk_id: chunk.chunk_id,
            chunk_id: chunk.chunk_id,
            sequence_idx: chunk.sequence_idx,
            slice_idx,
            cohort_id: chunk.cohort_id,
            cohort_idx: chunk.cohort_idx,
            expected_slice_hash: expected_hash,
            expected_left_arm: left_arm,
            expected_right_arm: right_arm,
            slice_len: expected_len,
        }
    }
}

// ---------------------------------------------------------------------------
// Free Parity Calculation Functions
// ---------------------------------------------------------------------------

/// Independent horizontal slice parity calculation:
/// P_h[s] = XOR_{i=0}^{k-1} D_i[s] for s in [0, num_rows - 1].
pub fn compute_horizontal_parity(
    cohort_chunks: &[BiologicalChunk],
    num_rows: usize,
) -> Vec<Vec<u8>> {
    StrandEncoder::compute_horizontal_parity_slices(cohort_chunks, num_rows)
}

/// Cyclic diagonal array parity calculation:
/// P_d[s] = XOR_{i=0}^{k-1} D_i[(s + i) mod M] for s in [0, num_rows - 1].
pub fn compute_diagonal_parity(
    cohort_chunks: &[BiologicalChunk],
    num_rows: usize,
) -> Vec<Vec<u8>> {
    StrandEncoder::compute_diagonal_parity_slices(cohort_chunks, num_rows)
}

// ---------------------------------------------------------------------------
// Strand Encoder & File Assembly
// ---------------------------------------------------------------------------

pub struct StrandEncoder;

impl StrandEncoder {
    /// Partition raw chunk payload bytes into 4 KB sub-packet slices with intra-chunk homology arms.
    pub fn slice_chunk(
        raw_chunk: &[u8],
        slice_size: usize,
        chunk_id: ChunkId,
        file_id: FileId,
        sequence_idx: u32,
        cohort_id: CohortId,
    ) -> Vec<BiologicalSlice> {
        if raw_chunk.is_empty() {
            return Vec::new();
        }

        let raw_slices: Vec<&[u8]> = raw_chunk.chunks(slice_size).collect();
        let total_slices = raw_slices.len();
        let slice_hashes: Vec<SliceId> = raw_slices.iter().map(|s| hash_sha256(s)).collect();

        let mut slices = Vec::with_capacity(total_slices);
        for (s_idx, raw_slice) in raw_slices.iter().enumerate() {
            let left_arm = if s_idx > 0 {
                Some(slice_hashes[s_idx - 1])
            } else {
                None
            };

            let right_arm = if s_idx + 1 < total_slices {
                Some(slice_hashes[s_idx + 1])
            } else {
                None
            };

            slices.push(BiologicalSlice {
                slice_idx: s_idx as u16,
                chunk_id,
                file_id,
                sequence_idx,
                offset: s_idx * slice_size,
                data: raw_slice.to_vec(),
                slice_hash: slice_hashes[s_idx],
                left_arm,
                right_arm,
                cohort_id,
            });
        }

        slices
    }

    /// Horizontal slice parity calculation:
    /// P_h[s] = XOR_{i=0}^{k-1} D_i[s]
    pub fn compute_horizontal_parity_slices(
        cohort_chunks: &[BiologicalChunk],
        num_rows: usize,
    ) -> Vec<Vec<u8>> {
        let mut horizontal_parities = Vec::with_capacity(num_rows);

        for s in 0..num_rows {
            let max_slice_len = cohort_chunks
                .iter()
                .filter_map(|c| c.slices.get(s).map(|sl| sl.data.len()))
                .max()
                .unwrap_or(0);

            let mut p_h = vec![0u8; max_slice_len];
            for chunk in cohort_chunks {
                if let Some(sl) = chunk.slices.get(s) {
                    for (b_idx, &byte) in sl.data.iter().enumerate() {
                        p_h[b_idx] ^= byte;
                    }
                }
            }
            horizontal_parities.push(p_h);
        }

        horizontal_parities
    }

    /// Diagonal array parity calculation:
    /// P_d[s] = XOR_{i=0}^{k-1} D_i[(s + i) mod M]
    pub fn compute_diagonal_parity_slices(
        cohort_chunks: &[BiologicalChunk],
        num_rows: usize,
    ) -> Vec<Vec<u8>> {
        if num_rows == 0 {
            return Vec::new();
        }

        let mut diagonal_parities = Vec::with_capacity(num_rows);

        for s in 0..num_rows {
            let max_slice_len = cohort_chunks
                .iter()
                .enumerate()
                .filter_map(|(i, c)| {
                    let r = (s + i) % num_rows;
                    c.slices.get(r).map(|sl| sl.data.len())
                })
                .max()
                .unwrap_or(0);

            let mut p_d = vec![0u8; max_slice_len];
            for (i, chunk) in cohort_chunks.iter().enumerate() {
                let r = (s + i) % num_rows;
                if let Some(sl) = chunk.slices.get(r) {
                    for (b_idx, &byte) in sl.data.iter().enumerate() {
                        p_d[b_idx] ^= byte;
                    }
                }
            }
            diagonal_parities.push(p_d);
        }

        diagonal_parities
    }

    /// Slice raw file bytes into a biological strand with sub-packet slices,
    /// homology arms, and dual horizontal/diagonal cohort spacers.
    pub fn encode_file(
        file_id: FileId,
        data: &[u8],
        chunk_size: usize,
        cohort_size: usize,
    ) -> Result<Strand, HadrError> {
        let slice_size = if chunk_size >= SLICE_SIZE {
            SLICE_SIZE
        } else {
            chunk_size
        };
        Self::encode_file_with_slice_size(file_id, data, chunk_size, cohort_size, slice_size)
    }

    /// Explicitly parameterized slice size encoding.
    pub fn encode_file_with_slice_size(
        file_id: FileId,
        data: &[u8],
        chunk_size: usize,
        cohort_size: usize,
        slice_size: usize,
    ) -> Result<Strand, HadrError> {
        if data.is_empty() {
            return Err(HadrError::EmptyData);
        }
        if chunk_size == 0 {
            return Err(HadrError::InvalidChunkSize);
        }
        if cohort_size == 0 {
            return Err(HadrError::InvalidCohortSize);
        }
        if slice_size == 0 {
            return Err(HadrError::InvalidSliceSize);
        }

        // 1. Partition data into chunks
        let raw_chunks: Vec<&[u8]> = data.chunks(chunk_size).collect();
        let total_chunks = raw_chunks.len() as u32;

        // 2. Pre-calculate content hashes for all chunks
        let chunk_hashes: Vec<ChunkId> = raw_chunks.iter().map(|c| hash_sha256(c)).collect();

        let mut chunks = Vec::with_capacity(raw_chunks.len());
        let mut spacers = Vec::new();

        // 3. Process by cohorts
        let num_cohorts = (raw_chunks.len() + cohort_size - 1) / cohort_size;

        for cohort_num in 0..num_cohorts {
            let start_idx = cohort_num * cohort_size;
            let end_idx = (start_idx + cohort_size).min(raw_chunks.len());
            let current_cohort_slice = &raw_chunks[start_idx..end_idx];

            let mut cohort_id = [0u8; 16];
            cohort_id[..8].copy_from_slice(&file_id[..8]);
            cohort_id[8..12].copy_from_slice(&(cohort_num as u32).to_be_bytes());
            cohort_id[12..16].copy_from_slice(&(start_idx as u32).to_be_bytes());

            let mut cohort_chunks = Vec::with_capacity(current_cohort_slice.len());
            let mut member_chunk_ids = Vec::with_capacity(current_cohort_slice.len());
            let mut member_lengths = Vec::with_capacity(current_cohort_slice.len());

            for (idx_in_cohort, raw_chunk) in current_cohort_slice.iter().enumerate() {
                let global_idx = (start_idx + idx_in_cohort) as u32;

                let left_arm = if global_idx > 0 {
                    Some(chunk_hashes[global_idx as usize - 1])
                } else {
                    None
                };

                let right_arm = if (global_idx as usize + 1) < raw_chunks.len() {
                    Some(chunk_hashes[global_idx as usize + 1])
                } else {
                    None
                };

                let chunk_id = chunk_hashes[global_idx as usize];
                member_chunk_ids.push(chunk_id);
                member_lengths.push(raw_chunk.len());

                // Partition raw chunk into slices with intra-chunk homology arms
                let slices = Self::slice_chunk(raw_chunk, slice_size, chunk_id, file_id, global_idx, cohort_id);

                cohort_chunks.push(BiologicalChunk {
                    chunk_id,
                    file_id,
                    sequence_idx: global_idx,
                    total_chunks,
                    left_arm,
                    right_arm,
                    data: raw_chunk.to_vec(),
                    slices,
                    slice_size,
                    cohort_id,
                    cohort_idx: idx_in_cohort as u16,
                    cohort_size: current_cohort_slice.len() as u16,
                });
            }

            // Determine maximum number of slices in this cohort (M)
            let max_slices = cohort_chunks.iter().map(|c| c.slices.len()).max().unwrap_or(0);

            // Compute Horizontal Parity Slices P_h[s]
            let horizontal_parity_slices =
                Self::compute_horizontal_parity_slices(&cohort_chunks, max_slices);

            // Compute Diagonal Array Parity Slices P_d[s]
            let diagonal_parity_slices =
                Self::compute_diagonal_parity_slices(&cohort_chunks, max_slices);

            // Populate monolithic parity_data buffer for backward compatibility
            let max_len = current_cohort_slice.iter().map(|c| c.len()).max().unwrap_or(0);
            let mut parity_data = vec![0u8; max_len];
            for raw_chunk in current_cohort_slice.iter() {
                for (b_idx, &byte) in raw_chunk.iter().enumerate() {
                    parity_data[b_idx] ^= byte;
                }
            }

            spacers.push(CohortSpacer {
                cohort_id,
                file_id,
                cohort_size: current_cohort_slice.len() as u16,
                parity_data,
                horizontal_parity_slices,
                diagonal_parity_slices,
                member_chunk_ids,
                member_lengths,
            });

            chunks.extend(cohort_chunks);
        }

        Ok(Strand {
            file_id,
            total_chunks,
            original_file_size: data.len(),
            chunk_size,
            chunks,
            spacers,
        })
    }

    /// Assemble a collection of biological chunks back into the original file byte stream.
    ///
    /// Validates both chunk-level continuity and intra-chunk sub-packet slice continuity.
    pub fn assemble_file(mut chunks: Vec<BiologicalChunk>) -> Result<Vec<u8>, HadrError> {
        if chunks.is_empty() {
            return Err(HadrError::EmptyData);
        }

        // Sort chunks by sequence index
        chunks.sort_by_key(|c| c.sequence_idx);

        let total = chunks[0].total_chunks;
        if chunks.len() != total as usize {
            for i in 0..total {
                if !chunks.iter().any(|c| c.sequence_idx == i) {
                    return Err(HadrError::MissingSequenceIndex(i));
                }
            }
        }

        let mut output = Vec::new();

        for i in 0..chunks.len() {
            let current = &chunks[i];

            // 1. Verify chunk sequence index order
            if current.sequence_idx != i as u32 {
                return Err(HadrError::MissingSequenceIndex(i as u32));
            }

            // 2. Verify inter-chunk left homology arm
            if i > 0 {
                let prev = &chunks[i - 1];
                if current.left_arm != Some(prev.chunk_id) || prev.right_arm != Some(current.chunk_id) {
                    return Err(HadrError::ContinuityViolation(prev.sequence_idx, current.sequence_idx));
                }
            } else if current.left_arm.is_some() {
                return Err(HadrError::ContinuityViolation(0, 0));
            }

            // 3. Verify inter-chunk right homology arm for tail chunk
            if i == chunks.len() - 1 && current.right_arm.is_some() {
                return Err(HadrError::ContinuityViolation(i as u32, i as u32));
            }

            // 4. Verify intra-chunk slice continuity (HADR-v2)
            if !current.slices.is_empty() {
                let num_slices = current.slices.len();
                let mut slice_acc = Vec::with_capacity(current.data.len());

                for s in 0..num_slices {
                    let slice = &current.slices[s];

                    // Verify slice index continuity
                    if slice.slice_idx != s as u16 {
                        return Err(HadrError::MissingSliceIndex {
                            chunk_seq: current.sequence_idx,
                            slice_idx: s as u16,
                        });
                    }

                    // Verify intra-chunk left arm
                    if s > 0 {
                        let prev_slice = &current.slices[s - 1];
                        if slice.left_arm != Some(prev_slice.slice_hash)
                            || prev_slice.right_arm != Some(slice.slice_hash)
                        {
                            return Err(HadrError::SliceContinuityViolation {
                                chunk_seq: current.sequence_idx,
                                slice_a: prev_slice.slice_idx,
                                slice_b: slice.slice_idx,
                            });
                        }
                    } else if slice.left_arm.is_some() {
                        return Err(HadrError::SliceContinuityViolation {
                            chunk_seq: current.sequence_idx,
                            slice_a: 0,
                            slice_b: 0,
                        });
                    }

                    // Verify intra-chunk right arm for tail slice
                    if s == num_slices - 1 && slice.right_arm.is_some() {
                        return Err(HadrError::SliceContinuityViolation {
                            chunk_seq: current.sequence_idx,
                            slice_a: s as u16,
                            slice_b: s as u16,
                        });
                    }

                    // Verify cryptographic slice content hash
                    let computed_slice_hash = hash_sha256(&slice.data);
                    if computed_slice_hash != slice.slice_hash {
                        return Err(HadrError::SliceHomologyMismatch {
                            slice_idx: slice.slice_idx,
                        });
                    }

                    slice_acc.extend_from_slice(&slice.data);
                }

                // Verify that concatenated slices match chunk payload exactly
                if slice_acc != current.data {
                    return Err(HadrError::HomologyMismatch);
                }
            }

            // 5. Verify whole chunk content hash integrity
            let computed_hash = hash_sha256(&current.data);
            if computed_hash != current.chunk_id {
                return Err(HadrError::HomologyMismatch);
            }

            output.extend_from_slice(&current.data);
        }

        Ok(output)
    }
}

// ---------------------------------------------------------------------------
// Decentralized Strand & Slice Break Detector
// ---------------------------------------------------------------------------

pub struct StrandDetector;

impl StrandDetector {
    /// Autonomous decentralized break detector.
    ///
    /// Scans peer chunk announcements across the swarm and detects un-annealed homology arms.
    pub fn detect_breaks(announcements: &[ChunkAnnouncement]) -> Vec<StrandBreak> {
        let mut known_chunks: HashMap<ChunkId, &ChunkAnnouncement> = HashMap::new();
        let mut file_chunks: HashMap<FileId, Vec<&ChunkAnnouncement>> = HashMap::new();

        for ann in announcements {
            known_chunks.insert(ann.chunk_id, ann);
            file_chunks.entry(ann.file_id).or_default().push(ann);
        }

        let mut breaks = Vec::new();
        let mut reported_missing: HashSet<ChunkId> = HashSet::new();

        for (&_file_id, list) in &file_chunks {
            for ann in list {
                // 1. Check right arm
                if let Some(right_target) = ann.right_arm {
                    if !known_chunks.contains_key(&right_target) && !reported_missing.contains(&right_target) {
                        reported_missing.insert(right_target);

                        let right_neighbor = list.iter().find(|other| other.left_arm == Some(right_target));

                        breaks.push(StrandBreak {
                            file_id: ann.file_id,
                            missing_chunk_id: right_target,
                            missing_sequence_idx: ann.sequence_idx + 1,
                            left_neighbor: Some((ann.node_id.clone(), ann.chunk_id)),
                            right_neighbor: right_neighbor.map(|o| (o.node_id.clone(), o.chunk_id)),
                            cohort_id: ann.cohort_id,
                        });
                    }
                }

                // 2. Check left arm
                if let Some(left_target) = ann.left_arm {
                    if !known_chunks.contains_key(&left_target) && !reported_missing.contains(&left_target) {
                        reported_missing.insert(left_target);

                        let left_neighbor = list.iter().find(|other| other.right_arm == Some(left_target));

                        breaks.push(StrandBreak {
                            file_id: ann.file_id,
                            missing_chunk_id: left_target,
                            missing_sequence_idx: ann.sequence_idx.saturating_sub(1),
                            left_neighbor: left_neighbor.map(|o| (o.node_id.clone(), o.chunk_id)),
                            right_neighbor: Some((ann.node_id.clone(), ann.chunk_id)),
                            cohort_id: ann.cohort_id,
                        });
                    }
                }
            }
        }

        breaks
    }

    /// Autonomous slice-level break detector.
    ///
    /// Scans a biological chunk's internal slices for:
    /// 1. Missing slices in the expected sequence 0..expected_slices.
    /// 2. Corrupted slice payloads where SHA-256(data) != slice_hash.
    /// 3. Broken intra-chunk homology arm continuity between adjacent slices.
    pub fn detect_slice_breaks(chunk: &BiologicalChunk, expected_slices: usize) -> Vec<SliceBreak> {
        let mut breaks = Vec::new();
        let slices = &chunk.slices;

        let mut slice_map: HashMap<u16, &BiologicalSlice> = HashMap::new();
        for s in slices {
            slice_map.insert(s.slice_idx, s);
        }

        for s_idx in 0..expected_slices as u16 {
            match slice_map.get(&s_idx) {
                None => {
                    // Missing slice: derive expected hash from adjacent homology arms
                    let expected_hash = if s_idx > 0 {
                        slice_map.get(&(s_idx - 1)).and_then(|s| s.right_arm)
                    } else {
                        None
                    }
                    .or_else(|| {
                        slice_map.get(&(s_idx + 1)).and_then(|s| s.left_arm)
                    });

                    let left_hash = if s_idx > 0 {
                        slice_map.get(&(s_idx - 1)).map(|s| s.slice_hash)
                    } else {
                        None
                    };
                    let right_hash = slice_map.get(&(s_idx + 1)).map(|s| s.slice_hash);

                    breaks.push(SliceBreak {
                        file_id: chunk.file_id,
                        chunk_id: chunk.chunk_id,
                        chunk_sequence_idx: chunk.sequence_idx,
                        slice_idx: s_idx,
                        missing_slice_idx: s_idx,
                        cohort_id: chunk.cohort_id,
                        cohort_idx: chunk.cohort_idx,
                        expected_hash,
                        left_neighbor_hash: left_hash,
                        right_neighbor_hash: right_hash,
                        reason: SliceBreakReason::Missing,
                    });
                }
                Some(slice) => {
                    // 1. Content hash verification
                    let computed_hash = hash_sha256(&slice.data);
                    if computed_hash != slice.slice_hash {
                        breaks.push(SliceBreak {
                            file_id: chunk.file_id,
                            chunk_id: chunk.chunk_id,
                            chunk_sequence_idx: chunk.sequence_idx,
                            slice_idx: s_idx,
                            missing_slice_idx: s_idx,
                            cohort_id: chunk.cohort_id,
                            cohort_idx: chunk.cohort_idx,
                            expected_hash: Some(slice.slice_hash),
                            left_neighbor_hash: slice.left_arm,
                            right_neighbor_hash: slice.right_arm,
                            reason: SliceBreakReason::CorruptedHash,
                        });
                        continue;
                    }

                    // 2. Intra-chunk left arm continuity
                    if s_idx > 0 {
                        if let Some(prev) = slice_map.get(&(s_idx - 1)) {
                            if slice.left_arm != Some(prev.slice_hash) {
                                breaks.push(SliceBreak {
                                    file_id: chunk.file_id,
                                    chunk_id: chunk.chunk_id,
                                    chunk_sequence_idx: chunk.sequence_idx,
                                    slice_idx: s_idx,
                                    missing_slice_idx: s_idx,
                                    cohort_id: chunk.cohort_id,
                                    cohort_idx: chunk.cohort_idx,
                                    expected_hash: Some(slice.slice_hash),
                                    left_neighbor_hash: Some(prev.slice_hash),
                                    right_neighbor_hash: slice.right_arm,
                                    reason: SliceBreakReason::ContinuityViolation,
                                });
                            }
                        }
                    } else if slice.left_arm.is_some() {
                        breaks.push(SliceBreak {
                            file_id: chunk.file_id,
                            chunk_id: chunk.chunk_id,
                            chunk_sequence_idx: chunk.sequence_idx,
                            slice_idx: 0,
                            missing_slice_idx: 0,
                            cohort_id: chunk.cohort_id,
                            cohort_idx: chunk.cohort_idx,
                            expected_hash: Some(slice.slice_hash),
                            left_neighbor_hash: None,
                            right_neighbor_hash: slice.right_arm,
                            reason: SliceBreakReason::ContinuityViolation,
                        });
                    }

                    // 3. Intra-chunk right arm continuity
                    if (s_idx as usize) + 1 == expected_slices {
                        if slice.right_arm.is_some() {
                            breaks.push(SliceBreak {
                                file_id: chunk.file_id,
                                chunk_id: chunk.chunk_id,
                                chunk_sequence_idx: chunk.sequence_idx,
                                slice_idx: s_idx,
                                missing_slice_idx: s_idx,
                                cohort_id: chunk.cohort_id,
                                cohort_idx: chunk.cohort_idx,
                                expected_hash: Some(slice.slice_hash),
                                left_neighbor_hash: if s_idx > 0 { slice_map.get(&(s_idx - 1)).map(|s| s.slice_hash) } else { None },
                                right_neighbor_hash: None,
                                reason: SliceBreakReason::ContinuityViolation,
                            });
                        }
                    } else if let Some(next) = slice_map.get(&(s_idx + 1)) {
                        if slice.right_arm != Some(next.slice_hash) {
                            breaks.push(SliceBreak {
                                file_id: chunk.file_id,
                                chunk_id: chunk.chunk_id,
                                chunk_sequence_idx: chunk.sequence_idx,
                                slice_idx: s_idx,
                                missing_slice_idx: s_idx,
                                cohort_id: chunk.cohort_id,
                                cohort_idx: chunk.cohort_idx,
                                expected_hash: Some(slice.slice_hash),
                                left_neighbor_hash: if s_idx > 0 { slice_map.get(&(s_idx - 1)).map(|s| s.slice_hash) } else { None },
                                right_neighbor_hash: Some(next.slice_hash),
                                reason: SliceBreakReason::ContinuityViolation,
                            });
                        }
                    }
                }
            }
        }

        breaks
    }
}

// ---------------------------------------------------------------------------
// Cas Repair Engine (Sub-Packet Sliced Recovery)
// ---------------------------------------------------------------------------

pub struct CasRepairEngine;

impl CasRepairEngine {
    /// Resurrect a single 4 KB slice using surviving cohort slices and horizontal slice parity ($P_h[s]$).
    ///
    /// Mathematical Formulation:
    ///   $D_m[s] = P_h[s] \oplus \bigoplus_{j \neq m} D_j[s]$
    ///
    /// Network Transfer:
    ///   Transfers $(k - 1) \times 4\text{ KB}$ surviving data slices + $1 \times 4\text{ KB}$ parity slice.
    pub fn repair_slice(
        guide: &SliceGuideRNA,
        surviving_slices: &[BiologicalSlice],
        parity_slice_data: &[u8],
    ) -> Result<BiologicalSlice, HadrError> {
        let s = guide.slice_idx;

        if parity_slice_data.is_empty() {
            return Err(HadrError::InsufficientSlices { found: 0, needed: 1 });
        }

        // Linear XOR inversion: D_m[s] = P_h[s] ^ (Sum of D_j[s])
        let mut reconstructed_data = parity_slice_data.to_vec();

        for sl in surviving_slices {
            if sl.slice_idx != s
                || sl.cohort_id != guide.cohort_id
                || (sl.chunk_id == guide.target_chunk_id && sl.sequence_idx == guide.sequence_idx)
            {
                continue;
            }

            for (idx, &byte) in sl.data.iter().enumerate() {
                if idx < reconstructed_data.len() {
                    reconstructed_data[idx] ^= byte;
                }
            }
        }

        // Truncate to expected slice length (for variable-length tail slices)
        if guide.slice_len > 0 && guide.slice_len < reconstructed_data.len() {
            reconstructed_data.truncate(guide.slice_len);
        }

        // Homology Proof Verification
        let computed_hash = hash_sha256(&reconstructed_data);
        if let Some(expected_hash) = guide.expected_slice_hash {
            if computed_hash != expected_hash {
                return Err(HadrError::SliceHomologyMismatch { slice_idx: s });
            }
        }

        let offset = (s as usize) * SLICE_SIZE;

        Ok(BiologicalSlice {
            slice_idx: s,
            chunk_id: guide.target_chunk_id,
            file_id: guide.file_id,
            sequence_idx: guide.sequence_idx,
            offset,
            data: reconstructed_data,
            slice_hash: computed_hash,
            left_arm: guide.expected_left_arm,
            right_arm: guide.expected_right_arm,
            cohort_id: guide.cohort_id,
        })
    }

    /// Resurrect a single 4 KB slice using surviving cohort slices and diagonal array parity ($P_d$).
    ///
    /// Mathematical Formulation:
    ///   Given diagonal index $d = ((s - m) \bmod M + M) \bmod M$:
    ///   $D_m[s] = P_d[d] \oplus \bigoplus_{j \neq m} D_j[(d + j) \bmod M]$
    pub fn repair_slice_diagonal(
        guide: &SliceGuideRNA,
        surviving_slices: &[BiologicalSlice],
        diagonal_parity_data: &[u8],
        member_chunk_ids: &[ChunkId],
        slices_per_chunk: usize,
    ) -> Result<BiologicalSlice, HadrError> {
        let s = guide.slice_idx as usize;
        let m = guide.cohort_idx as usize;
        let m_mod = slices_per_chunk;

        if m_mod == 0 {
            return Err(HadrError::InvalidSliceSize);
        }
        if diagonal_parity_data.is_empty() {
            return Err(HadrError::InsufficientSlices { found: 0, needed: 1 });
        }

        // Compute diagonal index d intersecting cell (m, s):
        // s = (d + m) mod M  =>  d = (s - m) mod M
        let d = ((s as isize - m as isize).rem_euclid(m_mod as isize)) as usize;

        let mut reconstructed_data = diagonal_parity_data.to_vec();
        let cohort_start = guide.sequence_idx.saturating_sub(guide.cohort_idx as u32);

        for sl in surviving_slices {
            if sl.cohort_id != guide.cohort_id
                || (sl.chunk_id == guide.target_chunk_id && sl.sequence_idx == guide.sequence_idx)
            {
                continue;
            }

            // Find cohort index j of this survivor
            let j = if sl.sequence_idx >= cohort_start
                && ((sl.sequence_idx - cohort_start) as usize) < member_chunk_ids.len()
            {
                (sl.sequence_idx - cohort_start) as usize
            } else {
                match member_chunk_ids.iter().position(|id| id == &sl.chunk_id) {
                    Some(idx) => idx,
                    None => continue,
                }
            };

            // Expected slice index for member j in diagonal d is (d + j) mod M
            let expected_s_j = (d + j) % m_mod;
            if sl.slice_idx as usize != expected_s_j {
                continue;
            }

            for (idx, &byte) in sl.data.iter().enumerate() {
                if idx < reconstructed_data.len() {
                    reconstructed_data[idx] ^= byte;
                }
            }
        }

        // Truncate to expected slice length
        if guide.slice_len > 0 && guide.slice_len < reconstructed_data.len() {
            reconstructed_data.truncate(guide.slice_len);
        }

        // Homology Proof Verification
        let computed_hash = hash_sha256(&reconstructed_data);
        if let Some(expected_hash) = guide.expected_slice_hash {
            if computed_hash != expected_hash {
                return Err(HadrError::SliceHomologyMismatch { slice_idx: guide.slice_idx });
            }
        }

        let offset = (guide.slice_idx as usize) * SLICE_SIZE;

        Ok(BiologicalSlice {
            slice_idx: guide.slice_idx,
            chunk_id: guide.target_chunk_id,
            file_id: guide.file_id,
            sequence_idx: guide.sequence_idx,
            offset,
            data: reconstructed_data,
            slice_hash: computed_hash,
            left_arm: guide.expected_left_arm,
            right_arm: guide.expected_right_arm,
            cohort_id: guide.cohort_id,
        })
    }

    /// Resurrect an entire chunk byte-for-byte by reconstructing its sub-packet slices.
    ///
    /// Employs slice-level reconstruction when horizontal parity slices are present in the spacer,
    /// maintaining full backward compatibility with legacy monolithic spacers.
    pub fn repair_chunk(
        guide: &GuideRNA,
        surviving_chunks: &[BiologicalChunk],
        spacer: &CohortSpacer,
    ) -> Result<BiologicalChunk, HadrError> {
        // 1. Validate cohort matching
        if spacer.cohort_id != guide.cohort_id {
            return Err(HadrError::CohortMismatch {
                expected: guide.cohort_id,
                found: spacer.cohort_id,
            });
        }

        // 2. Locate target index within cohort
        let target_pos_in_cohort = spacer
            .member_chunk_ids
            .iter()
            .position(|&id| id == guide.target_chunk_id)
            .ok_or(HadrError::TargetNotInCohort)?;

        let target_expected_length = spacer.member_lengths[target_pos_in_cohort];

        // 3. Ensure sufficient survivors
        let needed_survivors = spacer.cohort_size as usize - 1;
        if surviving_chunks.len() < needed_survivors {
            return Err(HadrError::InsufficientChunks {
                found: surviving_chunks.len(),
                needed: needed_survivors,
            });
        }

        let total_chunks = surviving_chunks
            .first()
            .map(|c| c.total_chunks)
            .unwrap_or(guide.sequence_idx + 1);

        // 4. HADR-v2 Sub-Packet Sliced Reconstruction Path
        if !spacer.horizontal_parity_slices.is_empty() {
            let slice_size = surviving_chunks
                .first()
                .map(|c| c.slice_size)
                .filter(|&s| s > 0)
                .unwrap_or(SLICE_SIZE);

            let num_slices = if target_expected_length == 0 {
                0
            } else {
                (target_expected_length + slice_size - 1) / slice_size
            };

            let mut reconstructed_slices = Vec::with_capacity(num_slices);

            for s in 0..num_slices {
                let slice_len = if (s + 1) * slice_size <= target_expected_length {
                    slice_size
                } else {
                    target_expected_length - s * slice_size
                };

                // Gather row s slices from surviving chunks
                let mut row_survivors = Vec::with_capacity(surviving_chunks.len());
                for chunk in surviving_chunks {
                    if chunk.cohort_id == guide.cohort_id && chunk.chunk_id != guide.target_chunk_id {
                        if let Some(sl) = chunk.slices.get(s) {
                            row_survivors.push(sl.clone());
                        }
                    }
                }

                let p_h_data = &spacer.horizontal_parity_slices[s];

                let slice_guide = SliceGuideRNA {
                    file_id: guide.file_id,
                    target_chunk_id: guide.target_chunk_id,
                    chunk_id: guide.target_chunk_id,
                    sequence_idx: guide.sequence_idx,
                    slice_idx: s as u16,
                    cohort_id: guide.cohort_id,
                    cohort_idx: target_pos_in_cohort as u16,
                    expected_slice_hash: None, // Verified via full chunk hash
                    expected_left_arm: None,
                    expected_right_arm: None,
                    slice_len,
                };

                let slice = Self::repair_slice(&slice_guide, &row_survivors, p_h_data)?;
                reconstructed_slices.push(slice);
            }

            // Concatenate slice payloads into complete chunk buffer
            let mut reconstructed_data = Vec::with_capacity(target_expected_length);
            for sl in &reconstructed_slices {
                reconstructed_data.extend_from_slice(&sl.data);
            }
            reconstructed_data.truncate(target_expected_length);

            // Verify content hash of assembled chunk
            let computed_chunk_hash = hash_sha256(&reconstructed_data);
            if computed_chunk_hash != guide.target_chunk_id {
                return Err(HadrError::HomologyMismatch);
            }

            // Anneal intra-chunk homology arms
            let total_s = reconstructed_slices.len();
            for i in 0..total_s {
                let left = if i > 0 { Some(reconstructed_slices[i - 1].slice_hash) } else { None };
                let right = if i + 1 < total_s { Some(reconstructed_slices[i + 1].slice_hash) } else { None };
                reconstructed_slices[i].left_arm = left;
                reconstructed_slices[i].right_arm = right;
            }

            Ok(BiologicalChunk {
                chunk_id: computed_chunk_hash,
                file_id: guide.file_id,
                sequence_idx: guide.sequence_idx,
                total_chunks,
                left_arm: guide.expected_left_arm,
                right_arm: guide.expected_right_arm,
                data: reconstructed_data,
                slices: reconstructed_slices,
                slice_size,
                cohort_id: guide.cohort_id,
                cohort_idx: target_pos_in_cohort as u16,
                cohort_size: spacer.cohort_size,
            })
        } else {
            // Legacy HADR-v1 Monolithic XOR Fallback
            let mut reconstructed_data = spacer.parity_data.clone();

            for survivor in surviving_chunks {
                if survivor.cohort_id != guide.cohort_id || survivor.chunk_id == guide.target_chunk_id {
                    continue;
                }
                for (idx, &byte) in survivor.data.iter().enumerate() {
                    if idx < reconstructed_data.len() {
                        reconstructed_data[idx] ^= byte;
                    }
                }
            }

            reconstructed_data.truncate(target_expected_length);
            let reconstructed_hash = hash_sha256(&reconstructed_data);
            if reconstructed_hash != guide.target_chunk_id {
                return Err(HadrError::HomologyMismatch);
            }

            let slice_size = surviving_chunks
                .first()
                .map(|c| c.slice_size)
                .filter(|&s| s > 0)
                .unwrap_or(SLICE_SIZE);

            let slices = StrandEncoder::slice_chunk(
                &reconstructed_data,
                slice_size,
                reconstructed_hash,
                guide.file_id,
                guide.sequence_idx,
                guide.cohort_id,
            );

            Ok(BiologicalChunk {
                chunk_id: reconstructed_hash,
                file_id: guide.file_id,
                sequence_idx: guide.sequence_idx,
                total_chunks,
                left_arm: guide.expected_left_arm,
                right_arm: guide.expected_right_arm,
                data: reconstructed_data,
                slices,
                slice_size,
                cohort_id: guide.cohort_id,
                cohort_idx: target_pos_in_cohort as u16,
                cohort_size: spacer.cohort_size,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Partial Write Update Optimization: SliceDeltaJournal
// ---------------------------------------------------------------------------

/// Engine for partial write delta generation and incremental parity maintenance.
pub struct SliceDeltaJournal;

impl SliceDeltaJournal {
    /// Generate a SliceDelta from an old slice and newly written data bytes.
    ///
    /// Mathematical formula:
    /// Delta[s][o + b] = D_old[s][o + b] XOR D_new[s][o + b]
    pub fn create_delta(
        old_slice: &BiologicalSlice,
        new_data: &[u8],
        offset_in_slice: usize,
        cohort_id: CohortId,
        cohort_idx: u16,
    ) -> Result<SliceDelta, HadrError> {
        if offset_in_slice > old_slice.data.len() {
            return Err(HadrError::DeltaOffsetOutOfBounds {
                offset: offset_in_slice,
                slice_len: old_slice.data.len(),
            });
        }

        // 1. Calculate bitwise XOR delta
        let mut delta_bytes = Vec::with_capacity(new_data.len());
        for (i, &new_byte) in new_data.iter().enumerate() {
            let old_byte = old_slice.data.get(offset_in_slice + i).copied().unwrap_or(0);
            delta_bytes.push(old_byte ^ new_byte);
        }

        // 2. Simulate updated slice payload and compute expected SHA-256 hash
        let mut simulated = old_slice.data.clone();
        let target_len = offset_in_slice + new_data.len();
        if simulated.len() < target_len {
            simulated.resize(target_len, 0);
        }
        simulated[offset_in_slice..target_len].copy_from_slice(new_data);
        let new_slice_hash = hash_sha256(&simulated);

        Ok(SliceDelta {
            file_id: old_slice.file_id,
            cohort_id,
            sequence_idx: old_slice.sequence_idx,
            slice_idx: old_slice.slice_idx,
            cohort_idx,
            offset_in_slice: offset_in_slice as u16,
            delta_bytes,
            new_slice_hash,
        })
    }

    /// Update slice data in place and verify the new slice hash.
    ///
    /// Formula: D_new[s] = D_old[s] XOR Delta[s]
    pub fn apply_delta_to_slice(
        slice: &mut BiologicalSlice,
        delta: &SliceDelta,
    ) -> Result<(), HadrError> {
        if slice.slice_idx != delta.slice_idx {
            return Err(HadrError::MissingSliceIndex {
                chunk_seq: delta.sequence_idx,
                slice_idx: delta.slice_idx,
            });
        }

        let end = delta.offset_in_slice as usize + delta.delta_bytes.len();
        if slice.data.len() < end {
            slice.data.resize(end, 0);
        }

        let start = delta.offset_in_slice as usize;
        for (i, &d) in delta.delta_bytes.iter().enumerate() {
            slice.data[start + i] ^= d;
        }

        // Recompute slice SHA-256 digest
        slice.slice_hash = hash_sha256(&slice.data);

        // Verify cryptographic integrity
        if slice.slice_hash != delta.new_slice_hash {
            return Err(HadrError::SliceHomologyMismatch {
                slice_idx: slice.slice_idx,
            });
        }

        Ok(())
    }

    /// Incrementally update horizontal parity slice without whole-chunk read-modify-write.
    ///
    /// Mathematical formula:
    /// P_h_new[s] = P_h_old[s] XOR Delta_i[s]
    pub fn apply_delta_to_horizontal_parity(
        parity_slice: &mut BiologicalSlice,
        delta: &SliceDelta,
    ) -> Result<(), HadrError> {
        if parity_slice.slice_idx != delta.slice_idx {
            return Err(HadrError::MissingSliceIndex {
                chunk_seq: delta.sequence_idx,
                slice_idx: delta.slice_idx,
            });
        }

        let end = delta.offset_in_slice as usize + delta.delta_bytes.len();
        if parity_slice.data.len() < end {
            parity_slice.data.resize(end, 0);
        }

        let start = delta.offset_in_slice as usize;
        for (i, &d) in delta.delta_bytes.iter().enumerate() {
            parity_slice.data[start + i] ^= d;
        }

        parity_slice.slice_hash = hash_sha256(&parity_slice.data);
        Ok(())
    }

    /// Incrementally update raw horizontal parity byte buffer in CohortSpacer.
    pub fn apply_delta_to_horizontal_buffer(
        parity_buffer: &mut Vec<u8>,
        delta: &SliceDelta,
    ) -> Result<(), HadrError> {
        let end = delta.offset_in_slice as usize + delta.delta_bytes.len();
        if parity_buffer.len() < end {
            parity_buffer.resize(end, 0);
        }
        let start = delta.offset_in_slice as usize;
        for (i, &d) in delta.delta_bytes.iter().enumerate() {
            parity_buffer[start + i] ^= d;
        }
        Ok(())
    }

    /// Incrementally update diagonal array parity slice without whole-chunk read-modify-write.
    ///
    /// Mathematical formula:
    /// d = (s - i) mod M
    /// P_d_new[(s - i) mod M] = P_d_old[(s - i) mod M] XOR Delta_i[s]
    pub fn apply_delta_to_diagonal_parity(
        diagonal_slices: &mut [BiologicalSlice],
        delta: &SliceDelta,
    ) -> Result<(), HadrError> {
        let m = diagonal_slices.len();
        if m == 0 {
            return Err(HadrError::InvalidSliceSize);
        }

        // Calculate target diagonal slice index d: s = (d + i) mod M => d = (s - i) mod M
        let d = ((delta.slice_idx as isize - delta.cohort_idx as isize).rem_euclid(m as isize)) as usize;
        let target = &mut diagonal_slices[d];

        let end = delta.offset_in_slice as usize + delta.delta_bytes.len();
        if target.data.len() < end {
            target.data.resize(end, 0);
        }

        let start = delta.offset_in_slice as usize;
        for (i, &d_val) in delta.delta_bytes.iter().enumerate() {
            target.data[start + i] ^= d_val;
        }

        target.slice_hash = hash_sha256(&target.data);
        Ok(())
    }

    /// Incrementally update raw diagonal parity byte buffers in CohortSpacer.
    pub fn apply_delta_to_diagonal_buffers(
        diagonal_buffers: &mut [Vec<u8>],
        delta: &SliceDelta,
    ) -> Result<(), HadrError> {
        let m = diagonal_buffers.len();
        if m == 0 {
            return Err(HadrError::InvalidSliceSize);
        }

        let d = ((delta.slice_idx as isize - delta.cohort_idx as isize).rem_euclid(m as isize)) as usize;
        let target = &mut diagonal_buffers[d];

        let end = delta.offset_in_slice as usize + delta.delta_bytes.len();
        if target.len() < end {
            target.resize(end, 0);
        }

        let start = delta.offset_in_slice as usize;
        for (i, &d_val) in delta.delta_bytes.iter().enumerate() {
            target[start + i] ^= d_val;
        }

        Ok(())
    }

    /// Decompose an arbitrary chunk write (potentially spanning multiple slices)
    /// into a vector of discrete SliceDelta records.
    pub fn create_deltas_for_range(
        slices: &[BiologicalSlice],
        write_offset_in_chunk: usize,
        new_data: &[u8],
        cohort_id: CohortId,
        cohort_idx: u16,
        slice_size: usize,
    ) -> Result<Vec<SliceDelta>, HadrError> {
        if slice_size == 0 {
            return Err(HadrError::InvalidSliceSize);
        }

        let mut deltas = Vec::new();
        let mut bytes_processed = 0;

        while bytes_processed < new_data.len() {
            let current_chunk_offset = write_offset_in_chunk + bytes_processed;
            let slice_idx = (current_chunk_offset / slice_size) as u16;
            let offset_in_slice = current_chunk_offset % slice_size;
            let slice_remaining = slice_size - offset_in_slice;
            let write_remaining = new_data.len() - bytes_processed;
            let chunk_take = slice_remaining.min(write_remaining);

            let old_slice = slices
                .iter()
                .find(|s| s.slice_idx == slice_idx)
                .ok_or(HadrError::MissingSliceIndex {
                    chunk_seq: 0,
                    slice_idx,
                })?;

            let sub_data = &new_data[bytes_processed..bytes_processed + chunk_take];
            let delta = Self::create_delta(
                old_slice,
                sub_data,
                offset_in_slice,
                cohort_id,
                cohort_idx,
            )?;

            deltas.push(delta);
            bytes_processed += chunk_take;
        }

        Ok(deltas)
    }
}

// ============================================================================
// Unit Tests & Mathematical Verification
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use nifsi_core::protocol::{
        BiologicalSliceWire, ChunkAnnouncement as WireChunkAnnouncement, FsMutation, IpcRequest,
        IpcResponse, SliceAnnouncement, SliceParityKind, SliceTransferRequest,
        SliceTransferResponse,
    };

    fn dummy_file_id() -> FileId {
        [0xAA, 0xBB, 0xCC, 0xDD, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xEE, 0xFF]
    }

    fn make_test_slice(slice_idx: u16, data: Vec<u8>) -> BiologicalSlice {
        let h = hash_sha256(&data);
        BiologicalSlice {
            slice_idx,
            chunk_id: [0x55; 32],
            file_id: dummy_file_id(),
            sequence_idx: 0,
            offset: slice_idx as usize * 4096,
            data,
            slice_hash: h,
            left_arm: None,
            right_arm: None,
            cohort_id: [0x88; 16],
        }
    }

    // -----------------------------------------------------------------------
    // Test 1: File Encoding into Biological Strand with Homology Arms
    // -----------------------------------------------------------------------
    #[test]
    fn test_strand_encoding_and_homology_arms() {
        let file_id = dummy_file_id();
        let payload = b"The quick brown fox jumps over the lazy dog. Biomimetic distributed storage in Rust!";
        let chunk_size = 16;
        let cohort_size = 3;

        let strand = StrandEncoder::encode_file(file_id, payload, chunk_size, cohort_size).unwrap();

        assert_eq!(strand.file_id, file_id);
        assert!(!strand.chunks.is_empty());
        assert!(!strand.spacers.is_empty());

        // Verify head chunk has no left arm
        assert!(strand.chunks[0].left_arm.is_none());
        assert!(strand.chunks[0].right_arm.is_some());

        // Verify tail chunk has no right arm
        let last_idx = strand.chunks.len() - 1;
        assert!(strand.chunks[last_idx].right_arm.is_none());
        assert!(strand.chunks[last_idx].left_arm.is_some());

        // Verify internal homology arm linkages
        for i in 1..last_idx {
            assert_eq!(strand.chunks[i].left_arm, Some(strand.chunks[i - 1].chunk_id));
            assert_eq!(strand.chunks[i].right_arm, Some(strand.chunks[i + 1].chunk_id));
        }

        // Verify file assembly works when all chunks are present
        let assembled = StrandEncoder::assemble_file(strand.chunks.clone()).unwrap();
        assert_eq!(assembled, payload);
    }

    // -----------------------------------------------------------------------
    // Test 2: Decentralized Strand Break Detection (No Central Master)
    // -----------------------------------------------------------------------
    #[test]
    fn test_decentralized_break_detection() {
        let file_id = dummy_file_id();
        let payload = b"Alpha Beta Gamma Delta Epsilon Zeta Eta Theta Iota Kappa Lambda Mu";
        let strand = StrandEncoder::encode_file(file_id, payload, 8, 4).unwrap();

        let mut announcements: Vec<ChunkAnnouncement> = strand
            .chunks
            .iter()
            .map(|c| ChunkAnnouncement {
                node_id: format!("node_{}", c.sequence_idx),
                chunk_id: c.chunk_id,
                file_id: c.file_id,
                sequence_idx: c.sequence_idx,
                left_arm: c.left_arm,
                right_arm: c.right_arm,
                cohort_id: c.cohort_id,
            })
            .collect();

        let breaks = StrandDetector::detect_breaks(&announcements);
        assert!(breaks.is_empty(), "Healthy swarm must have zero breaks");

        let lost_chunk = strand.chunks[2].clone();
        announcements.retain(|a| a.sequence_idx != 2);

        let breaks = StrandDetector::detect_breaks(&announcements);
        assert_eq!(breaks.len(), 1, "Must detect exactly 1 strand break");

        let detected = &breaks[0];
        assert_eq!(detected.missing_chunk_id, lost_chunk.chunk_id);
        assert_eq!(detected.missing_sequence_idx, 2);
        assert_eq!(detected.left_neighbor.as_ref().unwrap().1, strand.chunks[1].chunk_id);
        assert_eq!(detected.right_neighbor.as_ref().unwrap().1, strand.chunks[3].chunk_id);
    }

    // -----------------------------------------------------------------------
    // Test 3: Homology-Directed Repair (Resurrecting a Middle Chunk)
    // -----------------------------------------------------------------------
    #[test]
    fn test_homology_directed_repair_middle_chunk() {
        let file_id = dummy_file_id();
        let payload = b"Data persistence without central masters: CRISPR homology-directed repair in Nifsi!";
        let strand = StrandEncoder::encode_file(file_id, payload, 16, 4).unwrap();

        let lost_idx = 2;
        let lost_chunk = strand.chunks[lost_idx].clone();

        let announcements: Vec<ChunkAnnouncement> = strand
            .chunks
            .iter()
            .filter(|c| c.sequence_idx != lost_idx as u32)
            .map(|c| ChunkAnnouncement {
                node_id: format!("node_{}", c.sequence_idx),
                chunk_id: c.chunk_id,
                file_id: c.file_id,
                sequence_idx: c.sequence_idx,
                left_arm: c.left_arm,
                right_arm: c.right_arm,
                cohort_id: c.cohort_id,
            })
            .collect();

        let breaks = StrandDetector::detect_breaks(&announcements);
        assert_eq!(breaks.len(), 1);

        let guide = GuideRNA::from_break(&breaks[0]);
        assert_eq!(guide.target_chunk_id, lost_chunk.chunk_id);

        let spacer = strand.spacers.iter().find(|s| s.cohort_id == guide.cohort_id).unwrap();
        let surviving: Vec<BiologicalChunk> = strand
            .chunks
            .iter()
            .filter(|c| c.cohort_id == guide.cohort_id && c.sequence_idx != lost_idx as u32)
            .cloned()
            .collect();

        let resurrected = CasRepairEngine::repair_chunk(&guide, &surviving, spacer).unwrap();

        assert_eq!(resurrected.chunk_id, lost_chunk.chunk_id);
        assert_eq!(resurrected.data, lost_chunk.data);
        assert_eq!(resurrected.left_arm, lost_chunk.left_arm);
        assert_eq!(resurrected.right_arm, lost_chunk.right_arm);
        assert_eq!(resurrected.sequence_idx, lost_chunk.sequence_idx);

        let mut full_strand = strand.chunks.clone();
        full_strand[lost_idx] = resurrected;

        let assembled = StrandEncoder::assemble_file(full_strand).unwrap();
        assert_eq!(assembled, payload);
    }

    // -----------------------------------------------------------------------
    // Test 4: Repair First Chunk (Head) and Last Chunk (Variable Length Tail)
    // -----------------------------------------------------------------------
    #[test]
    fn test_repair_head_and_tail_chunks() {
        let file_id = dummy_file_id();
        let payload = b"Short string for boundary repair";
        let strand = StrandEncoder::encode_file(file_id, payload, 8, 4).unwrap();

        // A. Repair Head Chunk (Index 0)
        {
            let lost = strand.chunks[0].clone();
            let guide = GuideRNA {
                file_id,
                target_chunk_id: lost.chunk_id,
                sequence_idx: 0,
                cohort_id: lost.cohort_id,
                expected_left_arm: None,
                expected_right_arm: Some(strand.chunks[1].chunk_id),
            };

            let spacer = strand.spacers.iter().find(|s| s.cohort_id == lost.cohort_id).unwrap();
            let surviving: Vec<BiologicalChunk> = strand
                .chunks
                .iter()
                .filter(|c| c.cohort_id == lost.cohort_id && c.sequence_idx != 0)
                .cloned()
                .collect();

            let resurrected = CasRepairEngine::repair_chunk(&guide, &surviving, spacer).unwrap();
            assert_eq!(resurrected.data, lost.data);
            assert_eq!(resurrected.left_arm, None);
        }

        // B. Repair Tail Chunk (Last index, uneven bytes)
        {
            let last_idx = strand.chunks.len() - 1;
            let lost = strand.chunks[last_idx].clone();
            let prev_idx = last_idx - 1;
            let guide = GuideRNA {
                file_id,
                target_chunk_id: lost.chunk_id,
                sequence_idx: last_idx as u32,
                cohort_id: lost.cohort_id,
                expected_left_arm: Some(strand.chunks[prev_idx].chunk_id),
                expected_right_arm: None,
            };

            let spacer = strand.spacers.iter().find(|s| s.cohort_id == lost.cohort_id).unwrap();
            let surviving: Vec<BiologicalChunk> = strand
                .chunks
                .iter()
                .filter(|c| c.cohort_id == lost.cohort_id && c.sequence_idx != last_idx as u32)
                .cloned()
                .collect();

            let resurrected = CasRepairEngine::repair_chunk(&guide, &surviving, spacer).unwrap();
            assert_eq!(resurrected.data, lost.data);
            assert_eq!(resurrected.right_arm, None);
        }
    }

    // -----------------------------------------------------------------------
    // Test 5: Rejection of Corrupted / Mismatched Homology Data
    // -----------------------------------------------------------------------
    #[test]
    fn test_reject_corrupted_homology_repair() {
        let file_id = dummy_file_id();
        let payload = b"Tamper resistance test string for biological rejection";
        let strand = StrandEncoder::encode_file(file_id, payload, 8, 4).unwrap();

        let lost = strand.chunks[1].clone();
        let guide = GuideRNA {
            file_id,
            target_chunk_id: lost.chunk_id,
            sequence_idx: 1,
            cohort_id: lost.cohort_id,
            expected_left_arm: lost.left_arm,
            expected_right_arm: lost.right_arm,
        };

        let mut corrupted_spacer = strand.spacers[0].clone();
        corrupted_spacer.horizontal_parity_slices[0][0] ^= 0xFF; // flip bits
        corrupted_spacer.parity_data[0] ^= 0xFF;

        let surviving: Vec<BiologicalChunk> = strand
            .chunks
            .iter()
            .filter(|c| c.cohort_id == lost.cohort_id && c.sequence_idx != 1)
            .cloned()
            .collect();

        let result = CasRepairEngine::repair_chunk(&guide, &surviving, &corrupted_spacer);
        assert_eq!(result.unwrap_err(), HadrError::HomologyMismatch);
    }

    // -----------------------------------------------------------------------
    // Test 6: Sub-Packet Slice Decomposition & Horizontal Parity Row Independence
    // -----------------------------------------------------------------------
    #[test]
    fn test_slice_horizontal_parity_independence() {
        let file_id = dummy_file_id();
        let slice_size = 4096;
        let chunk_size = 16 * 1024; // 16 KB = 4 slices per chunk
        let cohort_size = 4;
        let payload = vec![0x42; 64 * 1024]; // 64 KB = 4 chunks = 1 cohort

        let strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &payload,
            chunk_size,
            cohort_size,
            slice_size,
        ).unwrap();

        assert_eq!(strand.chunks.len(), 4);
        assert_eq!(strand.spacers.len(), 1);

        let spacer = &strand.spacers[0];
        assert_eq!(spacer.horizontal_parity_slices.len(), 4);
        assert_eq!(spacer.diagonal_parity_slices.len(), 4);

        // Modify row 1 of chunk 0: must change P_h[1] only, leaving P_h[0], P_h[2], P_h[3] identical
        let mut modified_chunks = strand.chunks.clone();
        modified_chunks[0].slices[1].data[0] ^= 0xAA;
        modified_chunks[0].slices[1].slice_hash = hash_sha256(&modified_chunks[0].slices[1].data);

        let p_h_mod = compute_horizontal_parity(&modified_chunks, 4);

        assert_eq!(p_h_mod[0], spacer.horizontal_parity_slices[0]);
        assert_ne!(p_h_mod[1], spacer.horizontal_parity_slices[1]);
        assert_eq!(p_h_mod[2], spacer.horizontal_parity_slices[2]);
        assert_eq!(p_h_mod[3], spacer.horizontal_parity_slices[3]);
    }

    // -----------------------------------------------------------------------
    // Test 7: Diagonal Array Parity Cyclic Slope
    // -----------------------------------------------------------------------
    #[test]
    fn test_slice_diagonal_parity_cyclic_slope() {
        let file_id = dummy_file_id();
        let slice_size = 4096;
        let chunk_size = 16 * 1024; // 4 slices per chunk (M = 4)
        let cohort_size = 3;
        let payload = vec![0x00; 48 * 1024];

        let mut strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &payload,
            chunk_size,
            cohort_size,
            slice_size,
        ).unwrap();

        // Inject non-zero value at chunk 2, slice 1
        strand.chunks[2].slices[1].data = vec![0x55; slice_size];
        strand.chunks[2].slices[1].slice_hash = hash_sha256(&strand.chunks[2].slices[1].data);

        let p_d = compute_diagonal_parity(&strand.chunks, 4);

        // In diagonal parity: r = (s + i) mod M
        // For chunk i=2, slice r=1: 1 = (s + 2) mod 4 => s = (1 - 2 + 4) mod 4 = 3
        assert_eq!(p_d[3], vec![0x55; slice_size]);
        assert_eq!(p_d[0], vec![0x00; slice_size]);
        assert_eq!(p_d[1], vec![0x00; slice_size]);
        assert_eq!(p_d[2], vec![0x00; slice_size]);
    }

    // -----------------------------------------------------------------------
    // Test 8: Single-Slice Degraded Read Recovery via Horizontal Parity
    // -----------------------------------------------------------------------
    #[test]
    fn test_single_slice_degraded_read_horizontal() {
        let file_id = dummy_file_id();
        let slice_size = 4096;
        let chunk_size = 16 * 1024;
        let cohort_size = 4;
        let mut data = Vec::with_capacity(64 * 1024);
        for i in 0..(64 * 1024) {
            data.push((i % 251) as u8);
        }

        let strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &data,
            chunk_size,
            cohort_size,
            slice_size,
        ).unwrap();

        // Suppose chunk 1, slice 2 is degraded/missing
        let target_chunk = &strand.chunks[1];
        let target_slice = &target_chunk.slices[2];

        let guide = SliceGuideRNA::for_slice(
            target_chunk,
            2,
            Some(target_slice.slice_hash),
            slice_size,
        );

        // Collect surviving slices across cohort at row 2
        let mut survivors = Vec::new();
        for chunk in &strand.chunks {
            if chunk.chunk_id != target_chunk.chunk_id {
                survivors.push(chunk.slices[2].clone());
            }
        }

        let p_h = &strand.spacers[0].horizontal_parity_slices[2];
        let reconstructed = CasRepairEngine::repair_slice(&guide, &survivors, p_h).unwrap();

        assert_eq!(reconstructed.data, target_slice.data);
        assert_eq!(reconstructed.slice_hash, target_slice.slice_hash);
        assert_eq!(reconstructed.slice_idx, 2);
    }

    // -----------------------------------------------------------------------
    // Test 9: Alternate Degraded Read Recovery via Diagonal Array Parity
    // -----------------------------------------------------------------------
    #[test]
    fn test_single_slice_degraded_read_diagonal() {
        let file_id = dummy_file_id();
        let slice_size = 4096;
        let chunk_size = 16 * 1024;
        let cohort_size = 3;
        let m = 4;
        let mut data = Vec::with_capacity(48 * 1024);
        for i in 0..(48 * 1024) {
            data.push(((i * 7) % 249) as u8);
        }

        let strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &data,
            chunk_size,
            cohort_size,
            slice_size,
        ).unwrap();

        // Target: Chunk 1, Slice 2
        let target_chunk = &strand.chunks[1];
        let target_slice = &target_chunk.slices[2];

        let guide = SliceGuideRNA::for_slice(
            target_chunk,
            2,
            Some(target_slice.slice_hash),
            slice_size,
        );

        let spacer = &strand.spacers[0];
        // d = (s - m) mod M = (2 - 1) mod 4 = 1
        let d = (2 - 1 + m) % m;
        let p_d = &spacer.diagonal_parity_slices[d];

        // Gather all slices on this diagonal from surviving chunks:
        // for member j: row is (d + j) mod M
        let mut surviving_diagonal_slices = Vec::new();
        for (j, chunk) in strand.chunks.iter().enumerate() {
            if j != 1 {
                let row = (d + j) % m;
                surviving_diagonal_slices.push(chunk.slices[row].clone());
            }
        }

        let reconstructed = CasRepairEngine::repair_slice_diagonal(
            &guide,
            &surviving_diagonal_slices,
            p_d,
            &spacer.member_chunk_ids,
            m,
        ).unwrap();

        assert_eq!(reconstructed.data, target_slice.data);
        assert_eq!(reconstructed.slice_hash, target_slice.slice_hash);
    }

    // -----------------------------------------------------------------------
    // Test 10: Slice Break Detection
    // -----------------------------------------------------------------------
    #[test]
    fn test_autonomous_slice_break_detection() {
        let file_id = dummy_file_id();
        let chunk_size = 16 * 1024;
        let slice_size = 4096;
        let payload = vec![0x99; 16 * 1024];

        let strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &payload,
            chunk_size,
            1,
            slice_size,
        ).unwrap();

        let healthy_chunk = &strand.chunks[0];
        let breaks = StrandDetector::detect_slice_breaks(healthy_chunk, 4);
        assert!(breaks.is_empty(), "Healthy chunk must produce 0 slice breaks");

        // 1. Missing slice 2
        let mut broken_chunk = healthy_chunk.clone();
        broken_chunk.slices.retain(|s| s.slice_idx != 2);
        let breaks = StrandDetector::detect_slice_breaks(&broken_chunk, 4);
        assert_eq!(breaks.len(), 1);
        assert_eq!(breaks[0].slice_idx, 2);
        assert_eq!(breaks[0].reason, SliceBreakReason::Missing);

        // 2. Corrupted slice hash (bit rot)
        let mut corrupted_chunk = healthy_chunk.clone();
        corrupted_chunk.slices[1].data[10] ^= 0xFF; // silently flip bit without updating hash
        let breaks = StrandDetector::detect_slice_breaks(&corrupted_chunk, 4);
        assert_eq!(breaks.len(), 1);
        assert_eq!(breaks[0].slice_idx, 1);
        assert_eq!(breaks[0].reason, SliceBreakReason::CorruptedHash);
    }

    // -----------------------------------------------------------------------
    // Test 11: Slice Delta Journaling (Incremental Parity Updates)
    // -----------------------------------------------------------------------
    #[test]
    fn test_slice_delta_journal_parity_updates() {
        let old_slice = make_test_slice(2, vec![0x10; 4096]);
        let new_data = vec![0x20; 100];
        let offset = 50;

        let delta = SliceDeltaJournal::create_delta(
            &old_slice,
            &new_data,
            offset,
            [0x88; 16],
            1,
        ).unwrap();

        assert_eq!(delta.slice_idx, 2);
        assert_eq!(delta.offset_in_slice, 50);
        assert_eq!(delta.delta_bytes.len(), 100);
        assert_eq!(delta.delta_bytes[0], 0x10 ^ 0x20);

        // Apply delta to slice
        let mut updated_slice = old_slice.clone();
        SliceDeltaJournal::apply_delta_to_slice(&mut updated_slice, &delta).unwrap();
        assert_eq!(updated_slice.data[50], 0x20);
        assert_eq!(updated_slice.data[0], 0x10);

        // Incremental update of horizontal parity
        let mut p_h_slice = make_test_slice(2, vec![0x10 ^ 0x30; 4096]); // suppose other chunk had 0x30
        SliceDeltaJournal::apply_delta_to_horizontal_parity(&mut p_h_slice, &delta).unwrap();
        assert_eq!(p_h_slice.data[50], 0x20 ^ 0x30);
    }

    // -----------------------------------------------------------------------
    // Test 12: Multi-Slice Range Write Decomposition
    // -----------------------------------------------------------------------
    #[test]
    fn test_multi_slice_span_write() {
        let slice_size = 4096;
        let s0 = make_test_slice(0, vec![0x01; slice_size]);
        let s1 = make_test_slice(1, vec![0x02; slice_size]);
        let s2 = make_test_slice(2, vec![0x03; slice_size]);
        let slices = vec![s0, s1, s2];

        // Write spans offset 4000 to 4200 (length 200)
        let payload = vec![0xFF; 200];
        let deltas = SliceDeltaJournal::create_deltas_for_range(
            &slices,
            4000,
            &payload,
            [0xEE; 16],
            0,
            slice_size,
        ).unwrap();

        assert_eq!(deltas.len(), 2);
        assert_eq!(deltas[0].slice_idx, 0);
        assert_eq!(deltas[0].offset_in_slice, 4000);
        assert_eq!(deltas[0].delta_bytes.len(), 96);

        assert_eq!(deltas[1].slice_idx, 1);
        assert_eq!(deltas[1].offset_in_slice, 0);
        assert_eq!(deltas[1].delta_bytes.len(), 104);
    }

    // -----------------------------------------------------------------------
    // Test 13: Core Wire Schemas Bincode Roundtrip
    // -----------------------------------------------------------------------
    #[test]
    fn test_wire_schemas_roundtrip() {
        let wire_slice = BiologicalSliceWire {
            slice_id: [0x11; 32],
            chunk_id: [0x22; 32],
            file_id: [0x33; 16],
            chunk_sequence_idx: 4,
            slice_idx: 7,
            total_slices: 16,
            kind: SliceParityKind::Data,
            left_arm: Some([0x44; 32]),
            right_arm: Some([0x55; 32]),
            cohort_id: [0x66; 16],
            data: vec![0x99; 4096],
        };

        let encoded = bincode::serialize(&wire_slice).unwrap();
        let decoded: BiologicalSliceWire = bincode::deserialize(&encoded).unwrap();
        assert_eq!(wire_slice, decoded);

        let mutation = FsMutation::SliceDeltaJournal {
            version: 0,
            delta: SliceDelta {
                file_id: [0x33; 16],
                cohort_id: [0x66; 16],
                sequence_idx: 4,
                slice_idx: 7,
                cohort_idx: 1,
                offset_in_slice: 64,
                delta_bytes: vec![0x12, 0x34, 0x56],
                new_slice_hash: [0x77; 32],
            },
        };
        let encoded_mut = bincode::serialize(&mutation).unwrap();
        let decoded_mut: FsMutation = bincode::deserialize(&encoded_mut).unwrap();
        assert_eq!(mutation, decoded_mut);

        let ipc_req = IpcRequest::RepairSlice {
            file: "large_file.dat".to_string(),
            chunk_idx: 3,
            slice_idx: 12,
        };
        let encoded_ipc = bincode::serialize(&ipc_req).unwrap();
        let decoded_ipc: IpcRequest = bincode::deserialize(&encoded_ipc).unwrap();
        assert_eq!(ipc_req, decoded_ipc);

        // SliceAnnouncement roundtrip
        let slice_ann = SliceAnnouncement {
            node_id: "node_1".to_string(),
            file_id: [0x33; 16],
            chunk_id: [0x22; 32],
            chunk_sequence_idx: 4,
            slice_idx: 7,
            slice_id: [0x11; 32],
            cohort_id: [0x66; 16],
            kind: SliceParityKind::Data,
            left_arm: Some([0x44; 32]),
            right_arm: Some([0x55; 32]),
        };
        let encoded_ann = bincode::serialize(&slice_ann).unwrap();
        let decoded_ann: SliceAnnouncement = bincode::deserialize(&encoded_ann).unwrap();
        assert_eq!(slice_ann, decoded_ann);

        // ChunkAnnouncement roundtrip
        let chunk_ann = WireChunkAnnouncement {
            node_id: "node_1".to_string(),
            chunk_id: [0x22; 32],
            file_id: [0x33; 16],
            sequence_idx: 4,
            left_arm: Some([0x44; 32]),
            right_arm: Some([0x55; 32]),
            cohort_id: [0x66; 16],
            available_slices: vec![0, 1, 2, 7],
        };
        let encoded_cann = bincode::serialize(&chunk_ann).unwrap();
        let decoded_cann: WireChunkAnnouncement = bincode::deserialize(&encoded_cann).unwrap();
        assert_eq!(chunk_ann, decoded_cann);

        // SliceTransferRequest and SliceTransferResponse roundtrip
        let xfer_req = SliceTransferRequest {
            file_id: [0x33; 16],
            chunk_id: [0x22; 32],
            chunk_sequence_idx: 4,
            slice_idx: 7,
            kind: SliceParityKind::Data,
        };
        let encoded_xfer = bincode::serialize(&xfer_req).unwrap();
        let decoded_xfer: SliceTransferRequest = bincode::deserialize(&encoded_xfer).unwrap();
        assert_eq!(xfer_req, decoded_xfer);

        let xfer_resp = SliceTransferResponse {
            file_id: [0x33; 16],
            chunk_id: [0x22; 32],
            chunk_sequence_idx: 4,
            slice_idx: 7,
            kind: SliceParityKind::Data,
            slice: Some(wire_slice.clone()),
            error: None,
        };
        let encoded_resp = bincode::serialize(&xfer_resp).unwrap();
        let decoded_resp: SliceTransferResponse = bincode::deserialize(&encoded_resp).unwrap();
        assert_eq!(xfer_resp, decoded_resp);

        // IpcResponse roundtrip
        let ipc_resp = IpcResponse::Success("Repaired".to_string());
        let encoded_iresp = bincode::serialize(&ipc_resp).unwrap();
        let decoded_iresp: IpcResponse = bincode::deserialize(&encoded_iresp).unwrap();
        assert_eq!(ipc_resp, decoded_iresp);
    }

    #[test]
    fn test_slice_delta_journal_diagonal_parity_all_chunks() {
        let file_id = dummy_file_id();
        let slice_size = 4096;
        let num_slices = 4;
        let chunk_size = num_slices * slice_size;
        let cohort_size = 4;
        let payload = vec![0x5A; cohort_size * chunk_size];

        let strand = StrandEncoder::encode_file_with_slice_size(
            file_id,
            &payload,
            chunk_size,
            cohort_size,
            slice_size,
        )
        .unwrap();

        let spacer = &strand.spacers[0];

        // Sweep all cohort chunks i in 0..4 and all slices r in 0..4
        for cohort_idx in 0..cohort_size {
            for slice_idx in 0..num_slices {
                let mut updated_p_d = spacer.diagonal_parity_slices.clone();
                let target_chunk = &strand.chunks[cohort_idx];
                let old_slice = &target_chunk.slices[slice_idx];
                let new_data = vec![0xDE; 256];

                let delta = SliceDeltaJournal::create_delta(
                    old_slice,
                    &new_data,
                    128,
                    target_chunk.cohort_id,
                    target_chunk.cohort_idx,
                )
                .unwrap();

                // Apply delta to diagonal buffers
                SliceDeltaJournal::apply_delta_to_diagonal_buffers(&mut updated_p_d, &delta).unwrap();

                // Full fresh re-encode to compute true ground truth
                let mut modified_chunks = strand.chunks.clone();
                SliceDeltaJournal::apply_delta_to_slice(
                    &mut modified_chunks[cohort_idx].slices[slice_idx],
                    &delta,
                )
                .unwrap();
                let fresh_p_d =
                    StrandEncoder::compute_diagonal_parity_slices(&modified_chunks, num_slices);

                // Verify every diagonal slice matches fresh re-encode bit-for-bit
                for d in 0..num_slices {
                    assert_eq!(
                        updated_p_d[d], fresh_p_d[d],
                        "Diagonal parity mismatch at d={} when updating chunk={}, slice={}",
                        d, cohort_idx, slice_idx
                    );
                }
            }
        }
    }
}

