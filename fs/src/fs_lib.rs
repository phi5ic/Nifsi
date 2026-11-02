pub mod hadr;

pub use fuser::BackgroundSession;
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use hadr::{
    hash_sha256, BiologicalChunk, BiologicalSlice, CasRepairEngine, CohortSpacer, FileId, HomologyArm,
    SliceDeltaJournal, SliceGuideRNA, SliceId, Strand, StrandEncoder,
    DEFAULT_SLICE_SIZE,
};
use libc::{EEXIST, ENOENT, ENOTDIR};
use nifsi_core::protocol::{
    BiologicalSliceWire, FileInspectionResponse, FsMutation, SliceDelta, SliceInfo, SliceParityKind,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

const TTL: Duration = Duration::from_secs(1);
const ROOT_INODE: u64 = 1;

#[derive(Debug, Clone)]
pub enum InodeKind {
    Directory {
        entries: HashMap<String, u64>,
    },
    File {
        buffer: Vec<u8>,
        strand: Option<Strand>,
        file_id: FileId,
    },
}

#[derive(Debug, Clone)]
pub struct InodeRecord {
    pub ino: u64,
    pub kind: InodeKind,
    pub size: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub version: u64,
}

impl InodeRecord {
    pub fn to_file_attr(&self) -> FileAttr {
        let (kind, _perm) = match &self.kind {
            InodeKind::Directory { .. } => (FileType::Directory, self.perm | 0o040000),
            InodeKind::File { .. } => (FileType::RegularFile, self.perm | 0o100000),
        };

        FileAttr {
            ino: self.ino,
            size: self.size,
            blocks: (self.size + 511) / 512,
            atime: self.atime,
            mtime: self.mtime,
            ctime: self.ctime,
            crtime: self.ctime,
            kind,
            perm: self.perm,
            nlink: self.nlink,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
            blksize: 4096,
        }
    }
}
#[derive(Debug, Clone)]
pub struct NifsiFS {
    inner: Arc<RwLock<NifsiFSInner>>,
    chunk_size: usize,
    cohort_size: usize,
    mutation_tx: Option<tokio::sync::mpsc::UnboundedSender<FsMutation>>,
}

#[derive(Debug)]
struct NifsiFSInner {
    inodes: HashMap<u64, InodeRecord>,
    names: HashMap<u64, String>,
    next_ino: u64,
}

impl Default for NifsiFS {
    fn default() -> Self {
        Self::new(64 * 1024, 4) // 64 KB chunks, 4 chunks per cohort
    }
}

impl NifsiFS {
    pub fn new(chunk_size: usize, cohort_size: usize) -> Self {
        Self::new_with_channel(chunk_size, cohort_size, None)
    }

    pub fn new_with_channel(
        chunk_size: usize,
        cohort_size: usize,
        mutation_tx: Option<tokio::sync::mpsc::UnboundedSender<FsMutation>>,
    ) -> Self {
        let now = SystemTime::now();
        let mut entries = HashMap::new();
        entries.insert(".".to_string(), ROOT_INODE);
        entries.insert("..".to_string(), ROOT_INODE);

        let root_record = InodeRecord {
            ino: ROOT_INODE,
            kind: InodeKind::Directory { entries },
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            perm: 0o755,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            version: 0,
        };

        let mut inodes = HashMap::new();
        inodes.insert(ROOT_INODE, root_record);

        Self {
            inner: Arc::new(RwLock::new(NifsiFSInner {
                inodes,
                names: HashMap::new(),
                next_ino: 2,
            })),
            chunk_size,
            cohort_size,
            mutation_tx,
        }
    }

    /// Retrieve the HADR strand of a file by path.
    pub fn get_strand_by_path(&self, path: &str) -> Option<Strand> {
        let path = path.trim_matches('/');
        let inner = self.inner.read().unwrap();
        let mut curr_ino = ROOT_INODE;

        if path.is_empty() {
            return None;
        }

        for part in path.split('/') {
            let record = inner.inodes.get(&curr_ino)?;
            match &record.kind {
                InodeKind::Directory { entries } => {
                    curr_ino = *entries.get(part)?;
                }
                _ => return None,
            }
        }

        let record = inner.inodes.get(&curr_ino)?;
        match &record.kind {
            InodeKind::File { strand, .. } => strand.clone(),
            _ => None,
        }
    }

    /// Direct inmemory write helper (for programmatic testing without kernel mount)
    pub fn write_file_bytes(&self, name: &str, data: &[u8]) -> Result<u64, String> {
        let mut inner = self.inner.write().unwrap();
        let ino = inner.next_ino;
        inner.next_ino += 1;

        let now = SystemTime::now();
        let mut file_id = [0u8; 16];
        file_id[..8].copy_from_slice(&ino.to_le_bytes());

        let strand = if !data.is_empty() {
            Some(
                StrandEncoder::encode_file(file_id, data, self.chunk_size, self.cohort_size)
                    .map_err(|e| e.to_string())?,
            )
        } else {
            None
        };

        let record = InodeRecord {
            ino,
            kind: InodeKind::File {
                buffer: data.to_vec(),
                strand,
                file_id,
            },
            size: data.len() as u64,
            atime: now,
            mtime: now,
            ctime: now,
            perm: 0o644,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            version: 0,
        };

        inner.inodes.insert(ino, record);
        inner.names.insert(ino, name.to_string());

        // Add to root directory
        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get_mut(&ROOT_INODE)
        {
            entries.insert(name.to_string(), ino);
        }

        Ok(ino)
    }

    /// Read file bytes by inode.
    pub fn read_file_bytes(&self, ino: u64) -> Result<Vec<u8>, String> {
        let inner = self.inner.read().unwrap();
        let record = inner.inodes.get(&ino).ok_or("Inode not found")?;
        match &record.kind {
            InodeKind::File { buffer, strand, .. } => {
                if let Some(s) = strand {
                    StrandEncoder::assemble_file(s.chunks.clone())
                        .map_err(|e| format!("HADR strand assembly error: {:?}", e))
                } else {
                    Ok(buffer.clone())
                }
            }
            _ => Err("Not a regular file".to_string()),
        }
    }

    /// Gives the total number of active chunks and parity spacers stored across all files.
    pub fn chunk_count(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .inodes
            .values()
            .map(|rec| match &rec.kind {
                InodeKind::File {
                    strand: Some(s), ..
                } => s.chunks.len() + s.spacers.len(),
                _ => 0,
            })
            .sum()
    }
    pub fn apply_remote_write(&self, name: &str, data: &[u8], incoming_version: u64) -> Result<u64, String> {
        let mut inner = self.inner.write().unwrap();
        let existing_ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            entries.get(name).copied()
        } else {
            None
        };

        let now = SystemTime::now();

        if let Some(ino) = existing_ino {
            let chunk_size = self.chunk_size;
            let cohort_size = self.cohort_size;
            if let Some(record) = inner.inodes.get_mut(&ino) {
                // Reject stale replays: if incoming version is older than stored, drop it.
                if incoming_version > 0 && record.version >= incoming_version {
                    return Ok(ino);
                }
                // Do not overwrite a Raft-committed version with an unversioned periodic sync.
                if incoming_version == 0 && record.version > 0 {
                    return Ok(ino);
                }
                if let InodeKind::File { buffer, strand, file_id } = &mut record.kind {
                    if buffer.as_slice() == data {
                        return Ok(ino);
                    }
                    *buffer = data.to_vec();
                    record.size = data.len() as u64;
                    record.mtime = now;
                    record.version = incoming_version;
                    if !data.is_empty() {
                        if let Ok(new_strand) =
                            StrandEncoder::encode_file(*file_id, buffer, chunk_size, cohort_size)
                        {
                            *strand = Some(new_strand);
                        }
                    } else {
                        *strand = None;
                    }
                }
            }
            Ok(ino)
        } else {
            let ino = inner.next_ino;
            inner.next_ino += 1;

            let mut file_id = [0u8; 16];
            file_id[..8].copy_from_slice(&ino.to_le_bytes());

            let strand = if !data.is_empty() {
                Some(
                    StrandEncoder::encode_file(file_id, data, self.chunk_size, self.cohort_size)
                        .map_err(|e| e.to_string())?,
                )
            } else {
                None
            };

            let record = InodeRecord {
                ino,
                kind: InodeKind::File {
                    buffer: data.to_vec(),
                    strand,
                    file_id,
                },
                size: data.len() as u64,
                atime: now,
                mtime: now,
                ctime: now,
                perm: 0o644,
                nlink: 1,
                uid: 1000,
                gid: 1000,
                version: incoming_version,
            };

            inner.inodes.insert(ino, record);
            inner.names.insert(ino, name.to_string());

            if let Some(InodeRecord {
                kind: InodeKind::Directory { entries },
                ..
            }) = inner.inodes.get_mut(&ROOT_INODE)
            {
                entries.insert(name.to_string(), ino);
            }

            Ok(ino)
        }
    }

    /// Apply an inbound replicated delete from another cluster peer.
    pub fn apply_remote_delete(&self, name: &str, incoming_version: u64) -> Result<(), String> {
        let mut inner = self.inner.write().unwrap();
        let existing_ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            entries.get(name).copied()
        } else {
            None
        };

        if let Some(ino) = existing_ino {
            if let Some(record) = inner.inodes.get(&ino) {
                if incoming_version > 0 && record.version >= incoming_version {
                    return Ok(()); // Stale delete; newer write exists
                }
            }
            if let Some(InodeRecord {
                kind: InodeKind::Directory { entries },
                ..
            }) = inner.inodes.get_mut(&ROOT_INODE) {
                entries.remove(name);
            }
            inner.inodes.remove(&ino);
            inner.names.remove(&ino);
        }
        Ok(())
    }
    pub fn list_files(&self) -> Vec<(String, Vec<u8>)> {
        let inner = self.inner.read().unwrap();
        let mut files = Vec::new();
        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            for (name, ino) in entries {
                if name == "." || name == ".." {
                    continue;
                }
                if let Some(InodeRecord {
                    kind: InodeKind::File { buffer, .. },
                    ..
                }) = inner.inodes.get(ino) {
                    files.push((name.clone(), buffer.clone()));
                }
            }
        }
        files
    }
    pub fn apply_remote_slice(
        &self,
        file_id: FileId,
        chunk_sequence_idx: u32,
        slice_idx: u16,
        slice: &BiologicalSliceWire,
        incoming_version: u64,
    ) -> Result<(), String> {
        let mut inner = self.inner.write().unwrap();
        for record in inner.inodes.values_mut() {
            if let InodeKind::File { buffer, strand, file_id: id } = &mut record.kind {
                if *id == file_id {
                    if incoming_version > 0 && record.version >= incoming_version {
                        return Ok(()); // Stale slice write; ignore
                    }
                    if incoming_version > 0 {
                        record.version = incoming_version;
                    }
                    let chunk_size = self.chunk_size;
                    let slice_size = DEFAULT_SLICE_SIZE;
                    let global_slice_start = (chunk_sequence_idx as usize) * chunk_size + (slice_idx as usize) * slice_size;
                    let global_slice_end = global_slice_start + slice.data.len();
                    if buffer.len() < global_slice_end {
                        buffer.resize(global_slice_end, 0);
                    }
                    buffer[global_slice_start..global_slice_end].copy_from_slice(&slice.data);
                    record.size = buffer.len() as u64;
                    record.mtime = SystemTime::now();

                    if strand.is_none() {
                        *strand = Some(Strand {
                            file_id,
                            total_chunks: chunk_sequence_idx + 1,
                            original_file_size: buffer.len(),
                            chunk_size,
                            chunks: Vec::new(),
                            spacers: Vec::new(),
                        });
                    }

                    if let Some(s) = strand {
                        while s.chunks.len() <= chunk_sequence_idx as usize {
                            let seq = s.chunks.len() as u32;
                            let cohort_num = (seq as usize) / self.cohort_size;
                            let start_idx = cohort_num * self.cohort_size;
                            let mut cohort_id = [0u8; 16];
                            cohort_id[..8].copy_from_slice(&file_id[..8]);
                            cohort_id[8..12].copy_from_slice(&(cohort_num as u32).to_be_bytes());
                            cohort_id[12..16].copy_from_slice(&(start_idx as u32).to_be_bytes());
                            if seq == chunk_sequence_idx && slice.cohort_id != [0u8; 16] {
                                cohort_id = slice.cohort_id;
                            }
                            s.chunks.push(BiologicalChunk {
                                chunk_id: hash_sha256(&[]),
                                file_id,
                                sequence_idx: seq,
                                total_chunks: seq + 1,
                                left_arm: None,
                                right_arm: None,
                                data: Vec::new(),
                                slices: Vec::new(),
                                slice_size: DEFAULT_SLICE_SIZE,
                                cohort_id,
                                cohort_idx: ((seq as usize) % self.cohort_size) as u16,
                                cohort_size: self.cohort_size as u16,
                            });
                        }
                        if let Some(chunk) = s.chunks.iter_mut().find(|c| c.sequence_idx == chunk_sequence_idx) {
                            if slice.cohort_id != [0u8; 16] && chunk.cohort_id != slice.cohort_id {
                                chunk.cohort_id = slice.cohort_id;
                            }
                            if let Some(sl) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx) {
                                sl.data = slice.data.clone();
                                sl.slice_hash = slice.slice_id;
                                sl.left_arm = slice.left_arm;
                                sl.right_arm = slice.right_arm;
                                sl.cohort_id = slice.cohort_id;
                            } else {
                                chunk.slices.push(BiologicalSlice {
                                    slice_idx: slice.slice_idx,
                                    chunk_id: slice.chunk_id,
                                    file_id: slice.file_id,
                                    sequence_idx: slice.chunk_sequence_idx,
                                    offset: (slice.slice_idx as usize) * DEFAULT_SLICE_SIZE,
                                    data: slice.data.clone(),
                                    slice_hash: slice.slice_id,
                                    left_arm: slice.left_arm,
                                    right_arm: slice.right_arm,
                                    cohort_id: slice.cohort_id,
                                });
                                chunk.slices.sort_by_key(|sl| sl.slice_idx);
                            }
                            let slice_offset = (slice_idx as usize) * chunk.slice_size;
                            let end = slice_offset + slice.data.len();
                            if chunk.data.len() < end {
                                chunk.data.resize(end, 0);
                            }
                            chunk.data[slice_offset..end].copy_from_slice(&slice.data);
                            chunk.chunk_id = hash_sha256(&chunk.data);
                            for sl in &mut chunk.slices {
                                sl.chunk_id = chunk.chunk_id;
                            }
                            let is_authentic = hash_sha256(&slice.data) == slice.slice_id;
                            if is_authentic {
                                let updated_hash = slice.slice_id;
                                if slice_idx > 0 {
                                    if let Some(prev) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx - 1) {
                                        prev.right_arm = Some(updated_hash);
                                    }
                                }
                                if let Some(next) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx + 1) {
                                    next.left_arm = Some(updated_hash);
                                }
                            }
                        }
                        let num_c = s.chunks.len();
                        s.total_chunks = num_c as u32;
                        s.original_file_size = buffer.len();
                        for i in 0..num_c {
                            s.chunks[i].total_chunks = num_c as u32;
                            s.chunks[i].left_arm = if i > 0 { Some(s.chunks[i - 1].chunk_id) } else { None };
                            s.chunks[i].right_arm = if i + 1 < num_c { Some(s.chunks[i + 1].chunk_id) } else { None };
                        }
                        let target_cohort_id = s.chunks
                            .iter()
                            .find(|c| c.sequence_idx == chunk_sequence_idx)
                            .map(|c| c.cohort_id)
                            .unwrap_or(slice.cohort_id);

                        let cohort_num = (chunk_sequence_idx as usize) / self.cohort_size;
                        while s.spacers.len() <= cohort_num {
                            let c_num = s.spacers.len();
                            let start_idx = c_num * self.cohort_size;
                            let mut c_id = [0u8; 16];
                            c_id[..8].copy_from_slice(&file_id[..8]);
                            c_id[8..12].copy_from_slice(&(c_num as u32).to_be_bytes());
                            c_id[12..16].copy_from_slice(&(start_idx as u32).to_be_bytes());
                            if c_num == cohort_num && target_cohort_id != [0u8; 16] {
                                c_id = target_cohort_id;
                            }
                            s.spacers.push(CohortSpacer {
                                cohort_id: c_id,
                                file_id,
                                cohort_size: self.cohort_size as u16,
                                parity_data: Vec::new(),
                                horizontal_parity_slices: Vec::new(),
                                diagonal_parity_slices: Vec::new(),
                                member_chunk_ids: Vec::new(),
                                member_lengths: Vec::new(),
                            });
                        }

                        let is_authentic = hash_sha256(&slice.data) == slice.slice_id;
                        if is_authentic {
                            if let Some(spacer) = s.spacers.iter_mut().find(|sp| sp.cohort_id == target_cohort_id) {
                                let cohort_chunks: Vec<BiologicalChunk> = s.chunks
                                    .iter()
                                    .filter(|c| c.cohort_id == target_cohort_id)
                                    .cloned()
                                    .collect();
                                let max_slices = cohort_chunks.iter().map(|c| c.slices.len()).max().unwrap_or(0);
                                if max_slices > 0 {
                                    spacer.cohort_size = cohort_chunks.len() as u16;
                                    spacer.horizontal_parity_slices =
                                        StrandEncoder::compute_horizontal_parity_slices(&cohort_chunks, max_slices);
                                    spacer.diagonal_parity_slices =
                                        StrandEncoder::compute_diagonal_parity_slices(&cohort_chunks, max_slices);
                                    spacer.member_chunk_ids = cohort_chunks.iter().map(|c| c.chunk_id).collect();
                                    spacer.member_lengths = cohort_chunks.iter().map(|c| c.data.len()).collect();

                                    let max_len = cohort_chunks.iter().map(|c| c.data.len()).max().unwrap_or(0);
                                    let mut parity_data = vec![0u8; max_len];
                                    for c in &cohort_chunks {
                                        for (b_idx, &byte) in c.data.iter().enumerate() {
                                            parity_data[b_idx] ^= byte;
                                        }
                                    }
                                    spacer.parity_data = parity_data;
                                }
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }
        Err("File not found for remote slice write".to_string())
    }

    /// Apply an inbound replicated slice delta journal from another cluster peer.
    pub fn apply_remote_slice_delta(&self, delta: &SliceDelta, incoming_version: u64) -> Result<(), String> {
        let mut inner = self.inner.write().unwrap();
        for record in inner.inodes.values_mut() {
            if let InodeKind::File { buffer, strand, file_id: id } = &mut record.kind {
                if *id == delta.file_id {
                    if incoming_version > 0 && record.version >= incoming_version {
                        return Ok(()); // Stale slice delta; ignore
                    }
                    if incoming_version > 0 {
                        record.version = incoming_version;
                    }
                    let chunk_size = self.chunk_size;
                    let slice_size = DEFAULT_SLICE_SIZE;
                    let global_slice_start = (delta.sequence_idx as usize) * chunk_size
                        + (delta.slice_idx as usize) * slice_size
                        + (delta.offset_in_slice as usize);
                    let global_slice_end = global_slice_start + delta.delta_bytes.len();
                    if buffer.len() < global_slice_end {
                        buffer.resize(global_slice_end, 0);
                    }
                    for (i, &d) in delta.delta_bytes.iter().enumerate() {
                        buffer[global_slice_start + i] ^= d;
                    }
                    record.mtime = SystemTime::now();

                    if let Some(s) = strand {
                        let mut updated_slice_hash = None;
                        if let Some(chunk) = s.chunks.iter_mut().find(|c| c.sequence_idx == delta.sequence_idx) {
                            if let Some(sl) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == delta.slice_idx) {
                                let _ = SliceDeltaJournal::apply_delta_to_slice(sl, delta);
                                let slice_offset = (delta.slice_idx as usize) * chunk.slice_size;
                                let end = slice_offset + sl.data.len();
                                if chunk.data.len() < end {
                                    chunk.data.resize(end, 0);
                                }
                                chunk.data[slice_offset..end].copy_from_slice(&sl.data);
                                chunk.chunk_id = hash_sha256(&chunk.data);
                                updated_slice_hash = Some(sl.slice_hash);
                            }

                            if let Some(new_hash) = updated_slice_hash {
                                if delta.slice_idx > 0 {
                                    if let Some(prev) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == delta.slice_idx - 1) {
                                        prev.right_arm = Some(new_hash);
                                    }
                                }
                                if let Some(next) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == delta.slice_idx + 1) {
                                    next.left_arm = Some(new_hash);
                                }
                            }

                            if let Some(spacer) = s.spacers.iter_mut().find(|sp| sp.cohort_id == chunk.cohort_id) {
                                if (delta.slice_idx as usize) < spacer.horizontal_parity_slices.len() {
                                    let _ = SliceDeltaJournal::apply_delta_to_horizontal_buffer(
                                        &mut spacer.horizontal_parity_slices[delta.slice_idx as usize],
                                        delta,
                                    );
                                }
                                if !spacer.diagonal_parity_slices.is_empty() {
                                    let _ = SliceDeltaJournal::apply_delta_to_diagonal_buffers(
                                        &mut spacer.diagonal_parity_slices,
                                        delta,
                                    );
                                }
                            }
                        }

                        let num_c = s.chunks.len();
                        for i in 0..num_c {
                            s.chunks[i].left_arm = if i > 0 { Some(s.chunks[i - 1].chunk_id) } else { None };
                            s.chunks[i].right_arm = if i + 1 < num_c { Some(s.chunks[i + 1].chunk_id) } else { None };
                        }
                    }
                    return Ok(());
                }
            }
        }
        Err("File not found for remote slice delta".to_string())
    }
    pub fn inspect_file_strand(&self, name: &str) -> Option<FileInspectionResponse> {
        let clean_name = name.trim().trim_start_matches('/');
        let inner = self.inner.read().unwrap();
        let ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            *entries.get(clean_name)?
        } else {
            return None;
        };

        let record = inner.inodes.get(&ino)?;
        match &record.kind {
            InodeKind::File { buffer, strand, file_id } => {
                let file_id_str = file_id.iter().map(|b| format!("{:02x}", b)).collect::<String>();
                if let Some(s) = strand {
                    let total_chunks = s.chunks.len() as u32;
                    let total_slices: usize = s.chunks.iter().map(|c| c.slices.len()).sum();
                    let cohorts = s.spacers.len();
                    let mut healthy = 0;
                    let mut degraded = 0;
                    for c in &s.chunks {
                        for sl in &c.slices {
                            if hash_sha256(&sl.data) == sl.slice_hash {
                                healthy += 1;
                            } else {
                                degraded += 1;
                            }
                        }
                    }
                    Some(FileInspectionResponse {
                        file_name: clean_name.to_string(),
                        file_id: file_id_str,
                        file_size: buffer.len(),
                        total_chunks,
                        total_slices,
                        cohorts,
                        healthy_slices: healthy,
                        degraded_slices: degraded,
                    })
                } else {
                    Some(FileInspectionResponse {
                        file_name: clean_name.to_string(),
                        file_id: file_id_str,
                        file_size: buffer.len(),
                        total_chunks: 0,
                        total_slices: 0,
                        cohorts: 0,
                        healthy_slices: 0,
                        degraded_slices: 0,
                    })
                }
            }
            _ => None,
        }
    }

    /// List subpacket slices for a given chunk sequence index.
    pub fn list_chunk_slices(&self, name: &str, chunk_seq: u32) -> Result<Vec<SliceInfo>, String> {
        let clean_name = name.trim().trim_start_matches('/');
        let inner = self.inner.read().unwrap();
        let ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            *entries.get(clean_name).ok_or("File not found")?
        } else {
            return Err("Root inode corrupted".to_string());
        };

        let record = inner.inodes.get(&ino).ok_or("Inode not found")?;
        match &record.kind {
            InodeKind::File { strand: Some(s), .. } => {
                if let Some(chunk) = s.chunks.iter().find(|c| c.sequence_idx == chunk_seq) {
                    let infos = chunk.slices.iter().map(|sl| SliceInfo {
                        slice_idx: sl.slice_idx,
                        slice_id: sl.slice_hash.iter().map(|b| format!("{:02x}", b)).collect::<String>(),
                        kind: SliceParityKind::Data,
                        size: sl.data.len(),
                        left_arm: sl.left_arm.map(|a| a.iter().map(|b| format!("{:02x}", b)).collect::<String>()),
                        right_arm: sl.right_arm.map(|a| a.iter().map(|b| format!("{:02x}", b)).collect::<String>()),
                    }).collect();
                    Ok(infos)
                } else {
                    let spacer_offset = chunk_seq.saturating_sub(s.chunks.len() as u32) as usize;
                    if let Some(spacer) = s.spacers.get(spacer_offset) {
                        let mut infos = Vec::new();
                        for (i, p) in spacer.horizontal_parity_slices.iter().enumerate() {
                            infos.push(SliceInfo {
                                slice_idx: i as u16,
                                slice_id: hash_sha256(p).iter().map(|b| format!("{:02x}", b)).collect(),
                                kind: SliceParityKind::HorizontalParity,
                                size: p.len(),
                                left_arm: None,
                                right_arm: None,
                            });
                        }
                        for (i, p) in spacer.diagonal_parity_slices.iter().enumerate() {
                            infos.push(SliceInfo {
                                slice_idx: i as u16,
                                slice_id: hash_sha256(p).iter().map(|b| format!("{:02x}", b)).collect(),
                                kind: SliceParityKind::DiagonalParity,
                                size: p.len(),
                                left_arm: None,
                                right_arm: None,
                            });
                        }
                        Ok(infos)
                    } else {
                        Err(format!("Chunk sequence {} not found", chunk_seq))
                    }
                }
            }
            _ => Err("No strand exists for file".to_string()),
        }
    }

    /// Derives the authentic expected slice hash from adjacent slice homology arms
    /// (right arm of slice s-1 or left arm of slice s+1).
    /// Returns None if adjacent homology arms are unavailable or absent.
    fn derive_expected_slice_hash(chunk: &BiologicalChunk, slice_idx: u16) -> Option<SliceId> {
        if slice_idx > 0 {
            chunk.slices.iter().find(|sl| sl.slice_idx == slice_idx - 1).and_then(|sl| sl.right_arm)
        } else {
            None
        }
        .or_else(|| {
            slice_idx.checked_add(1).and_then(|next_idx| {
                chunk.slices.iter().find(|sl| sl.slice_idx == next_idx).and_then(|sl| sl.left_arm)
            })
        })
    }

    /// Derives the expected left homology arm for slice s (the hash of slice s-1).
    fn derive_expected_left_arm(chunk: &BiologicalChunk, slice_idx: u16) -> Option<HomologyArm> {
        if slice_idx > 0 {
            chunk.slices.iter().find(|sl| sl.slice_idx == slice_idx - 1).map(|sl| sl.slice_hash)
        } else {
            None
        }
    }

    /// Derives the expected right homology arm for slice s (the hash of slice s+1).
    fn derive_expected_right_arm(chunk: &BiologicalChunk, slice_idx: u16) -> Option<HomologyArm> {
        slice_idx.checked_add(1).and_then(|next_idx| {
            chunk.slices.iter().find(|sl| sl.slice_idx == next_idx).map(|sl| sl.slice_hash)
        })
    }
    pub fn repair_file_slice(&self, name: &str, chunk_seq: u32, slice_idx: u16) -> Result<String, String> {
        let clean_name = name.trim().trim_start_matches('/');
        let mut inner = self.inner.write().unwrap();
        let ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&ROOT_INODE) {
            *entries.get(clean_name).ok_or("File not found")?
        } else {
            return Err("Root directory missing".to_string());
        };

        let cohort_size = self.cohort_size;
        let record = inner.inodes.get_mut(&ino).ok_or("Inode missing")?;

        match &mut record.kind {
            InodeKind::File { buffer, strand: Some(s), file_id } => {
                let cohort_num = (chunk_seq as usize) / cohort_size;
                let spacer = s.spacers.get(cohort_num).ok_or("Cohort spacer missing")?;
                let target_chunk = s.chunks.iter().find(|c| c.sequence_idx == chunk_seq).ok_or("Target chunk missing")?;

                let expected_slice_hash = Self::derive_expected_slice_hash(target_chunk, slice_idx);
                let expected_left_arm = Self::derive_expected_left_arm(target_chunk, slice_idx);
                let expected_right_arm = Self::derive_expected_right_arm(target_chunk, slice_idx);

                let guide = SliceGuideRNA {
                    file_id: *file_id,
                    target_chunk_id: target_chunk.chunk_id,
                    chunk_id: target_chunk.chunk_id,
                    sequence_idx: chunk_seq,
                    slice_idx,
                    cohort_id: spacer.cohort_id,
                    cohort_idx: ((chunk_seq as usize) % cohort_size) as u16,
                    expected_slice_hash,
                    expected_left_arm,
                    expected_right_arm,
                    slice_len: target_chunk.slice_size,
                };

                let mut surviving = Vec::new();
                for other_c in &s.chunks {
                    if other_c.cohort_id == spacer.cohort_id && other_c.sequence_idx != chunk_seq {
                        if let Some(other_sl) = other_c.slices.iter().find(|sl| sl.slice_idx == slice_idx) {
                            surviving.push(other_sl.clone());
                        }
                    }
                }

                let p_h = spacer.horizontal_parity_slices.get(slice_idx as usize).ok_or("Parity slice missing")?;
                let repaired = CasRepairEngine::repair_slice(&guide, &surviving, p_h).map_err(|e| format!("{:?}", e))?;

                if let Some(c) = s.chunks.iter_mut().find(|c| c.sequence_idx == chunk_seq) {
                    if let Some(sl) = c.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx) {
                        *sl = repaired.clone();
                    } else {
                        c.slices.push(repaired.clone());
                        c.slices.sort_by_key(|sl| sl.slice_idx);
                    }
                    let repaired_hash = repaired.slice_hash;
                    if slice_idx > 0 {
                        if let Some(prev) = c.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx - 1) {
                            prev.right_arm = Some(repaired_hash);
                        }
                    }
                    if let Some(next) = slice_idx.checked_add(1).and_then(|next_idx| c.slices.iter_mut().find(|sl| sl.slice_idx == next_idx)) {
                        next.left_arm = Some(repaired_hash);
                    }
                    let slice_offset = (slice_idx as usize) * c.slice_size;
                    let end = slice_offset + repaired.data.len();
                    if c.data.len() < end {
                        c.data.resize(end, 0);
                    }
                    c.data[slice_offset..end].copy_from_slice(&repaired.data);
                    c.chunk_id = hash_sha256(&c.data);

                    let global_offset = (chunk_seq as usize) * s.chunk_size + slice_offset;
                    let global_end = global_offset + repaired.data.len();
                    if buffer.len() < global_end {
                        buffer.resize(global_end, 0);
                    }
                    buffer[global_offset..global_end].copy_from_slice(&repaired.data);
                }

                let num_c = s.chunks.len();
                for i in 0..num_c {
                    s.chunks[i].left_arm = if i > 0 { Some(s.chunks[i - 1].chunk_id) } else { None };
                    s.chunks[i].right_arm = if i + 1 < num_c { Some(s.chunks[i + 1].chunk_id) } else { None };
                }

                Ok(format!("Slice {} in chunk {} repaired bit-for-bit", slice_idx, chunk_seq))
            }
            _ => Err("Not a regular file or missing strand".to_string()),
        }
    }
}

fn truncate_strand_slices(s: &mut Strand, new_size: usize, chunk_size: usize, cohort_size: usize) {
    let num_chunks = (new_size + chunk_size - 1) / chunk_size;
    s.chunks.truncate(num_chunks);
    s.total_chunks = num_chunks as u32;

    if let Some(tail_chunk) = s.chunks.last_mut() {
        tail_chunk.total_chunks = num_chunks as u32;
        tail_chunk.right_arm = None;
        let chunk_rem = new_size - (num_chunks - 1) * chunk_size;
        tail_chunk.data.truncate(chunk_rem);
        tail_chunk.chunk_id = hash_sha256(&tail_chunk.data);

        let slice_size = tail_chunk.slice_size;
        let num_slices = (chunk_rem + slice_size - 1) / slice_size;
        tail_chunk.slices.truncate(num_slices);

        if let Some(tail_slice) = tail_chunk.slices.last_mut() {
            let slice_rem = (chunk_rem - 1) % slice_size + 1;
            tail_slice.data.truncate(slice_rem);
            tail_slice.slice_hash = hash_sha256(&tail_slice.data);
            tail_slice.right_arm = None;
        }

        for i in 0..tail_chunk.slices.len() {
            tail_chunk.slices[i].left_arm = if i > 0 { Some(tail_chunk.slices[i - 1].slice_hash) } else { None };
            tail_chunk.slices[i].right_arm = if i + 1 < tail_chunk.slices.len() { Some(tail_chunk.slices[i + 1].slice_hash) } else { None };
        }
    }

    let num_cohorts = (num_chunks + cohort_size - 1) / cohort_size;
    s.spacers.truncate(num_cohorts);
}

impl Filesystem for NifsiFS {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let inner = self.inner.read().unwrap();
        if let Some(parent_record) = inner.inodes.get(&parent) {
            if let InodeKind::Directory { entries } = &parent_record.kind {
                if let Some(&child_ino) = entries.get(name_str) {
                    if let Some(child_record) = inner.inodes.get(&child_ino) {
                        reply.entry(&TTL, &child_record.to_file_attr(), 0);
                        return;
                    }
                }
            }
        }
        reply.error(ENOENT);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        let inner = self.inner.read().unwrap();
        if let Some(record) = inner.inodes.get(&ino) {
            reply.attr(&TTL, &record.to_file_attr());
        } else {
            reply.error(ENOENT);
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let mut inner = self.inner.write().unwrap();
        if let Some(record) = inner.inodes.get_mut(&ino) {
            if let Some(m) = mode {
                record.perm = m as u16;
            }
            if let Some(u) = uid {
                record.uid = u;
            }
            if let Some(g) = gid {
                record.gid = g;
            }
            if let Some(new_size) = size {
                let new_size = new_size as usize;
                let old_size = record.size as usize;
                record.size = new_size as u64;
                record.mtime = SystemTime::now();
                record.ctime = SystemTime::now();
                let chunk_size = self.chunk_size;
                let cohort_size = self.cohort_size;
                if let InodeKind::File { buffer, strand, file_id } = &mut record.kind {
                    if new_size == 0 {
                        buffer.clear();
                        *strand = None;
                    } else if new_size < old_size {
                        buffer.truncate(new_size);
                        if let Some(s) = strand {
                            truncate_strand_slices(s, new_size, chunk_size, cohort_size);
                        }
                    } else if new_size > old_size {
                        buffer.resize(new_size, 0);
                        if let Ok(new_strand) =
                            StrandEncoder::encode_file(*file_id, buffer, chunk_size, cohort_size)
                        {
                            *strand = Some(new_strand);
                        }
                    }
                }
            }
            reply.attr(&TTL, &record.to_file_attr());
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let inner = self.inner.read().unwrap();
        let record = match inner.inodes.get(&ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let entries = match &record.kind {
            InodeKind::Directory { entries } => entries,
            _ => {
                reply.error(ENOTDIR);
                return;
            }
        };

        let mut items: Vec<(&String, &u64)> = entries.iter().collect();
        items.sort_by_key(|(name, _)| *name);

        for (idx, (name, child_ino)) in items.iter().enumerate().skip(offset as usize) {
            let child_record = match inner.inodes.get(child_ino) {
                Some(r) => r,
                None => continue,
            };
            let kind = match &child_record.kind {
                InodeKind::Directory { .. } => FileType::Directory,
                InodeKind::File { .. } => FileType::RegularFile,
            };

            let next_offset = (idx + 1) as i64;
            if reply.add(**child_ino, next_offset, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn open(&mut self, _req: &Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let offset = offset as usize;
        let size = size as usize;

        let inner = self.inner.read().unwrap();
        let record = match inner.inodes.get(&ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if offset >= record.size as usize {
            reply.data(&[]);
            return;
        }

        let read_end = (offset + size).min(record.size as usize);
        let target_len = read_end - offset;

        match &record.kind {
            InodeKind::File { buffer, strand: None, .. } => {
                reply.data(&buffer[offset..read_end]);
            }
            InodeKind::File { buffer, strand: Some(s), file_id } => {
                let chunk_size = s.chunk_size;
                let slice_size = s.chunks.first().map(|c| c.slice_size).unwrap_or(DEFAULT_SLICE_SIZE);
                if chunk_size == 0 || slice_size == 0 {
                    reply.data(&buffer[offset..read_end]);
                    return;
                }

                let mut result = Vec::with_capacity(target_len);
                let mut cur = offset;

                while cur < read_end {
                    let chunk_seq = (cur / chunk_size) as u32;
                    let offset_in_chunk = cur % chunk_size;
                    let slice_idx = (offset_in_chunk / slice_size) as u16;
                    let offset_in_slice = offset_in_chunk % slice_size;
                    let bytes_left_in_slice = slice_size - offset_in_slice;
                    let to_read = bytes_left_in_slice.min(read_end - cur);

                    let target_chunk = s.chunks.iter().find(|c| c.sequence_idx == chunk_seq);
                    let target_slice = target_chunk.and_then(|c| c.slices.iter().find(|sl| sl.slice_idx == slice_idx));

                    let expected_slice_hash = target_chunk.and_then(|c| Self::derive_expected_slice_hash(c, slice_idx));

                    let is_healthy = if let Some(sl) = target_slice {
                        let self_hash_ok = !sl.data.is_empty() && hash_sha256(&sl.data) == sl.slice_hash;
                        let arm_hash_ok = if let Some(expected) = expected_slice_hash {
                            sl.slice_hash == expected
                        } else {
                            true
                        };
                        self_hash_ok && arm_hash_ok
                    } else {
                        false
                    };

                    if is_healthy {
                        let sl = target_slice.unwrap();
                        if offset_in_slice < sl.data.len() {
                            let available = (sl.data.len() - offset_in_slice).min(to_read);
                            result.extend_from_slice(&sl.data[offset_in_slice..offset_in_slice + available]);
                            cur += available;
                            if available < to_read {
                                break;
                            }
                        } else {
                            break;
                        }
                    } else {
                        // perform k * 4KB horizontal slice degraded read
                        let cohort_size = self.cohort_size;
                        let cohort_num = (chunk_seq as usize) / cohort_size;
                        let spacer_opt = s.spacers.get(cohort_num);

                        if let Some(spacer) = spacer_opt {
                            let mut surviving = Vec::new();
                            for other_c in &s.chunks {
                                if other_c.cohort_id == spacer.cohort_id && other_c.sequence_idx != chunk_seq {
                                    if let Some(other_sl) = other_c.slices.iter().find(|sl| sl.slice_idx == slice_idx) {
                                        surviving.push(other_sl.clone());
                                    }
                                }
                            }

                            let expected_left_arm = target_chunk.and_then(|c| Self::derive_expected_left_arm(c, slice_idx));
                            let expected_right_arm = target_chunk.and_then(|c| Self::derive_expected_right_arm(c, slice_idx));

                            let guide = SliceGuideRNA {
                                file_id: *file_id,
                                target_chunk_id: target_chunk.map(|c| c.chunk_id).unwrap_or([0u8; 32]),
                                chunk_id: target_chunk.map(|c| c.chunk_id).unwrap_or([0u8; 32]),
                                sequence_idx: chunk_seq,
                                slice_idx,
                                cohort_id: spacer.cohort_id,
                                cohort_idx: ((chunk_seq as usize) % cohort_size) as u16,
                                expected_slice_hash,
                                expected_left_arm,
                                expected_right_arm,
                                slice_len: slice_size,
                            };

                            let p_h = spacer.horizontal_parity_slices.get(slice_idx as usize);
                            let repaired = p_h.and_then(|p| CasRepairEngine::repair_slice(&guide, &surviving, p).ok());

                            if let Some(repaired_slice) = repaired {
                                if offset_in_slice < repaired_slice.data.len() {
                                    let available = (repaired_slice.data.len() - offset_in_slice).min(to_read);
                                    result.extend_from_slice(&repaired_slice.data[offset_in_slice..offset_in_slice + available]);
                                    cur += available;
                                    continue;
                                }
                            }
                        }
                        // Fallback to inmemory buffer if unrecoverable
                        if cur < buffer.len() {
                            let available = (buffer.len() - cur).min(to_read);
                            result.extend_from_slice(&buffer[cur..cur + available]);
                            cur += available;
                        } else {
                            break;
                        }
                    }
                }
                reply.data(&result);
            }
            _ => {
                reply.error(libc::EISDIR);
            }
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut inner = self.inner.write().unwrap();
        // Check if file already exists in parent
        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&parent)
        {
            if entries.contains_key(&name_str) {
                reply.error(EEXIST);
                return;
            }
        } else {
            reply.error(ENOENT);
            return;
        }

        let ino = inner.next_ino;
        inner.next_ino += 1;

        let now = SystemTime::now();
        let mut file_id = [0u8; 16];
        file_id[..8].copy_from_slice(&ino.to_le_bytes());

        let new_file = InodeRecord {
            ino,
            kind: InodeKind::File {
                buffer: Vec::new(),
                strand: None,
                file_id,
            },
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            perm: (mode & 0o777) as u16,
            nlink: 1,
            uid: 1000,
            gid: 1000,
            version: 0,
        };

        let attr = new_file.to_file_attr();
        inner.inodes.insert(ino, new_file);
        inner.names.insert(ino, name_str.clone());

        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get_mut(&parent)
        {
            entries.insert(name_str, ino);
        }

        reply.created(&TTL, &attr, 0, 0, 0);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        if offset < 0 {
            reply.error(libc::EINVAL);
            return;
        }
        let write_offset = offset as usize;
        let write_len = data.len();
        let write_end = write_offset + write_len;

        let mut inner = self.inner.write().unwrap();
        let chunk_size = self.chunk_size;
        let cohort_size = self.cohort_size;
        let _name_opt = inner.names.get(&ino).cloned();

        let record = match inner.inodes.get_mut(&ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut mutations_to_emit = Vec::new();

        match &mut record.kind {
            InodeKind::File {
                buffer,
                strand,
                file_id,
            } => {
                if write_end > buffer.len() {
                    buffer.resize(write_end, 0);
                }
                buffer[write_offset..write_end].copy_from_slice(data);
                record.size = buffer.len() as u64;
                record.mtime = SystemTime::now();

                if strand.is_none() {
                    if let Ok(new_strand) =
                        StrandEncoder::encode_file(*file_id, buffer, chunk_size, cohort_size)
                    {
                        for chunk in &new_strand.chunks {
                            let total_slices = chunk.slices.len() as u16;
                            for sl in &chunk.slices {
                                mutations_to_emit.push(FsMutation::WriteSlice {
                                    version: 0,
                                    file_id: *file_id,
                                    chunk_sequence_idx: chunk.sequence_idx,
                                    slice_idx: sl.slice_idx,
                                    slice: BiologicalSliceWire {
                                        slice_id: sl.slice_hash,
                                        chunk_id: sl.chunk_id,
                                        file_id: sl.file_id,
                                        chunk_sequence_idx: sl.sequence_idx,
                                        slice_idx: sl.slice_idx,
                                        total_slices,
                                        kind: SliceParityKind::Data,
                                        left_arm: sl.left_arm,
                                        right_arm: sl.right_arm,
                                        cohort_id: sl.cohort_id,
                                        data: sl.data.clone(),
                                    },
                                });
                            }
                        }
                        *strand = Some(new_strand);
                    }
                } else if let Some(s) = strand {
                    let slice_size = s.chunks.first().map(|c| c.slice_size).unwrap_or(DEFAULT_SLICE_SIZE);
                    let num_chunks = (write_end + chunk_size - 1) / chunk_size;
                    if s.chunks.len() < num_chunks {
                        if let Ok(full_strand) =
                            StrandEncoder::encode_file(*file_id, buffer, chunk_size, cohort_size)
                        {
                            for chunk in &full_strand.chunks {
                                let total_slices = chunk.slices.len() as u16;
                                let old_chunk = s.chunks.iter().find(|c| c.sequence_idx == chunk.sequence_idx);
                                for sl in &chunk.slices {
                                    let is_new_or_modified = match old_chunk {
                                        None => true, // Entirely new chunk
                                        Some(oc) => match oc.slices.iter().find(|old_sl| old_sl.slice_idx == sl.slice_idx) {
                                            None => true, // New slice allocated inside existing chunk
                                            Some(old_sl) => old_sl.slice_hash != sl.slice_hash, // Mutated slice
                                        },
                                    };

                                    if is_new_or_modified {
                                        mutations_to_emit.push(FsMutation::WriteSlice {
                                            version: 0,
                                            file_id: *file_id,
                                            chunk_sequence_idx: chunk.sequence_idx,
                                            slice_idx: sl.slice_idx,
                                            slice: BiologicalSliceWire {
                                                slice_id: sl.slice_hash,
                                                chunk_id: sl.chunk_id,
                                                file_id: sl.file_id,
                                                chunk_sequence_idx: sl.sequence_idx,
                                                slice_idx: sl.slice_idx,
                                                total_slices,
                                                kind: SliceParityKind::Data,
                                                left_arm: sl.left_arm,
                                                right_arm: sl.right_arm,
                                                cohort_id: sl.cohort_id,
                                                data: sl.data.clone(),
                                            },
                                        });
                                    }
                                }
                            }
                            *s = full_strand;
                        }
                    } else {
                        let mut bytes_written = 0;
                        while bytes_written < write_len {
                            let cur = write_offset + bytes_written;
                            let chunk_seq = (cur / chunk_size) as u32;
                            let offset_in_chunk = cur % chunk_size;
                            let slice_idx = (offset_in_chunk / slice_size) as u16;
                            let offset_in_slice = offset_in_chunk % slice_size;
                            let slice_rem = slice_size - offset_in_slice;
                            let write_rem = write_len - bytes_written;
                            let step = slice_rem.min(write_rem);
                            let new_slice_data = &data[bytes_written..bytes_written + step];

                            if let Some(chunk) = s.chunks.iter_mut().find(|c| c.sequence_idx == chunk_seq) {
                                let slice_exists = chunk.slices.iter().any(|sl| sl.slice_idx == slice_idx);
                                if slice_exists {
                                    let (delta_res, updated_hash, slice_data_len) = {
                                        let slice = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx).unwrap();
                                        let res = SliceDeltaJournal::create_delta(
                                            slice,
                                            new_slice_data,
                                            offset_in_slice,
                                            chunk.cohort_id,
                                            chunk.cohort_idx,
                                        );
                                        if let Ok(ref delta) = res {
                                            let _ = SliceDeltaJournal::apply_delta_to_slice(slice, delta);
                                            (res, slice.slice_hash, slice.data.len())
                                        } else {
                                            (res, [0u8; 32], 0)
                                        }
                                    };

                                    if let Ok(delta) = delta_res {
                                        let slice_offset_in_chunk = (slice_idx as usize) * slice_size;
                                        let slice_data_end = slice_offset_in_chunk + slice_data_len;
                                        if chunk.data.len() < slice_data_end {
                                            chunk.data.resize(slice_data_end, 0);
                                        }
                                        if let Some(sl) = chunk.slices.iter().find(|sl| sl.slice_idx == slice_idx) {
                                            chunk.data[slice_offset_in_chunk..slice_data_end].copy_from_slice(&sl.data);
                                        }
                                        chunk.chunk_id = hash_sha256(&chunk.data);

                                        
                                        if slice_idx > 0 {
                                            if let Some(prev) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx - 1) {
                                                prev.right_arm = Some(updated_hash);
                                            }
                                        }
                                        if let Some(next) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx + 1) {
                                            next.left_arm = Some(updated_hash);
                                        }

                                        if let Some(spacer) = s.spacers.iter_mut().find(|sp| sp.cohort_id == chunk.cohort_id) {
                                            if (slice_idx as usize) < spacer.horizontal_parity_slices.len() {
                                                let _ = SliceDeltaJournal::apply_delta_to_horizontal_buffer(
                                                    &mut spacer.horizontal_parity_slices[slice_idx as usize],
                                                    &delta,
                                                );
                                            }
                                            if !spacer.diagonal_parity_slices.is_empty() {
                                                let _ = SliceDeltaJournal::apply_delta_to_diagonal_buffers(
                                                    &mut spacer.diagonal_parity_slices,
                                                    &delta,
                                                );
                                            }
                                        }
                                        mutations_to_emit.push(FsMutation::SliceDeltaJournal { version: 0, delta });
                                    }
                                } else {
                                    let global_slice_start = (chunk_seq as usize) * chunk_size + (slice_idx as usize) * slice_size;
                                    let global_slice_end = (global_slice_start + slice_size).min(buffer.len());
                                    let slice_data = if global_slice_start < buffer.len() {
                                        buffer[global_slice_start..global_slice_end].to_vec()
                                    } else {
                                        new_slice_data.to_vec()
                                    };
                                    let slice_hash = hash_sha256(&slice_data);
                                    let left_arm = if slice_idx > 0 {
                                        chunk.slices.iter().find(|sl| sl.slice_idx == slice_idx - 1).map(|sl| sl.slice_hash)
                                    } else {
                                        None
                                    };
                                    if slice_idx > 0 {
                                        if let Some(prev) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx - 1) {
                                            prev.right_arm = Some(slice_hash);
                                        }
                                    }
                                    let right_arm = chunk.slices.iter().find(|sl| sl.slice_idx == slice_idx + 1).map(|sl| sl.slice_hash);
                                    if let Some(next) = chunk.slices.iter_mut().find(|sl| sl.slice_idx == slice_idx + 1) {
                                        next.left_arm = Some(slice_hash);
                                    }

                                    let new_slice = BiologicalSlice {
                                        slice_idx,
                                        chunk_id: chunk.chunk_id,
                                        file_id: *file_id,
                                        sequence_idx: chunk_seq,
                                        offset: (slice_idx as usize) * slice_size,
                                        data: slice_data,
                                        slice_hash,
                                        left_arm,
                                        right_arm,
                                        cohort_id: chunk.cohort_id,
                                    };

                                    chunk.slices.push(new_slice.clone());
                                    chunk.slices.sort_by_key(|sl| sl.slice_idx);

                                    let slice_offset_in_chunk = (slice_idx as usize) * slice_size;
                                    let slice_data_end = slice_offset_in_chunk + new_slice.data.len();
                                    if chunk.data.len() < slice_data_end {
                                        chunk.data.resize(slice_data_end, 0);
                                    }
                                    chunk.data[slice_offset_in_chunk..slice_data_end].copy_from_slice(&new_slice.data);
                                    chunk.chunk_id = hash_sha256(&chunk.data);
                                    for sl in &mut chunk.slices {
                                        sl.chunk_id = chunk.chunk_id;
                                    }

                                    let total_slices = chunk.slices.len() as u16;

                                    mutations_to_emit.push(FsMutation::WriteSlice {
                                        version: 0,
                                        file_id: *file_id,
                                        chunk_sequence_idx: chunk.sequence_idx,
                                        slice_idx: new_slice.slice_idx,
                                        slice: BiologicalSliceWire {
                                            slice_id: new_slice.slice_hash,
                                            chunk_id: chunk.chunk_id,
                                            file_id: *file_id,
                                            chunk_sequence_idx: chunk.sequence_idx,
                                            slice_idx: new_slice.slice_idx,
                                            total_slices,
                                            kind: SliceParityKind::Data,
                                            left_arm: new_slice.left_arm,
                                            right_arm: new_slice.right_arm,
                                            cohort_id: chunk.cohort_id,
                                            data: new_slice.data,
                                        },
                                    });
                                }
                            }
                            bytes_written += step;
                        }

                        // Resynchronize CohortSpacer parity slices with exact StrandEncoder formulas
                        for spacer in &mut s.spacers {
                            let cohort_chunks: Vec<BiologicalChunk> = s.chunks
                                .iter()
                                .filter(|c| c.cohort_id == spacer.cohort_id)
                                .cloned()
                                .collect();
                            let max_slices = cohort_chunks.iter().map(|c| c.slices.len()).max().unwrap_or(0);
                            if max_slices > 0 {
                                spacer.horizontal_parity_slices =
                                    StrandEncoder::compute_horizontal_parity_slices(&cohort_chunks, max_slices);
                                spacer.diagonal_parity_slices =
                                    StrandEncoder::compute_diagonal_parity_slices(&cohort_chunks, max_slices);
                            }
                        }
                        let num_c = s.chunks.len();
                        for i in 0..num_c {
                            s.chunks[i].left_arm = if i > 0 { Some(s.chunks[i - 1].chunk_id) } else { None };
                            s.chunks[i].right_arm = if i + 1 < num_c { Some(s.chunks[i + 1].chunk_id) } else { None };
                        }
                    }
                }

                reply.written(data.len() as u32);
            }
            _ => {
                reply.error(libc::EISDIR);
            }
        }

        drop(inner);

        if let Some(ref tx) = self.mutation_tx {
            for mutation in mutations_to_emit {
                let _ = tx.send(mutation);
            }
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        let (name_opt, buf_opt) = {
            let inner = self.inner.read().unwrap();
            let name = inner.names.get(&ino).cloned();
            let buf = match inner.inodes.get(&ino) {
                Some(InodeRecord {
                    kind: InodeKind::File { buffer, .. },
                    ..
                }) => Some(buffer.clone()),
                _ => None,
            };
            (name, buf)
        };
        if let (Some(name), Some(data)) = (name_opt, buf_opt) {
            if let Some(ref tx) = self.mutation_tx {
                let _ = tx.send(FsMutation::WriteFile { version: 0, name, data });
            }
        }
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = match name.to_str() {
            Some(s) => s.to_string(),
            None => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let mut inner = self.inner.write().unwrap();
        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get(&parent)
        {
            if entries.contains_key(&name_str) {
                reply.error(EEXIST);
                return;
            }
        } else {
            reply.error(ENOENT);
            return;
        }

        let ino = inner.next_ino;
        inner.next_ino += 1;

        let now = SystemTime::now();
        let mut dir_entries = HashMap::new();
        dir_entries.insert(".".to_string(), ino);
        dir_entries.insert("..".to_string(), parent);

        let new_dir = InodeRecord {
            ino,
            kind: InodeKind::Directory {
                entries: dir_entries,
            },
            size: 0,
            atime: now,
            mtime: now,
            ctime: now,
            perm: (mode & 0o777) as u16,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            version: 0,
        };

        let attr = new_dir.to_file_attr();
        inner.inodes.insert(ino, new_dir);

        if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get_mut(&parent)
        {
            entries.insert(name_str, ino);
        }

        reply.entry(&TTL, &attr, 0);
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let name_str = match name.to_str() {
            Some(s) => s,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut inner = self.inner.write().unwrap();
        let removed_ino = if let Some(InodeRecord {
            kind: InodeKind::Directory { entries },
            ..
        }) = inner.inodes.get_mut(&parent)
        {
            entries.remove(name_str)
        } else {
            None
        };

        if let Some(ino) = removed_ino {
            inner.inodes.remove(&ino);
            inner.names.remove(&ino);
            drop(inner);
            if let Some(ref tx) = self.mutation_tx {
                let _ = tx.send(FsMutation::DeleteFile {
                    version: 0,
                    name: name_str.to_string(),
                });
            }
            reply.ok();
        } else {
            reply.error(ENOENT);
        }
    }
}

pub fn mount_fs<P: AsRef<Path>>(
    mountpoint: P,
    fs: NifsiFS,
) -> Result<fuser::BackgroundSession, std::io::Error> {
    let mut options = vec![
        MountOption::RW,
        MountOption::FSName("nifsi".to_string()),
    ];
    if unsafe { libc::geteuid() == 0 } {
        options.push(MountOption::AutoUnmount);
        options.push(MountOption::AllowOther);
    }
    fuser::spawn_mount2(fs, mountpoint.as_ref(), &options)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nifsifs_write_and_hadr_strand_generation() {
        let fs = NifsiFS::new(16, 4); // 16 byte chunks, 4 per cohort
        let test_data = b"Hello, Nifsi Distributed Filesystem! Self healing strands in action.";

        let ino = fs.write_file_bytes("test.txt", test_data).unwrap();
        let read_back = fs.read_file_bytes(ino).unwrap();
        assert_eq!(read_back, test_data);
        let strand = fs.get_strand_by_path("test.txt").expect("Strand must exist");
        assert!(
            strand.chunks.len() >= 4,
            "Must have created multiple chunks"
        );
        assert!(!strand.spacers.is_empty(), "Must have cohort spacers");
        for (i, chunk) in strand.chunks.iter().enumerate() {
            if i == 0 {
                assert!(chunk.left_arm.is_none());
                assert!(chunk.right_arm.is_some());
            } else if i == strand.chunks.len() - 1 {
                assert!(chunk.left_arm.is_some());
                assert!(chunk.right_arm.is_none());
            } else {
                assert!(chunk.left_arm.is_some());
                assert!(chunk.right_arm.is_some());
            }
        }
    }
}

