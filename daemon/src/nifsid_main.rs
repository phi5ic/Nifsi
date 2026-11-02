use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};
use tracing_subscriber::EnvFilter;

use nifsi_core::protocol::{
    BiologicalSliceWire, ClusterNodeInfo, ClusterStatusResponse, FileInspectionResponse,
    FsMutation, IpcRequest, IpcResponse, SliceParityKind, SliceTransferRequest,
    SliceTransferResponse, DEFAULT_SOCKET_PATH,
};
use nifsi_fs::{hadr::hash_sha256, mount_fs, BackgroundSession, NifsiFS};
use nifsi_mesh::{start_mesh, FsMutationHandler, NodeHealthGossip, SwarmConfig, TcpMeshNetwork};
use nifsi_raft::{
    log::{LogStore, SQLiteLogStore},
    network::RaftNetwork,
    node::RaftNode,
    state::{HardStateStore, NoopMetadataStore, SqliteHardStateStore},
};

//Cli arg

fn parse_peer(s: &str) -> Result<(String, SocketAddr), String> {
    let (id, addr) = s
        .split_once('=')
        .ok_or_else(|| format!("peer must be in ID=HOST:PORT form, got '{s}'"))?;
    let addr: SocketAddr = addr
        .parse()
        .map_err(|e| format!("invalid address for peer '{id}': {e}"))?;
    Ok((id.to_owned(), addr))
}

#[derive(Parser, Debug)]
#[command(name = "nifsid", about = "Nifsi distributed OS-layer daemon")]
struct Args {
    //unique identifier
    #[arg(long)]
    id: String,

    //Adress for the node to listen on
    #[arg(long, default_value = "0.0.0.0:7331")]
    bind: SocketAddr,

    //Directory for storing durable state
    #[arg(long, default_value = "/var/lib/nifsi")]
    data_dir: PathBuf,

    //Peer nodes in 'ID=HOST:PORT' form. Repeat for each peer.
    #[arg(long, value_parser = parse_peer, number_of_values = 1)]
    peer: Vec<(String, SocketAddr)>,

    //How often in secs this node broadcasts its health to other nodes via Boids swarp gossip
    #[arg(long, default_value_t = 2)]
    gossip_interval_secs: u64,

    //Path to Unix domain socket for cli
    #[arg(long, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    //Optional path to mount filesystem
    #[arg(long)]
    mount: Option<PathBuf>,
}

struct DaemonState {
    fs: NifsiFS,
    active_mount: Mutex<Option<(String, BackgroundSession)>>,
    active_chunks: AtomicUsize,
    detected_breaks: AtomicUsize,
    repaired_chunks: AtomicUsize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //Logging - defaults to 'info'
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("NIFSI_LOG").unwrap_or_else(|_| EnvFilter::new("info")),    
        )
        .init();
    let args = Args::parse();

    info!(
        node_id = %args.id,
        bind    = %args.bind,
        peers   = args.peer.len(),
        data_dir= %args.data_dir.display(),
        socket  = %args.socket.display(),
        "Starting nifsid"
    );

    // Durable Storage

    std::fs::create_dir_all(&args.data_dir)?;

    let log_path = args.data_dir.join("raft_log.sqlite");
    let hard_state_path = args.data_dir.join("hard_state.sqlite");

    let log_store = Arc::new(SQLiteLogStore::new(&log_path)?);
    let log_store_replay = log_store.clone();
    let hard_state = Arc::new(
        SqliteHardStateStore::new(&hard_state_path)
            .map_err(|e| Box::<dyn std::error::Error>::from(e.to_string()))?,
    );
    let metadata = Arc::new(NoopMetadataStore);
    info!(
        log_db = %log_path.display(),
        hs_db = %hard_state_path.display(),
        "Storage opened"
    );
    
    let peer_ids: Vec<String> = args.peer.iter().map(|(id, _)| id.clone()).collect();
    let peer_addrs: HashMap<String, SocketAddr> = args.peer.clone().into_iter().collect();

    let network_bare: Arc<dyn RaftNetwork> = Arc::new(TcpMeshNetwork::new(peer_addrs.clone()));

    let handle = RaftNode::spawn(
        args.id.clone(),
        peer_ids.clone(),
        network_bare,
        log_store,
        hard_state.clone(),
        metadata,
    );

    info!("Raft actor spawned - election will start within 150-300 ms");

    //HADR Storage&filesystem initialization
    
    let (mutation_tx, mut mutation_rx) = tokio::sync::mpsc::unbounded_channel::<FsMutation>();
    let nifsi_fs = NifsiFS::new_with_channel(64 * 1024, 4, Some(mutation_tx));
    {
        let last_committed = {
            let (idx, _) = log_store_replay.last_index_and_term().await.unwrap_or((0, 0));
            idx
        };
        if last_committed > 0 {
            info!(
                entries = last_committed,
                "Replaying committed Raft log to restore FS state"
            );
            for log_idx in 1..=last_committed {
                if let Ok(Some(entry)) = log_store_replay.get_entry(log_idx).await {
                    if let Ok(mutation) = bincode::deserialize::<FsMutation>(&entry.data) {
                        match &mutation {
                            FsMutation::WriteFile { name, data, .. } => {
                                let _ = nifsi_fs.apply_remote_write(name, data, log_idx);
                            }
                            FsMutation::DeleteFile { name, .. } => {
                                let _ = nifsi_fs.apply_remote_delete(name, log_idx);
                            }
                            FsMutation::WriteSlice {
                                file_id,
                                chunk_sequence_idx,
                                slice_idx,
                                slice,
                                ..
                            } => {
                                let _ = nifsi_fs.apply_remote_slice(
                                    *file_id,
                                    *chunk_sequence_idx,
                                    *slice_idx,
                                    slice,
                                    log_idx,
                                );
                            }
                            FsMutation::SliceDeltaJournal { delta, .. } => {
                                let _ = nifsi_fs.apply_remote_slice_delta(delta, log_idx);
                            }
                        }
                    }
                }
            }
            info!("FS state restored from Raft log ({} entries)", last_committed);
        }
    }

    let daemon_state = Arc::new(DaemonState {
        fs: nifsi_fs.clone(),
        active_mount: Mutex::new(None),
        active_chunks: AtomicUsize::new(0),
        detected_breaks: AtomicUsize::new(0),
        repaired_chunks: AtomicUsize::new(0),
    });

    let fs_replica = nifsi_fs.clone();
    let on_mutation: FsMutationHandler = Arc::new(move |mut_event| {
        match mut_event {
            FsMutation::WriteSlice {
                file_id,
                chunk_sequence_idx,
                slice_idx,
                slice,
                version,
            } => {
                info!(
                    file_id = ?file_id,
                    chunk = chunk_sequence_idx,
                    slice = slice_idx,
                    bytes = slice.data.len(),
                    version,
                    "Applied replicated sub-packet slice write from peer"
                );
                let _ = fs_replica.apply_remote_slice(file_id, chunk_sequence_idx, slice_idx, &slice, version);
            }
            FsMutation::SliceDeltaJournal { delta, version } => {
                info!(
                    file_id = ?delta.file_id,
                    chunk = delta.sequence_idx,
                    slice = delta.slice_idx,
                    bytes = delta.delta_bytes.len(),
                    version,
                    "Applied replicated slice delta journal from peer"
                );
                let _ = fs_replica.apply_remote_slice_delta(&delta, version);
            }
            FsMutation::WriteFile { name, data, version } => {
                info!(file = %name, bytes = data.len(), version, "Applied replicated file from peer");
                let _ = fs_replica.apply_remote_write(&name, &data, version);
            }
            FsMutation::DeleteFile { name, version } => {
                info!(file = %name, version, "Applied replicated file deletion from peer");
                let _ = fs_replica.apply_remote_delete(&name, version);
            }
        }
    });

    // mesh listener and swarm router

    let (network, swarm) = start_mesh(
        args.bind,
        peer_addrs.clone(),
        handle.clone(),
        SwarmConfig::default(),
        Some(on_mutation),
    )
    .await?;

    info!(bind = %args.bind, "Mesh listener started (Raft + swarm gossip + fs sync)");

    let fs_for_slices = nifsi_fs.clone();
    network
        .set_slice_handler(Arc::new(move |req: SliceTransferRequest| {
            let mut found_slice: Option<BiologicalSliceWire> = None;

            let file_id_provided = req.file_id != [0u8; 16];
            let chunk_id_provided = req.chunk_id != [0u8; 32];
            if file_id_provided || chunk_id_provided {
                for (name, _) in fs_for_slices.list_files() {
                    if let Some(strand) = fs_for_slices.get_strand_by_path(&name) {
                        if file_id_provided && strand.file_id != req.file_id {
                            continue;
                        }
                        if !file_id_provided {
                            let has_chunk = strand.chunks.iter().any(|c| c.chunk_id == req.chunk_id);
                            let has_spacer = strand.spacers.iter().any(|sp| sp.member_chunk_ids.contains(&req.chunk_id));
                            if !has_chunk && !has_spacer {
                                continue;
                            }
                        }

                        match req.kind {
                            SliceParityKind::Data => {
                                let chunk_opt = if chunk_id_provided {
                                    strand.chunks.iter().find(|c| c.chunk_id == req.chunk_id)
                                } else {
                                    strand.chunks.iter().find(|c| c.sequence_idx == req.chunk_sequence_idx)
                                };

                                if let Some(chunk) = chunk_opt {
                                    if let Some(sl) = chunk.slices.iter().find(|s| s.slice_idx == req.slice_idx) {
                                        found_slice = Some(BiologicalSliceWire {
                                            slice_id: sl.slice_hash,
                                            chunk_id: sl.chunk_id,
                                            file_id: sl.file_id,
                                            chunk_sequence_idx: sl.sequence_idx,
                                            slice_idx: sl.slice_idx,
                                            total_slices: chunk.slices.len() as u16,
                                            kind: SliceParityKind::Data,
                                            left_arm: sl.left_arm,
                                            right_arm: sl.right_arm,
                                            cohort_id: sl.cohort_id,
                                            data: sl.data.clone(),
                                        });
                                        break;
                                    }
                                }
                            }
                            SliceParityKind::HorizontalParity => {
                                let spacer_opt = if chunk_id_provided {
                                    strand.spacers.iter().find(|sp| sp.member_chunk_ids.contains(&req.chunk_id))
                                } else {
                                    strand
                                        .chunks
                                        .iter()
                                        .find(|c| c.sequence_idx == req.chunk_sequence_idx)
                                        .and_then(|c| {
                                            strand.spacers.iter().find(|sp| sp.cohort_id == c.cohort_id)
                                        })
                                };

                                if let Some(spacer) = spacer_opt {
                                    if let Some(data) =
                                        spacer.horizontal_parity_slices.get(req.slice_idx as usize)
                                    {
                                        found_slice = Some(BiologicalSliceWire {
                                            slice_id: hash_sha256(data),
                                            chunk_id: req.chunk_id,
                                            file_id: strand.file_id,
                                            chunk_sequence_idx: req.chunk_sequence_idx,
                                            slice_idx: req.slice_idx,
                                            total_slices: spacer.horizontal_parity_slices.len() as u16,
                                            kind: SliceParityKind::HorizontalParity,
                                            left_arm: None,
                                            right_arm: None,
                                            cohort_id: spacer.cohort_id,
                                            data: data.clone(),
                                        });
                                        break;
                                    }
                                }
                            }
                            SliceParityKind::DiagonalParity => {
                                let spacer_opt = if chunk_id_provided {
                                    strand.spacers.iter().find(|sp| sp.member_chunk_ids.contains(&req.chunk_id))
                                } else {
                                    strand
                                        .chunks
                                        .iter()
                                        .find(|c| c.sequence_idx == req.chunk_sequence_idx)
                                        .and_then(|c| {
                                            strand.spacers.iter().find(|sp| sp.cohort_id == c.cohort_id)
                                        })
                                };

                                if let Some(spacer) = spacer_opt {
                                    if let Some(data) =
                                        spacer.diagonal_parity_slices.get(req.slice_idx as usize)
                                    {
                                        found_slice = Some(BiologicalSliceWire {
                                            slice_id: hash_sha256(data),
                                            chunk_id: req.chunk_id,
                                            file_id: strand.file_id,
                                            chunk_sequence_idx: req.chunk_sequence_idx,
                                            slice_idx: req.slice_idx,
                                            total_slices: spacer.diagonal_parity_slices.len() as u16,
                                            kind: SliceParityKind::DiagonalParity,
                                            left_arm: None,
                                            right_arm: None,
                                            cohort_id: spacer.cohort_id,
                                            data: data.clone(),
                                        });
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if let Some(wire) = found_slice {
                SliceTransferResponse {
                    file_id: req.file_id,
                    chunk_id: req.chunk_id,
                    chunk_sequence_idx: req.chunk_sequence_idx,
                    slice_idx: req.slice_idx,
                    kind: req.kind,
                    slice: Some(wire),
                    error: None,
                }
            } else {
                SliceTransferResponse {
                    file_id: req.file_id,
                    chunk_id: req.chunk_id,
                    chunk_sequence_idx: req.chunk_sequence_idx,
                    slice_idx: req.slice_idx,
                    kind: req.kind,
                    slice: None,
                    error: Some(format!(
                        "Slice {} (kind: {:?}) for chunk seq {} not found on node",
                        req.slice_idx, req.kind, req.chunk_sequence_idx
                    )),
                }
            }
        }))
        .await;
    let network_broadcast = Arc::new(network.clone());
    let raft_handle_for_mut = handle.clone();
    tokio::spawn(async move {
        while let Some(mut mutation) = mutation_rx.recv().await {
            // Encode the mutation for the Raft log.
            if let Ok(encoded) = bincode::serialize(&mutation) {
                match raft_handle_for_mut.propose(encoded).await {
                    Ok(log_index) => {
                        // Wait for the majority to commit this entry (poll with timeout).
                        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                        loop {
                            if raft_handle_for_mut.commit_index() >= log_index {
                                break;
                            }
                            if tokio::time::Instant::now() >= deadline {
                                warn!(
                                    log_index,
                                    "Timed out waiting for Raft commit; broadcasting anyway"
                                );
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                        // Stamp the committed log index as the version.
                        let committed_version = log_index;
                        match &mut mutation {
                            FsMutation::WriteFile { version, .. } => *version = committed_version,
                            FsMutation::DeleteFile { version, .. } => *version = committed_version,
                            FsMutation::WriteSlice { version, .. } => *version = committed_version,
                            FsMutation::SliceDeltaJournal { version, .. } => *version = committed_version,
                        }
                        debug!(log_index, "Mutation committed by Raft quorum, broadcasting");
                    }
                    Err(e) => {
                        // Not the leader or channel closed — broadcast without Raft stamp.
                        debug!("Raft propose skipped ({}), falling back to direct broadcast", e);
                    }
                }
            }
            network_broadcast.broadcast_fs_mutation(&mutation).await;
        }
    });

    // Periodic sync of existing files across the mesh
    let fs_periodic = nifsi_fs.clone();
    let network_periodic = Arc::new(network.clone());
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(3));
        loop {
            ticker.tick().await;
            for (name, data) in fs_periodic.list_files() {
                // version 0 signals "legacy/re-sync" — recipients apply only if their
                // stored version is 0 (i.e., the file was never committed via Raft on
                // that node), so we don't overwrite newer committed data.
                network_periodic.broadcast_fs_mutation(&FsMutation::WriteFile { version: 0, name, data }).await;
            }
        }
    });

    // Auto-mount if requested via CLI flag
    if let Some(mountpoint) = &args.mount {
        let mp_str = mountpoint.display().to_string();
        std::fs::create_dir_all(mountpoint)?;
        match mount_fs(mountpoint, nifsi_fs.clone()) {
            Ok(session) => {
                info!(mountpoint = %mp_str, "FUSE filesystem mounted successfully");
                *daemon_state.active_mount.lock().await = Some((mp_str, session));
            }
            Err(e) => {
                error!(error = %e, "Failed to mount FUSE filesystem at startup");
            }
        }
    }

    //Gossip broadcast loop

    let node_id_gossip = args.id.clone();
    let network_gossip = network.clone();
    let gossip_k = SwarmConfig::default().epidemic_k;
    let gossip_interval = Duration::from_secs(args.gossip_interval_secs);
    tokio::spawn(async move {
        let mut ticker = interval(gossip_interval);
        let mut epoch: u64 = 0;
        loop {
            ticker.tick().await;
            epoch += 1;
            let gossip = NodeHealthGossip {
                node_id: node_id_gossip.clone(),
                rtt_ms: 0.0,
                disk_usage: 0.0,
                queue_depth: 0,
                epoch,
                forwarded: false,
            };
            network_gossip.epidemic_gossip(&gossip, gossip_k).await;
        }
    });

    //local unix socket ipc sever for cli contrl
    if args.socket.exists() {
        let _ = std::fs::remove_file(&args.socket);

    }
    if let Some(parent) = args.socket.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let unix_listener = UnixListener::bind(&args.socket)?;
    info!(socket = %args.socket.display(), "CLI IPC socket listening");

    let socket_path_cleanup = args.socket.clone();
    let node_id = args.id.clone();
    let hard_state_ipc = hard_state.clone();
    let swarm_ipc = swarm.clone();
    let state_ipc = daemon_state.clone();
    let peer_addrs_ipc = peer_addrs.clone();

    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match unix_listener.accept().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("IPC accept error: {}", e);
                    continue;

                }
            };

            let node_id = node_id.clone();
            let hard_state = hard_state_ipc.clone();
            let swarm = swarm_ipc.clone();
            let state = state_ipc.clone();
            let peer_addrs = peer_addrs_ipc.clone();

            tokio::spawn(async move {
                let mut len_buf = [0u8; 4];
                if stream.read_exact(&mut len_buf).await.is_err() {
                    return;
                }
                let len = u32::from_le_bytes(len_buf) as usize;
                let mut req_buf = vec![0u8; len];
                if stream.read_exact(&mut req_buf).await.is_err() {
                    return;
                }

                let req: IpcRequest = match bincode::deserialize(&req_buf) {
                    Ok(r) => r,
                    Err(e) => {
                        let resp = IpcResponse::Error(format!("Malformed request: {e}"));
                        let resp_bytes = bincode::serialize(&resp).unwrap_or_default();
                        let resp_len = resp_bytes.len() as u32;
                        let _ = stream.write_all(&resp_len.to_le_bytes()).await;
                        let _ = stream.write_all(&resp_bytes).await;
                        return;
                    }
                };

                let response = match req {
                    IpcRequest::Status => {
                        let (term, voted_for) = hard_state.load().unwrap_or((0, None));
                        let live = swarm.live_peers();
                        let is_single = peer_addrs.is_empty();

                        let role = if is_single {
                            "Leader (Solo)".to_string()
                        } else if voted_for.as_deref() == Some(&node_id) {
                            "Leader/Candidate".to_string()
                        } else {
                            "Follower".to_string()
                        };

                        let leader_id = if is_single {
                            Some(node_id.clone())
                        } else {
                            voted_for
                        };

                        let mut peers_info = Vec::new();
                        for (p_id, p_addr) in &peer_addrs {
                            let gossip_info = live.iter().find(|g| &g.node_id == p_id);
                            peers_info.push(ClusterNodeInfo {
                                id: p_id.clone(),
                                address: p_addr.to_string(),
                                is_leader: leader_id.as_ref() == Some(p_id),
                                rtt_ms: gossip_info.map(|g| g.rtt_ms as f64).unwrap_or(0.0),
                                disk_usage: gossip_info.map(|g| g.disk_usage as f64).unwrap_or(0.0),
                            });
                        }

                        let mountpoint = state
                            .active_mount
                            .lock()
                            .await
                            .as_ref()
                            .map(|(mp, _)| mp.clone());
                        IpcResponse::Status(ClusterStatusResponse {
                            node_id: node_id.clone(),
                            term,
                            role,
                            leader_id,
                            peers: peers_info,
                            active_chunks: state.fs.chunk_count() + state.active_chunks.load(Ordering::Relaxed),
                            detected_breaks: state.detected_breaks.load(Ordering::Relaxed),
                            repaired_chunks: state.repaired_chunks.load(Ordering::Relaxed),
                            mountpoint,
                        })

                    }
                    IpcRequest::Mount {mountpoint} => {
                        let mut mount_lock = state.active_mount.lock().await;
                        if let Some((existing, _)) = mount_lock.as_ref() {
                            IpcResponse::Error(format!("Already mounted at {}", existing))
                        } else {
                            let p = PathBuf::from(&mountpoint);
                            let _ = std::fs::create_dir_all(&p);
                            match mount_fs(&p, state.fs.clone()) {
                                Ok(session) => {
                                    *mount_lock = Some((mountpoint.clone(), session));
                                    IpcResponse::Success(format!(
                                        "NifsiFS mounted successfully at {}",
                                        mountpoint
                                    ))
                                }
                                Err(e) => {
                                    IpcResponse::Error(format!("Failed to mount FUSE: {}", e))
                                }
                            }
                        }
                    }
                    IpcRequest::Join { address } => {
                        IpcResponse::Success(format!("Introduction request sent to {}", address))
                    }
                    IpcRequest::Leave => {
                        IpcResponse::Success("Leaving mesh....".to_string())
                    }
                    IpcRequest::Evict { node } => {
                        IpcResponse::Success(format!("Node {} evicted from the cluster", node))
                    }
                    IpcRequest::ListStrands => {
                        IpcResponse::Success("Strand inventory querried".to_string())
                    }
                    IpcRequest::InspectFile { name } => {
                        let clean_name = name.trim().trim_start_matches('/');
                        if let Some(resp) = state.fs.inspect_file_strand(clean_name) {
                            IpcResponse::FileInspection(resp)
                        } else if let Some(strand) = state.fs.get_strand_by_path(clean_name) {
                            let file_id_hex = strand
                                .file_id
                                .iter()
                                .map(|b| format!("{:02x}", b))
                                .collect::<String>();
                            let total_chunks = strand.chunks.len() as u32;
                            let cohorts = strand.spacers.len();
                            let mut healthy_slices = 0;
                            let mut degraded_slices = 0;
                            let mut total_slices = 0;

                            for chunk in &strand.chunks {
                                total_slices += chunk.slices.len();
                                for s in &chunk.slices {
                                    if hash_sha256(&s.data) == s.slice_hash {
                                        healthy_slices += 1;
                                    } else {
                                        degraded_slices += 1;
                                    }
                                }
                            }

                            IpcResponse::FileInspection(FileInspectionResponse {
                                file_name: clean_name.to_string(),
                                file_id: file_id_hex,
                                file_size: strand.original_file_size,
                                total_chunks,
                                total_slices,
                                cohorts,
                                healthy_slices,
                                degraded_slices,
                            })
                        } else {
                            IpcResponse::Error(format!("File '{}' not found", clean_name))
                        }
                    }
                    IpcRequest::ListSlices { file, chunk_idx } => {
                        let clean_name = file.trim().trim_start_matches('/');
                        match state.fs.list_chunk_slices(clean_name, chunk_idx) {
                            Ok(slices) => IpcResponse::SliceList(slices),
                            Err(e) => IpcResponse::Error(e),
                        }
                    }
                    IpcRequest::RepairSlice {
                        file,
                        chunk_idx,
                        slice_idx,
                    } => {
                        let clean_name = file.trim().trim_start_matches('/');
                        match state.fs.repair_file_slice(clean_name, chunk_idx, slice_idx) {
                            Ok(msg) => {
                                state.repaired_chunks.fetch_add(1, Ordering::Relaxed);
                                IpcResponse::Success(msg)
                            }
                            Err(e) => {
                                state.detected_breaks.fetch_add(1, Ordering::Relaxed);
                                IpcResponse::Error(e)
                            }
                        }
                    }
                };

                let resp_bytes = bincode::serialize(&response).unwrap_or_default();
                let resp_len = resp_bytes.len() as u32;
                let _ = stream.write_all(&resp_len.to_le_bytes()).await;
                let _ = stream.write_all(&resp_bytes).await;
            });
        }
    });

    info!("nifsid running, press <Ctrl+C> to stop");
    tokio::signal::ctrl_c().await?;
    info!("Shutting down nifsid");

    if socket_path_cleanup.exists() {
        let _ =std::fs::remove_file(socket_path_cleanup);
    }

    Ok(())

}
