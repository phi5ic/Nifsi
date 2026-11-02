use clap::{Parser, Subcommand};
use nifsi_core::protocol::{
    ClusterStatusResponse, IpcRequest, IpcResponse, DEFAULT_SOCKET_PATH,
};
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

#[derive(Parser)]
#[command(name = "nifsi")]
#[command(version)]
#[command(
    about = "Nifsi distributed mesh & storage CLI",
    long_about = "Command line interface for interacting with the local nifsid daemon."
)]
struct Cli {
    /// Path to nifsid UNIX domain socket
    #[arg(long, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Args, Clone, Debug)]
struct InspectArgs {
    /// Name of the file in the distributed filesystem
    file: String,
}

#[derive(clap::Args, Clone, Debug)]
struct ChunkArgs {
    /// Name of the file to inspect chunks for
    file: String,
}

#[derive(clap::Args, Clone, Debug)]
struct SliceQueryArgs {
    file: String,
    #[arg(short, long, default_value_t = 0)]
    chunk_idx: u32,
}

#[derive(clap::Args, Clone, Debug)]
struct RepairSliceArgs {
    file: String,
    #[arg(short, long)]
    chunk_idx: u32,
    #[arg(short, long)]
    slice_idx: u16,
}

#[derive(clap::Args, Clone, Debug)]
struct JoinArgs {
    /// Address of the introducer node (e.g. 192.168.1.10:7331)
    address: String,
}

#[derive(clap::Args, Clone, Debug)]
struct MountArgs {
    /// Local mountpoint path (e.g. /mnt/nifsi)
    mountpoint: String,
}

#[derive(clap::Args, Clone, Debug)]
struct EvictArgs {
    /// Node ID to evict
    node: String,
}

#[derive(Subcommand)]
enum Commands {
    Status,
    #[command(alias = "inspect-file")]
    Inspect(InspectArgs),
    #[command(alias = "chunks")]
    Chunk(ChunkArgs),
    #[command(alias = "slices")]
    SliceQuery(SliceQueryArgs),
    #[command(alias = "repair")]
    RepairSlice(RepairSliceArgs),
    Join(JoinArgs),
    Leave,
    Mount(MountArgs),
    Evict(EvictArgs),
}

async fn send_ipc(socket_path: &PathBuf, req: IpcRequest) -> Result<IpcResponse, String> {
    let mut stream = UnixStream::connect(socket_path).await.map_err(|e| {
        format!(
            "Failed to connect to nifsid socket at {}: {}. Is nifsid running?",
            socket_path.display(),
            e
        )
    })?;

    let req_bytes = bincode::serialize(&req).map_err(|e| e.to_string())?;
    let len = req_bytes.len() as u32;

    stream
        .write_all(&len.to_le_bytes())
        .await
        .map_err(|e| e.to_string())?;
    stream
        .write_all(&req_bytes)
        .await
        .map_err(|e| e.to_string())?;

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .await
        .map_err(|e| e.to_string())?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream
        .read_exact(&mut resp_buf)
        .await
        .map_err(|e| e.to_string())?;

    let response: IpcResponse =
        bincode::deserialize(&resp_buf).map_err(|e| format!("Invalid response: {e}"))?;
    Ok(response)
}

fn print_status(status: ClusterStatusResponse) {
    println!("============================================================");
    println!("                   NIFSI CLUSTER STATUS                     ");
    println!("============================================================");
    println!("Node ID:          {}", status.node_id);
    println!("Consensus Role:   {} (Term {})", status.role, status.term);
    println!(
        "Cluster Leader:   {}",
        status.leader_id.as_deref().unwrap_or("None (Electing)")
    );
    if let Some(mp) = status.mountpoint {
        println!("FUSE Mount:       {}", mp);
    } else {
        println!("FUSE Mount:       Unmounted");
    }
    println!("------------------------------------------------------------");
    println!("HADR Self-Healing Storage Engine:");
    println!("  Active Chunks:       {}", status.active_chunks);
    println!("  Detected Breaks:     {}", status.detected_breaks);
    println!("  Resurrected Chunks:  {}", status.repaired_chunks);
    println!("------------------------------------------------------------");
    println!("Mesh Peers ({} connected):", status.peers.len());
    if status.peers.is_empty() {
        println!("  (No external peers configured; running single-node)");
    } else {
        for peer in status.peers {
            let role_badge = if peer.is_leader { " [LEADER]" } else { "" };
            println!(
                "  - {:<12} {:<21} RTT: {:>5.1}ms  Disk: {:>4.1}%{}",
                peer.id, peer.address, peer.rtt_ms, peer.disk_usage, role_badge
            );
        }
    }
    println!("============================================================");
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Status => match send_ipc(&cli.socket, IpcRequest::Status).await {
            Ok(IpcResponse::Status(status)) => {
                print_status(status);
            }
            Ok(IpcResponse::Error(err)) => {
                eprintln!("Error from daemon: {}", err);
                std::process::exit(1);
            }
            Ok(_) => {
                eprintln!("Unexpected response from daemon");
            }
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        },
        Commands::Inspect(args) => match send_ipc(&cli.socket, IpcRequest::InspectFile { name: args.file.clone() }).await {
            Ok(IpcResponse::FileInspection(info)) => {
                println!("============================================================");
                println!("                   FILE STRAND INSPECTION                   ");
                println!("============================================================");
                println!("File Name:        {}", info.file_name);
                println!("File ID:          {}", info.file_id);
                println!("File Size:        {} bytes", info.file_size);
                println!("Total Chunks:     {}", info.total_chunks);
                println!("Total Slices:     {} (4 KB each)", info.total_slices);
                println!("Parity Cohorts:   {}", info.cohorts);
                println!("Healthy Slices:   {}", info.healthy_slices);
                println!("Degraded Slices:  {}", info.degraded_slices);
                if info.degraded_slices == 0 {
                    println!("Strand Integrity: \x1b[1;32m100% HEALTHY\x1b[0m");
                } else {
                    println!("Strand Integrity: \x1b[1;31mDEGRADED ({} breaks detected)\x1b[0m", info.degraded_slices);
                }
                println!("============================================================");
            }
            Ok(IpcResponse::Error(err)) => {
                eprintln!("Inspection error: {}", err);
                std::process::exit(1);
            }
            Ok(_) => eprintln!("Unexpected response from daemon"),
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        },
        Commands::Chunk(args) => match send_ipc(&cli.socket, IpcRequest::InspectFile { name: args.file.clone() }).await {
            Ok(IpcResponse::FileInspection(info)) => {
                println!("============================================================");
                println!("                   CHUNK TOPOLOGY BREAKDOWN                 ");
                println!("============================================================");
                println!("File:             {}", info.file_name);
                println!("Total Chunks:     {}", info.total_chunks);
                println!("Total Cohorts:    {}", info.cohorts);
                if info.total_chunks > 0 {
                    println!("Slices per Chunk: {}", info.total_slices / info.total_chunks as usize);
                }
                println!("Parity Engine:    HADR-v2 Dual Horizontal & Diagonal Array");
                println!("============================================================");
            }
            Ok(IpcResponse::Error(err)) => {
                eprintln!("Chunk query error: {}", err);
                std::process::exit(1);
            }
            Ok(_) => eprintln!("Unexpected response from daemon"),
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        },
        Commands::SliceQuery(args) => {
            match send_ipc(&cli.socket, IpcRequest::ListSlices { file: args.file.clone(), chunk_idx: args.chunk_idx }).await {
                Ok(IpcResponse::SliceList(slices)) => {
                    println!("========================================================================================================");
                    println!("SUB-PACKET SLICES (File: {}, Chunk #{}, Total: {} slices)", args.file, args.chunk_idx, slices.len());
                    println!("========================================================================================================");
                    println!("{:<4} | {:<34} | {:<16} | {:<8} | {:<12} | {:<12}", "Idx", "Slice ID", "Kind", "Size", "Left Arm", "Right Arm");
                    println!("-----+------------------------------------+------------------+----------+--------------+--------------");
                    for s in slices {
                        let left = s.left_arm.as_deref().map(|a| &a[..8.min(a.len())]).unwrap_or("HEAD");
                        let right = s.right_arm.as_deref().map(|a| &a[..8.min(a.len())]).unwrap_or("TAIL");
                        let id_short = &s.slice_id[..16.min(s.slice_id.len())];
                        println!("{:<4} | {:<34} | {:<16?} | {:<6} B | {:<12} | {:<12}", s.slice_idx, id_short, s.kind, s.size, left, right);
                    }
                    println!("========================================================================================================");
                }
                Ok(IpcResponse::Error(err)) => {
                    eprintln!("Slice query error: {}", err);
                    std::process::exit(1);
                }
                Ok(_) => eprintln!("Unexpected response from daemon"),
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        },
        Commands::RepairSlice(args) => {
            match send_ipc(&cli.socket, IpcRequest::RepairSlice { file: args.file.clone(), chunk_idx: args.chunk_idx, slice_idx: args.slice_idx }).await {
                Ok(IpcResponse::Success(msg)) => {
                    println!("============================================================");
                    println!("                 CRISPR/CAS REPAIR RESULT                   ");
                    println!("============================================================");
                    println!("File:             {}", args.file);
                    println!("Target Chunk:     #{}", args.chunk_idx);
                    println!("Target Slice:     #{}", args.slice_idx);
                    println!("Status:           \x1b[1;32mSUCCESS\x1b[0m");
                    println!("Message:          {}", msg);
                    println!("============================================================");
                }
                Ok(IpcResponse::Error(err)) => {
                    eprintln!("Repair error: {}", err);
                    std::process::exit(1);
                }
                Ok(_) => eprintln!("Unexpected response from daemon"),
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        },
        Commands::Join(args) => {
            match send_ipc(&cli.socket, IpcRequest::Join { address: args.address.clone() }).await {
                Ok(IpcResponse::Success(msg)) => {
                    println!("Successfully joined: {}", msg);
                }
                Ok(IpcResponse::Error(err)) => {
                    eprintln!("Failed to join {}: {}", args.address, err);
                    std::process::exit(1);
                }
                Ok(_) => eprintln!("Unexpected response from daemon"),
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        }
        Commands::Leave => match send_ipc(&cli.socket, IpcRequest::Leave).await {
            Ok(IpcResponse::Success(msg)) => {
                println!("{}", msg);
            }
            Ok(IpcResponse::Error(err)) => {
                eprintln!("Failed to leave mesh: {}", err);
                std::process::exit(1);
            }
            Ok(_) => eprintln!("Unexpected response"),
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        },
        Commands::Mount(args) => {
            match send_ipc(
                &cli.socket,
                IpcRequest::Mount {
                    mountpoint: args.mountpoint.clone(),
                },
            )
            .await
            {
                Ok(IpcResponse::Success(msg)) => {
                    println!("Mounted: {}", msg);
                }
                Ok(IpcResponse::Error(err)) => {
                    eprintln!("Mount error: {}", err);
                    std::process::exit(1);
                }
                Ok(_) => eprintln!("Unexpected response"),
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        }
        Commands::Evict(args) => {
            match send_ipc(&cli.socket, IpcRequest::Evict { node: args.node.clone() }).await {
                Ok(IpcResponse::Success(msg)) => {
                    println!("Node {} evicted: {}", args.node, msg);
                }
                Ok(IpcResponse::Error(err)) => {
                    eprintln!("Evict error: {}", err);
                    std::process::exit(1);
                }
                Ok(_) => eprintln!("Unexpected response"),
                Err(err) => {
                    eprintln!("{}", err);
                    std::process::exit(1);
                }
            }
        }
    }
}
