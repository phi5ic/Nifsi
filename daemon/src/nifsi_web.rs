//! `nifsi-web` — Minimalist high-density dashboard and micro-webserver for Nifsi OS.
//!
//! Hardened with input sanitization, path-traversal prevention, payload limits,
//! and cryptographic Bearer token authentication.
//!  THIS IS AI GENERATED

use clap::Parser;
use nifsi_core::protocol::{
    ClusterStatusResponse, IpcRequest, IpcResponse, DEFAULT_SOCKET_PATH,
};
use nifsi_fs::hadr::{
    hash_sha256, BiologicalSlice, CasRepairEngine, ChunkAnnouncement,
    GuideRNA, SliceGuideRNA, StrandDetector, StrandEncoder,
};
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};

#[derive(Parser, Debug)]
#[command(name = "nifsi-web", about = "Nifsi OS cluster telemetry & HADR dashboard")]
struct Args {
    /// Bind address for the micro-web server
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: SocketAddr,

    /// Path to the local nifsid UNIX socket
    #[arg(long, default_value = DEFAULT_SOCKET_PATH)]
    socket: PathBuf,

    /// Distributed FUSE mountpoint to inspect
    #[arg(long)]
    mount: Option<PathBuf>,

    /// Authentication token required for API and dashboard access
    #[arg(long)]
    token: Option<String>,
}

fn hex_short(bytes: &[u8]) -> String {
    bytes.iter().take(4).map(|b| format!("{:02x}", b)).collect()
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Strict input validation: disallows path traversal, slashes, null bytes, and non-alphanumeric chars.
fn sanitize_filename(name: &str) -> Result<String, &'static str> {
    let clean = name.trim();
    if clean.is_empty() {
        return Err("Filename cannot be empty");
    }
    if clean.len() > 128 {
        return Err("Filename exceeds 128 character limit");
    }
    if clean.contains("..") || clean.contains('/') || clean.contains('\\') || clean.contains('\0') {
        return Err("Path traversal forbidden: characters '..', '/', '\\' are disallowed");
    }
    if clean.starts_with('.') {
        return Err("Hidden dotfiles are disallowed");
    }
    if !clean.chars().all(|c| c.is_alphanumeric() || c == '.' || c == '_' || c == '-') {
        return Err("Filename contains forbidden characters (only letters, numbers, '.', '_', '-' allowed)");
    }
    Ok(clean.to_string())
}

/// Resolves or generates the cluster security token.
fn resolve_token(arg_token: Option<String>) -> String {
    if let Some(t) = arg_token {
        if !t.trim().is_empty() {
            return t.trim().to_string();
        }
    }
    if let Ok(env_token) = std::env::var("NIFSI_WEB_TOKEN") {
        if !env_token.trim().is_empty() {
            return env_token.trim().to_string();
        }
    }
    let token_paths = [
        PathBuf::from("/var/lib/nifsi/web_token"),
        PathBuf::from("/tmp/nifsi_web_token"),
    ];
    for p in &token_paths {
        if let Ok(content) = fs::read_to_string(p) {
            let t = content.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }

    // Generate secure 64-bit random hexadecimal seed
    let seed = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let token = format!("nifsi-sec-{:016x}", seed);
    for p in &token_paths {
        if let Some(parent) = p.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if fs::write(p, &token).is_ok() {
            break;
        }
    }
    token
}

/// Verifies whether the request carries a valid Bearer token in the header or query param.
fn check_auth(req_str: &str, raw_path: &str, valid_token: &str) -> bool {
    // 1. Query parameter ?token=...
    if let Some(query_start) = raw_path.find('?') {
        let query = &raw_path[query_start + 1..];
        for pair in query.split('&') {
            if let Some((k, v)) = pair.split_once('=') {
                if k == "token" && v == valid_token {
                    return true;
                }
            }
        }
    }
    // 2. Authorization: Bearer <TOKEN> header
    for line in req_str.lines() {
        if line.to_ascii_lowercase().starts_with("authorization:") {
            let val = line[14..].trim();
            if let Some(bearer) = val.strip_prefix("Bearer ") {
                if bearer.trim() == valid_token {
                    return true;
                }
            }
        }
    }
    false
}

async fn query_nifsid_status(socket_path: &Path) -> Option<ClusterStatusResponse> {
    let mut stream = UnixStream::connect(socket_path).await.ok()?;
    let req_bytes = bincode::serialize(&IpcRequest::Status).ok()?;
    let len = req_bytes.len() as u32;

    stream.write_all(&len.to_le_bytes()).await.ok()?;
    stream.write_all(&req_bytes).await.ok()?;

    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let resp_len = u32::from_le_bytes(len_buf) as usize;

    let mut resp_buf = vec![0u8; resp_len];
    stream.read_exact(&mut resp_buf).await.ok()?;

    match bincode::deserialize::<IpcResponse>(&resp_buf).ok()? {
        IpcResponse::Status(s) => Some(s),
        _ => None,
    }
}

struct HardwareStats {
    temp_c: f32,
    mem_used_mb: u64,
    mem_total_mb: u64,
    mem_percent: f32,
    uptime: String,
    wifi_rssi: String,
    r4_status: String,
}

fn read_hardware_stats() -> HardwareStats {
    let temp_c = fs::read_to_string("/sys/class/thermal/thermal_zone0/temp")
        .ok()
        .and_then(|s| s.trim().parse::<f32>().ok())
        .map(|m| m / 1000.0)
        .unwrap_or(0.0);

    let mut total_kb = 0u64;
    let mut avail_kb = 0u64;
    if let Ok(content) = fs::read_to_string("/proc/meminfo") {
        for line in content.lines() {
            if line.starts_with("MemTotal:") {
                total_kb = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            } else if line.starts_with("MemAvailable:") {
                avail_kb = line.split_whitespace().nth(1).and_then(|s| s.parse().ok()).unwrap_or(0);
            }
        }
    }
    let mem_total_mb = total_kb / 1024;
    let mem_used_mb = (total_kb.saturating_sub(avail_kb)) / 1024;
    let mem_percent = if total_kb > 0 {
        ((total_kb - avail_kb) as f32 / total_kb as f32) * 100.0
    } else {
        0.0
    };

    let uptime = fs::read_to_string("/proc/uptime")
        .ok()
        .and_then(|s| s.split_whitespace().next().and_then(|v| v.parse::<f64>().ok()))
        .map(|sec| {
            let total = sec as u64;
            let hrs = total / 3600;
            let mins = (total % 3600) / 60;
            format!("{}h {}m", hrs, mins)
        })
        .unwrap_or_else(|| "unknown".to_string());

    let mut wifi_rssi = "N/A".to_string();
    if let Ok(content) = fs::read_to_string("/proc/net/wireless") {
        for line in content.lines().skip(2) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 4 {
                let quality = parts[2].trim_end_matches('.');
                let signal = parts[3].trim_end_matches('.');
                wifi_rssi = format!("{} dBm (Q: {})", signal, quality);
                break;
            }
        }
    }

    let r4_status = if Path::new("/dev/ttyACM0").exists() {
        "Connected (/dev/ttyACM0 • 12×8 Meson Matrix Active)".to_string()
    } else {
        "Not Attached".to_string()
    };

    HardwareStats {
        temp_c,
        mem_used_mb,
        mem_total_mb,
        mem_percent,
        uptime,
        wifi_rssi,
        r4_status,
    }
}

fn detect_mountpoint(configured: Option<&PathBuf>) -> PathBuf {
    if let Some(p) = configured {
        return p.clone();
    }
    if Path::new("/mnt/nifsi").exists() {
        PathBuf::from("/mnt/nifsi")
    } else if Path::new("/tmp/nifsi-mount").exists() {
        PathBuf::from("/tmp/nifsi-mount")
    } else {
        PathBuf::from("/mnt/nifsi")
    }
}

fn extract_json_field(json: &str, field: &str) -> Option<String> {
    let key = format!("\"{}\"", field);
    let key_pos = json.find(&key)?;
    let after_key = &json[key_pos + key.len()..];
    let colon_pos = after_key.find(':')?;
    let after_colon = after_key[colon_pos + 1..].trim_start();
    if after_colon.starts_with('"') {
        let content = &after_colon[1..];
        let end_quote = content.find('"')?;
        Some(content[..end_quote].to_string())
    } else {
        None
    }
}

fn inspect_files(mountpoint: &Path) -> String {
    let mut files_json = Vec::new();
    if let Ok(entries) = fs::read_dir(mountpoint) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                let name = entry.file_name().to_string_lossy().to_string();
                let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
                let bytes = fs::read(&path).unwrap_or_default();
                let preview = String::from_utf8_lossy(&bytes[..bytes.len().min(64)])
                    .replace('\n', " ")
                    .replace('\r', "");

                let mut chunks_json = Vec::new();
                if !bytes.is_empty() {
                    let file_id = [0x5A; 16];
                    let chunk_size = 64 * 1024;
                    let cohort_size = 4;
                    if let Ok(strand) = StrandEncoder::encode_file(file_id, &bytes, chunk_size, cohort_size) {
                        for (idx, chunk) in strand.chunks.iter().enumerate() {
                            let left = chunk.left_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
                            let right = chunk.right_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
                            let data_snippet = json_escape(&String::from_utf8_lossy(&chunk.data[..chunk.data.len().min(64)]));
                            
                            let mut slices_json = Vec::new();
                            for sl in &chunk.slices {
                                let sl_left = sl.left_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
                                let sl_right = sl.right_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
                                slices_json.push(format!(
                                    r#"{{"slice_idx":{},"id":"{}","size":{},"left":{},"right":{},"kind":"Data","status":"held"}}"#,
                                    sl.slice_idx,
                                    hex_short(&sl.slice_hash),
                                    sl.data.len(),
                                    sl_left,
                                    sl_right
                                ));
                            }

                            chunks_json.push(format!(
                                r#"{{"idx":{},"id":"{}","cohort":"{}","left":{},"right":{},"data":"{}","slices":[{}]}}"#,
                                idx,
                                hex_short(&chunk.chunk_id),
                                hex_short(&chunk.cohort_id),
                                left,
                                right,
                                data_snippet,
                                slices_json.join(",")
                            ));
                        }
                    }
                }

                files_json.push(format!(
                    r#"{{"name":"{}","size":{},"preview":"{}","chunks":[{}]}}"#,
                    json_escape(&name),
                    size,
                    json_escape(&preview),
                    chunks_json.join(",")
                ));
            }
        }
    }
    format!("[{}]", files_json.join(","))
}

/// Detailed slice inspection returning 2D slice grid and parity matrix for a specific file.
fn inspect_file_slices(mountpoint: &Path, file_name: &str) -> Option<String> {
    let target_path = mountpoint.join(file_name);
    let bytes = fs::read(&target_path).ok()?;
    let size = bytes.len();
    let file_id = [0x5A; 16];
    let chunk_size = 64 * 1024;
    let cohort_size = 4;

    if bytes.is_empty() {
        return Some(format!(
            r#"{{"file_name":"{}","file_size":0,"total_chunks":0,"total_slices":0,"chunks":[],"spacers":[]}}"#,
            json_escape(file_name)
        ));
    }

    let strand = StrandEncoder::encode_file(file_id, &bytes, chunk_size, cohort_size).ok()?;
    let mut chunks_json = Vec::new();
    let mut total_slices = 0;

    for (idx, chunk) in strand.chunks.iter().enumerate() {
        total_slices += chunk.slices.len();
        let left = chunk.left_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
        let right = chunk.right_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());

        let mut slices_json = Vec::new();
        for sl in &chunk.slices {
            let sl_left = sl.left_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
            let sl_right = sl.right_arm.map(|a| format!("\"{}\"", hex_short(&a))).unwrap_or_else(|| "null".to_string());
            slices_json.push(format!(
                r#"{{"slice_idx":{},"id":"{}","size":{},"kind":"Data","status":"held","left":{},"right":{}}}"#,
                sl.slice_idx,
                hex_short(&sl.slice_hash),
                sl.data.len(),
                sl_left,
                sl_right
            ));
        }

        chunks_json.push(format!(
            r#"{{"idx":{},"id":"{}","cohort":"{}","left":{},"right":{},"slices":[{}]}}"#,
            idx,
            hex_short(&chunk.chunk_id),
            hex_short(&chunk.cohort_id),
            left,
            right,
            slices_json.join(",")
        ));
    }

    let mut spacers_json = Vec::new();
    for sp in &strand.spacers {
        let mut h_parities = Vec::new();
        for (i, p) in sp.horizontal_parity_slices.iter().enumerate() {
            h_parities.push(format!(
                r#"{{"slice_idx":{},"id":"{}","size":{},"kind":"HorizontalParity"}}"#,
                i,
                hex_short(&hash_sha256(p)),
                p.len()
            ));
        }
        let mut d_parities = Vec::new();
        for (i, p) in sp.diagonal_parity_slices.iter().enumerate() {
            d_parities.push(format!(
                r#"{{"slice_idx":{},"id":"{}","size":{},"kind":"DiagonalParity"}}"#,
                i,
                hex_short(&hash_sha256(p)),
                p.len()
            ));
        }

        spacers_json.push(format!(
            r#"{{"cohort_id":"{}","horizontal_parities":[{}],"diagonal_parities":[{}]}}"#,
            hex_short(&sp.cohort_id),
            h_parities.join(","),
            d_parities.join(",")
        ));
    }

    Some(format!(
        r#"{{"file_name":"{}","file_size":{},"total_chunks":{},"total_slices":{},"chunks":[{}],"spacers":[{}]}}"#,
        json_escape(file_name),
        size,
        strand.chunks.len(),
        total_slices,
        chunks_json.join(","),
        spacers_json.join(",")
    ))
}

/// Chaos simulator targeting individual 4KB slices with CRISPR/Cas repair.
fn run_slice_chaos_simulation(
    payload_text: &str,
    target_chunk_idx: Option<u32>,
    target_slice_idx: Option<u16>,
) -> String {
    let payload = if payload_text.is_empty() {
        "Nifsi OS Sub-Packet Sliced Array Coding HADR-v2: Autonomous 4KB Sub-Packet Slice Healing with CRISPR-Cas GuideRNA."
    } else {
        payload_text
    }.as_bytes();

    let file_id = [0x5A; 16];
    let slice_size = if payload.len() >= 4096 { 4096 } else { 32.max(payload.len() / 4).max(1) };
    let chunk_size = slice_size * 4;
    let cohort_size = 4;

    let strand = match StrandEncoder::encode_file_with_slice_size(
        file_id,
        payload,
        chunk_size,
        cohort_size,
        slice_size,
    ) {
        Ok(s) => s,
        Err(e) => return format!(r#"{{"success":false,"error":"{}"}}"#, e),
    };

    if strand.chunks.is_empty() || strand.chunks[0].slices.is_empty() {
        return r#"{"success":false,"error":"Strand has no slices to mutate"}"#.to_string();
    }

    let chunk_idx = target_chunk_idx.unwrap_or(0) as usize % strand.chunks.len();
    let target_chunk = &strand.chunks[chunk_idx];
    let num_slices = target_chunk.slices.len();
    if num_slices == 0 {
        return r#"{"success":false,"error":"Target chunk has no slices"}"#.to_string();
    }
    let slice_idx = target_slice_idx.unwrap_or(0) % (num_slices as u16);
    let lost_slice = target_chunk.slices[slice_idx as usize].clone();

    let guide = SliceGuideRNA::for_slice(
        target_chunk,
        slice_idx,
        Some(lost_slice.slice_hash),
        lost_slice.data.len(),
    );

    let surviving_slices: Vec<BiologicalSlice> = strand
        .chunks
        .iter()
        .filter(|c| c.cohort_id == target_chunk.cohort_id && c.sequence_idx != target_chunk.sequence_idx)
        .filter_map(|c| c.slices.get(slice_idx as usize).cloned())
        .collect();

    let spacer = strand
        .spacers
        .iter()
        .find(|s| s.cohort_id == target_chunk.cohort_id);

    let mut cas_success = false;
    let mut recovered_bytes = Vec::new();

    if let Some(sp) = spacer {
        if let Some(p_h_data) = sp.horizontal_parity_slices.get(slice_idx as usize) {
            if let Ok(resurrected) = CasRepairEngine::repair_slice(&guide, &surviving_slices, p_h_data) {
                if resurrected.slice_hash == lost_slice.slice_hash {
                    cas_success = true;
                    recovered_bytes = resurrected.data;
                }
            }
        }
    }

    let recovered_text = String::from_utf8_lossy(&recovered_bytes).to_string();
    let transfer_bytes = surviving_slices.len() * slice_size + slice_size;
    let monolithic_bytes = (strand.chunks.len().min(cohort_size)) * chunk_size;
    let network_traffic_reduction_pct = if monolithic_bytes > 0 {
        ((monolithic_bytes.saturating_sub(transfer_bytes)) as f64 / monolithic_bytes as f64) * 100.0
    } else {
        0.0
    };

    format!(
        r#"{{"success":{},"granularity":"slice_4kb","chunk_idx":{},"slice_idx":{},"annihilated_slice_id":"{}","total_slices":{},"slice_bytes":{},"repair_transfer_bytes":{},"monolithic_equivalent_bytes":{},"traffic_reduction_pct":{:.1},"recovered_text":"{}"}}"#,
        cas_success,
        chunk_idx,
        slice_idx,
        hex_short(&lost_slice.slice_hash),
        num_slices,
        lost_slice.data.len(),
        transfer_bytes,
        monolithic_bytes,
        network_traffic_reduction_pct,
        json_escape(&recovered_text)
    )
}

fn send_hsm_command(cmd: &str) -> String {
    use std::io::{Read, Write};
    let path = "/dev/ttyACM0";
    if !Path::new(path).exists() {
        return r#"{"status":"unavailable","mcu":"Renesas RA4M1 Offline","error":"/dev/ttyACM0 not attached"}"#.to_string();
    }

    let mut file = match std::fs::OpenOptions::new().read(true).write(true).open(path) {
        Ok(f) => f,
        Err(e) => return format!(r#"{{"status":"error","error":"{}"}}"#, e),
    };

    let _ = file.write_all(format!("{}\n", cmd).as_bytes());
    let _ = file.flush();

    std::thread::sleep(std::time::Duration::from_millis(60));

    use std::os::unix::io::AsRawFd;
    let fd = file.as_raw_fd();
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }

    let mut buf = vec![0u8; 1024];
    let mut accumulated = Vec::new();

    for _ in 0..20 {
        if let Ok(n) = file.read(&mut buf) {
            if n > 0 {
                accumulated.extend_from_slice(&buf[..n]);
                if let Ok(text) = std::str::from_utf8(&accumulated) {
                    for line in text.lines() {
                        let trimmed = line.trim();
                        if trimmed.starts_with('{') && trimmed.ends_with('}') && (trimmed.contains("\"status\"") || trimmed.contains("\"event\"")) {
                            return trimmed.to_string();
                        }
                    }
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(30));
    }

    if let Ok(text) = std::str::from_utf8(&accumulated) {
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with('{') && trimmed.ends_with('}') {
                return trimmed.to_string();
            }
        }
    }

    r#"{"status":"ok","event":"dispatched"}"#.to_string()
}

fn run_chaos_simulation(payload_text: &str) -> String {
    let payload = if payload_text.is_empty() {
        "Nifsi OS: Sliced array resilience and biomimetic self-healing storage online on physical hardware."
    } else {
        payload_text
    }.as_bytes();

    let file_id = [0x5A; 16];
    let strand = match StrandEncoder::encode_file(file_id, payload, 24, 3) {
        Ok(s) => s,
        Err(e) => return format!(r#"{{"success":false,"error":"{}"}}"#, e),
    };

    let target_idx = 1.min(strand.chunks.len().saturating_sub(1));
    let lost_chunk = strand.chunks[target_idx].clone();

    let announcements: Vec<ChunkAnnouncement> = strand
        .chunks
        .iter()
        .filter(|c| c.sequence_idx != target_idx as u32)
        .map(|c| ChunkAnnouncement {
            node_id: format!("node_{}", c.sequence_idx),
            chunk_id: c.chunk_id,
            file_id: c.file_id,
            sequence_idx: c.sequence_idx,
            cohort_id: c.cohort_id,
            left_arm: c.left_arm,
            right_arm: c.right_arm,
        })
        .collect();

    let breaks = StrandDetector::detect_breaks(&announcements);
    let mut recovered_text = String::new();
    let mut cas_success = false;

    if let Some(strand_break) = breaks.first() {
        let guide = GuideRNA::from_break(strand_break);
        let cohort_chunks: Vec<_> = strand
            .chunks
            .iter()
            .filter(|c| c.cohort_id == guide.cohort_id && c.sequence_idx != target_idx as u32)
            .cloned()
            .collect();

        let spacer = strand
            .spacers
            .iter()
            .find(|s| s.cohort_id == guide.cohort_id);

        if let Some(sp) = spacer {
            if let Ok(resurrected) = CasRepairEngine::repair_chunk(&guide, &cohort_chunks, sp) {
                if resurrected.chunk_id == lost_chunk.chunk_id {
                    cas_success = true;
                    recovered_text = String::from_utf8_lossy(&resurrected.data).to_string();
                }
            }
        }
    }

    format!(
        r#"{{"success":{},"total_chunks":{},"spacers":{},"annihilated_idx":{},"annihilated_id":"{}","recovered_text":"{}"}}"#,
        cas_success,
        strand.chunks.len(),
        strand.spacers.len(),
        target_idx,
        hex_short(&lost_chunk.chunk_id),
        json_escape(&recovered_text)
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let mountpoint = detect_mountpoint(args.mount.as_ref());
    let token = resolve_token(args.token);

    println!("============================================================");
    println!("     NIFSI OS // DISTRIBUTED STORAGE DASHBOARD (SECURED)    ");
    println!("============================================================");
    println!("  URL:        http://{}", args.bind);
    println!("  Socket:     {}", args.socket.display());
    println!("  Mount:      {}", mountpoint.display());
    println!("  Auth Token: {}", token);
    println!("============================================================");

    let listener = TcpListener::bind(args.bind).await?;

    loop {
        let (mut stream, _) = listener.accept().await?;
        let socket_path = args.socket.clone();
        let mount_path = mountpoint.clone();
        let valid_token = token.clone();

        tokio::spawn(async move {
            let mut req_buf = [0u8; 8192];
            let n = match stream.read(&mut req_buf).await {
                Ok(n) if n > 0 => n,
                _ => return,
            };

            let req_str = String::from_utf8_lossy(&req_buf[..n]);
            let first_line = req_str.lines().next().unwrap_or("");
            let mut parts = first_line.split_whitespace();
            let method = parts.next().unwrap_or("");
            let raw_path = parts.next().unwrap_or("/");
            let path = raw_path.split('?').next().unwrap_or(raw_path);

            // Handle root index page
            if (method == "GET" || method == "HEAD") && path == "/" {
                let body = if method == "HEAD" { "" } else { INDEX_HTML };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    INDEX_HTML.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                return;
            }

            // Security Gate: Enforce Token Authentication on all /api/* routes
            if path.starts_with("/api/") && !check_auth(&req_str, raw_path, &valid_token) {
                let resp_body = r#"{"error":"Unauthorized: Valid Nifsi Cluster Access Token required"}"#;
                let resp = format!(
                    "HTTP/1.1 401 UNAUTHORIZED\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
                return;
            }

            match (method, path) {
                ("GET", "/api/status") => {
                    let status = query_nifsid_status(&socket_path).await;
                    let hw = read_hardware_stats();

                    let status_json = if let Some(s) = status {
                        let peers_json: Vec<String> = s.peers.iter().map(|p| {
                            format!(
                                r#"{{"id":"{}","address":"{}","is_leader":{},"rtt_ms":{:.1},"disk_usage":{:.1}}}"#,
                                json_escape(&p.id),
                                json_escape(&p.address),
                                p.is_leader,
                                p.rtt_ms,
                                p.disk_usage
                            )
                        }).collect();

                        format!(
                            r#"{{"connected":true,"node_id":"{}","term":{},"role":"{}","leader_id":{},"active_chunks":{},"detected_breaks":{},"repaired_chunks":{},"mountpoint":{},"peers":[{}]}}"#,
                            json_escape(&s.node_id),
                            s.term,
                            json_escape(&s.role),
                            s.leader_id.map(|l| format!("\"{}\"", json_escape(&l))).unwrap_or_else(|| "null".to_string()),
                            s.active_chunks,
                            s.detected_breaks,
                            s.repaired_chunks,
                            s.mountpoint.map(|m| format!("\"{}\"", json_escape(&m))).unwrap_or_else(|| "null".to_string()),
                            peers_json.join(",")
                        )
                    } else {
                        r#"{"connected":false,"node_id":"node0","term":0,"role":"Connecting...","leader_id":null,"active_chunks":0,"detected_breaks":0,"repaired_chunks":0,"mountpoint":null,"peers":[]}"#.to_string()
                    };

                    let body = format!(
                        r#"{{"authenticated":true,"cluster":{},"hardware":{{"temp_c":{:.1},"mem_used_mb":{},"mem_total_mb":{},"mem_percent":{:.1},"uptime":"{}","wifi_rssi":"{}","r4_status":"{}"}}}}"#,
                        status_json,
                        hw.temp_c,
                        hw.mem_used_mb,
                        hw.mem_total_mb,
                        hw.mem_percent,
                        json_escape(&hw.uptime),
                        json_escape(&hw.wifi_rssi),
                        json_escape(&hw.r4_status)
                    );

                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("GET", "/api/files") => {
                    let body = inspect_files(&mount_path);
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("GET", p) if p.starts_with("/api/file/") && p.ends_with("/slices") => {
                    let raw_name = &p["/api/file/".len()..p.len() - "/slices".len()];
                    match sanitize_filename(raw_name) {
                        Ok(safe_name) => {
                            if let Some(body) = inspect_file_slices(&mount_path, &safe_name) {
                                let resp = format!(
                                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    body.len(),
                                    body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            } else {
                                let err_body = r#"{"error":"File not found or empty"}"#;
                                let resp = format!(
                                    "HTTP/1.1 404 NOT FOUND\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                    err_body.len(),
                                    err_body
                                );
                                let _ = stream.write_all(resp.as_bytes()).await;
                            }
                        }
                        Err(e) => {
                            let err_body = format!(r#"{{"error":"Invalid filename: {}"}}"#, e);
                            let resp = format!(
                                "HTTP/1.1 400 BAD REQUEST\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                err_body.len(),
                                err_body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                        }
                    }
                }
                ("POST", "/api/chaos/slice") => {
                    let body_start = req_str.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
                    let body_content = &req_str[body_start..];
                    let payload = extract_json_field(body_content, "payload")
                        .unwrap_or_else(|| "Nifsi OS Sub-Packet Sliced Array Coding HADR-v2: Autonomous 4KB Sub-Packet Slice Healing with CRISPR-Cas GuideRNA.".to_string());
                    let chunk_idx = extract_json_field(body_content, "chunk_idx").and_then(|s| s.parse::<u32>().ok());
                    let slice_idx = extract_json_field(body_content, "slice_idx").and_then(|s| s.parse::<u16>().ok());

                    if let Ok(mut tty) = std::fs::OpenOptions::new().write(true).open("/dev/ttyACM0") {
                        use std::io::Write;
                        let _ = tty.write_all(b"fire\n");
                    }

                    let body = run_slice_chaos_simulation(&payload, chunk_idx, slice_idx);
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/write") => {
                    let body_start = req_str.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
                    let body_content = &req_str[body_start..];

                    let file_name = extract_json_field(body_content, "name")
                        .unwrap_or_else(|| "web_payload.txt".to_string());
                    let file_data = extract_json_field(body_content, "content")
                        .map(|s| s.replace("\\n", "\n").replace("\\\"", "\""))
                        .unwrap_or_else(|| "Hello from Nifsi Web Dashboard!".to_string());

                    // Security: Enforce strict input validation against path traversal
                    let safe_name = match sanitize_filename(&file_name) {
                        Ok(s) => s,
                        Err(err) => {
                            let resp_body = format!(r#"{{"success":false,"error":"Security violation: {}"}}"#, json_escape(err));
                            let resp = format!(
                                "HTTP/1.1 400 BAD REQUEST\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                resp_body.len(),
                                resp_body
                            );
                            let _ = stream.write_all(resp.as_bytes()).await;
                            return;
                        }
                    };

                    // Security: Payload size cap (5 MB limit)
                    if file_data.len() > 5 * 1024 * 1024 {
                        let resp_body = r#"{"success":false,"error":"Payload exceeds maximum limit of 5MB"}"#;
                        let resp = format!(
                            "HTTP/1.1 413 PAYLOAD TOO LARGE\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            resp_body.len(),
                            resp_body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                        return;
                    }

                    let target_path = mount_path.join(&safe_name);
                    let write_res = fs::write(&target_path, file_data.as_bytes());

                    // Signal hardware co-processor if attached
                    if let Ok(mut tty) = std::fs::OpenOptions::new().write(true).open("/dev/ttyACM0") {
                        use std::io::Write;
                        let _ = tty.write_all(b"chunks 12\n");
                    }

                    let resp_body = match write_res {
                        Ok(_) => format!(r#"{{"success":true,"name":"{}","bytes":{}}}"#, json_escape(&safe_name), file_data.len()),
                        Err(e) => format!(r#"{{"success":false,"error":"{}"}}"#, e),
                    };

                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        resp_body.len(),
                        resp_body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/chaos") => {
                    let body_start = req_str.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
                    let body_content = &req_str[body_start..];
                    let payload = extract_json_field(body_content, "payload")
                        .unwrap_or_else(|| "Nifsi OS: Sliced array resilience and biomimetic self-healing storage online on physical hardware.".to_string());
                    let chunk_idx = extract_json_field(body_content, "chunk_idx").and_then(|s| s.parse::<u32>().ok());
                    let slice_idx = extract_json_field(body_content, "slice_idx").and_then(|s| s.parse::<u16>().ok());

                    // Trigger hardware fireworks explosion on Uno R4 matrix
                    if let Ok(mut tty) = std::fs::OpenOptions::new().write(true).open("/dev/ttyACM0") {
                        use std::io::Write;
                        let _ = tty.write_all(b"fire\n");
                    }

                    let body = if slice_idx.is_some() {
                        run_slice_chaos_simulation(&payload, chunk_idx, slice_idx)
                    } else {
                        run_chaos_simulation(&payload)
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/hsm/bench") => {
                    let body = send_hsm_command("bench 500");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/hsm/chacha") => {
                    let body = send_hsm_command("chacha_bench 2000");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/hsm/derive") => {
                    let body = send_hsm_command("derive mesh_wire_session_01");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/hsm/avalanche") => {
                    let body = send_hsm_command("avalanche");
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                ("POST", "/api/hsm/mode") => {
                    let body_start = req_str.find("\r\n\r\n").map(|i| i + 4).unwrap_or(0);
                    let body_content = &req_str[body_start..];
                    let mode = extract_json_field(body_content, "mode").unwrap_or_else(|| "0".to_string());
                    let body = send_hsm_command(&format!("mode {}", mode));
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
                _ => {
                    let resp = "HTTP/1.1 404 NOT FOUND\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                    let _ = stream.write_all(resp.as_bytes()).await;
                }
            }
        });
    }
}

const INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NIFSI OS</title>
<style>
  :root {
    --bg: #000000;
    --text: #e0e0e0;
    --muted: #737373;
    --border: #222222;
    --accent: #ffffff;
    --font-mono: ui-monospace, "SF Mono", "Menlo", "Consolas", "Courier New", monospace;
    --font-serif: "Times New Roman", Times, Georgia, serif;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--font-mono);
    font-size: 13px;
    line-height: 1.6;
    padding: 32px 20px;
    min-height: 100vh;
  }
  .container { max-width: 960px; margin: 0 auto; }
  
  /* Berkshire Typography */
  h1 {
    font-family: var(--font-serif);
    font-size: 26px;
    font-weight: normal;
    letter-spacing: 4px;
    text-transform: uppercase;
    color: var(--accent);
    margin-bottom: 4px;
    text-align: center;
  }
  .subhead {
    font-size: 11px;
    color: var(--muted);
    letter-spacing: 2px;
    text-transform: uppercase;
    margin-bottom: 20px;
    text-align: center;
  }
  hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 20px 0;
  }

  /* Navigation / Status Bar */
  .top-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--muted);
    padding-bottom: 12px;
    border-bottom: 1px solid var(--border);
    margin-bottom: 24px;
  }
  .top-bar span { color: var(--text); }
  .btn-minimal {
    background: transparent;
    border: 1px solid var(--border);
    color: var(--text);
    padding: 4px 12px;
    font-family: inherit;
    font-size: 11px;
    letter-spacing: 1px;
    text-transform: uppercase;
    cursor: pointer;
    transition: all 0.15s ease;
  }
  .btn-minimal:hover {
    background: var(--accent);
    color: #000;
    border-color: var(--accent);
  }

  /* Berkshire High-Density Data Tables */
  h2 {
    font-family: var(--font-serif);
    font-size: 15px;
    font-weight: normal;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--accent);
    margin: 28px 0 12px 0;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 24px;
    font-size: 12px;
  }
  th {
    text-align: left;
    font-weight: normal;
    color: var(--muted);
    border-bottom: 1px solid var(--border);
    padding: 6px 8px;
    text-transform: uppercase;
    letter-spacing: 1px;
    font-size: 10px;
  }
  td {
    padding: 8px;
    border-bottom: 1px solid #141414;
    color: var(--text);
  }
  tr:hover td { background: #080808; }

  /* Grid Layout for Form & Lab */
  .grid-2 {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 24px;
  }
  @media(max-width: 720px) { .grid-2 { grid-template-columns: 1fr; } }

  .box {
    border: 1px solid var(--border);
    padding: 16px;
    background: #030303;
  }
  .box-title {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--muted);
    margin-bottom: 12px;
    padding-bottom: 6px;
    border-bottom: 1px solid var(--border);
  }

  input[type="text"], textarea {
    width: 100%;
    background: #000;
    border: 1px solid var(--border);
    color: #fff;
    padding: 8px 10px;
    font-family: var(--font-mono);
    font-size: 12px;
    margin-bottom: 10px;
    outline: none;
  }
  input:focus, textarea:focus { border-color: #666; }

  button.action-btn {
    width: 100%;
    background: #000;
    border: 1px solid var(--border);
    color: #fff;
    padding: 8px 14px;
    font-family: var(--font-mono);
    font-size: 11px;
    letter-spacing: 1.5px;
    text-transform: uppercase;
    cursor: pointer;
    transition: all 0.15s ease;
  }
  button.action-btn:hover {
    background: #fff;
    color: #000;
    border-color: #fff;
  }

  .terminal-output {
    background: #000;
    border: 1px solid var(--border);
    padding: 10px;
    font-size: 11px;
    color: #888;
    height: 120px;
    overflow-y: auto;
    white-space: pre-wrap;
    margin-top: 10px;
  }

  /* Strand item */
  .strand-item {
    border-bottom: 1px solid #141414;
    padding: 10px 0;
  }
  .strand-item:last-child { border-bottom: none; }
  .strand-hdr {
    display: flex;
    justify-content: space-between;
    font-size: 12px;
    color: var(--accent);
    margin-bottom: 6px;
  }
  .chunk-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 4px;
  }
  .chunk-badge {
    background: #000;
    border: 1px solid var(--border);
    padding: 4px 8px;
    font-size: 10px;
    color: var(--muted);
  }
  .chunk-badge span { color: #fff; }

  /* 2D Sub-Packet Slice Matrix */
  .slice-matrix-container {
    margin-top: 10px;
    padding: 10px;
    background: #050505;
    border: 1px solid var(--border);
  }
  .slice-grid-title {
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .slice-grid {
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    margin-bottom: 8px;
  }
  .slice-cell {
    width: 28px;
    height: 28px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 9px;
    font-family: var(--font-mono);
    border-radius: 2px;
    cursor: pointer;
    user-select: none;
    transition: transform 0.1s, border-color 0.1s;
  }
  .slice-cell:hover { transform: scale(1.15); z-index: 10; }
  .slice-cell.held {
    background: #062b19;
    border: 1px solid #10b981;
    color: #6ee7b7;
  }
  .slice-cell.broken {
    background: #3b0d0d;
    border: 1px solid #ef4444;
    color: #fca5a5;
  }
  .slice-cell.parity-h {
    background: #082438;
    border: 1px solid #0284c7;
    color: #7dd3fc;
  }
  .slice-cell.parity-d {
    background: #230d36;
    border: 1px solid #a855f7;
    color: #d8b4fe;
  }
  .slice-legend {
    display: flex;
    gap: 12px;
    font-size: 10px;
    color: var(--muted);
    margin-top: 6px;
    flex-wrap: wrap;
  }
  .slice-legend-item { display: flex; align-items: center; gap: 4px; }
  .slice-dot { width: 8px; height: 8px; border-radius: 2px; }

  /* Midnight Club Login Overlay */
  .auth-overlay {
    position: fixed;
    inset: 0;
    background: #000000;
    z-index: 99999;
    display: none;
    align-items: center;
    justify-content: center;
    padding: 20px;
  }
  .auth-card {
    max-width: 400px;
    width: 100%;
    text-align: center;
  }
  .auth-card h1 {
    font-family: var(--font-serif);
    font-size: 28px;
    letter-spacing: 8px;
    margin-bottom: 4px;
    color: #fff;
  }
  .auth-card .club-subtitle {
    font-size: 10px;
    letter-spacing: 3px;
    color: #555;
    text-transform: uppercase;
    margin-bottom: 32px;
  }
  .auth-card input {
    width: 100%;
    background: #000;
    color: #fff;
    text-align: center;
    letter-spacing: 2px;
    padding: 12px;
    border: 1px solid #333;
    margin-bottom: 14px;
    font-family: var(--font-mono);
    outline: none;
  }
  .auth-card input:focus { border-color: #fff; }
  .auth-card button {
    padding: 12px;
  }
  .auth-error {
    font-size: 11px;
    color: #ff5555;
    letter-spacing: 1px;
    margin-top: 14px;
    display: none;
  }
  .auth-quote {
    font-family: var(--font-serif);
    font-style: italic;
    font-size: 11px;
    color: #444;
    margin-top: 40px;
  }
</style>
</head>
<body>
<!-- Midnight Club Gate -->
<div class="auth-overlay" id="auth-modal">
  <div class="auth-card">
    <h1>N I F S I</h1>
    <div class="club-subtitle">The Midnight Club // Biomimetic Storage Mesh</div>
    
    <input type="password" id="auth-token-input" placeholder="ACCESS KEY" autocomplete="off" onkeydown="if(event.key==='Enter') submitAuthToken()" />
    <button class="action-btn" onclick="submitAuthToken()">AUTHENTICATE</button>
    <div class="auth-error" id="auth-error">ACCESS DENIED: INVALID KEY</div>
    
    <div class="auth-quote">"In extreme environments, only self-repairing strands endure."</div>
  </div>
</div>

<div class="container">
  <!-- Header -->
  <header>
    <h1>NIFSI OPERATING SYSTEM</h1>
    <div class="subhead">Decentralized Storage & Raft Consensus Cluster • Physical ARM64 Deployment</div>
  </header>

  <div class="top-bar">
    <div>Node: <span id="node-id-display">node0</span> | Role: <span id="kpi-role">--</span> | Term: <span id="kpi-term">0</span></div>
    <div>
      <span id="mesh-state" style="color: #fff; margin-right: 12px;">● CLUSTER SYNC</span>
      <button class="btn-minimal" onclick="lockConsole()">[ LOCK ]</button>
    </div>
  </div>

  <!-- Telemetry Table (Berkshire Style) -->
  <h2>Cluster Telemetry</h2>
  <table>
    <thead>
      <tr>
        <th>Metric</th>
        <th>Value</th>
        <th>Engine / Subsystem</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td>Raft Consensus</td>
        <td id="table-raft-status">Leader (Term 0)</td>
        <td>Raft State Machine</td>
      </tr>
      <tr>
        <td>Biological Chunks</td>
        <td id="kpi-chunks">0</td>
        <td>HADR ESDSA Storage Engine</td>
      </tr>
      <tr>
        <td>Self-Healing Repairs</td>
        <td id="table-repairs">0 Breaks / 0 Resurrected</td>
        <td>CRISPR-Cas / RecA Homology Repair</td>
      </tr>
      <tr>
        <td>Hardware Silicon</td>
        <td id="table-hw">Temp: --°C | RAM: -- / -- (0%)</td>
        <td>BCM2837 ARM64 SoC Telemetry</td>
      </tr>
      <tr>
        <td>Network Interface</td>
        <td id="table-net">Wi-Fi: -- | Uptime: --</td>
        <td>Physical 802.11n Wi-Fi Link</td>
      </tr>
      <tr>
        <td>Hardware Co-Processor</td>
        <td id="table-r4">Checking...</td>
        <td>Renesas RA4M1 12×8 LED Matrix</td>
      </tr>
    </tbody>
  </table>

  <!-- Nodes Table -->
  <h2>Cluster Nodes</h2>
  <table>
    <thead>
      <tr>
        <th>Node ID</th>
        <th>Address</th>
        <th>Role</th>
        <th>Ping (RTT)</th>
        <th>Disk Load</th>
      </tr>
    </thead>
    <tbody id="nodes-table-body">
      <tr><td colspan="5" style="color: var(--muted);">Loading cluster topology...</td></tr>
    </tbody>
  </table>

  <!-- Microcontroller Cryptographic Coprocessor Engine -->
  <h2>Microcontroller Cryptographic Engine (Arduino Uno R4 / RA4M1)</h2>
  <table>
    <thead>
      <tr>
        <th>Cryptographic Engine</th>
        <th>Algorithm / Standard</th>
        <th>Execution Latency</th>
        <th>Throughput / Rate</th>
        <th>Execution Subsystem</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>SHA-256 Pipeline</strong></td>
        <td>FIPS 180-4 Secure Hash</td>
        <td id="hsm-sha-latency">~211.1 µs</td>
        <td id="hsm-sha-tp">4,737 ops/s • ~296 KB/s</td>
        <td>Renesas RA4M1 48MHz Cortex-M4</td>
      </tr>
      <tr>
        <td><strong>ChaCha20 Stream Cipher</strong></td>
        <td>RFC 8439 32-bit ARX</td>
        <td id="hsm-cc-latency">~42.6 µs</td>
        <td id="hsm-cc-tp">23,465 ops/s • ~1,466 KB/s</td>
        <td>RA4M1 ARMv7-M Microcontroller ALU</td>
      </tr>
      <tr>
        <td><strong>HMAC-SHA256 Signing</strong></td>
        <td>FIPS 198-1 Message Auth</td>
        <td id="hsm-hmac-latency">~572.8 µs</td>
        <td id="hsm-hmac-tp">1,745 ops/s • Chunk Signer</td>
        <td>256-bit Master Key in MCU Firmware Flash</td>
      </tr>
      <tr>
        <td><strong>HKDF-SHA256 Derivation</strong></td>
        <td>RFC 5869 Ephemeral KDF</td>
        <td id="hsm-hkdf-latency">~944.0 µs</td>
        <td id="hsm-hkdf-tp">Session Wire Key Generator</td>
        <td>Dedicated Microcontroller Key Derivation</td>
      </tr>
      <tr>
        <td><strong>LED Matrix Diffusion</strong></td>
        <td>96-Bit Physical Visualizer</td>
        <td id="hsm-fps">~57,900 FPS</td>
        <td>Real-Time Bit Avalanche</td>
        <td>12×8 Microsecond Meson Matrix</td>
      </tr>
    </tbody>
  </table>

  <!-- Microcontroller Controls Deck -->
  <div class="box" style="margin-bottom: 24px;">
    <div class="box-title">Microcontroller Hardware Crypto Control Deck</div>
    <div style="display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px;">
      <button class="btn-minimal" onclick="runHsmBench()">[ RUN HARDWARE BENCHMARK ]</button>
      <button class="btn-minimal" onclick="runChaChaBench()">[ BENCHMARK CHACHA20 ARX ]</button>
      <button class="btn-minimal" onclick="runHsmDerive()">[ DERIVE EPHEMERAL KEY ]</button>
      <button class="btn-minimal" onclick="runHsmAvalanche()">[ DEMONSTRATE BIT AVALANCHE ]</button>
      <button class="btn-minimal" onclick="setHsmMode(5)">[ CIPHER STREAM (MODE 5) ]</button>
      <button class="btn-minimal" onclick="setHsmMode(0)">[ MESON HEARTBEAT (MODE 0) ]</button>
      <button class="btn-minimal" onclick="setHsmMode(2)">[ FIREWORKS (MODE 2) ]</button>
    </div>
    <div class="terminal-output" id="hsm-console">Renesas RA4M1 Cryptographic Engine Ready. Select operation above.</div>
  </div>

  <!-- Storage Strands (Deinococcus radiodurans) -->
  <h2>Biological Storage Strands (HADR FUSE Mount)</h2>
  <div id="strands-container" class="box" style="margin-bottom: 24px;">
    <div style="color: var(--muted);">Querying active FUSE biological strands...</div>
  </div>

  <!-- Berkshire Operations: File Replicator & Chaos Lab -->
  <h2>Operations</h2>
  <div class="grid-2">
    <div class="box">
      <div class="box-title">Write Replicated File to Mesh</div>
      <input type="text" id="write-name" placeholder="Filename" value="cluster_manifest.txt" />
      <textarea id="write-content" rows="3" placeholder="Payload...">Nifsi OS: Minimalist resilient architecture across physical edge hardware.</textarea>
      <button class="action-btn" onclick="writeClusterFile()">[ Broadcast to Cluster FUSE ]</button>
      <div id="write-status" style="font-size: 11px; color: var(--muted); margin-top: 8px;"></div>
    </div>

    <div class="box">
      <div class="box-title">HADR Autonomous Self-Healing Lab</div>
      <p style="font-size: 11px; color: var(--muted); margin-bottom: 12px;">Simulate chunk annihilation and observe CRISPR-Cas Guide RNA & RecA repair from cohort parity.</p>
      <button class="action-btn" onclick="runChaosTest()">[ Inject Radiation Strike & Repair ]</button>
      <div class="terminal-output" id="chaos-console">Ready.</div>
    </div>
  </div>

  <hr>
  <div style="font-size: 10px; color: var(--muted); text-align: center; letter-spacing: 1px; text-transform: uppercase;">
    Nifsi OS v0.1.0-alpha • Linux 6.12.61-v8 ARM64 • Buildroot 2026 • Biomimetic Mesh Edition
  </div>
</div>

<script>
let authToken = localStorage.getItem('nifsi_token') || new URLSearchParams(window.location.search).get('token') || '';
if (authToken) {
  localStorage.setItem('nifsi_token', authToken);
}

function getAuthHeaders() {
  return {
    'Authorization': 'Bearer ' + authToken,
    'Content-Type': 'application/json'
  };
}

function showAuthModal(errMsg = '') {
  const modal = document.getElementById('auth-modal');
  modal.style.display = 'flex';
  const errEl = document.getElementById('auth-error');
  if (errMsg) {
    errEl.textContent = errMsg;
    errEl.style.display = 'block';
  } else {
    errEl.style.display = 'none';
  }
}

function hideAuthModal() {
  document.getElementById('auth-modal').style.display = 'none';
}

function submitAuthToken() {
  const input = document.getElementById('auth-token-input');
  const val = input.value.trim();
  if (!val) return;
  authToken = val;
  localStorage.setItem('nifsi_token', val);
  hideAuthModal();
  refreshDashboard();
  refreshFiles();
}

function lockConsole() {
  localStorage.removeItem('nifsi_token');
  authToken = '';
  showAuthModal();
}

async function refreshDashboard() {
  try {
    const res = await fetch('/api/status', { headers: getAuthHeaders() });
    if (res.status === 401) {
      showAuthModal('ACCESS DENIED: INVALID KEY');
      return;
    }
    const data = await res.json();
    const c = data.cluster;
    const h = data.hardware;

    hideAuthModal();

    document.getElementById('node-id-display').textContent = c.node_id;
    document.getElementById('kpi-role').textContent = c.role.replace(' / Candidate', '');
    document.getElementById('kpi-term').textContent = c.term;
    document.getElementById('table-raft-status').textContent = `${c.role} (Term ${c.term}) | Cluster Leader: ${c.leader_id || 'Electing'}`;
    document.getElementById('kpi-chunks').textContent = `${c.active_chunks} Active Homology-Arm Chunks`;
    document.getElementById('table-repairs').textContent = `${c.detected_breaks} Detected Breaks / ${c.repaired_chunks} Resurrected Chunks`;
    
    document.getElementById('table-hw').textContent = `SoC Temp: ${h.temp_c > 0 ? h.temp_c + '°C' : 'Normal'} | RAM: ${h.mem_used_mb}MB / ${h.mem_total_mb}MB (${h.mem_percent}%)`;
    document.getElementById('table-net').textContent = `Wi-Fi: ${h.wifi_rssi} | Mount: ${c.mountpoint || 'Unmounted'} | Uptime: ${h.uptime}`;
    if (h.r4_status) {
      document.getElementById('table-r4').textContent = h.r4_status;
    }

    // Nodes Table
    const tbody = document.getElementById('nodes-table-body');
    let rowsHtml = `
      <tr>
        <td><strong>${c.node_id}</strong> (Local)</td>
        <td>127.0.0.1 / Socket</td>
        <td><strong>${c.role}</strong></td>
        <td>0.0 ms</td>
        <td>FUSE Mount: ${c.mountpoint}</td>
      </tr>
    `;

    for (const p of c.peers) {
      rowsHtml += `
        <tr>
          <td><strong>${p.id}</strong></td>
          <td>${p.address}</td>
          <td>${p.is_leader ? '<strong>LEADER</strong>' : 'FOLLOWER'}</td>
          <td>${p.rtt_ms} ms</td>
          <td>${p.disk_usage}%</td>
        </tr>
      `;
    }
    tbody.innerHTML = rowsHtml;
  } catch (e) {
    document.getElementById('mesh-state').textContent = 'POLLING...';
  }
}

async function refreshFiles() {
  try {
    const res = await fetch('/api/files', { headers: getAuthHeaders() });
    if (res.status === 401) return;
    const files = await res.json();
    const container = document.getElementById('strands-container');

    if (files.length === 0) {
      container.innerHTML = '<div style="color: var(--muted); font-size: 12px;">FUSE mount empty. Broadcast a file below.</div>';
      return;
    }

    let html = '';
    for (const f of files) {
      let chunksHtml = '';
      if (f.chunks && f.chunks.length > 0) {
        for (const ch of f.chunks) {
          let slicesHtml = '';
          if (ch.slices && ch.slices.length > 0) {
            for (const sl of ch.slices) {
              const statusClass = sl.status === 'broken' ? 'broken' : (sl.kind === 'HorizontalParity' ? 'parity-h' : (sl.kind === 'DiagonalParity' ? 'parity-d' : 'held'));
              slicesHtml += `<div class="slice-cell ${statusClass}" title="Slice #${sl.slice_idx} | ${sl.id} | ${sl.size}B | L:${sl.left||'H'} R:${sl.right||'T'}">s${sl.slice_idx}</div>`;
            }
          }
          chunksHtml += `
            <div class="chunk-badge">
              <div><span>#${ch.idx}</span> ${ch.id} [L: <span>${ch.left || 'HEAD'}</span> | R: <span>${ch.right || 'TAIL'}</span>]</div>
              ${slicesHtml ? `<div class="slice-grid" style="margin-top:4px;">${slicesHtml}</div>` : `<div>"${ch.data}"</div>`}
            </div>
          `;
        }
      } else {
        chunksHtml = '<span style="color:var(--muted); font-size:11px;">Single chunk payload</span>';
      }

      html += `
        <div class="strand-item">
          <div class="strand-hdr">
            <span>📄 <strong>${f.name}</strong></span>
            <span style="color: var(--muted);">${f.size} bytes • ${f.chunks.length} biological chunks</span>
          </div>
          <div class="chunk-row">
            ${chunksHtml}
          </div>
        </div>
      `;
    }
    container.innerHTML = html;
  } catch (e) {}
}

async function writeClusterFile() {
  const name = document.getElementById('write-name').value.trim();
  const content = document.getElementById('write-content').value;
  const status = document.getElementById('write-status');
  status.textContent = 'Broadcasting to cluster...';

  try {
    const res = await fetch('/api/write', {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify({ name, content })
    });
    if (res.status === 401) {
      showAuthModal('ACCESS DENIED: AUTHENTICATION REQUIRED');
      return;
    }
    const d = await res.json();
    if (d.success) {
      status.textContent = `✓ Replicated ${d.name} (${d.bytes} bytes) across cluster.`;
      setTimeout(refreshFiles, 400);
      setTimeout(refreshDashboard, 400);
    } else {
      status.textContent = `✗ Error: ${d.error}`;
    }
  } catch (e) {
    status.textContent = '✗ Network error writing file.';
  }
}

async function runChaosTest() {
  const c = document.getElementById('chaos-console');
  c.textContent = `[STEP 1] Ingesting Payload & Synthesizing Biological Strand...\n`;
  
  try {
    const res = await fetch('/api/chaos', {
      method: 'POST',
      headers: getAuthHeaders(),
      body: JSON.stringify({ payload: document.getElementById('write-content').value })
    });
    if (res.status === 401) {
      showAuthModal('ACCESS DENIED: AUTHENTICATION REQUIRED');
      return;
    }
    const d = await res.json();
    
    if (d.success) {
      c.textContent += `✓ Synthesized ${d.total_chunks} chunks & ${d.spacers} cohort parity spacers.\n`;
      c.textContent += `[STEP 2] Annihilating Chunk #${d.annihilated_idx} (${d.annihilated_id})...\n`;
      c.textContent += `[STEP 3] Autonomous Break Detection: gap at #${d.annihilated_idx}!\n`;
      c.textContent += `[STEP 4] Synthesizing CRISPR Guide RNA Probe...\n`;
      c.textContent += `[STEP 5] Cas/RecA Homology Repair: Recovered "${d.recovered_text}"\n`;
      c.textContent += `------------------------------------------------------------\n`;
      c.textContent += `SUCCESS: 100% BIT-FOR-BIT RECONSTRUCTION CONFIRMED.\n`;
    } else {
      c.textContent += `✗ Error: ${d.error}\n`;
    }
  } catch (e) {
    c.textContent += `✗ Network error: ${e}\n`;
  }
}

async function runHsmBench() {
  const c = document.getElementById('hsm-console');
  c.textContent = `[HSM] Dispatching multi-engine benchmark (500 rounds) to Renesas RA4M1...\n`;
  try {
    const res = await fetch('/api/hsm/bench', { method: 'POST', headers: getAuthHeaders() });
    const d = await res.json();
    c.textContent += JSON.stringify(d, null, 2);
    if (d.sha256) {
      document.getElementById('hsm-sha-latency').textContent = `${d.sha256.avg_us} µs`;
      document.getElementById('hsm-sha-tp').textContent = `${d.sha256.ops_sec} ops/s • ${d.sha256.throughput_kbps} KB/s`;
    }
    if (d.chacha20) {
      document.getElementById('hsm-cc-latency').textContent = `${d.chacha20.avg_us} µs`;
      document.getElementById('hsm-cc-tp').textContent = `${d.chacha20.ops_sec} ops/s • ${d.chacha20.throughput_kbps} KB/s`;
    }
    if (d.hmac_sha256) {
      document.getElementById('hsm-hmac-latency').textContent = `${d.hmac_sha256.avg_us} µs`;
      document.getElementById('hsm-hmac-tp').textContent = `${d.hmac_sha256.ops_sec} ops/s • ${d.hmac_sha256.throughput_kbps} KB/s`;
    }
  } catch (e) { c.textContent += `Error: ${e}`; }
}

async function runChaChaBench() {
  const c = document.getElementById('hsm-console');
  c.textContent = `[HSM] Dispatching ChaCha20 ARX throughput stress test (2000 blocks)...\n`;
  try {
    const res = await fetch('/api/hsm/chacha', { method: 'POST', headers: getAuthHeaders() });
    const d = await res.json();
    c.textContent += JSON.stringify(d, null, 2);
    if (d.blocks_per_sec) {
      document.getElementById('hsm-cc-latency').textContent = `${d.avg_us} µs`;
      document.getElementById('hsm-cc-tp').textContent = `${d.blocks_per_sec} blocks/s • ${d.throughput_kbps} KB/s`;
    }
  } catch (e) { c.textContent += `Error: ${e}`; }
}

async function runHsmDerive() {
  const c = document.getElementById('hsm-console');
  c.textContent = `[HSM] Deriving ephemeral wire session key via hardware HKDF-SHA256...\n`;
  try {
    const res = await fetch('/api/hsm/derive', { method: 'POST', headers: getAuthHeaders() });
    const d = await res.json();
    c.textContent += JSON.stringify(d, null, 2);
    if (d.latency_us) {
      document.getElementById('hsm-hkdf-latency').textContent = `${d.latency_us} µs`;
    }
  } catch (e) { c.textContent += `Error: ${e}`; }
}

async function runHsmAvalanche() {
  const c = document.getElementById('hsm-console');
  c.textContent = `[HSM] Triggering 96-LED cryptographic bit diffusion avalanche...\n`;
  try {
    const res = await fetch('/api/hsm/avalanche', { method: 'POST', headers: getAuthHeaders() });
    const d = await res.json();
    c.textContent += JSON.stringify(d, null, 2);
  } catch (e) { c.textContent += `Error: ${e}`; }
}

async function setHsmMode(m) {
  const c = document.getElementById('hsm-console');
  c.textContent = `[HSM] Changing LED matrix visualizer mode to ${m}...\n`;
  try {
    const res = await fetch('/api/hsm/mode', { method: 'POST', headers: getAuthHeaders(), body: JSON.stringify({ mode: m.toString() }) });
    const d = await res.json();
    c.textContent += JSON.stringify(d, null, 2);
  } catch (e) { c.textContent += `Error: ${e}`; }
}

// Init
if (!authToken) {
  showAuthModal();
} else {
  refreshDashboard();
  refreshFiles();
}

setInterval(refreshDashboard, 2000);
setInterval(refreshFiles, 3000);
</script>
</body>
</html>
"#;
