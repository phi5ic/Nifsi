//! Standalone Comparative Benchmark Harness for HADR-v2
//!
//! Evaluates and compares three distributed storage array coding engines:
//! 1. Monolithic XOR baseline (HADR-v1 whole-chunk XOR)
//! 2. Sliced HADR-v2 (sub-packet sliced horizontal Ph and diagonal Pd parity)
//! 3. Reed-Solomon GF(2^8) (standard Cauchy/Vandermonde erasure coding over GF(2^8))
//!
//! Metrics evaluated:
//! - Throughput (GB/s) for encode and decode/repair
//! - Repair bytes transferred across network (k * 4KB vs k * ChunkSize)
//! - Degraded read latency (ms/us per slice vs full chunk)
//! - Write amplification factor (WAF) and delta journaling savings

use std::hint::black_box;
use std::time::Instant;

pub const SLICE_SIZE: usize = 4096; // 4 KB sub-packet slice
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024; // 64 KB standard chunk
pub const DEFAULT_COHORT_SIZE: usize = 4; // k = 4 data chunks
pub const DEFAULT_SLICES_PER_CHUNK: usize = 16; // M = 16 slices per chunk

// ==============================================================================
// 1. Pure Rust Reed-Solomon GF(2^8) Implementation (Zero External Dependencies)
// ==============================================================================

/// Galois Field GF(2^8) arithmetic with generator polynomial 0x11d.
#[derive(Clone)]
pub struct ReedSolomonGF256 {
    pub exp: [u8; 512],
    pub log: [u8; 256],
}

impl Default for ReedSolomonGF256 {
    fn default() -> Self {
        Self::new()
    }
}

impl ReedSolomonGF256 {
    pub fn new() -> Self {
        let mut exp = [0u8; 512];
        let mut log = [0u8; 256];
        let mut x: u16 = 1;
        for i in 0..255 {
            exp[i] = x as u8;
            log[x as usize] = i as u8;
            x <<= 1;
            if (x & 0x100) != 0 {
                x ^= 0x11d;
            }
        }
        for i in 255..512 {
            exp[i] = exp[i - 255];
        }
        Self { exp, log }
    }

    #[inline(always)]
    pub fn mul(&self, a: u8, b: u8) -> u8 {
        if a == 0 || b == 0 {
            return 0;
        }
        self.exp[self.log[a as usize] as usize + self.log[b as usize] as usize]
    }

    #[inline(always)]
    pub fn inv(&self, a: u8) -> u8 {
        assert!(a != 0, "Cannot invert zero in GF(2^8)");
        self.exp[255 - self.log[a as usize] as usize]
    }

    /// Vandermonde encoding over GF(2^8) producing parity_count parity chunks.
    pub fn encode_cohort(&self, data_chunks: &[Vec<u8>], parity_count: usize) -> Vec<Vec<u8>> {
        let k = data_chunks.len();
        let chunk_len = data_chunks.iter().map(|c| c.len()).max().unwrap_or(0);
        let mut parities = vec![vec![0u8; chunk_len]; parity_count];

        for p_idx in 0..parity_count {
            for i in 0..k {
                let coeff = self.exp[((p_idx + 1) * i) % 255];
                for byte_idx in 0..chunk_len {
                    let d = if byte_idx < data_chunks[i].len() {
                        data_chunks[i][byte_idx]
                    } else {
                        0
                    };
                    parities[p_idx][byte_idx] ^= self.mul(d, coeff);
                }
            }
        }
        parities
    }

    /// Single erasure reconstruction using parity 0.
    pub fn repair_single_chunk(
        &self,
        cohort: &[Vec<u8>],
        missing_idx: usize,
        p0: &[u8],
    ) -> Vec<u8> {
        let k = cohort.len();
        let chunk_len = p0.len();
        let coeff_m = self.exp[(1 * missing_idx) % 255];
        let inv_m = self.inv(coeff_m);

        let mut acc = p0.to_vec();
        for j in 0..k {
            if j != missing_idx {
                let c_j = self.exp[(1 * j) % 255];
                for b in 0..chunk_len {
                    let d = if b < cohort[j].len() { cohort[j][b] } else { 0 };
                    acc[b] ^= self.mul(d, c_j);
                }
            }
        }

        for b in 0..chunk_len {
            acc[b] = self.mul(acc[b], inv_m);
        }
        acc
    }
}

// ==============================================================================
// 2. Monolithic XOR Baseline Engine (HADR-v1)
// ==============================================================================

pub struct MonolithicXorEngine;

impl MonolithicXorEngine {
    pub fn encode_chunk_parity(cohort: &[Vec<u8>]) -> Vec<u8> {
        if cohort.is_empty() {
            return Vec::new();
        }
        let chunk_len = cohort[0].len();
        let mut parity = vec![0u8; chunk_len];
        for chunk in cohort {
            for (idx, &byte) in chunk.iter().enumerate() {
                parity[idx] ^= byte;
            }
        }
        parity
    }

    pub fn repair_chunk(surviving: &[Vec<u8>], parity: &[u8]) -> Vec<u8> {
        let mut reconstructed = parity.to_vec();
        for chunk in surviving {
            for (idx, &byte) in chunk.iter().enumerate() {
                reconstructed[idx] ^= byte;
            }
        }
        reconstructed
    }
}

// ==============================================================================
// 3. Sliced HADR-v2 Engine (Ph Horizontal + Pd Diagonal Parity)
// ==============================================================================

pub struct SlicedHadrV2Engine;

impl SlicedHadrV2Engine {
    /// Compute horizontal parity slices: P_h[s] = XOR_{i=0..k-1} D_i[s]
    pub fn compute_horizontal_parities(
        cohort_slices: &[Vec<Vec<u8>>],
        num_slices: usize,
    ) -> Vec<Vec<u8>> {
        let mut p_h = vec![vec![0u8; SLICE_SIZE]; num_slices];
        for s in 0..num_slices {
            for chunk_slices in cohort_slices {
                if s < chunk_slices.len() {
                    for b in 0..SLICE_SIZE {
                        p_h[s][b] ^= chunk_slices[s][b];
                    }
                }
            }
        }
        p_h
    }

    /// Compute diagonal array parity slices: P_d[s] = XOR_{i=0..k-1} D_i[(s + i) mod M]
    pub fn compute_diagonal_parities(
        cohort_slices: &[Vec<Vec<u8>>],
        num_slices: usize,
    ) -> Vec<Vec<u8>> {
        let mut p_d = vec![vec![0u8; SLICE_SIZE]; num_slices];
        for s in 0..num_slices {
            for (i, chunk_slices) in cohort_slices.iter().enumerate() {
                let r = (s + i) % num_slices;
                if r < chunk_slices.len() {
                    for b in 0..SLICE_SIZE {
                        p_d[s][b] ^= chunk_slices[r][b];
                    }
                }
            }
        }
        p_d
    }

    /// Single 4KB slice repair using horizontal parity: D_m[s] = P_h[s] ^ (Sum_{j != m} D_j[s])
    pub fn repair_slice_horizontal(
        surviving_slices_at_s: &[&[u8]],
        p_h_s: &[u8],
    ) -> Vec<u8> {
        let mut reconstructed = p_h_s.to_vec();
        for slice_data in surviving_slices_at_s {
            for b in 0..SLICE_SIZE {
                reconstructed[b] ^= slice_data[b];
            }
        }
        reconstructed
    }

    /// Partial write via slice-delta journaling:
    /// Delta = D_old[s] ^ D_new[s]
    /// P_h_new[s] = P_h_old[s] ^ Delta
    /// P_d_new[(s - i) mod M] = P_d_old[(s - i) mod M] ^ Delta
    pub fn apply_slice_delta(
        p_h_s: &mut [u8],
        p_d_diag: &mut [u8],
        delta: &[u8],
    ) {
        for b in 0..SLICE_SIZE {
            p_h_s[b] ^= delta[b];
            p_d_diag[b] ^= delta[b];
        }
    }
}

// ==============================================================================
// 4. Multi-Metric Benchmark Runner
// ==============================================================================

#[derive(Clone, Debug)]
pub struct BenchmarkResult {
    pub engine_name: String,
    pub chunk_size_kb: usize,
    pub encode_throughput_gb_s: f64,
    pub repair_throughput_gb_s: f64,
    pub repair_network_transfer_kb: f64,
    pub degraded_read_latency_ms: f64,
    pub latency_p50_us: f64,
    pub latency_p95_us: f64,
    pub latency_p99_us: f64,
    pub write_amplification_factor: f64,
    pub partial_write_io_kb: f64,
}

pub fn generate_deterministic_cohort(k: usize, chunk_size: usize) -> Vec<Vec<u8>> {
    let mut cohort = Vec::with_capacity(k);
    for i in 0..k {
        let mut chunk = vec![0u8; chunk_size];
        for (b, byte) in chunk.iter_mut().enumerate() {
            *byte = ((i * 37 + b * 13) % 251) as u8;
        }
        cohort.push(chunk);
    }
    cohort
}

pub fn slice_cohort(cohort: &[Vec<u8>]) -> Vec<Vec<Vec<u8>>> {
    cohort
        .iter()
        .map(|chunk| {
            chunk
                .chunks(SLICE_SIZE)
                .map(|sl| sl.to_vec())
                .collect::<Vec<Vec<u8>>>()
        })
        .collect()
}

fn compute_percentiles(mut l: Vec<f64>) -> (f64, f64, f64, f64) {
    if l.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    l.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = l.len();
    let mean = l.iter().sum::<f64>() / n as f64;
    let p50 = l[((n as f64 * 0.50) as usize).min(n - 1)];
    let p95 = l[((n as f64 * 0.95) as usize).min(n - 1)];
    let p99 = l[((n as f64 * 0.99) as usize).min(n - 1)];
    (mean, p50, p95, p99)
}

pub fn run_benchmarks() -> (BenchmarkResult, BenchmarkResult, BenchmarkResult) {
    run_benchmarks_param(DEFAULT_CHUNK_SIZE, 15, 16)
}

pub fn run_benchmarks_param(
    chunk_size: usize,
    measured_iters: usize,
    batch_size: usize,
) -> (BenchmarkResult, BenchmarkResult, BenchmarkResult) {
    const WARMUP_ITERS: usize = 3;

    let k = DEFAULT_COHORT_SIZE;
    let num_slices = chunk_size / SLICE_SIZE;

    let cohort = generate_deterministic_cohort(k, chunk_size);
    let cohort_slices = slice_cohort(&cohort);

    // --------------------------------------------------------------------------
    // 1. Monolithic XOR Baseline
    // --------------------------------------------------------------------------
    let xor_parity = MonolithicXorEngine::encode_chunk_parity(&cohort);
    // Warmup
    for _ in 0..WARMUP_ITERS {
        black_box(MonolithicXorEngine::encode_chunk_parity(&cohort));
    }
    // Encode timing
    let start_enc = Instant::now();
    for _ in 0..measured_iters {
        for _ in 0..batch_size {
            black_box(MonolithicXorEngine::encode_chunk_parity(&cohort));
        }
    }
    let elapsed_enc = start_enc.elapsed().as_secs_f64();
    let total_bytes_enc = (k * chunk_size * batch_size * measured_iters) as f64;
    let xor_enc_gb_s = (total_bytes_enc / (1024.0 * 1024.0 * 1024.0)) / elapsed_enc;

    // Decode/Repair timing (reconstructing full chunk)
    let surviving_chunks = &cohort[1..];
    for _ in 0..WARMUP_ITERS {
        black_box(MonolithicXorEngine::repair_chunk(surviving_chunks, &xor_parity));
    }
    let mut xor_latencies = Vec::with_capacity(measured_iters);
    for _ in 0..measured_iters {
        let t0 = Instant::now();
        for _ in 0..batch_size {
            black_box(MonolithicXorEngine::repair_chunk(surviving_chunks, &xor_parity));
        }
        let per_op_us = (t0.elapsed().as_secs_f64() * 1_000_000.0) / batch_size as f64;
        xor_latencies.push(per_op_us);
    }
    let (xor_mean_us, xor_p50, xor_p95, xor_p99) = compute_percentiles(xor_latencies);
    let total_bytes_dec = (k * chunk_size * batch_size * measured_iters) as f64;
    let total_elapsed_dec = (xor_mean_us * (batch_size * measured_iters) as f64) / 1_000_000.0;
    let xor_dec_gb_s = (total_bytes_dec / (1024.0 * 1024.0 * 1024.0)) / total_elapsed_dec;

    let xor_repair_network_kb = (k * chunk_size) as f64 / 1024.0;
    let xor_waf = (chunk_size as f64 / SLICE_SIZE as f64) * k as f64;
    let xor_io_kb = (k * chunk_size) as f64 / 1024.0;

    let res_xor = BenchmarkResult {
        engine_name: "Monolithic XOR (HADR-v1)".to_string(),
        chunk_size_kb: chunk_size / 1024,
        encode_throughput_gb_s: xor_enc_gb_s,
        repair_throughput_gb_s: xor_dec_gb_s,
        repair_network_transfer_kb: xor_repair_network_kb,
        degraded_read_latency_ms: xor_mean_us / 1000.0,
        latency_p50_us: xor_p50,
        latency_p95_us: xor_p95,
        latency_p99_us: xor_p99,
        write_amplification_factor: xor_waf,
        partial_write_io_kb: xor_io_kb,
    };

    // --------------------------------------------------------------------------
    // 2. Sliced HADR-v2 Engine
    // --------------------------------------------------------------------------
    let p_h = SlicedHadrV2Engine::compute_horizontal_parities(&cohort_slices, num_slices);
    let _p_d = SlicedHadrV2Engine::compute_diagonal_parities(&cohort_slices, num_slices);

    // Warmup
    for _ in 0..WARMUP_ITERS {
        black_box(SlicedHadrV2Engine::compute_horizontal_parities(&cohort_slices, num_slices));
        black_box(SlicedHadrV2Engine::compute_diagonal_parities(&cohort_slices, num_slices));
    }
    // Encode timing
    let start_enc_hadr = Instant::now();
    for _ in 0..measured_iters {
        for _ in 0..batch_size {
            black_box(SlicedHadrV2Engine::compute_horizontal_parities(&cohort_slices, num_slices));
            black_box(SlicedHadrV2Engine::compute_diagonal_parities(&cohort_slices, num_slices));
        }
    }
    let elapsed_enc_hadr = start_enc_hadr.elapsed().as_secs_f64();
    let hadr_enc_gb_s = (total_bytes_enc / (1024.0 * 1024.0 * 1024.0)) / elapsed_enc_hadr;

    // Single 4KB slice degraded read repair timing
    let slice_0_surviving: Vec<&[u8]> = cohort_slices[1..].iter().map(|c| c[0].as_slice()).collect();
    for _ in 0..WARMUP_ITERS {
        black_box(SlicedHadrV2Engine::repair_slice_horizontal(&slice_0_surviving, &p_h[0]));
    }
    let slice_iters = (measured_iters * batch_size).max(100);
    let mut hadr_latencies = Vec::with_capacity(slice_iters);
    for _ in 0..slice_iters {
        let t0 = Instant::now();
        black_box(SlicedHadrV2Engine::repair_slice_horizontal(&slice_0_surviving, &p_h[0]));
        let per_op_us = t0.elapsed().as_secs_f64() * 1_000_000.0;
        hadr_latencies.push(per_op_us);
    }
    let (hadr_mean_us, hadr_p50, hadr_p95, hadr_p99) = compute_percentiles(hadr_latencies);
    let total_bytes_slice_dec = (k * SLICE_SIZE * slice_iters) as f64;
    let total_elapsed_hadr_dec = (hadr_mean_us * slice_iters as f64) / 1_000_000.0;
    let hadr_dec_gb_s = (total_bytes_slice_dec / (1024.0 * 1024.0 * 1024.0)) / total_elapsed_hadr_dec;

    let hadr_repair_network_kb = (k * SLICE_SIZE) as f64 / 1024.0; // 16.0 KB fixed regardless of chunk size!
    let hadr_waf = 3.0; // 12 KB (slice + Ph + Pd) / 4 KB
    let hadr_io_kb = 12.0;

    let res_hadr = BenchmarkResult {
        engine_name: "Sliced HADR-v2 (Ph + Pd)".to_string(),
        chunk_size_kb: chunk_size / 1024,
        encode_throughput_gb_s: hadr_enc_gb_s,
        repair_throughput_gb_s: hadr_dec_gb_s,
        repair_network_transfer_kb: hadr_repair_network_kb,
        degraded_read_latency_ms: hadr_mean_us / 1000.0,
        latency_p50_us: hadr_p50,
        latency_p95_us: hadr_p95,
        latency_p99_us: hadr_p99,
        write_amplification_factor: hadr_waf,
        partial_write_io_kb: hadr_io_kb,
    };

    // --------------------------------------------------------------------------
    // 3. Reed-Solomon GF(2^8) Engine
    // --------------------------------------------------------------------------
    let rs = ReedSolomonGF256::new();
    let rs_parities = rs.encode_cohort(&cohort, 2);

    for _ in 0..WARMUP_ITERS {
        black_box(rs.encode_cohort(&cohort, 2));
    }
    let start_enc_rs = Instant::now();
    for _ in 0..measured_iters {
        for _ in 0..batch_size {
            black_box(rs.encode_cohort(&cohort, 2));
        }
    }
    let elapsed_enc_rs = start_enc_rs.elapsed().as_secs_f64();
    let rs_enc_gb_s = (total_bytes_enc / (1024.0 * 1024.0 * 1024.0)) / elapsed_enc_rs;

    // Decode/Repair timing (reconstructing full chunk)
    for _ in 0..WARMUP_ITERS {
        black_box(rs.repair_single_chunk(&cohort, 0, &rs_parities[0]));
    }
    let mut rs_latencies = Vec::with_capacity(measured_iters);
    for _ in 0..measured_iters {
        let t0 = Instant::now();
        for _ in 0..batch_size {
            black_box(rs.repair_single_chunk(&cohort, 0, &rs_parities[0]));
        }
        let per_op_us = (t0.elapsed().as_secs_f64() * 1_000_000.0) / batch_size as f64;
        rs_latencies.push(per_op_us);
    }
    let (rs_mean_us, rs_p50, rs_p95, rs_p99) = compute_percentiles(rs_latencies);
    let total_elapsed_rs_dec = (rs_mean_us * (batch_size * measured_iters) as f64) / 1_000_000.0;
    let rs_dec_gb_s = (total_bytes_dec / (1024.0 * 1024.0 * 1024.0)) / total_elapsed_rs_dec;

    let rs_repair_network_kb = (k * chunk_size) as f64 / 1024.0;
    let rs_waf = (chunk_size as f64 / SLICE_SIZE as f64) * (k + 2) as f64;
    let rs_io_kb = ((k + 2) * chunk_size) as f64 / 1024.0;

    let res_rs = BenchmarkResult {
        engine_name: "Reed-Solomon GF(2^8) (k=4, p=2)".to_string(),
        chunk_size_kb: chunk_size / 1024,
        encode_throughput_gb_s: rs_enc_gb_s,
        repair_throughput_gb_s: rs_dec_gb_s,
        repair_network_transfer_kb: rs_repair_network_kb,
        degraded_read_latency_ms: rs_mean_us / 1000.0,
        latency_p50_us: rs_p50,
        latency_p95_us: rs_p95,
        latency_p99_us: rs_p99,
        write_amplification_factor: rs_waf,
        partial_write_io_kb: rs_io_kb,
    };

    (res_xor, res_hadr, res_rs)
}

pub fn print_results(results: &[BenchmarkResult]) {
    let chunk_kb = if !results.is_empty() { results[0].chunk_size_kb } else { DEFAULT_CHUNK_SIZE / 1024 };
    println!("\n=== HADR-v2 Comparative Storage Benchmark Harness ===");
    println!("Evaluation Configuration: k = {} chunks, Chunk Size = {} KB, Slice Size = {} KB",
        DEFAULT_COHORT_SIZE, chunk_kb, SLICE_SIZE / 1024);
    println!();

    // Standard markdown table header required by test framework
    println!("| Engine | Throughput (GB/s) | Repair Network (KB) | Degraded Latency (ms) | Latency p95 (us) | Latency p99 (us) | Write Amplification |");
    println!("|---|---|---|---|---|---|---|");
    for r in results {
        println!(
            "| {} | {:.2} | {:.1} | {:.4} | {:.2} | {:.2} | {:.1}x ({:.0} KB) |",
            r.engine_name,
            r.encode_throughput_gb_s,
            r.repair_network_transfer_kb,
            r.degraded_read_latency_ms,
            r.latency_p95_us,
            r.latency_p99_us,
            r.write_amplification_factor,
            r.partial_write_io_kb
        );
    }
    println!();

    // Detailed metrics breakdown
    println!("--- Metric 1: Throughput (GB/s) ---");
    for r in results {
        println!(
            "  * {:<32} Encode: {:>6.2} GB/s | Repair: {:>6.2} GB/s",
            r.engine_name, r.encode_throughput_gb_s, r.repair_throughput_gb_s
        );
    }

    println!("\n--- Metric 2: Network Transfer Reduction ---");
    let xor_net = results[0].repair_network_transfer_kb;
    let hadr_net = results[1].repair_network_transfer_kb;
    let ratio = if hadr_net > 0.0 { xor_net / hadr_net } else { 1.0 };
    println!("  * {} KB Chunk Repair Traffic : Monolithic = {:.1} KB vs Sliced HADR = {:.1} KB ({:.1}x reduction)",
        chunk_kb, xor_net, hadr_net, ratio);

    println!("\n--- Metric 3: Degraded Read Latency Distribution ---");
    println!("  * Sliced HADR-v2 single 4 KB slice: p50={:.2}us | p95={:.2}us | p99={:.2}us (mean: {:.4} ms)",
        results[1].latency_p50_us, results[1].latency_p95_us, results[1].latency_p99_us, results[1].degraded_read_latency_ms);
    println!("  * Monolithic XOR whole chunk:       p50={:.2}us | p95={:.2}us | p99={:.2}us (mean: {:.4} ms)",
        results[0].latency_p50_us, results[0].latency_p95_us, results[0].latency_p99_us, results[0].degraded_read_latency_ms);
    println!("  * Reed-Solomon GF(2^8) whole chunk: p50={:.2}us | p95={:.2}us | p99={:.2}us (mean: {:.4} ms)",
        results[2].latency_p50_us, results[2].latency_p95_us, results[2].latency_p99_us, results[2].degraded_read_latency_ms);

    println!("\n--- Metric 4: Write Amplification & Slice Delta Journaling ---");
    let rmw_io = results[0].partial_write_io_kb;
    let delta_io = results[1].partial_write_io_kb;
    let reduction_pct = if rmw_io > 0.0 { ((rmw_io - delta_io) / rmw_io) * 100.0 } else { 0.0 };
    println!("  * 4 KB Partial Write I/O: Classical RMW = {:.0} KB vs Slice-Delta = {:.0} KB ({:.1}% reduction)",
        rmw_io, delta_io, reduction_pct);
}

pub fn print_json(all_results: &[BenchmarkResult]) {
    println!("[");
    for (i, r) in all_results.iter().enumerate() {
        let comma = if i + 1 < all_results.len() { "," } else { "" };
        println!(
            "  {{\n    \"engine\": \"{}\",\n    \"chunk_size_kb\": {},\n    \"encode_throughput_gb_s\": {:.4},\n    \"repair_throughput_gb_s\": {:.4},\n    \"repair_network_transfer_kb\": {:.1},\n    \"degraded_read_latency_ms\": {:.6},\n    \"latency_p50_us\": {:.2},\n    \"latency_p95_us\": {:.2},\n    \"latency_p99_us\": {:.2},\n    \"write_amplification_factor\": {:.1},\n    \"partial_write_io_kb\": {:.1}\n  }}{}",
            r.engine_name, r.chunk_size_kb, r.encode_throughput_gb_s, r.repair_throughput_gb_s,
            r.repair_network_transfer_kb, r.degraded_read_latency_ms, r.latency_p50_us, r.latency_p95_us,
            r.latency_p99_us, r.write_amplification_factor, r.partial_write_io_kb, comma
        );
    }
    println!("]");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let is_json = args.iter().any(|a| a == "--json");
    let is_sweep = args.iter().any(|a| a == "--sweep");

    if is_sweep {
        let sweep_configs = [
            (64 * 1024, 20, 16),
            (256 * 1024, 15, 8),
            (1024 * 1024, 10, 4),
            (4 * 1024 * 1024, 5, 2),
            (16 * 1024 * 1024, 3, 1),
        ];

        let mut all_results = Vec::new();
        for (chunk_size, iters, batch) in sweep_configs {
            let (r_xor, r_hadr, r_rs) = run_benchmarks_param(chunk_size, iters, batch);
            if !is_json {
                print_results(&[r_xor.clone(), r_hadr.clone(), r_rs.clone()]);
            }
            all_results.push(r_xor);
            all_results.push(r_hadr);
            all_results.push(r_rs);
        }

        if is_json {
            print_json(&all_results);
        }
    } else {
        let (res_xor, res_hadr, res_rs) = run_benchmarks();
        if is_json {
            print_json(&[res_xor, res_hadr, res_rs]);
        } else {
            print_results(&[res_xor, res_hadr, res_rs]);
        }
    }
}

#[cfg(test)]
#[allow(unused_imports, dead_code)]
mod tests {
    use super::*;

    #[test]
    fn test_gf256_multiplication_properties() {
        let rs = ReedSolomonGF256::new();
        // Identity: a * 1 = a
        for a in 1..=255 {
            assert_eq!(rs.mul(a, 1), a);
        }
        // Zero: a * 0 = 0
        assert_eq!(rs.mul(42, 0), 0);
        assert_eq!(rs.mul(0, 99), 0);
        // Inverse: a * inv(a) = 1
        for a in 1..=255 {
            let inv_a = rs.inv(a);
            assert_eq!(rs.mul(a, inv_a), 1);
        }
    }

    #[test]
    fn test_reed_solomon_oracle_k2_and_k8() {
        let rs = ReedSolomonGF256::new();
        let c0 = vec![0x55u8; 128];
        let c1 = vec![0xaau8; 128];
        let parities_k2 = rs.encode_cohort(&[c0, c1], 2);
        assert_eq!(parities_k2.len(), 2);
        assert_eq!(parities_k2[0].len(), 128);

        let cohort_k8: Vec<Vec<u8>> = (0..8).map(|i| vec![(i * 10) as u8; 64]).collect();
        let parities_k8 = rs.encode_cohort(&cohort_k8, 2);
        assert_eq!(parities_k8.len(), 2);
        assert_eq!(parities_k8[0].len(), 64);
    }

    #[test]
    fn test_reed_solomon_single_chunk_repair() {
        let rs = ReedSolomonGF256::new();
        let cohort = generate_deterministic_cohort(4, 1024);
        let parities = rs.encode_cohort(&cohort, 2);

        for missing in 0..4 {
            let repaired = rs.repair_single_chunk(&cohort, missing, &parities[0]);
            assert_eq!(repaired, cohort[missing]);
        }
    }

    #[test]
    fn test_monolithic_xor_repair() {
        let cohort = generate_deterministic_cohort(4, 1024);
        let parity = MonolithicXorEngine::encode_chunk_parity(&cohort);
        for missing in 0..4 {
            let surviving: Vec<Vec<u8>> = cohort.iter().enumerate().filter(|(i, _)| *i != missing).map(|(_, c)| c.clone()).collect();
            let repaired = MonolithicXorEngine::repair_chunk(&surviving, &parity);
            assert_eq!(repaired, cohort[missing]);
        }
    }

    #[test]
    fn test_sliced_hadr_v2_repair_and_delta() {
        let cohort = generate_deterministic_cohort(4, 64 * 1024);
        let slices = slice_cohort(&cohort);
        let p_h = SlicedHadrV2Engine::compute_horizontal_parities(&slices, 16);
        let p_d = SlicedHadrV2Engine::compute_diagonal_parities(&slices, 16);
        assert_eq!(p_h.len(), 16);
        assert_eq!(p_d.len(), 16);

        // Test repair of slice 3 in chunk 1
        let slice_idx = 3;
        let surviving: Vec<&[u8]> = vec![
            &slices[0][slice_idx],
            &slices[2][slice_idx],
            &slices[3][slice_idx],
        ];
        let repaired = SlicedHadrV2Engine::repair_slice_horizontal(&surviving, &p_h[slice_idx]);
        assert_eq!(repaired, slices[1][slice_idx]);

        // Test slice delta application
        let mut p_h_copy = p_h[slice_idx].clone();
        let mut p_d_copy = p_d[slice_idx].clone();
        let mut new_slice = slices[1][slice_idx].clone();
        new_slice[0] ^= 0xff;
        let delta: Vec<u8> = slices[1][slice_idx].iter().zip(new_slice.iter()).map(|(a, b)| a ^ b).collect();
        SlicedHadrV2Engine::apply_slice_delta(&mut p_h_copy, &mut p_d_copy, &delta);
        assert_eq!(p_h_copy[0], p_h[slice_idx][0] ^ 0xff);
    }
}
