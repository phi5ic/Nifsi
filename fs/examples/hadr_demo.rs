//!THIS IS AI GENERATED

use nifsi_fs::hadr::{
    CasRepairEngine, SliceDeltaJournal, SliceGuideRNA, StrandDetector, StrandEncoder, SLICE_SIZE,
};
use std::env;

fn main() {
    println!("\x1b[1;36m============================================================\x1b[0m");
    println!("\x1b[1;36m  HADR-v2: SUB-PACKET SLICED ARRAY CODING DEMONSTRATOR     \x1b[0m");
    println!("\x1b[1;35m  Biomimetic Self-Healing inspired by Deinococcus radiodurans\x1b[0m");
    println!("\x1b[1;36m============================================================\x1b[0m\n");

    let mut payload = Vec::with_capacity(48 * 1024);
    let base_text = env::args().nth(1).unwrap_or_else(|| {
        "Nifsi OS HADR-v2: 4KB Sub-Packet Sliced Array Coding with Horizontal and Diagonal Parity! ".to_string()
    });
    while payload.len() < 48 * 1024 {
        payload.extend_from_slice(base_text.as_bytes());
    }
    payload.truncate(48 * 1024); // 48 KB total payload

    let file_id = [0x5A; 16];
    let chunk_size = 16 * 1024; // 16 KB chunks = 4 sub-packet slices of 4KB each
    let cohort_size = 3;        // 3 chunks per parity cohort (48 KB cohort)
    let slice_size = SLICE_SIZE; // 4096 bytes per slice

    println!("\x1b[1;33m[STEP 1] Ingesting Payload & Synthesizing Biological Strand\x1b[0m");
    println!("Payload Size : {} bytes ({} KB)", payload.len(), payload.len() / 1024);
    println!("Chunk Size   : {} KB ({} slices @ 4KB)", chunk_size / 1024, chunk_size / slice_size);
    println!("Cohort Size  : {} chunks (48 KB cohort array)\n", cohort_size);

    let strand = StrandEncoder::encode_file_with_slice_size(file_id, &payload, chunk_size, cohort_size, slice_size)
        .expect("Failed to encode biological strand");

    println!("\x1b[1;32m✓ Successfully synthesized HADR-v2 strand with {} chunks and {} parity spacers.\x1b[0m\n", 
        strand.chunks.len(), strand.spacers.len());

    println!("\x1b[1;33m[STEP 2] 2D Sub-Packet Sliced Matrix & Parity Equations\x1b[0m");
    for (i, c) in strand.chunks.iter().enumerate() {
        println!("  ├─ \x1b[1;37mChunk #{:<2}\x1b[0m ID: \x1b[36m{}\x1b[0m Cohort: \x1b[35m{}\x1b[0m Slices: {}", 
            i, hex_short(&c.chunk_id), hex_short(&c.cohort_id), c.slices.len());
        for (s_idx, s) in c.slices.iter().enumerate() {
            let left_str = s.left_arm.map(|a| hex_short(&a)).unwrap_or_else(|| "HEAD".to_string());
            let right_str = s.right_arm.map(|a| hex_short(&a)).unwrap_or_else(|| "TAIL".to_string());
            println!("  │  ├─ \x1b[36mSlice [{}, {}]\x1b[0m Hash: \x1b[32m{}\x1b[0m Arms: [L: \x1b[33m{}\x1b[0m, R: \x1b[33m{}\x1b[0m] ({} B)",
                i, s_idx, hex_short(&s.slice_hash), left_str, right_str, s.data.len());
        }
    }
    let spacer = &strand.spacers[0];
    println!("  └─ \x1b[1;35mCohort Parity Spacer\x1b[0m:");
    println!("     ├─ Horizontal Parity P_h: {} slices (4KB each)", spacer.horizontal_parity_slices.len());
    println!("     └─ Diagonal Parity P_d  : {} slices (4KB each)\n", spacer.diagonal_parity_slices.len());

    // STEP 3: Radiation Damage Simulation on Single 4KB Slice
    let target_chunk_idx = 1;
    let target_slice_idx = 2;
    println!("\x1b[1;31m[STEP 3] SIMULATING TARGETED RADIATION DAMAGE: CORRUPTING SLICE [1, 2] (4KB)\x1b[0m");
    let mut corrupted_chunk = strand.chunks[target_chunk_idx].clone();
    let original_slice = corrupted_chunk.slices[target_slice_idx].clone();
    // Mutate 64 bytes inside slice payload
    corrupted_chunk.slices[target_slice_idx].data[50..114].fill(0xFF);
    println!("  ✗ Radiation pulse struck Chunk #1, Slice #2 at offset 50..114!\n");

    // Optional delta computation demonstration
    let delta = SliceDeltaJournal::create_delta(
        &original_slice,
        &corrupted_chunk.slices[target_slice_idx].data[50..114],
        50,
        corrupted_chunk.cohort_id,
        target_chunk_idx as u16,
    ).expect("Delta creation failed");
    println!("  Delta journal computed for radiation damage: diff len={}\n", delta.delta_bytes.len());

    // STEP 4: Slice Break Detection
    println!("\x1b[1;33m[STEP 4] Autonomous Sub-Packet Slice Break Detection (No Central Master)\x1b[0m");
    let breaks = StrandDetector::detect_slice_breaks(&corrupted_chunk, 4);
    if breaks.is_empty() {
        panic!("Fatal: Slice break detector failed to pinpoint corrupted slice!");
    }
    let detected_break = &breaks[0];
    println!("  \x1b[1;31m⚠ SUB-PACKET SLICE CORRUPTION DETECTED!\x1b[0m");
    println!("  Target Chunk Index: #{}", detected_break.chunk_sequence_idx);
    println!("  Corrupted Slice   : #{}", detected_break.slice_idx);
    println!("  Break Reason      : {:?}", detected_break.reason);
    println!("  Expected Hash     : \x1b[32m{}\x1b[0m", hex_short(&detected_break.expected_hash.unwrap()));
    println!();

    // STEP 5: Guide RNA Synthesis & Horizontal CRISPR/Cas Repair
    println!("\x1b[1;33m[STEP 5] Synthesizing SliceGuideRNA & Invoking CasRepairEngine (k x 4KB Read)\x1b[0m");
    let guide = SliceGuideRNA::from_break(detected_break);
    println!("  Synthesized SliceGuideRNA targeting Cohort \x1b[35m{}\x1b[0m Row #{}", 
        hex_short(&guide.cohort_id), guide.slice_idx);

    let mut surviving_row_slices = Vec::new();
    for c in &strand.chunks {
        if c.sequence_idx != target_chunk_idx as u32 {
            surviving_row_slices.push(c.slices[target_slice_idx].clone());
        }
    }

    println!("  Surviving Slices Transferred : {} x 4KB = {} KB", surviving_row_slices.len(), surviving_row_slices.len() * 4);
    println!("  Parity Slice Transferred    : 1 x 4KB = 4 KB");
    println!("  \x1b[1;32mTotal Network Bandwidth     : 12 KB (vs 48 KB in monolithic HADR-v1, 75% savings!)\x1b[0m");

    let p_h = &spacer.horizontal_parity_slices[target_slice_idx];
    let resurrected_slice = CasRepairEngine::repair_slice(&guide, &surviving_row_slices, p_h)
        .expect("CasRepairEngine horizontal repair failed!");

    assert_eq!(resurrected_slice.data, original_slice.data, "Bit-for-bit payload mismatch!");
    assert_eq!(resurrected_slice.slice_hash, original_slice.slice_hash, "Hash mismatch!");
    println!("  \x1b[1;32m✓ Slice [1, 2] RESURRECTED BIT-FOR-BIT VIA HORIZONTAL PARITY!\x1b[0m");
    println!("    Hash Verification: \x1b[32m{}\x1b[0m == \x1b[32m{}\x1b[0m", 
        hex_short(&resurrected_slice.slice_hash), hex_short(&original_slice.slice_hash));
    println!();

    // STEP 6: Alternate Diagonal Array Parity Recovery
    println!("\x1b[1;33m[STEP 6] Alternate Degraded Read Recovery via Diagonal Array Parity P_d\x1b[0m");
    let m = 4; // 4 slices per chunk
    let d = (target_slice_idx as isize - target_chunk_idx as isize).rem_euclid(m as isize) as usize;
    let p_d = &spacer.diagonal_parity_slices[d];

    let mut surviving_diagonal_slices = Vec::new();
    for (j, c) in strand.chunks.iter().enumerate() {
        if j != target_chunk_idx {
            let row = (d + j) % m;
            surviving_diagonal_slices.push(c.slices[row].clone());
        }
    }

    let resurrected_diag = CasRepairEngine::repair_slice_diagonal(
        &guide,
        &surviving_diagonal_slices,
        p_d,
        &spacer.member_chunk_ids,
        m,
    ).expect("Diagonal CasRepairEngine repair failed!");

    assert_eq!(resurrected_diag.data, original_slice.data);
    println!("  \x1b[1;32m✓ Slice [1, 2] ALSO RESURRECTED BIT-FOR-BIT VIA DIAGONAL ARRAY PARITY!\x1b[0m\n");

    // STEP 7: Re-Annealing & Final File Verification
    println!("\x1b[1;33m[STEP 7] Re-Annealing Strand & Bit-For-Bit Payload Verification\x1b[0m");
    let mut repaired_chunks = strand.chunks.clone();
    repaired_chunks[target_chunk_idx].slices[target_slice_idx] = resurrected_slice;

    let assembled = StrandEncoder::assemble_file(repaired_chunks)
        .expect("Strand re-annealing failed!");
    assert_eq!(assembled, payload);

    println!("\x1b[1;32m============================================================\x1b[0m");
    println!("\x1b[1;32m   SUCCESS: 100% BIT-FOR-BIT HADR-v2 RECONSTRUCTION CONFIRMED! \x1b[0m");
    println!("\x1b[1;32m============================================================\x1b[0m");
}

fn hex_short(b: &[u8]) -> String {
    b.iter().take(4).map(|x| format!("{:02x}", x)).collect::<Vec<_>>().join("") + ".."
}

