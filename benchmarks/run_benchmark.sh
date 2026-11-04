#!/usr/bin/env bash
# ==============================================================================
# Nifsi OS — Scientific Benchmark & Chaos Stress Suite Runner
# ==============================================================================
# Executes the full automated empirical stress test suite:
# 1. Storage microbenchmarks across 64KB, 256KB, 1MB, 4MB, 16MB cohorts
# 2. Multi-node virtual consensus mesh (4 nodes) + physical Raspberry Pi 3B+
# 3. High-concurrency FUSE ingestion across parallel workers
# 4. Partial slice-delta journaling and write amplification reduction
# 5. Chaos injection & autonomous CRISPR/Cas homology arm repair
# 6. Ungraceful node failure, majority quorum survival & catch-up replay
# 7. Physical ARM Cortex-A53 + Renesas RA4M1 cipher coprocessor verification
# 8. Generation of publication-grade BENCHMARK_SCIENTIFIC_REPORT.md
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# ANSI color codes
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RESET='\033[0m'

echo -e "${CYAN}==============================================================================${RESET}"
echo -e "${CYAN}             NIFSI-CORE SCIENTIFIC BENCHMARK & CHAOS STRESS SUITE             ${RESET}"
echo -e "${CYAN}==============================================================================${RESET}"
echo -e "Base directory: ${CORE_DIR}"
echo -e "Harness script: ${SCRIPT_DIR}/scientific_benchmark_suite.py"
echo ""

# Ensure release binaries are built
if [ ! -f "${CORE_DIR}/target/release/nifsid" ] || [ ! -f "${CORE_DIR}/target/release/nifsi" ] || [ ! -f "${CORE_DIR}/target/release/hadr_bench" ]; then
    echo -e "${YELLOW}Building release binaries (nifsid, nifsi, hadr_bench)...${RESET}"
    cd "${CORE_DIR}"
    cargo build --release --bin nifsid --bin nifsi --bench hadr_bench
    ln -sf $(ls -t "${CORE_DIR}"/target/release/deps/hadr_bench-* | grep -v '\.d$' | head -n1) "${CORE_DIR}/target/release/hadr_bench"
    echo -e "${GREEN}✓ Release binaries compiled successfully.${RESET}\n"
fi

# Run the master python benchmark suite
exec python3 "${SCRIPT_DIR}/scientific_benchmark_suite.py" "$@"
