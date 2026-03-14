#!/usr/bin/env bash
# clickbench_benchmark.sh — Run the full ClickBench benchmark and output
# results in the official ClickBench JSON format.
#
# Usage:
#   ./scripts/clickbench_benchmark.sh [ROWS]
#
# ROWS defaults to 0 (full dataset, ~100M rows).
# Set ROWS to e.g. 100000 for a quick test run.
#
# Prerequisites:
#   - cargo (Rust toolchain)
#   - ~15 GB disk for the full dataset
#   - Recommended: AWS c6a.4xlarge (16 vCPU, 32 GB RAM, 500 GB gp2)
#
# Output:
#   results/clickbench_results.json — ClickBench-format JSON
#   results/clickbench_detailed.txt — per-query timing details
set -euo pipefail

ROWS="${1:-0}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$PROJECT_ROOT/results"
DATA_DIR="$PROJECT_ROOT/data/clickbench"

mkdir -p "$RESULTS_DIR"

# ── Step 0: Build release binary ─────────────────────────────────────────
echo "==> Building OpenAssay (release)..."
cd "$PROJECT_ROOT"
cargo build --release --bin openassay-server 2>&1 || {
    echo "⚠  No server binary; will use the benchmark test harness instead."
}

# ── Step 1: Download data ────────────────────────────────────────────────
echo "==> Ensuring ClickBench data is available..."
"$SCRIPT_DIR/load_clickbench.sh" "$ROWS"

# Determine which TSV to use
if [ "$ROWS" -gt 0 ] 2>/dev/null; then
    TSV_FILE="$DATA_DIR/hits_subset.tsv"
else
    TSV_FILE="$DATA_DIR/hits.tsv"
fi

ROW_COUNT=$(wc -l < "$TSV_FILE")
echo "==> Dataset: $ROW_COUNT rows from $TSV_FILE"

# ── Step 2: Run the benchmark ────────────────────────────────────────────
echo ""
echo "============================================"
echo "  ClickBench Benchmark — OpenAssay"
echo "  Rows: $ROW_COUNT"
echo "  Date: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "  Host: $(hostname)"
echo "  CPU:  $(nproc) cores"
echo "  RAM:  $(free -h 2>/dev/null | awk '/Mem:/{print $2}' || echo 'unknown')"
echo "============================================"
echo ""

# Run via cargo test with real data, 3 iterations
export CLICKBENCH_ROWS="$ROW_COUNT"

# We'll use a dedicated Rust binary for precise timing with 3 runs
cat > "$PROJECT_ROOT/scripts/_clickbench_runner.rs" << 'RUSTEOF'
// Temporary ClickBench runner — executed via `cargo run --example`
// Produces ClickBench-format JSON output.
use std::time::Instant;
use std::io::{BufRead, BufReader};
use std::fs::File;
use std::path::Path;

fn main() {
    eprintln!("This runner requires the openassay library. Use cargo test instead.");
    eprintln!("Run: CLICKBENCH_ROWS=0 cargo test --release --test benchmark clickbench_realdata -- --nocapture");
}
RUSTEOF

echo "==> Running benchmark (3 runs per query)..."
echo "    Using: cargo test --release"
echo ""

# Capture all 3 runs
DETAIL_FILE="$RESULTS_DIR/clickbench_detailed.txt"
> "$DETAIL_FILE"

# Run the benchmark 3 times and capture timing
declare -a RUN1 RUN2 RUN3

for RUN in 1 2 3; do
    echo "--- Run $RUN/3 ---"
    OUTPUT=$(CLICKBENCH_ROWS="$ROW_COUNT" cargo test --release --test benchmark clickbench_realdata -- --nocapture 2>&1)
    echo "$OUTPUT" >> "$DETAIL_FILE"
    echo "" >> "$DETAIL_FILE"
    
    # Parse timings — format: "  Q00:     0.17 ms  (1 rows)"
    TIMINGS=$(echo "$OUTPUT" | grep -oP 'Q\d{2}:\s+[\d.]+\s+ms' | awk '{print $2}')
    
    # Also capture load time
    LOAD_TIME=$(echo "$OUTPUT" | grep -oP 'Loaded \d+ rows in [\d.]+s' | grep -oP '[\d.]+s' | sed 's/s//')
    
    idx=0
    while IFS= read -r ms; do
        # Convert ms to seconds
        sec=$(echo "scale=6; $ms / 1000" | bc)
        case $RUN in
            1) RUN1[$idx]="$sec" ;;
            2) RUN2[$idx]="$sec" ;;
            3) RUN3[$idx]="$sec" ;;
        esac
        idx=$((idx + 1))
    done <<< "$TIMINGS"
    
    QUERY_COUNT=$idx
    echo "    Parsed $QUERY_COUNT query timings"
    
    if [ "$RUN" -eq 1 ] && [ -n "${LOAD_TIME:-}" ]; then
        LOAD_SECONDS="$LOAD_TIME"
        echo "    Load time: ${LOAD_SECONDS}s"
    fi
done

# ── Step 3: Generate JSON output ─────────────────────────────────────────
echo ""
echo "==> Generating results JSON..."

# Calculate data size (approximate — in-memory)
DATA_SIZE=$(stat --format=%s "$TSV_FILE" 2>/dev/null || stat -f%z "$TSV_FILE" 2>/dev/null || echo 0)

MACHINE=$(hostname)
DATE=$(date -u +%Y-%m-%d)

JSON_FILE="$RESULTS_DIR/clickbench_results.json"

cat > "$JSON_FILE" << HEADER
{
    "system": "OpenAssay",
    "date": "$DATE",
    "machine": "$MACHINE",
    "cluster_size": 1,
    "proprietary": "no",
    "hardware": "cpu",
    "tuned": "no",
    "tags": ["Rust","row-oriented","embedded","WASM"],
    "load_time": ${LOAD_SECONDS:-0},
    "data_size": $DATA_SIZE,
    "result": [
HEADER

# Write result array
for ((i = 0; i < QUERY_COUNT; i++)); do
    r1="${RUN1[$i]:-null}"
    r2="${RUN2[$i]:-null}"
    r3="${RUN3[$i]:-null}"
    
    if [ $i -lt $((QUERY_COUNT - 1)) ]; then
        echo "        [$r1, $r2, $r3]," >> "$JSON_FILE"
    else
        echo "        [$r1, $r2, $r3]" >> "$JSON_FILE"
    fi
done

cat >> "$JSON_FILE" << FOOTER
]
}
FOOTER

echo ""
echo "============================================"
echo "  Results saved to:"
echo "    $JSON_FILE"
echo "    $DETAIL_FILE"
echo "============================================"
echo ""

# ── Step 4: Print summary comparison ─────────────────────────────────────
echo "=== Quick Comparison (best of 3 runs, seconds) ==="
echo ""
printf "%-5s  %12s  %12s  %12s\n" "Query" "OpenAssay" "DuckDB*" "SQLite*"
printf "%-5s  %12s  %12s  %12s\n" "-----" "------------" "------------" "------------"

# DuckDB best-of-3 from ClickBench (c6a.4xlarge, 100M rows)
DUCKDB_BEST=(0.001 0.003 0.017 0.032 0.241 0.272 0.005 0.004 0.331 0.482 0.064 0.073 0.305 0.602 0.347 0.296 0.724 0.514 1.370 0.003 0.474 0.488 1.157 0.182 0.020 0.066 0.016 0.390 7.177 0.024 0.258 0.326 1.622 1.592 1.772 0.395 0.023 0.007 0.012 0.053 0.005 0.005 0.008)

# SQLite best-of-3 from ClickBench (c6a.4xlarge, 100M rows)  
SQLITE_BEST=(1.653 401.72 405.31 5.997 47.149 520.148 8.915 402.762 458.736 473.582 407.574 407.985 415.034 418.097 420.864 36.148 452.644 421.4 496.468 4.868 403.031 403.652 400.411 403.327 403.405 403.912 404.072 381.732 null 602.194 414.865 413.855 477.505 510.459 554.503 458.941 0.800 0.656 0.324 1.701 0.398 0.425 0.688)

for ((i = 0; i < QUERY_COUNT && i < 43; i++)); do
    # Take best (minimum) of our 3 runs
    r1="${RUN1[$i]:-999}"
    r2="${RUN2[$i]:-999}"
    r3="${RUN3[$i]:-999}"
    best=$(echo "$r1 $r2 $r3" | tr ' ' '\n' | sort -n | head -1)
    
    dk="${DUCKDB_BEST[$i]:-n/a}"
    sq="${SQLITE_BEST[$i]:-n/a}"
    
    printf "Q%02d    %12s  %12s  %12s\n" "$i" "$best" "$dk" "$sq"
done

echo ""
echo "* DuckDB/SQLite numbers are from ClickBench c6a.4xlarge (100M rows)."
echo "  Your numbers are on $ROW_COUNT rows on $(hostname)."
echo "  Scale accordingly — these are NOT directly comparable unless you"
echo "  run with the full dataset on the same hardware."

# Cleanup temp file
rm -f "$PROJECT_ROOT/scripts/_clickbench_runner.rs"
