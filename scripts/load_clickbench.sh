#!/usr/bin/env bash
# load_clickbench.sh — Download the ClickBench "hits" dataset.
#
# Usage:
#   ./scripts/load_clickbench.sh [MAX_ROWS]
#
# MAX_ROWS defaults to 1000000 (1M) for CI.  Pass 0 for the full dataset (~100M rows).
#
# The data lands in data/clickbench/ as a TSV file.
set -euo pipefail

MAX_ROWS="${1:-1000000}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data/clickbench"
FULL_FILE="$DATA_DIR/hits.tsv"
SUBSET_FILE="$DATA_DIR/hits_subset.tsv"
URL="https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz"

echo "==> ClickBench data download"
echo "    Output: $DATA_DIR"
mkdir -p "$DATA_DIR"

# ── Step 1: Download if not cached ───────────────────────────────────────
if [ ! -f "$FULL_FILE" ] && [ ! -f "${FULL_FILE}.gz" ]; then
    echo "==> Downloading hits.tsv.gz (~1.2 GB compressed)..."
    curl -L --progress-bar -o "${FULL_FILE}.gz" "$URL"
fi

# ── Step 2: Decompress if needed ─────────────────────────────────────────
if [ -f "${FULL_FILE}.gz" ] && [ ! -f "$FULL_FILE" ]; then
    echo "==> Decompressing..."
    gunzip -k "${FULL_FILE}.gz"
fi

# ── Step 3: Create subset for CI ─────────────────────────────────────────
if [ "$MAX_ROWS" -gt 0 ] 2>/dev/null; then
    echo "==> Creating ${MAX_ROWS}-row subset..."
    head -n "$MAX_ROWS" "$FULL_FILE" > "$SUBSET_FILE"
    ROW_COUNT=$(wc -l < "$SUBSET_FILE")
    echo "    Subset: $ROW_COUNT rows"
else
    echo "==> Using full dataset"
    ROW_COUNT=$(wc -l < "$FULL_FILE")
    echo "    Full: $ROW_COUNT rows"
fi

# ── Step 4: Print summary ────────────────────────────────────────────────
echo ""
echo "==> ClickBench data ready in $DATA_DIR"
ls -lh "$DATA_DIR"/*.tsv 2>/dev/null || echo "    (no TSV files found)"
echo ""
echo "To load into OpenAssay, use COPY FROM in a psql session:"
if [ "$MAX_ROWS" -gt 0 ] 2>/dev/null; then
    echo "    COPY hits FROM '$SUBSET_FILE' DELIMITER E'\\t';"
else
    echo "    COPY hits FROM '$FULL_FILE' DELIMITER E'\\t';"
fi
