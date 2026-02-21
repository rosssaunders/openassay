#!/usr/bin/env bash
# load_tpch.sh — Download, build, and run TPC-H dbgen to produce CSV data.
#
# Usage:
#   ./scripts/load_tpch.sh [SCALE_FACTOR]
#
# SCALE_FACTOR defaults to 0.01 (~10 MB).  Use 0.1 for local profiling (~100 MB).
#
# The generated CSVs land in data/tpch/sf<SF>/ and can be loaded via COPY FROM.
set -euo pipefail

SCALE_FACTOR="${1:-0.01}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DATA_DIR="$PROJECT_ROOT/data/tpch/sf${SCALE_FACTOR}"
DBGEN_DIR="$PROJECT_ROOT/.cache/tpch-dbgen"

echo "==> TPC-H data generation (SF=$SCALE_FACTOR)"
echo "    Output: $DATA_DIR"

# ── Step 1: Clone / update dbgen ──────────────────────────────────────────
if [ ! -d "$DBGEN_DIR" ]; then
    echo "==> Cloning TPC-H dbgen..."
    mkdir -p "$(dirname "$DBGEN_DIR")"
    git clone --depth 1 https://github.com/electrum/tpch-dbgen.git "$DBGEN_DIR"
fi

# ── Step 2: Build dbgen ──────────────────────────────────────────────────
echo "==> Building dbgen..."
pushd "$DBGEN_DIR" > /dev/null
if [ ! -f dbgen ]; then
    make -j"$(nproc 2>/dev/null || echo 4)"
fi
popd > /dev/null

# ── Step 3: Generate data ────────────────────────────────────────────────
echo "==> Generating TPC-H data at SF=$SCALE_FACTOR..."
mkdir -p "$DATA_DIR"
pushd "$DATA_DIR" > /dev/null

# dbgen puts trailing pipe separators — we strip them
"$DBGEN_DIR/dbgen" -s "$SCALE_FACTOR" -f

# Convert .tbl files to .csv with proper formatting
for tbl in *.tbl; do
    base="${tbl%.tbl}"
    # Remove trailing '|' from each line
    sed 's/|$//' "$tbl" > "${base}.csv"
    rm "$tbl"
    echo "    Generated ${base}.csv ($(wc -l < "${base}.csv") rows)"
done

popd > /dev/null

# ── Step 4: Print summary ────────────────────────────────────────────────
echo ""
echo "==> TPC-H data ready in $DATA_DIR"
echo "    Files:"
ls -lh "$DATA_DIR"/*.csv 2>/dev/null || echo "    (no CSV files found)"
echo ""
echo "To load into OpenAssay, use COPY FROM in a psql session:"
echo "    COPY region    FROM '$DATA_DIR/region.csv'    DELIMITER '|';"
echo "    COPY nation    FROM '$DATA_DIR/nation.csv'    DELIMITER '|';"
echo "    COPY part      FROM '$DATA_DIR/part.csv'      DELIMITER '|';"
echo "    COPY supplier  FROM '$DATA_DIR/supplier.csv'  DELIMITER '|';"
echo "    COPY partsupp  FROM '$DATA_DIR/partsupp.csv'  DELIMITER '|';"
echo "    COPY customer  FROM '$DATA_DIR/customer.csv'  DELIMITER '|';"
echo "    COPY orders    FROM '$DATA_DIR/orders.csv'    DELIMITER '|';"
echo "    COPY lineitem  FROM '$DATA_DIR/lineitem.csv'  DELIMITER '|';"
