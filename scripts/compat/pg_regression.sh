#!/bin/bash
#
# PostgreSQL Regression Test Compatibility Runner
#
# This script runs PostgreSQL regression tests against postrust to measure
# compatibility. It builds postrust, starts pg_server on a temporary port,
# runs each test .sql file, and compares output against expected results.
#
# Usage: ./scripts/compat/pg_regression.sh
#

set -euo pipefail

# Configuration
POSTRUST_PORT=${POSTRUST_PORT:-55433}
POSTRUST_HOST=${POSTRUST_HOST:-127.0.0.1}
TEST_DB=${TEST_DB:-postgres}
TEST_DIR="tests/regression/pg_compat"
SQL_DIR="$TEST_DIR/sql"
EXPECTED_DIR="$TEST_DIR/expected"
RESULTS_DIR="$TEST_DIR/results"
LOG_FILE="$RESULTS_DIR/pg_regression.log"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Stats
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Cleanup function
cleanup() {
    echo -e "\n${BLUE}Cleaning up...${NC}"
    if [[ -n "${PG_SERVER_PID:-}" ]]; then
        echo "Stopping pg_server (PID: $PG_SERVER_PID)"
        kill $PG_SERVER_PID 2>/dev/null || true
        wait $PG_SERVER_PID 2>/dev/null || true
    fi
}

# Set up signal handlers
trap cleanup EXIT INT TERM

log() {
    echo "$@" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}ERROR: $@${NC}" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}SUCCESS: $@${NC}" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}WARNING: $@${NC}" | tee -a "$LOG_FILE"
}

log_info() {
    echo -e "${BLUE}INFO: $@${NC}" | tee -a "$LOG_FILE"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    if ! command -v psql >/dev/null 2>&1; then
        log_error "psql not found. Please install PostgreSQL client tools."
        exit 1
    fi
    
    if ! command -v cargo >/dev/null 2>&1; then
        log_error "cargo not found. Please install Rust."
        exit 1
    fi
    
    if [[ ! -d "$SQL_DIR" ]]; then
        log_error "Test SQL directory not found: $SQL_DIR"
        exit 1
    fi
    
    if [[ ! -d "$EXPECTED_DIR" ]]; then
        log_error "Expected results directory not found: $EXPECTED_DIR"
        exit 1
    fi
    
    log_success "Prerequisites check passed"
}

# Build postrust
build_postrust() {
    log_info "Building postrust..."
    if ! cargo build --release --bin pg_server; then
        log_error "Failed to build postrust pg_server"
        exit 1
    fi
    log_success "postrust built successfully"
}

# Start pg_server
start_pg_server() {
    log_info "Starting pg_server on $POSTRUST_HOST:$POSTRUST_PORT..."
    
    # Create results directory
    mkdir -p "$RESULTS_DIR"
    rm -f "$LOG_FILE"
    
    # Start pg_server in background
    cargo run --release --bin pg_server -- "$POSTRUST_HOST:$POSTRUST_PORT" > "$RESULTS_DIR/pg_server.log" 2>&1 &
    PG_SERVER_PID=$!
    
    # Wait for server to start
    log_info "Waiting for pg_server to start..."
    for i in {1..30}; do
        if psql -h "$POSTRUST_HOST" -p "$POSTRUST_PORT" -d "$TEST_DB" -c "SELECT 1;" >/dev/null 2>&1; then
            log_success "pg_server started successfully (PID: $PG_SERVER_PID)"
            return 0
        fi
        sleep 1
    done
    
    log_error "pg_server failed to start within 30 seconds"
    if [[ -f "$RESULTS_DIR/pg_server.log" ]]; then
        log_error "Server log:"
        cat "$RESULTS_DIR/pg_server.log"
    fi
    exit 1
}

# Run a single test
run_test() {
    local test_file="$1"
    local test_name="${test_file%.sql}"
    local sql_file="$SQL_DIR/$test_file"
    local expected_file="$EXPECTED_DIR/${test_name}.out"
    local result_file="$RESULTS_DIR/${test_name}.out"
    local diff_file="$RESULTS_DIR/${test_name}.diff"
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo -n "Running $test_name... "
    
    # Check if expected file exists
    if [[ ! -f "$expected_file" ]]; then
        log_warning "No expected output file for $test_name, skipping"
        return
    fi
    
    # Run the test
    if psql -h "$POSTRUST_HOST" -p "$POSTRUST_PORT" -d "$TEST_DB" -f "$sql_file" > "$result_file" 2>&1; then
        # Compare output with expected
        if diff -u "$expected_file" "$result_file" > "$diff_file" 2>&1; then
            echo -e "${GREEN}PASS${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
            rm -f "$diff_file"  # Clean up successful diffs
        else
            echo -e "${RED}FAIL (output mismatch)${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            log "  Diff saved to $diff_file"
        fi
    else
        echo -e "${RED}FAIL (execution error)${NC}"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        log "  Error output in $result_file"
    fi
}

# Run all tests
run_tests() {
    log_info "Running PostgreSQL regression tests..."
    
    # Get list of test files
    local test_files
    test_files=($(ls "$SQL_DIR"/*.sql 2>/dev/null | sort | xargs -n1 basename))
    
    if [[ ${#test_files[@]} -eq 0 ]]; then
        log_error "No test files found in $SQL_DIR"
        exit 1
    fi
    
    log_info "Found ${#test_files[@]} test files"
    
    # Run each test
    for test_file in "${test_files[@]}"; do
        run_test "$test_file"
    done
}

# Generate report
generate_report() {
    local pass_rate=0
    if [[ $TOTAL_TESTS -gt 0 ]]; then
        pass_rate=$(( (PASSED_TESTS * 100) / TOTAL_TESTS ))
    fi
    
    log ""
    log "=========================================="
    log "PostgreSQL Compatibility Test Results"
    log "=========================================="
    log "Total tests:    $TOTAL_TESTS"
    log "Passed:         $PASSED_TESTS"
    log "Failed:         $FAILED_TESTS"
    log "Pass rate:      ${pass_rate}%"
    log "=========================================="
    
    if [[ $FAILED_TESTS -gt 0 ]]; then
        log ""
        log_warning "Failed tests (check $RESULTS_DIR for details):"
        find "$RESULTS_DIR" -name "*.diff" -exec basename {} .diff \; | sort
    fi
    
    log ""
    log_info "Detailed results saved to: $RESULTS_DIR"
    log_info "Full log saved to: $LOG_FILE"
    
    # Return non-zero if any tests failed for CI/automation
    return $FAILED_TESTS
}

# Main function
main() {
    log_info "Starting PostgreSQL regression test suite for postrust"
    log_info "Test port: $POSTRUST_PORT"
    log_info "Test database: $TEST_DB"
    log_info "Results directory: $RESULTS_DIR"
    log ""
    
    check_prerequisites
    build_postrust
    start_pg_server
    run_tests
    generate_report
}

# Run main function if script is executed directly
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi