# OpenAssay Copilot Instructions

## Build, Test, and Lint Commands

**All commands run from repository root.**

### Build
```bash
cargo build                    # Debug build
cargo build --release         # Optimized release build
cargo run --bin pg_server -- 55432    # Start PostgreSQL-compatible server on port 55432
```

### Test
```bash
cargo test                     # Run unit tests only (661 tests)
cargo test --lib              # Same as above; library tests
cargo test --lib <test_name>  # Run single test by function name (e.g., `test_string_concatenation`)
cargo test --test regression  # Run PostgreSQL regression tests (opt-in, not included by default)
cargo test --test differential # Run differential tests (opt-in, not included by default)
```

### Lint
```bash
cargo clippy -- -D warnings    # Clippy (most strict; used in CI)
cargo fmt --check             # Check formatting
cargo fmt                      # Auto-format
```

### Browser/WASM
```bash
scripts/build_wasm.sh         # Build WASM bundle to web/pkg/
cargo run --bin web_server -- 8080  # Start web server on port 8080
```

### Benchmarks
```bash
cargo bench --bench engine_bench     # Microbenchmarks
cargo bench --bench tpch_bench       # TPC-H benchmarks
cargo bench --bench clickbench_bench # ClickBench benchmarks
```

## Architecture Overview

**OpenAssay is a PostgreSQL-compatible query engine, not a database server.** Key implications:

### Core Design Principles
- **Async-native:** All I/O is async via Tokio (native) or web-sys (WASM). No blocking operations, allowing concurrent queries on a single thread.
- **No durability:** Tables are ephemeral. Data lives in APIs, files, and streams—the source of truth is external.
- **No MVCC/locking:** No tuple versioning, vacuum, or deadlock detection needed since there are no concurrent writers.
- **Query engine, not storage engine:** Transforms and joins data on-the-fly; doesn't persist data between restarts.

### Query Pipeline
The query flows through 5 major stages:

1. **Parser** (`src/parser/`) — Lexical and syntactic analysis
   - `lexer.rs`: Tokenization
   - `ast.rs`: AST node definitions (74 types)
   - `sql_parser.rs`: Recursive-descent parser
   - Output: `Statement` AST node

2. **Analyzer** (`src/analyzer/`) — Name binding, type checking, function resolution
   - `binding.rs`: Scope resolution and name binding
   - `types.rs`: Type inference and coercion
   - `functions.rs`: Function signature lookup and validation
   - Output: Validated `Statement` with resolved types

3. **Planner** (`src/planner/`) — Logical and physical query plans
   - `logical.rs`: Logical plan generation
   - `physical.rs`: Physical plan with concrete operations
   - `cost.rs`: Cardinality and cost estimation
   - Output: Physical plan ready for execution

4. **Executor** (`src/executor/`) — Query execution (async)
   - `exec_main.rs`: Dispatch to executor nodes
   - `exec_expr.rs`: Expression evaluation (scalar and aggregate functions)
   - `exec_scan.rs`: Sequential scan nodes
   - `node_*.rs`: Specific operators (joins, aggregation, window functions, sorts, etc.)
   - Output: Row results

5. **Traffic Cop** (`src/tcop/`) — Query dispatch and protocol handling
   - `engine.rs`: Main execution engine (3k lines)
   - `postgres.rs`: PostgreSQL wire protocol handler (5k lines)
   - Calls into analyzer→planner→executor pipeline

### Data Organization
- **Catalog** (`src/catalog/`) — In-memory schema metadata (tables, types, functions, OIDs)
- **Storage** (`src/storage/`) — Ephemeral row storage in memory
  - `tuple.rs`: Scalar values and row representation
  - `heap.rs`: Heap table storage
- **Type System** (`src/utils/adt/`) — Built-in data type operations
  - `json.rs`, `datetime.rs`, `string_functions.rs`, `math_functions.rs`, `misc.rs`
  - 170+ built-in functions

### Async Execution Model
- All executor nodes are async (`async fn execute()`)
- Expression evaluation is async to support `http_get()`, `http_post()`, etc.
- WebSocket and HTTP I/O doesn't block other queries
- Network timeouts are handled via `tokio::time`

### Integration with External Data
- **HTTP Extension** (`CREATE EXTENSION http`) — `http_get()`, `http_post()`, etc.
- **WebSocket Extension** — `ws.connect()`, `ws.send()`, `ws.recv()`
- **Logical Replication** — Subscribe to PostgreSQL publications in real-time

## Design Rules

- **Use a hard cutover approach and never implement backward compatibility.**

## Key Conventions

### Test Patterns
Engine tests use helper functions in `src/tcop/engine_tests.rs`:
```rust
fn run(sql: &str) -> QueryResult                          // Simple single-statement test
fn run_statement(sql: &str, params: &[Option<String>])   // With parameters
fn run_batch(statements: &[&str]) -> Vec<QueryResult>   // Multiple statements
fn with_isolated_state<T>(f: impl FnOnce() -> T) -> T    // Fresh catalog/storage per test
```

Test assertions on `QueryResult`:
```rust
let result = run("SELECT 1");
assert_eq!(result.rows[0][0], ScalarValue::Int(1));
```

For tests needing catalog state:
```rust
fn test_something() {
    let result = with_isolated_state(|| {
        run("CREATE TABLE foo (id INT)");
        run("INSERT INTO foo VALUES (1)")
    });
}
```

### Error Handling
- Use `EngineError` for all runtime errors (defined in `tcop/engine.rs`)
- Parser/lexer return `ParseError`
- Functions return `Result<ScalarValue, EngineError>`
- NULL handling: functions return `ScalarValue::Null` if any input is NULL (PostgreSQL-compatible)

### Built-in Function Implementation
To add a new built-in function:

1. **Register in `src/analyzer/functions.rs`:**
   ```rust
   ("my_function", 1, 2),  // min_args=1, max_args=2
   ```

2. **Add dispatcher in `src/utils/fmgr.rs` `eval_scalar_function()`:**
   ```rust
   "my_function" => my_function_impl(args, ...),
   ```

3. **Implement in appropriate `src/utils/adt/*.rs`:**
   ```rust
   fn my_function_impl(args: &[ScalarValue]) -> Result<ScalarValue, EngineError> {
       // NULL check for any NULL inputs
       if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
           return Ok(ScalarValue::Null);
       }
       // Implementation...
   }
   ```

### Type Casting
Use validated casting functions from `src/utils/adt/int_arithmetic.rs`:
- `validate_int2()`, `validate_int4()` — Range validation for 16-bit and 32-bit integers
- All internally stored as `i64` in `ScalarValue::Int`, but CAST validates ranges

### Date/Time Handling
Date/time parsing in `src/utils/adt/datetime.rs`:
- Supports multiple formats: ISO (YYYY-MM-DD), month names, compact (YYYYMMDD), Julian, year.day
- Smart separator detection: only splits on space if followed by time parts (colons)
- Functions: `parse_datetime_scalar()`, `format_time()`, `parse_datetime_text()`

### Operator Type Awareness
Some operators are polymorphic:
- `||` → string concatenation or JSONB merge (type-checked)
- `-` → subtraction or JSONB key deletion (type-checked)

### Window Function Frame Modes
`ROWS`, `RANGE`, `GROUPS` modes have different semantics:
- **ROWS:** Frame in individual rows
- **RANGE:** Frame in value ranges (sorted by ORDER BY)
- **GROUPS:** Frame in peer groups (rows with equal ORDER BY values)
- **EXCLUDE:** 4 modes — CURRENT ROW, GROUP, TIES, NO OTHERS

Each mode has dedicated implementation in `src/executor/exec_expr.rs`.

### Clippy Configuration
The codebase uses aggressive clippy settings (all/pedantic/nursery deny) with targeted allowlists:
- **Suppressed for codebase reasons:** Too many arguments, cognitive complexity, cast lints (SQL type coercion requires many casts)
- **Suppressed for API reasons:** Documentation lints, module name repetitions, match_same_arms (identical arms clarify intent)
- See `Cargo.toml` `[lints.clippy]` for full list with rationales

### Code Organization
- Large match arms in executor are unavoidable (not subject to `too_many_lines` lint)
- AST nodes legitimately have many boolean fields; `struct_excessive_bools` is allowed
- Async functions must be async for trait/interface reasons; `unused_async` is allowed
- Most files are organized as `mod.rs` with submodule imports

## PostgreSQL Compatibility

OpenAssay passes **12,329 / 12,329 statements** (100%) on 39 PostgreSQL 18 regression test files. Target is PostgreSQL 18 (no backwards compatibility with older versions).

Key limitations (by design):
- No WAL, vacuum, or durability
- No per-connection process model (async instead)
- No table inheritance or partitioning
- No triggers or rules
- No COPY/COPY2 variants (basic COPY TO/FROM works)
- 84 regression test files not included (indexing, transactions, roles, domains, etc.)

See README.md for complete feature matrix and regression test results.

## Debugging Tips

### Enable Tokio Debug Logging
Set environment variable: `RUST_LOG=debug` before running tests or server.

### Inspect Query Plans
Use `EXPLAIN` and `EXPLAIN ANALYZE`:
```sql
EXPLAIN SELECT * FROM table WHERE id = 1;
EXPLAIN ANALYZE SELECT * FROM table WHERE id = 1;
```

### Unit Test Isolation
Tests run with fresh catalog and storage via `with_isolated_state()`. No side effects between tests.

### WASM Builds
Requires `wasm-pack`: `cargo install wasm-pack`. Then `scripts/build_wasm.sh`.

### PostgreSQL Integration Tests
```bash
cd tests/integration
npm install
npm test
