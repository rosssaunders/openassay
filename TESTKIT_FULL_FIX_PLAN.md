# OpenAssay Testkit — Full-Fix Plan

Goal: make OpenAssay a spec-conforming in-process Postgres for unit-testing a
Postgres-compatible Rust library. Real Postgres is still authoritative for
integration tests; OpenAssay is for fast, isolated, per-test runs.

Written against the gap list from the tokio-postgres / raw-pgwire audit run on
2026-04-18. The plan says what "spec-correct" means for each subsystem, what
specifically changes in the code, rough size, and dependencies. No shortcuts:
each item lands the behavior you'd actually see talking to Postgres 18.

Exit criterion for *every* phase: the existing 12,329-statement regression suite
(`tests/regression/pg_compat.rs`) still passes 100%, and the WASM build
(`scripts/build_wasm.sh`) still succeeds.

Size key: **S** = hours, **M** = a day or two, **L** = a week, **L+** = multi-week.

---

## Phase 1 — Unblock Rust ORMs (no parameterised query works today)

These four together are the reason `client.query(&stmt, &[&42i32])` crashes
every tokio-postgres/sqlx/diesel-async caller with a stack overflow.

### 1.1 Catalog bootstrap uses fixed, PG-canonical OIDs — **M**

**Gap:** `pg_catalog` and `public` schema OIDs come from `OidGenerator::default()`
starting at `FIRST_NORMAL_OID = 16_384` (`src/catalog/oid.rs:5`). Meanwhile
`pg_type` rows hardcode `typnamespace = 11`
(`src/executor/exec_main/table_functions.rs:1532-1551`). The JOIN

```sql
SELECT t.typname, t.typtype, ...
FROM pg_catalog.pg_type t
INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
WHERE t.oid = $1
```

— exactly the query tokio-postgres runs to resolve an unknown type OID —
returns zero rows. The client re-queries, nests futures, stack-overflows.

**Spec-correct behavior:** Postgres reserves low OIDs for bootstrap catalog
objects. `pg_catalog` nspname = 11 (`FirstNormalObjectId` in `access/transam.h`
is 16384; catalog objects are below it). `public` has OID 2200.

**Change:**
- `src/catalog/oid.rs`: add `pub const PG_CATALOG_NAMESPACE_OID: Oid = 11;
  pub const PUBLIC_NAMESPACE_OID: Oid = 2200;`
- `src/catalog/mod.rs::new_bootstrap`: don't call `create_schema` through the
  OID generator. Insert `pg_catalog` and `public` with the canonical fixed OIDs
  directly. The generator stays at `FIRST_NORMAL_OID` for user-created objects.
- `table_functions.rs` virtual rows for `pg_namespace`, `pg_class`,
  `pg_attribute`: emit the fixed OIDs for builtins.

Verified before filing: `FIRST_NORMAL_OID` appears only in
`src/catalog/oid.rs` (constant and default). Nothing downstream assumes
`pg_catalog` is the first allocated OID, so switching to fixed OIDs is a
local change.

**Depends on:** nothing. **Unblocks:** 1.2, 1.3.

### 1.2 `pg_type` carries every built-in type the engine can produce — **M**

**Gap:** `pg_type` virtual rows list seven types: bool, int8, text, float8,
date, timestamp, numeric (`table_functions.rs:1531-1551`). Every client that
resolves an OID outside this set either recurses or falls back to text. No
int2, int4, float4, varchar, uuid, bytea, timestamptz, json, jsonb, oid, name,
void, interval, time, timetz, any array type, any range type.

**Spec-correct behavior:** Postgres ships ~400 entries in `pg_type` at cluster
init. For a pg-compatible test engine, the shippable subset is the OIDs the
planner/executor can produce. The canonical OIDs and `typlen`/`typbyval`/
`typtype`/`typcategory` values are fixed by `src/include/catalog/pg_type.dat` in
the Postgres source tree.

**Change:**
- New file `src/catalog/builtin_types.rs`: `pub struct BuiltinType { oid, name,
  typlen, typbyval, typtype, typcategory, typelem, typarray, typbasetype,
  typinput, typoutput, typnamespace }` plus a `pub const BUILTIN_TYPES: &[...]`
  array covering at minimum: bool(16), bytea(17), char(18), name(19), int8(20),
  int2(21), int4(23), regproc(24), text(25), oid(26), xid(28), cid(29), json(114),
  xml(142), float4(700), float8(701), unknown(705), varchar(1043), bpchar(1042),
  date(1082), time(1083), timestamp(1114), timestamptz(1184), interval(1186),
  timetz(1266), numeric(1700), uuid(2950), jsonb(3802), void(2278),
  record(2249), anyarray(2277), anyelement(2283), plus the `_`-prefixed array
  variants (`_int4` = 1007, `_text` = 1009, `_float8` = 1022, etc.), plus
  the range types from 1.3.
- `table_functions.rs::virtual_relation_rows` for `pg_type`: iterate
  `BUILTIN_TYPES` plus any user-created types (enums/domains/composites from
  the catalog). Emit `typnamespace = PG_CATALOG_NAMESPACE_OID` for builtins,
  user-schema OID for user types.
- `system_catalogs.rs::virtual_relation_column_defs` for `pg_type`: extend to
  list every column a pg-driver actually SELECTs — that full set includes
  `typrelid`, `typdelim`, `typnotnull`, `typstorage`, `typsend`, `typreceive`,
  `typmodin`, `typmodout`, `typcollation`, `typdefault`, `typacl`. (tokio-postgres
  selects `typrelid` and clients like asyncpg select `typreceive`/`typsend`.)

**Depends on:** 1.1. **Unblocks:** 1.3, 3.x.

### 1.3 `pg_range`, `pg_proc`, `pg_operator`, `pg_enum`, `pg_description` virtual relations exist — **M**

**Gap:** `pg_range` isn't listed in
`is_pg_catalog_virtual_relation` (`system_catalogs.rs:46-67`), so
`SELECT * FROM pg_catalog.pg_range` errors with `42P01 relation does not
exist`. This is the *first* thing tokio-postgres hits when resolving a type.
`pg_proc`, `pg_operator`, `pg_enum`, `pg_description` are listed but their
`virtual_relation_rows` implementations may be missing or sparse — verify case
by case.

**Spec-correct behavior:** these relations exist in every Postgres install.
Even when empty they must respond to `SELECT ... FROM` without error, and they
must have all the columns drivers reference.

**Change:**
- `system_catalogs.rs`: add `"pg_range"`, `"pg_enum"`, `"pg_description"`,
  `"pg_operator"`, `"pg_collation"`, `"pg_language"` to
  `is_pg_catalog_virtual_relation` and `virtual_relation_column_defs`.
  Columns for each from the PG source tree (`pg_range.h`, `pg_enum.h`, etc.).
- `table_functions.rs::virtual_relation_rows`: add cases for each. Minimum
  rows: `pg_range` has the five canonical ranges (int4range=3904, numrange=3906,
  tsrange=3908, tstzrange=3910, daterange=3912) plus int8range=3926.
  `pg_language` has `internal`, `c`, `sql`, `plpgsql`.
  `pg_enum` returns user-declared enum values.

**Depends on:** 1.1, 1.2.

### 1.4 `Execute` must not emit `RowDescription` — **S**

**Gap:** `src/tcop/postgres/mod.rs:819-824`:

```rust
if !prev_desc_sent {
    out.push(BackendMessage::RowDescription { fields: fields.clone() });
    portal.row_description_sent = true;
}
```

PG 18 protocol spec, Message Flow / Extended Query: *"Execute doesn't cause
ReadyForQuery or RowDescription to be issued."* RowDescription is sent only
in response to Describe (statement or portal) or as part of the simple-query
response. tokio-postgres's state machine rejects the unexpected message.

**Spec-correct behavior:** Execute's response is exactly one of
CommandComplete, DataRow×N + CommandComplete, DataRow×N + PortalSuspended,
EmptyQueryResponse, or ErrorResponse. Nothing else.

**Change:**
- `src/tcop/postgres/mod.rs:819-824`: delete the block.
- `Portal::row_description_sent`: delete the field and its initializer.
- `src/tcop/postgres/tests.rs:100-126 extended_query_flow_requires_sync_for_ready`:
  the test asserts `out[3]` is RowDescription after Bind+Execute+Sync with no
  Describe. That test encodes the bug; rewrite to assert `out[3]` is DataRow
  directly. Add a new test that sends Parse+Bind+Describe(portal)+Execute+Sync
  and asserts Describe returns RowDescription exactly once.

Caveat worth flagging before deleting the block: psql's prepared-statement
flow is Parse → Bind → Describe(portal) → Execute → Sync, so psql *does*
receive RowDescription via Describe and shouldn't regress. But audit every
in-tree client/test that goes through the extended protocol and confirm
each sends Describe before Execute. If any skips Describe and depends on
the out-of-spec RowDescription, it will silently lose column metadata after
the fix — update it alongside the removal. `src/tcop/postgres/tests.rs` and
the `pg_compat.rs` regression harness are the two places to look.

**Depends on:** nothing. **Unblocks:** tokio-postgres's `query()` path for
already-prepared statements, but on its own it's not sufficient — 1.1/1.2/1.3
must also land or type-OID lookup still loops.

---

## Phase 2 — Typed parameter binding and result encoding — **L**

### 2.1 Introduce `SqlType` carried separately from `ScalarValue`'s storage

**Gap:** `ScalarValue` (`src/storage/tuple.rs:19-31`) has variants Null, Bool,
Int(i64), Float(f64), Numeric, Text, Array, Record, Vector. It cannot
distinguish int2 from int4 from int8, or varchar from text, or timestamp from
timestamptz (Date/Timestamp are stored as `Text`). The result is that
`RowDescription` always reports int8 for any integer column and text for any
date/timestamp/varchar/uuid/bytea/json/jsonb. Any ORM that `try_get::<i32>`s or
decodes uuid fails.

**Spec-correct behavior:** Every column in `RowDescription` carries a specific
type OID that matches the declared SQL type of the expression. Per PG docs
RowDescription fields: *"typeid — the object ID of the field's data type.
typsize — the data type size … typmod — type modifier …"*. Clients decode the
value using that OID.

**Design:**
- Add `pub enum SqlType { Bool, Int2, Int4, Int8, Float4, Float8, Numeric{p:Option<u8>, s:Option<i16>}, Text, Varchar(Option<u32>), BpChar(Option<u32>), Bytea, Uuid, Date, Time, Timetz, Timestamp, Timestamptz, Interval, Json, Jsonb, Oid, Array(Box<SqlType>), Domain{base: Box<SqlType>, oid: Oid}, Enum(Oid), Composite(Oid), Range(Oid), Vector(usize), Unknown }` in `src/types/sql_type.rs`.
- `impl SqlType { fn oid(&self) -> u32; fn typmod(&self) -> i32; fn typlen(&self) -> i16; }` — computes PG wire-level fields.
- Storage (`ScalarValue`) stays tagged by *runtime* representation. Type
  information lives in the column descriptor, not the value.
- Planner column descriptors (`src/planner/...`) grow a `sql_type: SqlType`
  field alongside the existing internal representation.
- Analyzer (`src/analyzer/types.rs`) computes `SqlType` during type inference.
  `CAST(x AS int4)` yields `SqlType::Int4`; the executor still stores the
  value as `ScalarValue::Int`, but the descriptor says int4.

**Change scope:**
- `src/types/sql_type.rs` — new file, ~400 LOC.
- `src/analyzer/types.rs` — extend inference to produce SqlType for every
  expression.
- `src/planner/logical.rs`, `src/planner/physical.rs` — carry SqlType on column
  descriptors.
- `src/tcop/postgres/mod.rs` — `describe_fields_for_plan`,
  `infer_row_description_fields` read SqlType off the plan and emit the
  correct OID / typmod / typlen.
- `src/tcop/postgres/encoding.rs::encode_binary_scalar` — extend to every
  SqlType, not just the 6 OIDs it handles today. Date→days-since-2000 i32,
  Timestamp→microseconds-since-2000 i64, Timestamptz→same (UTC), UUID→16 bytes,
  Numeric→PG's binary numeric format, Bytea→raw bytes, Json→text, Jsonb→1-byte
  version prefix + text, Arrays→recursive binary array format per PG docs.

**Size:** L+ (realistically two to three weeks). `src/analyzer/types.rs`
plus every `src/planner/` site plus the binary encoders for ~20 types plus
plumbing SqlType through every executor node is the item most likely to
balloon. This is the primary risk item in the plan, alongside 3.1.

**Depends on:** 1.1, 1.2 (need the full pg_type to validate against).
**Unblocks:** 2.2, 2.3, 3.x.

### 2.2 Binary parameter decode stops round-tripping through text — **M**

**Gap:** `src/tcop/postgres/extended_query.rs:298`:

```rust
Ok(decode_binary_scalar(raw, type_oid, "bind parameter")?.render())
```

Binary bytes are decoded to a typed value, then `render()`ed back to text,
then the rest of the pipeline treats the parameter as text. Precision and
type information are lost at this boundary. Raw-wire probe showed binding
`42i32` (`0x0000002a` in 4 BE bytes) round-trips through the query pipeline
and comes back as `0` — the value itself is lost.

**Spec-correct behavior:** Bind decodes bytes using the parameter's declared
type OID (from Parse or from the Bind message itself) and stores the typed
value. The executor substitutes that typed value wherever `$n` appears; no
text re-parsing, no precision loss.

**Change:**
- `decode_bind_parameter` / `decode_binary_bind_parameter`: return
  `(ScalarValue, SqlType)`, not `Option<String>`.
- `Portal::params` changes type from `Vec<Option<String>>` to
  `Vec<Option<(ScalarValue, SqlType)>>`.
- Every consumer of `Portal::params` in the executor (primarily parameter
  substitution in `exec_expr.rs` and the planner's bound-param resolver)
  reads typed values instead of text.
- Text-format Bind (`format_code = 0`) still parses the text, but uses the
  declared type's parser (int4 → i32, not blanket text).

**Depends on:** 2.1. **Size:** M.

### 2.3 `NULL` no longer forces binary encoding for the whole row — **S**

**Gap:** `src/tcop/postgres/encoding.rs:13-14`:

```rust
let requires_binary = fields.iter().any(|field| field.format_code == 1)
    || row.iter().any(|value| matches!(value, ScalarValue::Null));
```

NULL is format-independent at the wire level — `DataRow` encodes NULL as a
length-prefix of `-1` regardless of the column's format code. Forcing binary
for any row that contains a NULL means a text-format result with one NULL
column gets binary-encoded, and clients asking for text get binary bytes.

**Spec-correct behavior:** Format code is per-column, determined by the Bind
message's `result_format_codes`, not by whether the value is NULL.

**Change:** delete the `|| row.iter().any(...)` disjunct. `encode_result_field`
already handles NULL correctly (returns `None` → length-prefix `-1`).

**Depends on:** nothing.

---

## Phase 3 — Test isolation: real per-database catalogs — **L+**

### 3.1 `CREATE DATABASE` / `DROP DATABASE` with actual storage isolation

**Gap:** OpenAssay's catalog and heap storage are process-global (see
`catalog/mod.rs::with_global_state_lock` and the comments in `AGENTS.md`
about global state). `CREATE DATABASE foo` fails at parse — `CREATE` doesn't
list `DATABASE` in its alternatives.

For sqlx specifically, `#[sqlx::test]` creates one fresh database per test
via `CREATE DATABASE _sqlx_test_<n>`, runs migrations into it, hands a pool
scoped to that database to the test, and drops the DB at teardown. Without
real per-database isolation, the whole macro is unusable.

**Spec-correct behavior:** A Postgres cluster hosts multiple databases. Each
database has its own catalog (its own pg_class, pg_attribute, pg_type for
user-defined types) and its own heap. A connection selects one database at
startup (`dbname=foo`) and cannot see other databases' user objects.
`pg_database` lists all databases in the cluster; it is cluster-global.

**Design:**
- Lift the process-global `CATALOG` and `STORAGE` statics into a
  `Cluster { databases: HashMap<Oid, Database> }` where each `Database` owns
  its own `Catalog` and `Storage`.
- `pg_database` becomes the cluster-level catalog, backed by the set of
  databases.
- The pg_server session picks a Database at startup based on the `dbname`
  parameter and dispatches all work against its catalog + storage.
- WASM `src/browser.rs` creates a cluster with a single default database.
  The public signatures `execute_sql`, `execute_sql_json`, `execute_sql_arrow`,
  `execute_sql_http`, `execute_sql_http_json` and the state-snapshot helpers
  stay byte-identical — they take `&str` and return `String`, which doesn't
  change under a Cluster lift. Sub-item to confirm during 3.1: build
  `scripts/build_wasm.sh` and diff the emitted `.d.ts` against pre-lift; zero
  diff required to pass this phase.
- `CREATE DATABASE name [WITH TEMPLATE t]`: parser, analyzer, executor.
  Creates a new empty Database with its own fresh Catalog/Storage bootstrap.
- `DROP DATABASE name [WITH (FORCE)]`: removes the Database. Must fail if
  any connection is currently attached to it (return SQLSTATE 55006
  `object_in_use`).
- The "global state lock" pattern used today for test isolation
  (`with_global_state_lock`) must keep working — test helpers create a
  scoped cluster and destroy it.

**Files touched:** `src/catalog/mod.rs` (Cluster/Database split),
`src/tcop/engine.rs` (sessions pick a Database), `src/tcop/postgres/mod.rs`
(startup message reads dbname, attaches to Database), `src/browser.rs`
(single-Database shim), `src/parser/sql_parser/admin.rs` (parse CREATE/DROP
DATABASE), new `src/commands/database.rs`, `src/tcop/utility.rs` (dispatch).

**Size:** L+ (multi-week). Largest item in the plan.

**Depends on:** 1.1-1.4 (so the test suite still runs after the lift).

### 3.2 Per-test transactional fixtures (testkit API surface) — **M**

**Gap:** Even with 3.1 the ergonomics of "start a fresh DB per test, run
schema, run the test" are slow (CREATE DATABASE + migrations per test). The
PGlite-style pattern — spin one database with schema pre-loaded, then wrap
each test in `BEGIN; ... ROLLBACK;` — is 10-100× faster for suites that only
read/write and don't depend on mid-test committed state.

**Spec-correct behavior:** OpenAssay already supports BEGIN/COMMIT/ROLLBACK
and SAVEPOINT/ROLLBACK TO (`src/tcop/postgres/transaction.rs`). What's
missing is a stable Rust API in the `openassay` crate that exposes this
without needing to go through pgwire over TCP.

**Design:**
- New module `src/testkit.rs` (feature-gated) exposing:
  ```rust
  pub struct TestCluster { /* owns a Cluster */ }
  impl TestCluster {
      pub fn new() -> Self;
      pub fn load_schema(&self, sql: &str) -> Result<()>;
      pub fn connect(&self) -> TestClient; // tokio_postgres-like handle
      pub async fn run_in_rollback<F, T>(&self, f: F) -> T; // BEGIN; f; ROLLBACK
  }
  ```
- `TestClient` wraps an in-process session — no sockets. Provides
  `query`, `execute`, `transaction` with signatures close to
  `tokio_postgres::Client` so library tests that are written against
  tokio-postgres compile unchanged or with a single type alias.
- Documented pattern in `testkit/README.md`: "wrap each test in
  `run_in_rollback`."

**Size:** M. **Depends on:** 3.1, 5.2 (typed parameters).

---

## Phase 4 — Error-code fidelity — **M**

### 4.1 SQLSTATE on every error path

**Gap:** Syntax errors arrive at the client with no SQLSTATE. CHECK
constraint violations return `XX000` (internal_error). FK violations return
`XX000`. Unique violations (`23505`) and NOT NULL violations (`23502`) are
correctly coded.

**Spec-correct behavior:** Every error has an SQLSTATE. The Postgres source
tree's `errcodes.txt` is the canonical mapping. For this work the required
subset is:
- `42601` syntax_error
- `42P01` undefined_table
- `42703` undefined_column
- `42804` datatype_mismatch
- `22P02` invalid_text_representation (bad input to a type parser)
- `23502` not_null_violation (already correct)
- `23503` foreign_key_violation (currently XX000)
- `23505` unique_violation (already correct)
- `23514` check_violation (currently XX000)
- `25P02` in_failed_sql_transaction
- `40001` serialization_failure (not reachable yet but reserve)
- `3D000` invalid_catalog_name
- `55006` object_in_use (for DROP DATABASE)

**Change:**
- `EngineError` (`src/tcop/engine.rs`) grows a `sqlstate: Option<&'static str>`
  field and a `EngineError::with_sqlstate` builder.
- Every error-raising site in the executor passes the right code. The
  compiler will help find sites; grep for `EngineError { message:` and fix
  each. Pay particular attention to constraint-violation paths in
  `src/executor/`.
- `src/parser/...` errors need a `ParseError::sqlstate = "42601"` default.
- `src/tcop/postgres/mod.rs::encode_error_response` reads the code off the
  error and emits an `S` field (`SQLSTATE`) in the ErrorResponse message.
  It may already do this for some paths — audit and make it mandatory.

**Size:** M. **Depends on:** nothing (can ship earlier, but 1.x/2.x take
priority).

### 4.2 Aborted-transaction semantics — **S**

**Gap:** Not audited in this round. The hook exists
(`is_aborted_transaction_block` in `src/tcop/postgres/mod.rs`) but it hasn't
been verified that mid-TX errors correctly set the block to aborted, that
subsequent statements return `25P02`, and that `ROLLBACK TO SAVEPOINT`
recovers the block to non-aborted state.

**Spec-correct behavior:** Once any statement in an explicit transaction
errors, all subsequent statements until `ROLLBACK` or `ROLLBACK TO SAVEPOINT`
return `ERROR: current transaction is aborted, commands ignored until end of
transaction block` with SQLSTATE `25P02`. After `ROLLBACK TO SAVEPOINT` the
block returns to a clean state at that savepoint.

**Change:** write a test in `tests/regression/` that drives the extended
protocol through an erroring statement and asserts the subsequent
CommandComplete / ErrorResponse sequence. Fix any drift.

**Size:** S.

---

## Phase 5 — Parser holes — **M total**

Each sub-item is S unless noted. All touch `src/parser/sql_parser/*.rs`,
`src/parser/ast.rs`, and the relevant planner/executor dispatch.

### 5.1 Date/time keyword literals
`CURRENT_TIMESTAMP`, `CURRENT_DATE`, `CURRENT_TIME`, `LOCALTIMESTAMP`,
`LOCALTIME`. Per PG these are parsed as special keyword calls (not regular
functions); lexer recognises them as keywords, parser emits
`Expr::SpecialFunctionCall { name: CurrentTimestamp, precision: Option<u32> }`.
Executor dispatches to the same implementation as `now()` / `current_date()`.

### 5.2 `AT TIME ZONE` clause
`timestamp AT TIME ZONE 'UTC'` is a binary operator in PG's grammar, not a
function. Parser: new production in expression hierarchy at the same level
as `::` cast. AST: `Expr::AtTimeZone { value: Box<Expr>, zone: Box<Expr> }`.
Executor: `timestamp AT TIME ZONE` returns timestamptz, `timestamptz AT TIME
ZONE` returns timestamp. Implementation exists in `src/utils/adt/datetime.rs`
— just wire the syntax.

### 5.3 `CREATE TYPE foo AS (col1 type, col2 type)` — composite types — **M**
Parser: already has `CREATE TYPE ... AS ENUM`; add the composite branch.
Catalog: new `CompositeType { oid, schema, attributes: Vec<(String, SqlType)> }`
alongside enums. `pg_type` lists it with `typtype='c'`, `typrelid` points at
a `pg_class` row of `relkind='c'`. `pg_attribute` lists the attributes.
Analyzer/executor: values of composite types constructed via `ROW(...)` or
directly, accessed via `.field`.

### 5.4 `CREATE TYPE foo AS RANGE (subtype=...)` — range types — **M**
Parser: new branch. Catalog: range entries in `pg_range`.
Operators: `<@`, `@>`, `&&`, `<<`, `>>`, plus range constructors and
`lower()`/`upper()`/`isempty()` functions. This is substantial — the range
algebra is non-trivial. If out of scope, mark explicitly: range types are
rare in library tests; can be gated on user demand.

### 5.5 `CREATE [OR REPLACE] PROCEDURE` — **L+ if plpgsql, S if SQL-only**
`CREATE PROCEDURE name(args) LANGUAGE sql AS 'BEGIN ...'` is tractable as a
prepared-SQL wrapper. `LANGUAGE plpgsql` requires implementing a whole
procedural language (variables, IF/LOOP/WHILE, EXCEPTION blocks, FOR record
iteration). **Declared out of scope for testkit v1.** Anything sqlx/Diesel
do with procedures is usually via raw SQL in migrations; if a user's code
under test calls plpgsql they must use real Postgres for that test.

### 5.6 `EXCLUDE USING gist (col WITH &&)` — **M, conditional**
Exclusion constraints with GiST require an actual GiST index. Mark
out-of-scope for testkit v1; document as "use real Postgres for GiST/GIN
index tests."

### 5.7 `uuid_generate_v1mc()` — **S**
Part of the `uuid-ossp` extension. Already registered as a CREATE EXTENSION
no-op. Implement `uuid_generate_v1()`, `uuid_generate_v1mc()`,
`uuid_generate_v4()`, `uuid_generate_v5(namespace, name)` in
`src/utils/adt/` and register in `src/analyzer/functions.rs`. The `uuid`
crate already has v1/v4/v5 generators.

---

## Phase 6 — Extensions sqlx's `setup.sql` uses — **L total, optional**

Each is independent. Declared optional because library code under test
usually doesn't use these types — they show up in sqlx's own test fixtures
but not in application schemas.

- **citext** — M. Case-insensitive text. New type OID reserved by the
  extension at install; comparisons lowercase both sides. Declare and
  implement alongside text.
- **hstore** — L. Key-value map. New type + operators (`->`, `?`, `?&`,
  `?|`, `||`, `-`) + functions (`hstore_to_json`, `hstore_to_array`, etc.).
- **ltree** — L. Labeled tree paths. New type + operators (`@>`, `<@`,
  `~`, `?`, `@`) + GiST index support (without GiST the ops still work,
  just unindexed).
- **cube** — L. N-dim cube type. Similar scope to ltree.

Recommendation: ship citext (it's used widely in real apps), defer the
other three unless a concrete user asks.

---

## Phase 7 — Protocol and misc — **M**

### 7.1 `COPY FROM STDIN` — **S to M, depends on root cause**
`COPY ci FROM STDIN` returns "unexpected message from server" under
tokio-postgres. Root cause not yet traced. Likely a CopyInResponse /
CopyData / CopyDone sequencing bug in `src/tcop/postgres/copy_protocol.rs`.
Trace via socat and fix. **First: investigate. Estimate locks once root
cause is known.**

### 7.2 Extended-protocol LISTEN/NOTIFY — **S**
Simple-query path works. Verify extended-protocol path; tokio-postgres'
`client.batch_execute("LISTEN ch")` and the notification delivery loop
should fire. Write a tokio-postgres integration test.

---

## Cross-cutting: regressions and CI

Each phase has a non-negotiable exit check:

- `cargo test` green (unit tests).
- `cargo test --test regression` green (full 12,329-statement PG compat
  suite).
- `cargo clippy -- -D warnings` clean.
- `scripts/build_wasm.sh` succeeds.
- New tokio-postgres-targeted test file `tests/tokio_postgres_compat.rs`:
  one case per gap fixed this phase. It becomes the regression harness for
  "unit-testing a Postgres-compatible lib works."

---

## Sequencing summary

| Phase | Items | Size | Blocks |
|-------|-------|------|--------|
| 1 | Catalog fixed OIDs, full `pg_type`, `pg_range`, Execute-no-RowDesc | M+M+M+S | *anything talking to OpenAssay with params* |
| 2 | `SqlType` refactor, typed binds, NULL-format fix | L++M+S | ORM decoding correctness |
| 3 | `CREATE DATABASE` + testkit API | L+ + M | sqlx::test, testkit v1 |
| 4 | SQLSTATE fidelity + tx-aborted semantics | M+S | client error-matching |
| 5 | Parser holes (date keywords, AT TIME ZONE, composite, uuid_generate) | S×4 | sqlx setup.sql |
| 6 | Extensions (citext first, rest optional) | M–L each | app schemas using them |
| 7 | COPY IN, LISTEN/NOTIFY extended | S–M | protocol completeness |

Phase 1 alone (roughly 3–5 days of focused work) is the "tokio-postgres can
talk to OpenAssay" milestone. Phase 1+2 is "sqlx/Diesel queries decode
correctly." Phase 1+2+3 is "`#[sqlx::test]` works." Phase 1–4 is "real
libraries' existing test suites run against OpenAssay without patching."

## Out of scope for testkit v1 (call out explicitly)

- plpgsql (Phase 5.5)
- GiST / GIN / SP-GiST indexes and `EXCLUDE USING gist` (Phase 5.6)
- hstore / ltree / cube (Phase 6 optional)
- Triggers, RULES, partitioning, inheritance (mentioned in README as
  out-of-scope; reconfirmed)
- `PREPARE TRANSACTION` / two-phase commit
- Replication slots, logical replication as a *source* (current
  LOGICAL REPLICATION support is as a *sink*)

If a user's test suite hits any of these, the guidance is: use real Postgres
for that test. Testkit's job is to cover the common path fast; integration
tests against real Postgres stay the source of truth.
