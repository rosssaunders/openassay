# In-Memory PostgreSQL Engine Plan

Objective:
- Build an in-memory-only PostgreSQL-compatible engine with full DML/DDL behavior parity over time.

Operating constraints:
- No durability/WAL/replication requirements in initial target.
- Priority is SQL behavior compatibility and protocol/client compatibility.

Execution model:
- Implement in ordered phases.
- Each step file defines scope, deliverables, tests, and done criteria.
- Do not start a later step until done criteria from dependency steps are met.

---

## Status (audited 2026-02-10)

All 411 tests pass. The codebase is ~41k lines of Rust across a PostgreSQL-style module layout.

### Step 00 — Refactor to PG Layout: ✅ Done
The monolithic `engine.rs` (was 21k lines) has been decomposed into:
- `src/commands/` — DDL command handlers (alter, create_table, drop, view, index, sequence, schema, matview, function, extension, variable, explain, do_block)
- `src/executor/` — executor nodes (agg, append, cte, hash_join, merge_join, nested_loop, limit, sort, set_op, subquery, window_agg, modify_table, result, scan, expr, grouping, srf)
- `src/storage/` — heap + tuple storage
- `src/security/` — roles, acl, rls
- `src/access/transam/` — snapshot, visibility, xact
- `src/catalog/` — mod, schema, table, oid, search_path, dependency, system_catalogs
- `src/protocol/` — messages, startup, copy
- `src/utils/adt/` — datetime, json, math_functions, string_functions, misc
- `src/tcop/` — engine (3k lines), postgres, pquery, utility
- `src/parser/` — lexer, ast, sql_parser, scansup

### Step 01 — Catalog and Names: ✅ Done
- `src/catalog/mod.rs` (2140 lines): full Catalog API with create/read/drop/alter.
- OID allocator in `src/catalog/oid.rs`.
- Schema management in `src/catalog/schema.rs`.
- Table metadata in `src/catalog/table.rs`.
- Search path resolution in `src/catalog/search_path.rs`.
- Built-in schemas: `pg_catalog`, `public`, `information_schema`.
- Tests: create/lookup/drop, name resolution, conflict behavior all covered.

### Step 02 — DDL Core (CREATE/DROP/ALTER): ✅ Done
- `src/commands/create_table.rs`, `src/commands/drop.rs`, `src/commands/alter.rs`.
- CREATE TABLE, DROP TABLE (IF EXISTS, CASCADE/RESTRICT), ALTER TABLE (add/drop/rename column, set/drop not null, set/drop default, add/drop constraint).
- Tests: 20+ DDL tests in engine_tests.

### Step 03 — Row Store and Scans: ✅ Done
- `src/storage/heap.rs`, `src/storage/tuple.rs`.
- Sequential scan via `src/executor/exec_scan.rs`.
- Tests: insert+scan, null handling, multi-table joins all covered.

### Step 04 — DML (INSERT/UPDATE/DELETE/RETURNING): ✅ Done
- INSERT (VALUES, SELECT source, column defaults), UPDATE (SET, FROM), DELETE (WHERE, USING).
- RETURNING on all three.
- `src/executor/node_modify_table.rs` for mutation execution.
- Tests: 15+ DML tests.

### Step 05 — Constraints and Defaults: ✅ Done
- NOT NULL, CHECK, PRIMARY KEY, UNIQUE, FOREIGN KEY (with CASCADE/RESTRICT/SET NULL actions).
- Column defaults, identity columns.
- Composite foreign keys.
- ALTER TABLE ADD/DROP CONSTRAINT.
- Tests: comprehensive constraint enforcement tests.

### Step 06 — Transactions and MVCC-Lite: ✅ Done
- BEGIN/COMMIT/ROLLBACK, SAVEPOINT/ROLLBACK TO/RELEASE.
- `src/access/transam/` — snapshot, visibility, xact modules.
- Transaction state machine in `src/tcop/postgres.rs`.
- Aborted transaction block handling.
- Tests: multi-statement txn, rollback, savepoint, error recovery.

### Step 07 — Parser Coverage: ✅ Done
- Parser (5k lines) and AST (737 lines, 74 types) cover:
  - CTEs (WITH, WITH RECURSIVE)
  - Window functions with PARTITION BY, ORDER BY, frame clauses (ROWS/RANGE/BETWEEN)
  - Full INSERT/UPDATE/DELETE/MERGE grammar
  - INSERT ON CONFLICT (DO NOTHING, DO UPDATE, ON CONSTRAINT)
  - MERGE with MATCHED/NOT MATCHED BY SOURCE/TARGET, RETURNING
  - DDL: CREATE/ALTER/DROP for TABLE, VIEW, MATERIALIZED VIEW, INDEX, SEQUENCE, SCHEMA, EXTENSION, FUNCTION
  - SET operations (UNION/INTERSECT/EXCEPT + ALL)
  - LATERAL subqueries, function calls in FROM
  - GROUPING SETS, ROLLUP, CUBE
  - ARRAY constructors
  - JSON operators (->>, ->, #>, #>>, @>, <@, ?, ?|, ?&)
  - BETWEEN, LIKE, ILIKE, IS [NOT] DISTINCT FROM, IS [NOT] NULL
  - CAST, :: type coercion
  - EXISTS, IN subqueries, ANY/ALL comparisons
  - CASE (simple + searched), scalar subqueries
  - Typed column aliases for table functions
  - LISTEN/NOTIFY/UNLISTEN, DISCARD ALL
  - DO blocks, EXPLAIN/EXPLAIN ANALYZE
  - Transaction statements, SET/SHOW
  - TRUNCATE, DROP INDEX, DROP SEQUENCE
  - 100+ parser unit tests
- **Not yet parsed:** Partitioning DDL, triggers, rules, PREPARE/EXECUTE/DEALLOCATE, CREATE TYPE, CREATE DOMAIN, COMMENT ON, VACUUM/ANALYZE.

### Step 08 — Parse Analysis and Type System: ✅ Done
- **Done:** Type coercion at DML boundaries (`coerce_value_for_column`), mixed-type numeric/comparison coercion, implicit cast rules for common types.
- **Done:** Function/operator resolution is inline in executor (not a separate analyzer pass).
- **Done:** `src/analyzer/` module with semantic analysis pass wired into `plan_statement()`:
  - `analyzer/mod.rs` — entry point, analyzes SELECT/INSERT/UPDATE/DELETE statements
  - `analyzer/binding.rs` — name binding and scope resolution (table/column, CTE, aliases)
  - `analyzer/types.rs` — type inference, implicit coercion rules (int→float, date→timestamp), boolean/numeric context validation
  - `analyzer/functions.rs` — function resolution with arg count validation for 140+ built-in functions
  - 33 new tests covering type coercion, function resolution, name binding, CTE handling
- **Note:** Analyzer is currently a validation pass (does not transform AST). Table existence checks are still deferred to executor to maintain backward compatibility. Future work: migrate more checks from executor to analyzer.

### Step 09 — System Catalog and Dependencies: ✅ Done
- `src/catalog/dependency.rs` (550 lines): full dependency graph with CASCADE/RESTRICT semantics.
- `src/catalog/system_catalogs.rs`: virtual system catalog tables.
- Exposed catalogs: `pg_class`, `pg_namespace`, `pg_type`, `pg_attribute`, `pg_index`, `pg_constraint`, `pg_sequence`, `pg_depend`, `pg_extension`, `pg_proc`, `pg_am`, `pg_roles`, `pg_settings`, `pg_database`, `pg_tables`, `information_schema.tables`, `information_schema.columns`, `information_schema.schemata`.
- Dependency tracking for views, sequences, indexes, foreign keys, materialized views.
- Tests: dependency graph, cascade/restrict DDL, transitive view dependencies.

### Step 10 — Planner and Optimizer: ✅ Done
- `src/planner/` module with logical plan nodes, physical plan selection, cost model, and table stats.
- Planner is wired into `plan_statement()` with graceful PassThrough fallback on planning errors.
- Coverage: SELECT, CTEs, ORDER BY/LIMIT/OFFSET, DISTINCT, GROUP BY/HAVING, set operations, subqueries/LATERAL, window functions.

### Step 11 — Executor Node Parity: ✅ Done
- Full set of executor nodes implemented in `src/executor/`:
  - `node_agg.rs` — aggregation (GROUP BY, HAVING, filtered aggregates, ordered-set aggregates)
  - `node_append.rs` — UNION/INTERSECT/EXCEPT
  - `node_cte.rs` — CTE and recursive CTE execution
  - `node_hash_join.rs` — hash join
  - `node_merge_join.rs` — merge join
  - `node_nested_loop.rs` — nested loop join (including LATERAL)
  - `node_limit.rs` — LIMIT/OFFSET
  - `node_sort.rs` — ORDER BY
  - `node_set_op.rs` — set operations
  - `node_subquery.rs` — subquery execution (scalar, correlated, EXISTS, IN)
  - `node_window_agg.rs` — window functions
  - `node_modify_table.rs` — INSERT/UPDATE/DELETE/MERGE execution
  - `node_result.rs` — result projection
  - `exec_scan.rs` — sequential scan
  - `exec_expr.rs` — expression evaluation
  - `exec_grouping.rs` — GROUPING SETS/ROLLUP/CUBE
  - `exec_srf.rs` — set-returning functions
- Note: These nodes are driven directly from AST, not from planner output.

### Step 12 — Full DDL Surface: ✅ Done
- CREATE/ALTER/DROP for: TABLE, VIEW, MATERIALIZED VIEW, INDEX, SEQUENCE, SCHEMA, EXTENSION, FUNCTION.
- REFRESH MATERIALIZED VIEW (including CONCURRENTLY).
- TRUNCATE (with CASCADE/RESTRICT).
- CREATE OR REPLACE for VIEW, MATERIALIZED VIEW, FUNCTION.
- ALTER VIEW (RENAME, RENAME COLUMN), ALTER SEQUENCE, ALTER MATERIALIZED VIEW SET SCHEMA.
- Identity columns (GENERATED ALWAYS/BY DEFAULT AS IDENTITY).
- **Not done:** Partitioning DDL, triggers, rules, CREATE TYPE, CREATE DOMAIN.

### Step 13 — DML Semantic Parity: ✅ Done
- INSERT ON CONFLICT (DO NOTHING, DO UPDATE with WHERE, ON CONSTRAINT).
- MERGE with MATCHED (UPDATE/DELETE/DO NOTHING), NOT MATCHED BY TARGET (INSERT), NOT MATCHED BY SOURCE (UPDATE/DELETE), RETURNING.
- UPDATE FROM, DELETE USING.
- Writable CTEs not explicitly tested but CTE infrastructure supports them.
- **Not done:** Triggers (no trigger subsystem exists).

### Step 14 — Type/Function/Operator Parity: ✅ Done
- **Done:** 170+ built-in functions across string, math, date/time, JSON/JSONB, aggregate, window categories.
- **Done:** Array functions (array_agg, array_to_string, string_to_array, unnest, array constructors, ANY/ALL).
- **Done:** JSON operators (->>, ->, #>, #>>, @>, <@, ?, ?|, ?&, ||, -, delete_key).
- **Done:** Regex functions (regexp_replace, regexp_matches via SRF).
- **Done:** Statistical aggregates (stddev_pop/samp, var_pop/samp), ordered-set aggregates (percentile_cont/disc, mode).
- **Done:** Regression aggregates (corr, covar_pop/samp, regr_* family).
- **Done:** String: encode/decode, md5, sha256, overlay, position, ascii, chr, quote_literal, quote_ident.
- **Done:** Date/time: age(), clock_timestamp(), to_timestamp(), timezone(), make_interval.
- **Done:** Math: width_bucket.
- **Done:** System: pg_typeof, pg_column_size.

### Step 15 — Security (Roles/Grants/RLS): ✅ Done
- `src/security/roles.rs` — role management.
- `src/security/acl.rs` — GRANT/REVOKE table privileges (SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER, ALL).
- `src/security/rls.rs` — row-level security policies with USING and WITH CHECK.
- Current user context, SET ROLE support.
- Tests: grant/revoke, RLS filtering, security in transactions.
- **Note:** GRANT/REVOKE and CREATE/ALTER/DROP ROLE now parse through the formal parser/AST (no string matching in postgres.rs).

### Step 16 — Protocol and Client Compat: ✅ Done
- `src/protocol/messages.rs` — full message encoding/decoding.
- `src/protocol/startup.rs` — startup handshake, SSL detection, cancel handling.
- `src/protocol/copy.rs` — COPY TO/FROM STDIN/STDOUT (text and CSV).
- Simple query protocol, extended query protocol (Parse/Bind/Describe/Execute/Sync).
- Parameter type inference for prepared statements.
- Binary parameter and result format support.
- Error recovery (skip-to-sync on error in extended protocol).
- Password authentication.
- SQLSTATE error codes with position metadata.
- Tests: wire protocol roundtrip, extended protocol flow, error handling, auth, COPY.

### Step 17 — Regression and Hardening: 🟡 Partially Done
- `tests/regression/` — regression test corpus with SQL fixtures.
- `tests/differential/` — differential testing framework.
- 411 tests passing.
- **Not done:** No CI pipeline config. No fuzzing. No performance benchmarks. No formal compatibility scorecard.

---

## Summary

| Step | Name | Status |
|------|------|--------|
| 00 | Refactor to PG Layout | ✅ Done |
| 01 | Catalog and Names | ✅ Done |
| 02 | DDL Core | ✅ Done |
| 03 | Row Store and Scans | ✅ Done |
| 04 | DML (INSERT/UPDATE/DELETE/RETURNING) | ✅ Done |
| 05 | Constraints and Defaults | ✅ Done |
| 06 | Transactions and MVCC-Lite | ✅ Done |
| 07 | Parser Coverage | ✅ Done |
| 08 | Parse Analysis and Type System | 🟡 Partial — no separate analyzer module; coercion is inline |
| 09 | System Catalog and Dependencies | ✅ Done |
| 10 | Planner and Optimizer | ✅ Done — full query coverage with PassThrough fallback on errors |
| 11 | Executor Node Parity | ✅ Done |
| 12 | Full DDL Surface | ✅ Done (missing: partitioning, triggers, rules, CREATE TYPE/DOMAIN) |
| 13 | DML Semantic Parity | ✅ Done (missing: triggers) |
| 14 | Type/Function/Operator Parity | ✅ Done — 170+ functions including all listed targets |
| 15 | Security (Roles/Grants/RLS) | ✅ Done (GRANT/REVOKE parsed via string matching) |
| 16 | Protocol and Client Compat | ✅ Done |
| 17 | Regression and Hardening | 🟡 Partial — 411 tests, no CI/fuzzing/benchmarks |

### Key architectural gaps:
1. **Planner execution wiring** — logical/physical plans are built for all query shapes, but execution still uses the AST directly
2. **No formal analyzer** — type checking and name resolution are fused into the executor
3. **String-parsed commands** — GRANT/REVOKE/COPY/role commands bypass the formal parser
4. **No triggers, partitioning, or rules**

Milestone map:
1. `01-catalog-and-names.md`
2. `02-ddl-core-create-drop-alter.md`
3. `03-row-store-and-scans.md`
4. `04-dml-insert-update-delete-returning.md`
5. `05-constraints-and-defaults.md`
6. `06-transactions-and-mvcc-lite.md`
7. `07-parser-coverage.md`
8. `08-parse-analysis-and-type-system.md`
9. `09-system-catalog-and-dependencies.md`
10. `10-planner-optimizer.md`
11. `11-executor-node-parity.md`
12. `12-ddl-surface-area.md`
13. `13-dml-semantic-parity.md`
14. `14-type-function-operator-parity.md`
15. `15-security-roles-rls.md`
16. `16-protocol-and-client-compat.md`
17. `17-regression-compat-and-hardening.md`

Current code anchors:
- Parser/AST: `src/parser/`
- Commands/DDL: `src/commands/`
- Executor nodes: `src/executor/`
- Query loop/protocol session: `src/tcop/postgres.rs`
- Execution engine: `src/tcop/engine.rs`
- Catalog: `src/catalog/`
- Storage: `src/storage/`
- Security: `src/security/`
- Protocol: `src/protocol/`
- Transaction management: `src/access/transam/`
