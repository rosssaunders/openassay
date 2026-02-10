# Compatibility Scorecard

Postrust targets PostgreSQL surface-area parity for an in-memory engine. This scorecard tracks what's implemented and what remains.

## Supported

- DDL: CREATE/ALTER/DROP for tables, views, materialized views, indexes, sequences, schemas, extensions, and SQL functions
- DML: INSERT/UPDATE/DELETE/MERGE, RETURNING, ON CONFLICT, UPDATE FROM, DELETE USING
- Queries: joins, subqueries, CTEs (including recursive), window functions, aggregates, GROUPING SETS/ROLLUP/CUBE, set operations
- Types: numeric, text, boolean, date/time, JSON/JSONB, arrays, UUID, bytea
- Built-ins: 170+ functions across string, math, date/time, JSON, aggregate, window, system
- Transactions: BEGIN/COMMIT/ROLLBACK, savepoints, MVCC-lite visibility
- System catalogs: pg_class/pg_namespace/pg_type/pg_attribute/pg_index/pg_constraint/pg_sequence/pg_proc, information_schema tables
- PostgreSQL wire protocol (simple + extended), COPY (text/CSV), authentication

## Partial

- Planner coverage: complex queries fall back to pass-through execution
- Analyzer: validation pass exists but some checks remain in executor

## Not Yet Supported

- Triggers and rules
- Table partitioning
- CREATE TYPE / CREATE DOMAIN
- PREPARE / EXECUTE / DEALLOCATE
- VACUUM / ANALYZE
- Durability (WAL), replication, background maintenance
