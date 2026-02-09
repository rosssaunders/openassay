# Postrust

**An async-native, PostgreSQL-compatible SQL engine in Rust — where network data sources are first-class SQL primitives.**

Postrust treats HTTP APIs, WebSockets, and remote data feeds as things you query, not things you preprocess. Write SQL that fetches, transforms, and joins live data in a single statement — no Python glue, no ETL pipelines, no waiting.

```sql
-- Fetch live market data and query it with SQL
SELECT value->>'last' AS last_price,
       value->>'volume' AS volume
FROM json_each((SELECT http_get('https://api.exchange.com/ticker')))
WHERE key LIKE 'BTC%';
```

Runs natively on Linux/macOS **and in the browser via WASM**. [Try it live →](https://rosssaunders.github.io/postrust)

## Why async matters

Most embeddable SQL engines block on I/O. That's fine for local files, but useless when your data lives behind an API.

Postrust is **async all the way through** — from expression evaluation to query execution. This means:

- **`http_get()` and `http_json()`** are regular SQL functions that fetch data without blocking the engine
- **WebSocket streams** can be queried as virtual tables (`SELECT * FROM ws.messages`)
- **WASM builds** use native browser `fetch()` and `WebSocket` — no sync XHR hacks, no thread emulation
- **Multiple concurrent data sources** can be queried in the same statement without serialising requests

This makes Postrust uniquely suited for **data analytics against live APIs** — the kind of work that usually requires Python/Pandas/requests just to get data into a queryable shape.

## What it supports

**PostgreSQL compatibility:**
- Full SQL parser: SELECT, INSERT, UPDATE, DELETE, MERGE, CTEs, subqueries
- Window functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE, FIRST_VALUE, LAST_VALUE + aggregates over windows
- 50+ built-in functions including JSON/JSONB, string, math, date/time
- Type system: TEXT, INTEGER, BIGINT, FLOAT, DOUBLE, BOOLEAN, NUMERIC, DATE, TIMESTAMP, INTERVAL, JSON/JSONB, UUID, BYTEA, arrays
- DISTINCT ON, DO blocks, EXPLAIN/ANALYZE, SET/SHOW/RESET
- System catalogs (pg_class, pg_namespace, pg_type, pg_attribute, pg_extension...)
- PostgreSQL wire protocol — connect with psql, DBeaver, any PG client
- Extension infrastructure: CREATE/DROP EXTENSION

**Async data sources:**
- `http_get(url)` — fetch any URL as text, returns inline
- `http_json(url)` — fetch and parse JSON
- WebSocket extension: `ws.connect()`, `ws.send()`, `ws.recv()`, `ws.messages` virtual table
- CREATE FUNCTION with SQL bodies for custom logic

**Platforms:**
- Native (Linux, macOS) via Tokio + reqwest
- Browser/WASM via wasm-bindgen + web-sys fetch/WebSocket
- 350 tests passing on both targets

## Screenshot

![Postrust Browser SQL Harness](assets/postrust-browser-harness.png)

## Quick Start

Build and test:

```bash
cargo test
```

Run the PostgreSQL-compatible server:

```bash
cargo run --bin pg_server -- 55432
# Connect: psql -h 127.0.0.1 -p 55432
```

Run the browser/WASM harness:

```bash
scripts/build_wasm.sh
cargo run --bin web_server -- 8080
# Open: http://127.0.0.1:8080
```

## Project Layout

- `src/` — core engine, parser, protocol, security, transactions
- `src/tcop/engine.rs` — async query execution engine
- `src/tcop/postgres.rs` — PostgreSQL wire protocol session
- `src/browser.rs` — WASM/browser bindings
- `web/` — browser harness UI
- `tests/` — regression + differential test suites
- `implementation-plan/` — staged PostgreSQL parity roadmap

## The vision

SQL is the best language for data analysis. But getting data *into* SQL is the hard part — you end up writing Python scripts to fetch from APIs, parse JSON, clean it up, load it into a database, and *then* query it.

Postrust collapses that pipeline. Your SQL *is* the data pipeline. Fetch, transform, join, and analyse — all in one async query.

## License

MIT
