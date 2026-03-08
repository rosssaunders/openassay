# Browser / WASM

Build the WASM package:

```bash
scripts/build_wasm.sh
```

This writes `web/pkg/openassay.js` + `web/pkg/openassay_bg.wasm`.

Run the local static server:

```bash
cargo run --bin web_server -- 8080
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080).

The browser harness calls one of these exports from the WASM module:

- `execute_sql(sql)`
- `execute_sql_json(sql)` (structured JSON payload with `results[{columns,rows,...}]`)
- `exec_sql(sql)`
- `run_sql(sql)`

It also uses snapshot helpers for browser persistence:

- `export_state_snapshot()`
- `import_state_snapshot(snapshot)`
- `reset_state_snapshot()`

HTTP helpers:

- `http_get(url)` (async, browser fetch)
- `execute_sql_http(sql)` / `run_sql_http(sql)` (async SQL runner that resolves `http_get('...')` calls before execution)
- `execute_sql_http_json(sql)` / `run_sql_http_json(sql)` (same as above, with structured JSON result payload)

Behavior in `web/index.html`:

- the browser harness has two persisted UI modes: `basic` keeps the global URL/preset/path header with results, raw response, and history; `advanced` switches to a full-height workspace with results plus SQL/response/history tabs and compact in-panel mode/status chrome
- successful SQL runs auto-save a snapshot into `localStorage`
- load-time auto-restore replays the saved snapshot
- SQL execution prefers `execute_sql_http` when available
- `web/index.html` also renders tabular result sets into AG Grid when a structured JSON runner is available
- URL runs in Basic mode still generate the backing `json_table(...)` SQL so switching to Advanced immediately shows the current query
- Dockview layouts are stored per mode in `localStorage`, and the last selected mode is restored on reload
- persistence keys were bumped to `v2`, so older `v1` browser layout/UI state is intentionally ignored
