//! Phase 1 regression harness: drives real tokio-postgres against the
//! in-process pgwire server to lock in the gaps closed in this phase.
//!
//! What this guards against:
//! - Execute emitting RowDescription (spec violation; breaks every Rust ORM's
//!   extended-query path).
//! - pg_catalog.pg_range missing (tokio-postgres' type resolver errors on it).
//! - pg_catalog.pg_type → pg_namespace JOIN returning zero rows (type cache
//!   stays unresolved, client recurses to stack-overflow).
//! - pg_catalog.pg_type missing rows for common types (int4, uuid, timestamptz,
//!   varchar, bytea, jsonb, …).
//!
//! The server is spawned in a background thread on an ephemeral port. Each
//! test gets its own listener so they can run in parallel even though the
//! underlying engine is process-global.

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use openassay::protocol::messages::{
    StartupAction, decode_frontend_message, decode_startup_action, encode_backend_message,
};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

// ─── test-local pg_server (plain TCP, no TLS) ───────────────────────────────

fn spawn_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || {
                let _ = handle_connection(stream);
            });
        }
    });

    port
}

fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let _ = stream.set_nodelay(true);
    let mut session = PostgresSession::new_startup_required();

    if !run_startup_handshake(&mut stream, &mut session)? {
        return Ok(());
    }

    loop {
        let Some((tag, payload)) = read_tagged_message(&mut stream)? else {
            return Ok(());
        };

        let frontend = match decode_frontend_message(tag, &payload) {
            Ok(message) => message,
            Err(err) => {
                send_error(&mut stream, &err.message)?;
                return Ok(());
            }
        };
        if matches!(frontend, FrontendMessage::Terminate) {
            return Ok(());
        }

        let out = session.run_sync([frontend]);
        let out = trim_leading_ready(out);
        send_backend_messages(&mut stream, &out)?;

        if out.iter().any(|m| matches!(m, BackendMessage::Terminate)) {
            return Ok(());
        }
    }
}

fn run_startup_handshake(
    stream: &mut TcpStream,
    session: &mut PostgresSession,
) -> io::Result<bool> {
    loop {
        let Some(packet) = read_startup_packet(stream)? else {
            return Ok(false);
        };
        match decode_startup_action(&packet) {
            Ok(StartupAction::SslRequest) => {
                stream.write_all(b"N")?;
                stream.flush()?;
            }
            Ok(StartupAction::CancelRequest { .. }) => return Ok(false),
            Ok(StartupAction::Startup(startup)) => {
                let startup_msg = FrontendMessage::Startup {
                    user: startup.user,
                    database: startup.database,
                    parameters: startup.parameters,
                };
                let out = session.run_sync([startup_msg]);
                send_backend_messages(stream, &out)?;
                if out
                    .iter()
                    .any(|m| matches!(m, BackendMessage::ReadyForQuery { .. }))
                {
                    return Ok(true);
                }

                loop {
                    let Some((tag, payload)) = read_tagged_message(stream)? else {
                        return Ok(false);
                    };
                    let frontend = match decode_frontend_message(tag, &payload) {
                        Ok(msg) => msg,
                        Err(err) => {
                            send_error(stream, &err.message)?;
                            return Ok(false);
                        }
                    };
                    if matches!(frontend, FrontendMessage::Terminate) {
                        return Ok(false);
                    }
                    let out = session.run_sync([frontend]);
                    let out = trim_leading_ready(out);
                    send_backend_messages(stream, &out)?;
                    if out
                        .iter()
                        .any(|m| matches!(m, BackendMessage::ReadyForQuery { .. }))
                    {
                        return Ok(true);
                    }
                    if out
                        .iter()
                        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
                    {
                        return Ok(false);
                    }
                }
            }
            Err(err) => {
                send_error(stream, &err.message)?;
                return Ok(false);
            }
        }
    }
}

fn trim_leading_ready(mut out: Vec<BackendMessage>) -> Vec<BackendMessage> {
    if out.len() > 1 && matches!(out.first(), Some(BackendMessage::ReadyForQuery { .. })) {
        out.remove(0);
    }
    out
}

fn send_backend_messages(stream: &mut TcpStream, messages: &[BackendMessage]) -> io::Result<()> {
    for message in messages {
        if let Some(frame) = encode_backend_message(message) {
            stream.write_all(&frame)?;
        }
    }
    stream.flush()
}

fn send_error(stream: &mut TcpStream, message: &str) -> io::Result<()> {
    let error = BackendMessage::ErrorResponse {
        message: message.to_string(),
        code: "XX000".to_string(),
        detail: None,
        hint: None,
        position: None,
    };
    if let Some(frame) = encode_backend_message(&error) {
        stream.write_all(&frame)?;
        stream.flush()?;
    }
    Ok(())
}

fn read_startup_packet(stream: &mut TcpStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "startup packet too short",
        ));
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body)?;
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&len_buf);
    out.extend_from_slice(&body);
    Ok(Some(out))
}

fn read_tagged_message(stream: &mut TcpStream) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut tag_buf = [0u8; 1];
    match stream.read_exact(&mut tag_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err),
    }
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frontend message length invalid",
        ));
    }
    let mut payload = vec![0u8; len - 4];
    stream.read_exact(&mut payload)?;
    Ok(Some((tag_buf[0], payload)))
}

// ─── tokio-postgres driven tests ────────────────────────────────────────────

fn conn_str(port: u16) -> String {
    format!("host=127.0.0.1 port={port} user=postgres password=any dbname=openassay")
}

async fn connect(port: u16) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&conn_str(port), tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// The single exchange that used to crash every Rust ORM: Parse + Bind +
/// Execute with a parameter, requiring tokio-postgres' type resolver to
/// succeed before it can decode the result. If any of Phase 1 regresses,
/// this stack-overflows in the client.
///
/// Note: OpenAssay still widens `int4` columns to int8 on the wire (Phase 2
/// will preserve declared SQL types through RowDescription). Phase 1's
/// guarantee is only that the *type-resolver loop* no longer diverges and
/// parameterised queries round-trip for currently-supported wire types.
#[tokio::test(flavor = "multi_thread")]
async fn parameterised_query_does_not_stack_overflow() {
    let port = spawn_server();
    let client = connect(port).await;

    // int8 (currently the only integer width emitted on the wire). This
    // exercises the full Parse+Bind+Describe+Execute path and forces the
    // client's type resolver to run.
    let row = client
        .query_one("SELECT $1::int8 AS a", &[&42i64])
        .await
        .expect("query_one int8");
    let v: i64 = row.try_get(0).expect("try_get");
    assert_eq!(v, 42);
}

/// Parameter-type inference: `WHERE col = $1` against a known-typed column
/// must tell the client that `$1` matches the column's type (not OID 0).
/// Without this, tokio-postgres' type resolver recurses to stack overflow.
#[tokio::test(flavor = "multi_thread")]
async fn parameter_type_inferred_from_column_comparison() {
    let port = spawn_server();
    let client = connect(port).await;

    // pg_type.oid is declared int8 in our catalog. The driver should be able
    // to prepare + bind this with an int8 value.
    let rows = client
        .query(
            "SELECT typname FROM pg_catalog.pg_type WHERE oid = $1",
            &[&23i64],
        )
        .await
        .expect("query with inferred param type");
    assert!(!rows.is_empty(), "int4 (OID 23) should exist in pg_type");
    let typname: String = rows[0].try_get(0).expect("typname");
    assert_eq!(typname, "int4");
}

/// pg_type must list rows for every common type, and every row's typnamespace
/// must JOIN cleanly to pg_namespace. This is exactly the query tokio-postgres
/// runs to resolve an unknown type OID.
#[tokio::test(flavor = "multi_thread")]
async fn pg_type_join_pg_namespace_returns_rows_for_core_types() {
    let port = spawn_server();
    let client = connect(port).await;

    // Run tokio-postgres' own type-resolver shape, one OID at a time.
    // (We skip the LEFT OUTER JOIN on pg_range — that's proven to exist
    // separately in the pg_range_exists test below.)
    for (oid, expected_typname) in [
        (16u32, "bool"),
        (17u32, "bytea"),
        (20u32, "int8"),
        (21u32, "int2"),
        (23u32, "int4"),
        (25u32, "text"),
        (700u32, "float4"),
        (701u32, "float8"),
        (1043u32, "varchar"),
        (1082u32, "date"),
        (1114u32, "timestamp"),
        (1184u32, "timestamptz"),
        (1700u32, "numeric"),
        (2950u32, "uuid"),
        (3802u32, "jsonb"),
    ] {
        let rows = client
            .query(
                "SELECT t.typname, t.typtype, n.nspname \
                 FROM pg_catalog.pg_type t \
                 INNER JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid \
                 WHERE t.oid = $1",
                &[&(oid as i64)],
            )
            .await
            .unwrap_or_else(|e| panic!("query for oid={oid} ({expected_typname}): {e}"));
        assert!(
            !rows.is_empty(),
            "pg_type INNER JOIN pg_namespace returned 0 rows for oid={oid} ({expected_typname}); \
             typnamespace must link to a real pg_namespace entry"
        );
        let typname: String = rows[0].try_get(0).unwrap();
        assert_eq!(
            typname, expected_typname,
            "wrong typname for oid={oid}: {typname} vs expected {expected_typname}"
        );
        let nspname: String = rows[0].try_get(2).unwrap();
        assert_eq!(
            nspname, "pg_catalog",
            "builtin types must live in pg_catalog; oid={oid} found in {nspname}"
        );
    }
}

/// pg_range must exist and answer SELECTs without error. tokio-postgres'
/// type resolver does an outer join on pg_range when inspecting range types;
/// if the relation is missing, it errors (42P01) and then retries, setting
/// up the stack-overflow loop. Presence + basic shape is the guarantee.
#[tokio::test(flavor = "multi_thread")]
async fn pg_range_exists_with_canonical_rows() {
    let port = spawn_server();
    let client = connect(port).await;

    let rows = client
        .query(
            "SELECT rngtypid, rngsubtype FROM pg_catalog.pg_range ORDER BY rngtypid",
            &[],
        )
        .await
        .expect("SELECT from pg_range");
    assert!(
        rows.len() >= 5,
        "pg_range should contain at least the 5 canonical PG range types, found {}",
        rows.len()
    );
    let pairs: Vec<(i64, i64)> = rows
        .iter()
        .map(|r| (r.get::<_, i64>(0), r.get::<_, i64>(1)))
        .collect();
    // int4range=3904 over int4=23 must be present.
    assert!(
        pairs.iter().any(|&(t, s)| t == 3904 && s == 23),
        "int4range (3904, 23) not in pg_range: {pairs:?}"
    );
}

/// pg_language must respond (drivers SELECT from it when introspecting
/// procedures). We expect the canonical `plpgsql` and `sql` rows even if
/// plpgsql itself isn't implemented — the row's existence matters to tools.
#[tokio::test(flavor = "multi_thread")]
async fn pg_language_has_canonical_rows() {
    let port = spawn_server();
    let client = connect(port).await;

    let rows = client
        .query(
            "SELECT lanname FROM pg_catalog.pg_language ORDER BY lanname",
            &[],
        )
        .await
        .expect("SELECT from pg_language");
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for expected in ["c", "internal", "plpgsql", "sql"] {
        assert!(
            names.iter().any(|n| n == expected),
            "pg_language missing '{expected}': found {names:?}"
        );
    }
}

/// Phase 1.4 direct regression: tokio-postgres' `client.query` on an already-
/// prepared statement path must succeed. When OpenAssay emitted RowDescription
/// during Execute, this failed with "unexpected message from server".
#[tokio::test(flavor = "multi_thread")]
async fn prepared_then_execute_does_not_emit_extra_row_description() {
    let port = spawn_server();
    let client = connect(port).await;

    // Prepare (Parse + Describe(statement) + Sync) gives us the stmt handle.
    let stmt = client
        .prepare("SELECT 1::int8 AS a")
        .await
        .expect("prepare");
    // Now Bind + Execute + Sync on the prepared handle. Pre-Phase-1.4 this
    // path crashed; we assert it runs and returns the row.
    let rows = client.query(&stmt, &[]).await.expect("query on prepared");
    assert_eq!(rows.len(), 1);
    let v: i64 = rows[0].try_get(0).expect("try_get i64");
    assert_eq!(v, 1);
}
