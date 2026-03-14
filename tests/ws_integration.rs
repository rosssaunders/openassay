//! WebSocket integration tests — exercises the ws extension against a real echo server.

use openassay::tcop::engine::{restore_state, snapshot_state};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};
use std::net::TcpListener;
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::Duration;

// ─── helpers ────────────────────────────────────────────────────────────────

/// Serialize all WS integration tests — they share global engine state.
fn ws_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Start a WebSocket echo server on an ephemeral port. Returns the port.
/// The server runs in a background thread and echoes every text message back.
fn start_echo_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || {
                let Ok(mut ws) = tungstenite::accept(stream) else {
                    return;
                };
                loop {
                    match ws.read() {
                        Ok(tungstenite::Message::Text(msg)) => {
                            if ws.write(tungstenite::Message::Text(msg)).is_err() {
                                break;
                            }
                            if ws.flush().is_err() {
                                break;
                            }
                        }
                        Ok(tungstenite::Message::Close(_)) | Err(_) => break,
                        _ => {}
                    }
                }
            });
        }
    });

    port
}

/// Run a SQL query and return all backend messages.
fn sql(session: &mut PostgresSession, query: &str) -> Vec<BackendMessage> {
    session.run_sync([FrontendMessage::Query {
        sql: query.to_string(),
    }])
}

/// Get the first column value from the first DataRow (handles both text and binary encoding).
/// Returns `None` when the result is NULL or there are no data rows.
fn first_val(msgs: &[BackendMessage]) -> Option<String> {
    for m in msgs {
        match m {
            BackendMessage::DataRow { values } if !values.is_empty() => {
                return Some(values[0].clone());
            }
            BackendMessage::DataRowBinary { values } if !values.is_empty() => {
                return values[0]
                    .as_ref()
                    .map(|v| String::from_utf8_lossy(v).to_string());
            }
            _ => {}
        }
    }
    None
}

/// Check if any backend message is an ErrorResponse.
fn has_error(msgs: &[BackendMessage]) -> bool {
    msgs.iter()
        .any(|m| matches!(m, BackendMessage::ErrorResponse { .. }))
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[test]
fn ws_connect_real_server() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    let out = sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );
    assert!(!has_error(&out), "ws.connect should succeed: {out:?}");
    assert_eq!(first_val(&out), Some("1".to_string()));

    // Real connection → state "open"
    let out = sql(&mut s, "SELECT state FROM ws.connections WHERE id = 1");
    assert_eq!(first_val(&out), Some("open".to_string()));

    sql(&mut s, "SELECT ws.close(1)");
    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_send_recv_echo() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );

    let out = sql(&mut s, "SELECT ws.send(1, 'hello world')");
    assert!(!has_error(&out), "ws.send should succeed: {out:?}");

    thread::sleep(Duration::from_millis(200));

    let out = sql(&mut s, "SELECT ws.recv(1)");
    assert_eq!(first_val(&out), Some("hello world".to_string()));

    sql(&mut s, "SELECT ws.close(1)");
    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_multiple_messages_roundtrip() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );

    for i in 1..=3 {
        sql(&mut s, &format!("SELECT ws.send(1, 'msg{i}')"));
    }
    thread::sleep(Duration::from_millis(300));

    assert_eq!(
        first_val(&sql(&mut s, "SELECT ws.recv(1)")),
        Some("msg1".to_string())
    );
    assert_eq!(
        first_val(&sql(&mut s, "SELECT ws.recv(1)")),
        Some("msg2".to_string())
    );
    assert_eq!(
        first_val(&sql(&mut s, "SELECT ws.recv(1)")),
        Some("msg3".to_string())
    );

    // Queue empty → NULL
    assert_eq!(first_val(&sql(&mut s, "SELECT ws.recv(1)")), None);

    sql(&mut s, "SELECT ws.close(1)");
    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_close_real_connection() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );

    let out = sql(&mut s, "SELECT ws.close(1)");
    assert!(!has_error(&out), "ws.close should succeed: {out:?}");
    assert_eq!(first_val(&out), Some("t".to_string()));

    let out = sql(&mut s, "SELECT state FROM ws.connections WHERE id = 1");
    assert_eq!(first_val(&out), Some("closed".to_string()));

    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_connection_refused() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());

    // Bind then drop to get a port that is definitely not listening.
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    let out = sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );
    assert!(
        !has_error(&out),
        "connect should succeed in simulated mode: {out:?}"
    );

    // Falls back to simulated mode → state "connecting"
    let out = sql(&mut s, "SELECT state FROM ws.connections WHERE id = 1");
    assert_eq!(first_val(&out), Some("connecting".to_string()));

    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_invalid_url() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    let out = sql(&mut s, "SELECT ws.connect('not-a-valid-url')");
    assert!(
        !has_error(&out),
        "connect should succeed in simulated mode: {out:?}"
    );

    let out = sql(&mut s, "SELECT state FROM ws.connections WHERE id = 1");
    assert_eq!(first_val(&out), Some("connecting".to_string()));

    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_concurrent_connections() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    let url = format!("ws://127.0.0.1:{port}");
    sql(&mut s, "CREATE EXTENSION ws");
    sql(&mut s, &format!("SELECT ws.connect('{url}')"));
    sql(&mut s, &format!("SELECT ws.connect('{url}')"));

    // Both connections open
    let out = sql(
        &mut s,
        "SELECT count(*) FROM ws.connections WHERE state = 'open'",
    );
    assert_eq!(first_val(&out), Some("2".to_string()));

    // Send on each, verify independent echoes
    sql(&mut s, "SELECT ws.send(1, 'from-conn-1')");
    sql(&mut s, "SELECT ws.send(2, 'from-conn-2')");
    thread::sleep(Duration::from_millis(300));

    assert_eq!(
        first_val(&sql(&mut s, "SELECT ws.recv(1)")),
        Some("from-conn-1".to_string())
    );
    assert_eq!(
        first_val(&sql(&mut s, "SELECT ws.recv(2)")),
        Some("from-conn-2".to_string())
    );

    sql(&mut s, "SELECT ws.close(1)");
    sql(&mut s, "SELECT ws.close(2)");
    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}

#[test]
fn ws_send_on_closed_real_connection_errors() {
    let _g = ws_lock().lock().unwrap_or_else(|e| e.into_inner());
    let port = start_echo_server();

    let mut s = PostgresSession::new();
    sql(&mut s, "SELECT 1");
    let clean = snapshot_state();

    sql(&mut s, "CREATE EXTENSION ws");
    sql(
        &mut s,
        &format!("SELECT ws.connect('ws://127.0.0.1:{port}')"),
    );
    sql(&mut s, "SELECT ws.close(1)");

    let out = sql(&mut s, "SELECT ws.send(1, 'should fail')");
    assert!(
        has_error(&out),
        "send on closed connection should produce an error"
    );

    sql(&mut s, "DROP EXTENSION ws");
    restore_state(clean);
}
