use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;

use openassay::protocol::messages::{
    StartupAction, decode_frontend_message, decode_startup_action, encode_backend_message,
};
use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};
use rcgen::generate_simple_self_signed;
use rustls::{Certificate, PrivateKey, ServerConfig, ServerConnection, StreamOwned};

fn main() {
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:55432".to_string());
    let tls_config = build_tls_config().unwrap_or_else(|err| {
        eprintln!("failed to build TLS config: {err}");
        std::process::exit(1);
    });

    let listener = TcpListener::bind(&addr).unwrap_or_else(|err| {
        eprintln!("failed to bind {addr}: {err}");
        std::process::exit(1);
    });

    println!("openassay pgwire listening on {addr}");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let tls_config = tls_config.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_connection(stream, tls_config) {
                        eprintln!("connection error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }
}

fn build_tls_config() -> io::Result<Arc<ServerConfig>> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".to_string()])
        .map_err(|err| io::Error::other(format!("rcgen: {err}")))?;
    let cert_der = cert
        .serialize_der()
        .map_err(|err| io::Error::other(format!("cert: {err}")))?;
    let key_der = cert.serialize_private_key_der();

    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(vec![Certificate(cert_der)], PrivateKey(key_der))
        .map_err(|err| io::Error::other(format!("rustls: {err}")))?;
    Ok(Arc::new(config))
}

fn handle_connection(stream: TcpStream, tls_config: Arc<ServerConfig>) -> io::Result<()> {
    let mut stream = ClientStream::Plain(stream);
    stream.set_nodelay(true);
    let mut session = PostgresSession::new_startup_required();

    if !run_startup_handshake(&mut stream, &mut session, &tls_config)? {
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

        if out
            .iter()
            .any(|message| matches!(message, BackendMessage::Terminate))
        {
            return Ok(());
        }
    }
}

fn run_startup_handshake(
    stream: &mut ClientStream,
    session: &mut PostgresSession,
    tls_config: &Arc<ServerConfig>,
) -> io::Result<bool> {
    loop {
        let Some(packet) = read_startup_packet(stream)? else {
            return Ok(false);
        };
        match decode_startup_action(&packet) {
            Ok(StartupAction::SslRequest) => {
                stream.write_all(b"S")?;
                stream.flush()?;
                stream.upgrade_tls(tls_config.clone())?;
                continue;
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
                    .any(|message| matches!(message, BackendMessage::ReadyForQuery { .. }))
                {
                    return Ok(true);
                }

                loop {
                    let Some((tag, payload)) = read_tagged_message(stream)? else {
                        return Ok(false);
                    };
                    let frontend = match decode_frontend_message(tag, &payload) {
                        Ok(message) => message,
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
                        .any(|message| matches!(message, BackendMessage::ReadyForQuery { .. }))
                    {
                        return Ok(true);
                    }
                    if out
                        .iter()
                        .any(|message| matches!(message, BackendMessage::ErrorResponse { .. }))
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

fn send_backend_messages(stream: &mut ClientStream, messages: &[BackendMessage]) -> io::Result<()> {
    for message in messages {
        if let Some(frame) = encode_backend_message(message) {
            stream.write_all(&frame)?;
        }
    }
    stream.flush()
}

fn send_error(stream: &mut ClientStream, message: &str) -> io::Result<()> {
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

fn read_startup_packet(stream: &mut ClientStream) -> io::Result<Option<Vec<u8>>> {
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
            "startup packet length is invalid",
        ));
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body)?;
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&len_buf);
    out.extend_from_slice(&body);
    Ok(Some(out))
}

fn read_tagged_message(stream: &mut ClientStream) -> io::Result<Option<(u8, Vec<u8>)>> {
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
            "frontend message length is invalid",
        ));
    }
    let mut payload = vec![0u8; len - 4];
    stream.read_exact(&mut payload)?;
    Ok(Some((tag_buf[0], payload)))
}

#[allow(clippy::large_enum_variant)]
enum ClientStream {
    Plain(TcpStream),
    Tls(StreamOwned<ServerConnection, TcpStream>),
    Closed,
}

impl ClientStream {
    fn set_nodelay(&self, enabled: bool) {
        match self {
            Self::Plain(stream) => {
                let _ = stream.set_nodelay(enabled);
            }
            Self::Tls(stream) => {
                let _ = stream.sock.set_nodelay(enabled);
            }
            Self::Closed => {}
        }
    }

    fn upgrade_tls(&mut self, config: Arc<ServerConfig>) -> io::Result<()> {
        let plain = match std::mem::replace(self, Self::Closed) {
            Self::Plain(stream) => stream,
            other => {
                *self = other;
                return Ok(());
            }
        };
        let conn =
            ServerConnection::new(config).map_err(|err| io::Error::other(format!("tls: {err}")))?;
        *self = Self::Tls(StreamOwned::new(conn, plain));
        Ok(())
    }
}

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
            Self::Closed => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is closed",
            )),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
            Self::Closed => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is closed",
            )),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
            Self::Closed => Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection is closed",
            )),
        }
    }
}
