use std::collections::HashMap;
use std::str;

use byteorder::{BigEndian, ByteOrder};
use bytes::{Buf, Bytes, BytesMut};
use md5::{Digest, Md5};
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::{ErrorField, ErrorResponseBody, Header, Message};
use postgres_protocol::message::frontend;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::fallible_iterator::FallibleIterator;

use crate::replication::{Lsn, ReplicationError};

const COPY_BOTH_RESPONSE_TAG: u8 = b'W';

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub parameters: HashMap<String, String>,
}

impl ConnectionConfig {
    pub fn parse(conninfo: &str) -> Result<Self, ReplicationError> {
        let mut params = HashMap::new();
        let mut chars = conninfo.chars().peekable();
        while let Some(ch) = chars.peek().copied() {
            if ch.is_whitespace() {
                chars.next();
                continue;
            }
            let mut key = String::new();
            while let Some(ch) = chars.peek().copied() {
                if ch == '=' {
                    chars.next();
                    break;
                }
                if ch.is_whitespace() {
                    return Err(ReplicationError {
                        message: "invalid connection string: missing '='".to_string(),
                    });
                }
                key.push(ch);
                chars.next();
            }
            if key.is_empty() {
                break;
            }
            let value = parse_conninfo_value(&mut chars)?;
            params.insert(key.to_ascii_lowercase(), value);
        }

        let host = params
            .remove("host")
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let port = params
            .remove("port")
            .and_then(|raw| raw.parse::<u16>().ok())
            .unwrap_or(5432);
        let user = params
            .remove("user")
            .unwrap_or_else(|| "postgres".to_string());
        let password = params.remove("password");
        let database = params
            .remove("dbname")
            .or_else(|| params.remove("database"));

        Ok(Self {
            host,
            port,
            user,
            password,
            database,
            parameters: params,
        })
    }

    fn startup_parameters(&self, replication: bool) -> Vec<(String, String)> {
        let mut parameters = Vec::new();
        parameters.push(("user".to_string(), self.user.clone()));
        if let Some(db) = &self.database {
            parameters.push(("database".to_string(), db.clone()));
        }
        parameters.push(("client_encoding".to_string(), "UTF8".to_string()));
        if replication {
            parameters.push(("replication".to_string(), "database".to_string()));
        }
        for (key, value) in &self.parameters {
            parameters.push((key.clone(), value.clone()));
        }
        parameters
    }

    pub fn conninfo_string(&self, replication: bool) -> String {
        let mut parts = vec![
            format!("host={}", self.host),
            format!("port={}", self.port),
            format!("user={}", self.user),
        ];
        if let Some(password) = &self.password {
            parts.push(format!("password={password}"));
        }
        if let Some(db) = &self.database {
            parts.push(format!("dbname={db}"));
        }
        if replication {
            parts.push("replication=database".to_string());
        }
        for (key, value) in &self.parameters {
            if key == "replication" {
                continue;
            }
            parts.push(format!("{key}={value}"));
        }
        parts.join(" ")
    }
}

fn parse_conninfo_value(
    chars: &mut std::iter::Peekable<std::str::Chars<'_>>,
) -> Result<String, ReplicationError> {
    let mut value = String::new();
    match chars.peek().copied() {
        Some('\'') => {
            chars.next();
            while let Some(ch) = chars.next() {
                if ch == '\'' {
                    break;
                }
                if ch == '\\'
                    && let Some(next) = chars.next()
                {
                    value.push(next);
                    continue;
                }
                value.push(ch);
            }
        }
        _ => {
            while let Some(ch) = chars.peek().copied() {
                if ch.is_whitespace() {
                    break;
                }
                value.push(ch);
                chars.next();
            }
        }
    }
    if value.is_empty() {
        return Err(ReplicationError {
            message: "invalid connection string: empty value".to_string(),
        });
    }
    Ok(value)
}

#[derive(Debug)]
pub struct SystemInfo {
    pub system_id: String,
    pub timeline: String,
    pub xlog_pos: Lsn,
    pub db_name: Option<String>,
}

#[derive(Debug)]
pub struct ReplicationSlot {
    pub slot_name: String,
    pub consistent_point: Lsn,
    pub snapshot_name: Option<String>,
}

#[derive(Debug)]
pub struct CopyBothResponse {
    pub overall_format: u8,
    pub column_formats: Vec<i16>,
}

#[derive(Debug)]
pub enum ReplicationMessage {
    XLogData {
        wal_start: Lsn,
        wal_end: Lsn,
        timestamp: i64,
        data: Bytes,
    },
    PrimaryKeepalive {
        wal_end: Lsn,
        timestamp: i64,
        reply_requested: bool,
    },
}

pub struct ReplicationStream {
    connection: PgWireConnection,
}

impl ReplicationStream {
    pub async fn next_message(&mut self) -> Result<Option<ReplicationMessage>, ReplicationError> {
        loop {
            match self.connection.read_message().await? {
                WireMessage::Protocol(Message::CopyData(body)) => {
                    let data = body.into_bytes();
                    return Ok(Some(parse_replication_message(&data)?));
                }
                WireMessage::Protocol(Message::CopyDone) => return Ok(None),
                WireMessage::Protocol(Message::ErrorResponse(body)) => {
                    return Err(ReplicationError {
                        message: error_message(&body),
                    });
                }
                WireMessage::Protocol(_) => continue,
                WireMessage::CopyBothResponse(_) => continue,
            }
        }
    }

    pub async fn send_standby_status_update(
        &mut self,
        lsn: Lsn,
        reply_requested: bool,
    ) -> Result<(), ReplicationError> {
        let mut payload = [0u8; 34];
        payload[0] = b'r';
        let lsn_value = lsn.as_u64();
        BigEndian::write_u64(&mut payload[1..9], lsn_value);
        BigEndian::write_u64(&mut payload[9..17], lsn_value);
        BigEndian::write_u64(&mut payload[17..25], lsn_value);
        let now = chrono_timestamp_micros();
        BigEndian::write_i64(&mut payload[25..33], now);
        payload[33] = u8::from(reply_requested);
        self.connection
            .send_copy_data(Bytes::copy_from_slice(&payload))
            .await
    }
}

pub struct ReplicationClient {
    connection: PgWireConnection,
    config: ConnectionConfig,
}

impl ReplicationClient {
    pub async fn connect(conninfo: &str) -> Result<Self, ReplicationError> {
        let config = ConnectionConfig::parse(conninfo)?;
        let connection = PgWireConnection::connect(&config, true).await?;
        Ok(Self { connection, config })
    }

    pub async fn identify_system(&mut self) -> Result<SystemInfo, ReplicationError> {
        let rows = self.connection.simple_query("IDENTIFY_SYSTEM").await?;
        let row = rows.first().ok_or_else(|| ReplicationError {
            message: "IDENTIFY_SYSTEM returned no rows".to_string(),
        })?;
        let system_id = row.first().cloned().flatten().unwrap_or_default();
        let timeline = row.get(1).cloned().flatten().unwrap_or_default();
        let xlog_raw = row.get(2).cloned().flatten().unwrap_or_default();
        let xlog_pos = Lsn::parse(&xlog_raw)?;
        let db_name = row.get(3).cloned().flatten();
        Ok(SystemInfo {
            system_id,
            timeline,
            xlog_pos,
            db_name,
        })
    }

    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
    ) -> Result<ReplicationSlot, ReplicationError> {
        let sql = format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput");
        let rows = self.connection.simple_query(&sql).await?;
        let row = rows.first().ok_or_else(|| ReplicationError {
            message: "CREATE_REPLICATION_SLOT returned no rows".to_string(),
        })?;
        let slot_name = row
            .first()
            .cloned()
            .flatten()
            .unwrap_or_else(|| slot_name.to_string());
        let lsn = row
            .get(1)
            .cloned()
            .flatten()
            .ok_or_else(|| ReplicationError {
                message: "CREATE_REPLICATION_SLOT missing consistent point".to_string(),
            })?;
        let snapshot_name = row.get(2).cloned().flatten();
        Ok(ReplicationSlot {
            slot_name,
            consistent_point: Lsn::parse(&lsn)?,
            snapshot_name,
        })
    }

    pub async fn start_replication(
        mut self,
        slot_name: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> Result<ReplicationStream, ReplicationError> {
        let sql = format!(
            "START_REPLICATION SLOT {} LOGICAL {} (proto_version '1', publication_names '{}')",
            slot_name,
            start_lsn.format(),
            publication
        );
        self.connection.send_query(&sql).await?;
        loop {
            match self.connection.read_message().await? {
                WireMessage::CopyBothResponse(_) => break,
                WireMessage::Protocol(Message::ErrorResponse(body)) => {
                    return Err(ReplicationError {
                        message: error_message(&body),
                    });
                }
                WireMessage::Protocol(Message::NoticeResponse(_)) => continue,
                WireMessage::Protocol(_) => continue,
            }
        }
        Ok(ReplicationStream {
            connection: self.connection,
        })
    }

    pub fn config(&self) -> &ConnectionConfig {
        &self.config
    }
}

enum WireMessage {
    Protocol(Message),
    #[allow(dead_code)]
    CopyBothResponse(CopyBothResponse),
}

struct PgWireConnection {
    stream: TcpStream,
    read_buf: BytesMut,
}

impl PgWireConnection {
    async fn connect(
        config: &ConnectionConfig,
        replication: bool,
    ) -> Result<Self, ReplicationError> {
        let addr = format!("{}:{}", config.host, config.port);
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let mut connection = Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
        };
        connection.startup(config, replication).await?;
        Ok(connection)
    }

    async fn startup(
        &mut self,
        config: &ConnectionConfig,
        replication: bool,
    ) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(256);
        let params = config.startup_parameters(replication);
        let params_ref: Vec<(&str, &str)> = params
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
            .collect();
        frontend::startup_message(params_ref, &mut buf).map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
        self.stream.write_all(&buf).await?;
        self.authenticate(config).await
    }

    async fn authenticate(&mut self, config: &ConnectionConfig) -> Result<(), ReplicationError> {
        let mut scram: Option<ScramSha256> = None;
        loop {
            match self.read_message().await? {
                WireMessage::Protocol(Message::AuthenticationOk) => {}
                WireMessage::Protocol(Message::AuthenticationCleartextPassword) => {
                    let password = config.password.as_ref().ok_or_else(|| ReplicationError {
                        message: "cleartext password required but not provided".to_string(),
                    })?;
                    self.send_password(password.as_bytes()).await?;
                }
                WireMessage::Protocol(Message::AuthenticationMd5Password(body)) => {
                    let password = config.password.as_ref().ok_or_else(|| ReplicationError {
                        message: "md5 password required but not provided".to_string(),
                    })?;
                    let digest = md5_password(password.as_bytes(), &config.user, body.salt());
                    self.send_password(digest.as_bytes()).await?;
                }
                WireMessage::Protocol(Message::AuthenticationSasl(body)) => {
                    let mut mechanisms = body.mechanisms();
                    let mut selected: Option<String> = None;
                    while let Some(mech) = mechanisms.next().map_err(ReplicationError::from)? {
                        if mech == "SCRAM-SHA-256" {
                            selected = Some(mech.to_string());
                            break;
                        }
                    }
                    let mechanism = selected.ok_or_else(|| ReplicationError {
                        message: "SCRAM-SHA-256 is not offered by server".to_string(),
                    })?;
                    let password = config.password.as_ref().ok_or_else(|| ReplicationError {
                        message: "SCRAM password required but not provided".to_string(),
                    })?;
                    let scram_client =
                        ScramSha256::new(password.as_bytes(), ChannelBinding::unrequested());
                    let initial = scram_client.message().to_vec();
                    self.send_sasl_initial(&mechanism, &initial).await?;
                    scram = Some(scram_client);
                }
                WireMessage::Protocol(Message::AuthenticationSaslContinue(body)) => {
                    let scram_client = scram.as_mut().ok_or_else(|| ReplicationError {
                        message: "unexpected SASL continue message".to_string(),
                    })?;
                    scram_client
                        .update(body.data())
                        .map_err(|err| ReplicationError {
                            message: err.to_string(),
                        })?;
                    let response = scram_client.message().to_vec();
                    self.send_sasl_response(&response).await?;
                }
                WireMessage::Protocol(Message::AuthenticationSaslFinal(body)) => {
                    let scram_client = scram.as_mut().ok_or_else(|| ReplicationError {
                        message: "unexpected SASL final message".to_string(),
                    })?;
                    scram_client
                        .finish(body.data())
                        .map_err(|err| ReplicationError {
                            message: err.to_string(),
                        })?;
                }
                WireMessage::Protocol(Message::ErrorResponse(body)) => {
                    return Err(ReplicationError {
                        message: error_message(&body),
                    });
                }
                WireMessage::Protocol(Message::ReadyForQuery(_)) => return Ok(()),
                WireMessage::Protocol(_) => continue,
                WireMessage::CopyBothResponse(_) => continue,
            }
        }
    }

    async fn simple_query(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<Option<String>>>, ReplicationError> {
        self.send_query(sql).await?;
        let mut rows = Vec::new();
        loop {
            match self.read_message().await? {
                WireMessage::Protocol(Message::DataRow(body)) => {
                    let buffer = body.buffer();
                    let mut range_iter = body.ranges();
                    let mut row = Vec::new();
                    while let Some(range) = range_iter.next().map_err(ReplicationError::from)? {
                        match range {
                            Some(range) => {
                                let value = str::from_utf8(&buffer[range]).map_err(|err| {
                                    ReplicationError {
                                        message: err.to_string(),
                                    }
                                })?;
                                row.push(Some(value.to_string()));
                            }
                            None => row.push(None),
                        }
                    }
                    rows.push(row);
                }
                WireMessage::Protocol(Message::CommandComplete(_)) => {}
                WireMessage::Protocol(Message::RowDescription(_)) => {}
                WireMessage::Protocol(Message::ReadyForQuery(_)) => return Ok(rows),
                WireMessage::Protocol(Message::ErrorResponse(body)) => {
                    return Err(ReplicationError {
                        message: error_message(&body),
                    });
                }
                WireMessage::Protocol(_) => {}
                WireMessage::CopyBothResponse(_) => {}
            }
        }
    }

    async fn send_query(&mut self, sql: &str) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(sql.len() + 8);
        frontend::query(sql, &mut buf).map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_password(&mut self, password: &[u8]) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(password.len() + 8);
        frontend::password_message(password, &mut buf).map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_sasl_initial(
        &mut self,
        mechanism: &str,
        data: &[u8],
    ) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(data.len() + 32);
        frontend::sasl_initial_response(mechanism, data, &mut buf).map_err(|err| {
            ReplicationError {
                message: err.to_string(),
            }
        })?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_sasl_response(&mut self, data: &[u8]) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(data.len() + 8);
        frontend::sasl_response(data, &mut buf).map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn send_copy_data(&mut self, payload: Bytes) -> Result<(), ReplicationError> {
        let mut buf = BytesMut::with_capacity(payload.len() + 8);
        let message = frontend::CopyData::new(payload).map_err(|err| ReplicationError {
            message: err.to_string(),
        })?;
        message.write(&mut buf);
        self.stream.write_all(&buf).await?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<WireMessage, ReplicationError> {
        loop {
            if let Some(header) = Header::parse(&self.read_buf).map_err(ReplicationError::from)? {
                let total_len = header.len() as usize + 1;
                if self.read_buf.len() < total_len {
                    self.read_more().await?;
                    continue;
                }
                if header.tag() == COPY_BOTH_RESPONSE_TAG {
                    let response = parse_copy_both_response(&self.read_buf[..total_len])?;
                    self.read_buf.advance(total_len);
                    return Ok(WireMessage::CopyBothResponse(response));
                }
                if let Some(message) =
                    Message::parse(&mut self.read_buf).map_err(ReplicationError::from)?
                {
                    return Ok(WireMessage::Protocol(message));
                }
            }
            self.read_more().await?;
        }
    }

    async fn read_more(&mut self) -> Result<(), ReplicationError> {
        let read = self.stream.read_buf(&mut self.read_buf).await?;
        if read == 0 {
            return Err(ReplicationError {
                message: "replication connection closed".to_string(),
            });
        }
        Ok(())
    }
}

fn parse_copy_both_response(frame: &[u8]) -> Result<CopyBothResponse, ReplicationError> {
    if frame.len() < 8 {
        return Err(ReplicationError {
            message: "CopyBothResponse frame too short".to_string(),
        });
    }
    let overall_format = frame[5];
    let column_count = BigEndian::read_u16(&frame[6..8]) as usize;
    let mut column_formats = Vec::with_capacity(column_count);
    let mut idx = 8;
    for _ in 0..column_count {
        if idx + 2 > frame.len() {
            return Err(ReplicationError {
                message: "CopyBothResponse column format missing".to_string(),
            });
        }
        column_formats.push(BigEndian::read_i16(&frame[idx..idx + 2]));
        idx += 2;
    }
    Ok(CopyBothResponse {
        overall_format,
        column_formats,
    })
}

fn parse_replication_message(bytes: &[u8]) -> Result<ReplicationMessage, ReplicationError> {
    if bytes.is_empty() {
        return Err(ReplicationError {
            message: "replication message is empty".to_string(),
        });
    }
    match bytes[0] {
        b'w' => {
            if bytes.len() < 25 {
                return Err(ReplicationError {
                    message: "XLogData message too short".to_string(),
                });
            }
            let wal_start = Lsn(BigEndian::read_u64(&bytes[1..9]));
            let wal_end = Lsn(BigEndian::read_u64(&bytes[9..17]));
            let timestamp = BigEndian::read_i64(&bytes[17..25]);
            let data = Bytes::copy_from_slice(&bytes[25..]);
            Ok(ReplicationMessage::XLogData {
                wal_start,
                wal_end,
                timestamp,
                data,
            })
        }
        b'k' => {
            if bytes.len() < 18 {
                return Err(ReplicationError {
                    message: "PrimaryKeepalive message too short".to_string(),
                });
            }
            let wal_end = Lsn(BigEndian::read_u64(&bytes[1..9]));
            let timestamp = BigEndian::read_i64(&bytes[9..17]);
            let reply_requested = bytes[17] != 0;
            Ok(ReplicationMessage::PrimaryKeepalive {
                wal_end,
                timestamp,
                reply_requested,
            })
        }
        other => Err(ReplicationError {
            message: format!("unknown replication message tag {}", other as char),
        }),
    }
}

fn error_message(body: &ErrorResponseBody) -> String {
    let mut fields = body.fields();
    while let Ok(Some(field)) = fields.next() {
        if field.type_() == b'M' {
            return decode_error_field(&field);
        }
    }
    "unknown replication error".to_string()
}

fn decode_error_field(field: &ErrorField<'_>) -> String {
    std::str::from_utf8(field.value_bytes())
        .unwrap_or("unknown replication error")
        .to_string()
}

fn md5_password(password: &[u8], user: &str, salt: [u8; 4]) -> String {
    let inner = postgres_protocol::password::md5(password, user);
    let digest = inner.strip_prefix("md5").unwrap_or(&inner);
    let mut salted = Vec::with_capacity(digest.len() + salt.len());
    salted.extend_from_slice(digest.as_bytes());
    salted.extend_from_slice(&salt);
    let mut hash = Md5::new();
    hash.update(&salted);
    format!("md5{:x}", hash.finalize())
}

fn chrono_timestamp_micros() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let micros = now.as_micros() as i64;
    micros - 946684800000000
}
