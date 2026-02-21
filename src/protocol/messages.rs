use crate::protocol::startup::StartupPacket;
use crate::tcop::postgres::{BackendMessage, FrontendMessage, ReadyForQueryStatus};

const PROTOCOL_VERSION_3: u32 = 196_608;
const SSL_REQUEST_CODE: u32 = 80_877_103;
const CANCEL_REQUEST_CODE: u32 = 80_877_102;
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolError {
    pub message: String,
}

impl std::fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupAction {
    Startup(StartupPacket),
    SslRequest,
    CancelRequest { process_id: u32, secret_key: u32 },
}

pub fn encode_startup_packet(packet: &StartupPacket) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
    for (key, value) in &packet.parameters {
        payload.extend_from_slice(key.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
    }
    payload.push(0);
    with_leading_length(payload)
}

pub fn decode_startup_packet(bytes: &[u8]) -> Result<StartupPacket, ProtocolError> {
    match decode_startup_action(bytes)? {
        StartupAction::Startup(packet) => Ok(packet),
        StartupAction::SslRequest => Err(ProtocolError {
            message: "expected startup packet, found SSL request".to_string(),
        }),
        StartupAction::CancelRequest { .. } => Err(ProtocolError {
            message: "expected startup packet, found cancel request".to_string(),
        }),
    }
}

pub fn decode_startup_action(bytes: &[u8]) -> Result<StartupAction, ProtocolError> {
    if bytes.len() < 8 {
        return Err(ProtocolError {
            message: "startup packet is too short".to_string(),
        });
    }
    let declared_len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if declared_len != bytes.len() {
        return Err(ProtocolError {
            message: "startup packet length mismatch".to_string(),
        });
    }
    let code = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
    match code {
        PROTOCOL_VERSION_3 => parse_startup_parameters(bytes),
        SSL_REQUEST_CODE => {
            if bytes.len() != 8 {
                return Err(ProtocolError {
                    message: "SSL request packet has invalid length".to_string(),
                });
            }
            Ok(StartupAction::SslRequest)
        }
        CANCEL_REQUEST_CODE => {
            if bytes.len() != 16 {
                return Err(ProtocolError {
                    message: "cancel request packet has invalid length".to_string(),
                });
            }
            let process_id = u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
            let secret_key = u32::from_be_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
            Ok(StartupAction::CancelRequest {
                process_id,
                secret_key,
            })
        }
        other => Err(ProtocolError {
            message: format!("unsupported startup code {other}"),
        }),
    }
}

fn parse_startup_parameters(bytes: &[u8]) -> Result<StartupAction, ProtocolError> {
    let mut idx = 8usize;
    let mut params = Vec::new();
    while idx < bytes.len() {
        if bytes[idx] == 0 {
            idx += 1;
            break;
        }
        let key_end = find_zero(bytes, idx).ok_or_else(|| ProtocolError {
            message: "startup packet key terminator missing".to_string(),
        })?;
        let key = decode_utf8(&bytes[idx..key_end], "startup packet key")?;
        idx = key_end + 1;

        let value_end = find_zero(bytes, idx).ok_or_else(|| ProtocolError {
            message: "startup packet value terminator missing".to_string(),
        })?;
        let value = decode_utf8(&bytes[idx..value_end], "startup packet value")?;
        idx = value_end + 1;
        params.push((key, value));
    }
    if idx != bytes.len() {
        return Err(ProtocolError {
            message: "startup packet has trailing garbage".to_string(),
        });
    }

    let user = params
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("user"))
        .map(|(_, value)| value.clone())
        .ok_or_else(|| ProtocolError {
            message: "startup packet is missing user parameter".to_string(),
        })?;
    let database = params
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case("database"))
        .map(|(_, value)| value.clone());

    Ok(StartupAction::Startup(StartupPacket {
        user,
        database,
        parameters: params,
    }))
}

pub fn encode_query_message(sql: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    push_cstring(&mut payload, sql);
    frame_message(b'Q', payload)
}

pub fn decode_query_message(bytes: &[u8]) -> Result<String, ProtocolError> {
    if bytes.len() < 6 {
        return Err(ProtocolError {
            message: "query message is too short".to_string(),
        });
    }
    if bytes[0] != b'Q' {
        return Err(ProtocolError {
            message: "query message has invalid tag".to_string(),
        });
    }
    let declared_len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    if declared_len + 1 != bytes.len() {
        return Err(ProtocolError {
            message: "query message length mismatch".to_string(),
        });
    }
    if bytes[bytes.len() - 1] != 0 {
        return Err(ProtocolError {
            message: "query message must be null terminated".to_string(),
        });
    }
    decode_utf8(&bytes[5..bytes.len() - 1], "query payload")
}

pub fn decode_frontend_message(tag: u8, payload: &[u8]) -> Result<FrontendMessage, ProtocolError> {
    let mut cursor = Cursor::new(payload);
    let message = match tag {
        b'Q' => FrontendMessage::Query {
            sql: cursor.read_cstring()?,
        },
        b'P' => {
            let statement_name = cursor.read_cstring()?;
            let query = cursor.read_cstring()?;
            let count = cursor.read_i16()? as usize;
            let mut parameter_types = Vec::with_capacity(count);
            for _ in 0..count {
                parameter_types.push(cursor.read_i32()? as u32);
            }
            FrontendMessage::Parse {
                statement_name,
                query,
                parameter_types,
            }
        }
        b'B' => {
            let portal_name = cursor.read_cstring()?;
            let statement_name = cursor.read_cstring()?;

            let param_format_count = cursor.read_i16()? as usize;
            let mut param_formats = Vec::with_capacity(param_format_count);
            for _ in 0..param_format_count {
                param_formats.push(cursor.read_i16()?);
            }

            let param_count = cursor.read_i16()? as usize;
            let mut params = Vec::with_capacity(param_count);
            for _ in 0..param_count {
                let len = cursor.read_i32()?;
                if len == -1 {
                    params.push(None);
                } else if len < -1 {
                    return Err(ProtocolError {
                        message: "bind parameter length is invalid".to_string(),
                    });
                } else {
                    let raw = cursor.read_bytes(len as usize)?.to_vec();
                    params.push(Some(raw));
                }
            }

            let result_format_count = cursor.read_i16()? as usize;
            let mut result_formats = Vec::with_capacity(result_format_count);
            for _ in 0..result_format_count {
                result_formats.push(cursor.read_i16()?);
            }

            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                params,
                result_formats,
            }
        }
        b'E' => FrontendMessage::Execute {
            portal_name: cursor.read_cstring()?,
            max_rows: cursor.read_i32()? as i64,
        },
        b'D' => {
            let kind = cursor.read_u8()?;
            let name = cursor.read_cstring()?;
            match kind {
                b'S' => FrontendMessage::DescribeStatement {
                    statement_name: name,
                },
                b'P' => FrontendMessage::DescribePortal { portal_name: name },
                _ => {
                    return Err(ProtocolError {
                        message: "describe message kind must be S or P".to_string(),
                    });
                }
            }
        }
        b'C' => {
            let kind = cursor.read_u8()?;
            let name = cursor.read_cstring()?;
            match kind {
                b'S' => FrontendMessage::CloseStatement {
                    statement_name: name,
                },
                b'P' => FrontendMessage::ClosePortal { portal_name: name },
                _ => {
                    return Err(ProtocolError {
                        message: "close message kind must be S or P".to_string(),
                    });
                }
            }
        }
        b'H' => FrontendMessage::Flush,
        b'S' => FrontendMessage::Sync,
        b'X' => FrontendMessage::Terminate,
        b'd' => {
            let data = cursor.read_bytes(payload.len())?.to_vec();
            FrontendMessage::CopyData { data }
        }
        b'c' => {
            cursor.ensure_consumed()?;
            FrontendMessage::CopyDone
        }
        b'f' => FrontendMessage::CopyFail {
            message: cursor.read_cstring()?,
        },
        b'p' => {
            let message = decode_password_family_message(payload)?;
            let _ = cursor.read_bytes(payload.len())?;
            message
        }
        _ => {
            return Err(ProtocolError {
                message: format!("unsupported frontend message tag {}", tag as char),
            });
        }
    };
    cursor.ensure_consumed()?;
    Ok(message)
}

pub fn encode_backend_message(message: &BackendMessage) -> Option<Vec<u8>> {
    match message {
        BackendMessage::AuthenticationOk => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&0i32.to_be_bytes());
            Some(frame_message(b'R', payload))
        }
        BackendMessage::AuthenticationCleartextPassword => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&3i32.to_be_bytes());
            Some(frame_message(b'R', payload))
        }
        BackendMessage::AuthenticationSasl { mechanisms } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&10i32.to_be_bytes());
            for mechanism in mechanisms {
                push_cstring(&mut payload, mechanism);
            }
            payload.push(0);
            Some(frame_message(b'R', payload))
        }
        BackendMessage::AuthenticationSaslContinue { data } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&11i32.to_be_bytes());
            payload.extend_from_slice(data);
            Some(frame_message(b'R', payload))
        }
        BackendMessage::AuthenticationSaslFinal { data } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&12i32.to_be_bytes());
            payload.extend_from_slice(data);
            Some(frame_message(b'R', payload))
        }
        BackendMessage::ParameterStatus { name, value } => {
            let mut payload = Vec::new();
            push_cstring(&mut payload, name);
            push_cstring(&mut payload, value);
            Some(frame_message(b'S', payload))
        }
        BackendMessage::BackendKeyData {
            process_id,
            secret_key,
        } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&process_id.to_be_bytes());
            payload.extend_from_slice(&secret_key.to_be_bytes());
            Some(frame_message(b'K', payload))
        }
        BackendMessage::NoticeResponse {
            message,
            code,
            detail,
            hint,
        } => Some(frame_message(
            b'N',
            encode_error_or_notice(
                "NOTICE",
                code,
                message,
                detail.as_deref(),
                hint.as_deref(),
                None,
            ),
        )),
        BackendMessage::ReadyForQuery { status } => {
            let code = match status {
                ReadyForQueryStatus::Idle => b'I',
                ReadyForQueryStatus::InTransaction => b'T',
                ReadyForQueryStatus::FailedTransaction => b'E',
            };
            Some(frame_message(b'Z', vec![code]))
        }
        BackendMessage::ParseComplete => Some(frame_message(b'1', Vec::new())),
        BackendMessage::BindComplete => Some(frame_message(b'2', Vec::new())),
        BackendMessage::CloseComplete => Some(frame_message(b'3', Vec::new())),
        BackendMessage::EmptyQueryResponse => Some(frame_message(b'I', Vec::new())),
        BackendMessage::DataRow { values } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(values.len() as i16).to_be_bytes());
            for value in values {
                let bytes = value.as_bytes();
                payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                payload.extend_from_slice(bytes);
            }
            Some(frame_message(b'D', payload))
        }
        BackendMessage::DataRowBinary { values } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(values.len() as i16).to_be_bytes());
            for value in values {
                match value {
                    Some(bytes) => {
                        payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                        payload.extend_from_slice(bytes);
                    }
                    None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
                }
            }
            Some(frame_message(b'D', payload))
        }
        BackendMessage::CommandComplete { tag, rows } => {
            let rendered = render_command_complete(tag, *rows);
            let mut payload = Vec::new();
            push_cstring(&mut payload, &rendered);
            Some(frame_message(b'C', payload))
        }
        BackendMessage::ParameterDescription { parameter_types } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(parameter_types.len() as i16).to_be_bytes());
            for oid in parameter_types {
                payload.extend_from_slice(&oid.to_be_bytes());
            }
            Some(frame_message(b't', payload))
        }
        BackendMessage::RowDescription { fields } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(fields.len() as i16).to_be_bytes());
            for field in fields {
                push_cstring(&mut payload, &field.name);
                payload.extend_from_slice(&field.table_oid.to_be_bytes());
                payload.extend_from_slice(&field.column_attr.to_be_bytes());
                payload.extend_from_slice(&field.type_oid.to_be_bytes());
                payload.extend_from_slice(&field.type_size.to_be_bytes());
                payload.extend_from_slice(&field.type_modifier.to_be_bytes());
                payload.extend_from_slice(&field.format_code.to_be_bytes());
            }
            Some(frame_message(b'T', payload))
        }
        BackendMessage::NoData => Some(frame_message(b'n', Vec::new())),
        BackendMessage::PortalSuspended => Some(frame_message(b's', Vec::new())),
        BackendMessage::CopyInResponse {
            overall_format,
            column_formats,
        } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(*overall_format).to_be_bytes());
            payload.extend_from_slice(&(column_formats.len() as i16).to_be_bytes());
            for format in column_formats {
                payload.extend_from_slice(&format.to_be_bytes());
            }
            Some(frame_message(b'G', payload))
        }
        BackendMessage::CopyOutResponse {
            overall_format,
            column_formats,
        } => {
            let mut payload = Vec::new();
            payload.extend_from_slice(&(*overall_format).to_be_bytes());
            payload.extend_from_slice(&(column_formats.len() as i16).to_be_bytes());
            for format in column_formats {
                payload.extend_from_slice(&format.to_be_bytes());
            }
            Some(frame_message(b'H', payload))
        }
        BackendMessage::CopyData { data } => Some(frame_message(b'd', data.clone())),
        BackendMessage::CopyDone => Some(frame_message(b'c', Vec::new())),
        BackendMessage::ErrorResponse {
            message,
            code,
            detail,
            hint,
            position,
        } => Some(frame_message(
            b'E',
            encode_error_or_notice(
                "ERROR",
                code,
                message,
                detail.as_deref(),
                hint.as_deref(),
                *position,
            ),
        )),
        BackendMessage::NotificationResponse {
            process_id,
            channel,
            payload,
        } => {
            let mut buf = Vec::new();
            buf.extend_from_slice(&process_id.to_be_bytes());
            push_cstring(&mut buf, channel);
            push_cstring(&mut buf, payload);
            Some(frame_message(b'A', buf))
        }
        BackendMessage::FlushComplete => None,
        BackendMessage::Terminate => None,
    }
}

fn encode_error_or_notice(
    severity: &str,
    code: &str,
    message: &str,
    detail: Option<&str>,
    hint: Option<&str>,
    position: Option<u32>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(b'S');
    push_cstring(&mut payload, severity);
    payload.push(b'V');
    push_cstring(&mut payload, severity);
    payload.push(b'C');
    push_cstring(&mut payload, code);
    payload.push(b'M');
    push_cstring(&mut payload, message);
    if let Some(detail) = detail {
        payload.push(b'D');
        push_cstring(&mut payload, detail);
    }
    if let Some(hint) = hint {
        payload.push(b'H');
        push_cstring(&mut payload, hint);
    }
    if let Some(position) = position {
        payload.push(b'P');
        push_cstring(&mut payload, &position.to_string());
    }
    payload.push(0);
    payload
}

fn render_command_complete(tag: &str, rows: u64) -> String {
    let upper = tag.trim().to_ascii_uppercase();
    match upper.as_str() {
        "INSERT" => format!("INSERT 0 {rows}"),
        "SELECT" | "UPDATE" | "DELETE" | "MERGE" | "MOVE" | "FETCH" | "COPY" => {
            format!("{upper} {rows}")
        }
        _ => upper,
    }
}

fn with_leading_length(mut payload: Vec<u8>) -> Vec<u8> {
    let len = (payload.len() + 4) as u32;
    let mut out = Vec::with_capacity(payload.len() + 4);
    out.extend_from_slice(&len.to_be_bytes());
    out.append(&mut payload);
    out
}

fn frame_message(tag: u8, payload: Vec<u8>) -> Vec<u8> {
    let len = (payload.len() + 4) as u32;
    let mut out = Vec::with_capacity(payload.len() + 5);
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

fn push_cstring(out: &mut Vec<u8>, value: &str) {
    out.extend_from_slice(value.as_bytes());
    out.push(0);
}

fn find_zero(bytes: &[u8], from: usize) -> Option<usize> {
    bytes[from..]
        .iter()
        .position(|b| *b == 0)
        .map(|offset| from + offset)
}

fn decode_utf8(bytes: &[u8], context: &str) -> Result<String, ProtocolError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| ProtocolError {
        message: format!("{context} is not valid utf8"),
    })
}

fn decode_password_family_message(payload: &[u8]) -> Result<FrontendMessage, ProtocolError> {
    if payload.is_empty() {
        return Ok(FrontendMessage::SaslResponse { data: Vec::new() });
    }

    let mut cursor = Cursor::new(payload);
    if let Ok(mechanism) = cursor.read_cstring()
        && !mechanism.is_empty()
        && cursor.remaining() >= 4
    {
        let len = cursor.read_i32()?;
        if len >= 0 && cursor.remaining() == len as usize {
            let data = cursor.read_bytes(len as usize)?.to_vec();
            cursor.ensure_consumed()?;
            return Ok(FrontendMessage::SaslInitialResponse { mechanism, data });
        }
    }

    let mut cursor = Cursor::new(payload);
    if let Ok(password) = cursor.read_cstring()
        && cursor.remaining() == 0
    {
        return Ok(FrontendMessage::Password { password });
    }

    Ok(FrontendMessage::SaslResponse {
        data: payload.to_vec(),
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    idx: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, idx: 0 }
    }

    fn ensure_consumed(&self) -> Result<(), ProtocolError> {
        if self.idx == self.bytes.len() {
            Ok(())
        } else {
            Err(ProtocolError {
                message: "message payload has trailing bytes".to_string(),
            })
        }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.idx)
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        if self.idx + len > self.bytes.len() {
            return Err(ProtocolError {
                message: "message payload is truncated".to_string(),
            });
        }
        let out = &self.bytes[self.idx..self.idx + len];
        self.idx += len;
        Ok(out)
    }

    fn read_u8(&mut self) -> Result<u8, ProtocolError> {
        let bytes = self.read_bytes(1)?;
        Ok(bytes[0])
    }

    fn read_i16(&mut self) -> Result<i16, ProtocolError> {
        let bytes = self.read_bytes(2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_i32(&mut self) -> Result<i32, ProtocolError> {
        let bytes = self.read_bytes(4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_cstring(&mut self) -> Result<String, ProtocolError> {
        let start = self.idx;
        let Some(end) = find_zero(self.bytes, start) else {
            return Err(ProtocolError {
                message: "cstring terminator missing".to_string(),
            });
        };
        self.idx = end + 1;
        decode_utf8(&self.bytes[start..end], "cstring")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tcop::postgres::{BackendMessage, FrontendMessage, RowDescriptionField};

    fn startup_ssl_request() -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&8u32.to_be_bytes());
        out.extend_from_slice(&SSL_REQUEST_CODE.to_be_bytes());
        out
    }

    fn startup_cancel_request() -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&16u32.to_be_bytes());
        out.extend_from_slice(&CANCEL_REQUEST_CODE.to_be_bytes());
        out.extend_from_slice(&123u32.to_be_bytes());
        out.extend_from_slice(&456u32.to_be_bytes());
        out
    }

    #[test]
    fn startup_packet_roundtrip() {
        let packet = StartupPacket::new("alice", Some("appdb".to_string()))
            .with_parameter("application_name", "openassay-tests");
        let encoded = encode_startup_packet(&packet);
        let decoded = decode_startup_packet(&encoded).expect("startup decode should succeed");
        assert_eq!(decoded.user, "alice");
        assert_eq!(decoded.database.as_deref(), Some("appdb"));
        assert_eq!(
            decoded.parameter("application_name"),
            Some("openassay-tests")
        );
    }

    #[test]
    fn startup_action_detects_ssl_and_cancel() {
        assert_eq!(
            decode_startup_action(&startup_ssl_request()).expect("ssl request should decode"),
            StartupAction::SslRequest
        );
        assert_eq!(
            decode_startup_action(&startup_cancel_request()).expect("cancel request should decode"),
            StartupAction::CancelRequest {
                process_id: 123,
                secret_key: 456
            }
        );
    }

    #[test]
    fn query_message_roundtrip() {
        let encoded = encode_query_message("SELECT 1");
        let decoded = decode_query_message(&encoded).expect("query decode should succeed");
        assert_eq!(decoded, "SELECT 1");
    }

    #[test]
    fn decodes_parse_bind_execute_messages() {
        let parse_payload = {
            let mut payload = Vec::new();
            push_cstring(&mut payload, "s1");
            push_cstring(&mut payload, "SELECT $1");
            payload.extend_from_slice(&1i16.to_be_bytes());
            payload.extend_from_slice(&23u32.to_be_bytes());
            payload
        };
        let parse = decode_frontend_message(b'P', &parse_payload).expect("parse should decode");
        assert_eq!(
            parse,
            FrontendMessage::Parse {
                statement_name: "s1".to_string(),
                query: "SELECT $1".to_string(),
                parameter_types: vec![23],
            }
        );

        let bind_payload = {
            let mut payload = Vec::new();
            push_cstring(&mut payload, "p1");
            push_cstring(&mut payload, "s1");
            payload.extend_from_slice(&0i16.to_be_bytes());
            payload.extend_from_slice(&1i16.to_be_bytes());
            payload.extend_from_slice(&1i32.to_be_bytes());
            payload.extend_from_slice(b"7");
            payload.extend_from_slice(&0i16.to_be_bytes());
            payload
        };
        let bind = decode_frontend_message(b'B', &bind_payload).expect("bind should decode");
        assert_eq!(
            bind,
            FrontendMessage::Bind {
                portal_name: "p1".to_string(),
                statement_name: "s1".to_string(),
                param_formats: vec![],
                params: vec![Some(b"7".to_vec())],
                result_formats: vec![],
            }
        );

        let execute_payload = {
            let mut payload = Vec::new();
            push_cstring(&mut payload, "p1");
            payload.extend_from_slice(&100i32.to_be_bytes());
            payload
        };
        let execute =
            decode_frontend_message(b'E', &execute_payload).expect("execute should decode");
        assert_eq!(
            execute,
            FrontendMessage::Execute {
                portal_name: "p1".to_string(),
                max_rows: 100
            }
        );
    }

    #[test]
    fn decodes_password_and_sasl_messages() {
        let cleartext_payload = b"secret\0".to_vec();
        let cleartext =
            decode_frontend_message(b'p', &cleartext_payload).expect("password should decode");
        assert_eq!(
            cleartext,
            FrontendMessage::Password {
                password: "secret".to_string()
            }
        );

        let sasl_initial_payload = {
            let data = b"n,,n=u,r=nonce".to_vec();
            let mut payload = Vec::new();
            push_cstring(&mut payload, "SCRAM-SHA-256");
            payload.extend_from_slice(&(data.len() as i32).to_be_bytes());
            payload.extend_from_slice(&data);
            payload
        };
        let sasl_initial =
            decode_frontend_message(b'p', &sasl_initial_payload).expect("sasl initial decode");
        assert_eq!(
            sasl_initial,
            FrontendMessage::SaslInitialResponse {
                mechanism: "SCRAM-SHA-256".to_string(),
                data: b"n,,n=u,r=nonce".to_vec(),
            }
        );

        let sasl_response =
            decode_frontend_message(b'p', b"c=biws,r=abc,p=proof").expect("sasl response decode");
        assert_eq!(
            sasl_response,
            FrontendMessage::SaslResponse {
                data: b"c=biws,r=abc,p=proof".to_vec(),
            }
        );
    }

    #[test]
    fn encodes_backend_messages_into_wire_frames() {
        let frame = encode_backend_message(&BackendMessage::AuthenticationOk)
            .expect("auth ok should encode");
        assert_eq!(frame[0], b'R');

        let frame = encode_backend_message(&BackendMessage::RowDescription {
            fields: vec![RowDescriptionField {
                name: "id".to_string(),
                table_oid: 0,
                column_attr: 0,
                type_oid: 20,
                type_size: 8,
                type_modifier: -1,
                format_code: 0,
            }],
        })
        .expect("row description should encode");
        assert_eq!(frame[0], b'T');

        let frame = encode_backend_message(&BackendMessage::DataRow {
            values: vec!["1".to_string()],
        })
        .expect("data row should encode");
        assert_eq!(frame[0], b'D');

        let frame = encode_backend_message(&BackendMessage::DataRowBinary {
            values: vec![Some(vec![0, 0, 0, 0, 0, 0, 0, 1]), None],
        })
        .expect("binary data row should encode");
        assert_eq!(frame[0], b'D');

        let frame = encode_backend_message(&BackendMessage::CopyInResponse {
            overall_format: 1,
            column_formats: vec![1],
        })
        .expect("copy in response should encode");
        assert_eq!(frame[0], b'G');

        let frame = encode_backend_message(&BackendMessage::AuthenticationSasl {
            mechanisms: vec!["SCRAM-SHA-256".to_string()],
        })
        .expect("auth sasl should encode");
        assert_eq!(frame[0], b'R');
    }

    #[test]
    fn encodes_error_response_with_sqlstate_detail_hint_and_position() {
        let frame = encode_backend_message(&BackendMessage::ErrorResponse {
            message: "syntax error".to_string(),
            code: "42601".to_string(),
            detail: Some("unexpected token".to_string()),
            hint: Some("check query near SELECT".to_string()),
            position: Some(17),
        })
        .expect("error response should encode");
        assert_eq!(frame[0], b'E');
        let payload = &frame[5..];
        let expected = b"SERROR\0VERROR\0C42601\0Msyntax error\0Dunexpected token\0Hcheck query near SELECT\0P17\0\0";
        assert_eq!(payload, expected);
    }
}
