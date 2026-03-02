#[allow(clippy::wildcard_imports)]
use super::*;

impl PostgresSession {
    pub(super) fn copy_column_type_oids(
        &self,
        table_name: &[String],
    ) -> Result<Vec<PgType>, SessionError> {
        security::with_current_role(&self.current_role, || copy_table_column_oids(table_name))
            .map_err(SessionError::from)
    }

    pub(super) fn copy_column_type_oids_for_columns(
        &self,
        table_name: &[String],
        columns: &[String],
    ) -> Result<Vec<PgType>, SessionError> {
        let all_oids = self.copy_column_type_oids(table_name)?;
        let all_columns =
            security::with_current_role(&self.current_role, || copy_table_column_names(table_name))
                .map_err(SessionError::from)?;
        let mut result = Vec::with_capacity(columns.len());
        for col in columns {
            let col_lower = col.to_ascii_lowercase();
            let idx = all_columns
                .iter()
                .position(|c| c.to_ascii_lowercase() == col_lower)
                .ok_or_else(|| SessionError {
                    message: format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        col,
                        table_name.last().unwrap_or(&String::new())
                    ),
                })?;
            result.push(all_oids[idx]);
        }
        Ok(result)
    }

    pub(super) async fn copy_snapshot(
        &self,
        table_name: &[String],
    ) -> Result<crate::tcop::engine::CopyBinarySnapshot, SessionError> {
        security::with_current_role_async(&self.current_role, || async {
            copy_table_binary_snapshot(table_name).await
        })
        .await
        .map_err(SessionError::from)
    }

    pub(super) async fn copy_snapshot_in_transaction_scope(
        &mut self,
        table_name: &[String],
    ) -> Result<crate::tcop::engine::CopyBinarySnapshot, SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;
        restore_state(working);
        let result = security::with_current_role_async(&self.current_role, || async {
            copy_table_binary_snapshot(table_name).await
        })
        .await;
        restore_state(baseline);
        result.map_err(SessionError::from)
    }

    pub(super) fn exec_copy_data(&mut self, data: Vec<u8>) -> Result<(), SessionError> {
        let Some(state) = self.copy_in_state.as_mut() else {
            return Err(SessionError {
                message: "COPY data was sent without COPY IN state".to_string(),
            });
        };
        state.payload.extend_from_slice(&data);
        Ok(())
    }

    pub(super) async fn exec_copy_done(
        &mut self,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        let state = self.copy_in_state.take().ok_or_else(|| SessionError {
            message: "COPY done was sent without COPY IN state".to_string(),
        })?;
        let mut rows = match state.format {
            CopyFormat::Binary => {
                parse_copy_binary_stream(&state.payload, &state.column_type_oids)?
            }
            CopyFormat::Text | CopyFormat::Csv => parse_copy_text_stream(
                &state.payload,
                &state.column_type_oids,
                state.delimiter,
                &state.null_marker,
                matches!(state.format, CopyFormat::Csv),
                state.header,
            )?,
        };

        // If a column list was specified, expand partial rows to full table width
        if !state.columns.is_empty() {
            let all_columns = security::with_current_role(&self.current_role, || {
                copy_table_column_names(&state.table_name)
            })
            .map_err(SessionError::from)?;
            let all_oids = self.copy_column_type_oids(&state.table_name)?;
            // Build mapping: for each specified column, find its index in the full table
            let mut col_indices = Vec::with_capacity(state.columns.len());
            for col in &state.columns {
                let col_lower = col.to_ascii_lowercase();
                let idx = all_columns
                    .iter()
                    .position(|c| c.to_ascii_lowercase() == col_lower)
                    .ok_or_else(|| SessionError {
                        message: format!("column \"{col}\" does not exist"),
                    })?;
                col_indices.push(idx);
            }
            rows = rows
                .into_iter()
                .map(|partial_row| {
                    let mut full_row = vec![ScalarValue::Null; all_oids.len()];
                    for (src_idx, &dst_idx) in col_indices.iter().enumerate() {
                        if src_idx < partial_row.len() {
                            full_row[dst_idx] = partial_row[src_idx].clone();
                        }
                    }
                    full_row
                })
                .collect();
        }

        let inserted = match self.tx_state.visibility_mode() {
            VisibilityMode::Global => {
                security::with_current_role_async(&self.current_role, || async {
                    copy_insert_rows(&state.table_name, rows).await
                })
                .await
                .map_err(SessionError::from)?
            }
            VisibilityMode::TransactionLocal => {
                self.copy_insert_rows_in_transaction_scope(&state.table_name, rows)
                    .await?
            }
        };

        out.push(BackendMessage::CommandComplete {
            tag: "COPY".to_string(),
            rows: inserted,
        });
        self.finish_xact_command();
        Ok(())
    }

    pub(super) async fn copy_insert_rows_in_transaction_scope(
        &mut self,
        table_name: &[String],
        rows: Vec<Vec<ScalarValue>>,
    ) -> Result<u64, SessionError> {
        let baseline = snapshot_state();
        let working = self
            .tx_state
            .working_snapshot()
            .cloned()
            .or_else(|| self.tx_state.base_snapshot().cloned())
            .ok_or_else(|| SessionError {
                message: "transaction state missing working snapshot".to_string(),
            })?;
        restore_state(working);
        let executed = security::with_current_role_async(&self.current_role, || async {
            copy_insert_rows(table_name, rows).await
        })
        .await;
        let next_working = executed.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);
        if let Some(snapshot) = next_working {
            self.tx_state.set_working_snapshot(snapshot);
        }
        executed.map_err(SessionError::from)
    }

    pub(super) fn exec_copy_fail(&mut self, message: String) -> Result<(), SessionError> {
        if self.copy_in_state.is_none() {
            return Err(SessionError {
                message: "COPY fail was sent without COPY IN state".to_string(),
            });
        }
        self.copy_in_state = None;
        self.finish_xact_command();
        Err(SessionError {
            message: format!("COPY failed: {message}"),
        })
    }
}

#[derive(Debug, Clone)]
struct ParsedCopyTextField {
    value: String,
    quoted: bool,
}

pub(super) fn encode_copy_text_stream(
    rows: &[Vec<ScalarValue>],
    column_type_oids: &[PgType],
    delimiter: char,
    null_marker: &str,
    csv: bool,
    header: bool,
    column_names: &[String],
) -> Result<Vec<u8>, SessionError> {
    let mut out = String::new();
    if header && !column_names.is_empty() {
        out.push_str(
            &column_names
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>()
                .join(&delimiter.to_string()),
        );
        out.push('\n');
    }
    for row in rows {
        if row.len() != column_type_oids.len() {
            return Err(SessionError {
                message: "COPY row width does not match relation column count".to_string(),
            });
        }
        let line = if csv {
            encode_copy_csv_row(row, delimiter, null_marker)
        } else {
            encode_copy_text_row(row, delimiter, null_marker)
        };
        out.push_str(&line);
        out.push('\n');
    }
    Ok(out.into_bytes())
}

fn encode_copy_text_row(row: &[ScalarValue], delimiter: char, null_marker: &str) -> String {
    row.iter()
        .map(|value| match value {
            ScalarValue::Null => null_marker.to_string(),
            other => escape_copy_text_value(&other.render(), delimiter),
        })
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

fn escape_copy_text_value(value: &str, delimiter: char) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if ch == delimiter => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn encode_copy_csv_row(row: &[ScalarValue], delimiter: char, null_marker: &str) -> String {
    row.iter()
        .map(|value| match value {
            ScalarValue::Null => null_marker.to_string(),
            other => {
                let text = other.render();
                let must_quote = text.contains(delimiter)
                    || text.contains('\n')
                    || text.contains('\r')
                    || text.contains('"')
                    || text == null_marker;
                if must_quote {
                    let escaped = text.replace('"', "\"\"");
                    format!("\"{escaped}\"")
                } else {
                    text
                }
            }
        })
        .collect::<Vec<_>>()
        .join(&delimiter.to_string())
}

fn parse_copy_text_stream(
    payload: &[u8],
    column_type_oids: &[PgType],
    delimiter: char,
    null_marker: &str,
    csv: bool,
    header: bool,
) -> Result<Vec<Vec<ScalarValue>>, SessionError> {
    let text = String::from_utf8(payload.to_vec()).map_err(|_| SessionError {
        message: "COPY text payload is not valid utf8".to_string(),
    })?;
    if text.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    let lines = text.split('\n').collect::<Vec<_>>();
    let mut header_skipped = false;
    for (idx, raw_line) in lines.iter().enumerate() {
        let mut line = *raw_line;
        if let Some(stripped) = line.strip_suffix('\r') {
            line = stripped;
        }
        if line.is_empty() && idx + 1 == lines.len() {
            continue;
        }
        // Skip the first line if HEADER is specified (matches PG behavior)
        if header && !header_skipped {
            header_skipped = true;
            continue;
        }
        let fields = if csv {
            parse_copy_csv_line(line, delimiter)?
        } else {
            parse_copy_text_line(line, delimiter)
        };
        if fields.len() != column_type_oids.len() {
            return Err(SessionError {
                message: format!(
                    "COPY row has {} columns but relation expects {}",
                    fields.len(),
                    column_type_oids.len()
                ),
            });
        }
        let mut row = Vec::with_capacity(fields.len());
        for (field, type_oid) in fields.iter().zip(column_type_oids.iter()) {
            row.push(parse_copy_text_field(field, *type_oid, null_marker)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

fn parse_copy_text_line(line: &str, delimiter: char) -> Vec<ParsedCopyTextField> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            let decoded = match ch {
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                other => other,
            };
            current.push(decoded);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == delimiter {
            fields.push(ParsedCopyTextField {
                value: std::mem::take(&mut current),
                quoted: false,
            });
            continue;
        }
        current.push(ch);
    }
    if escaped {
        current.push('\\');
    }
    fields.push(ParsedCopyTextField {
        value: current,
        quoted: false,
    });
    fields
}

fn parse_copy_csv_line(
    line: &str,
    delimiter: char,
) -> Result<Vec<ParsedCopyTextField>, SessionError> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quoted = false;
    let chars = line.chars().collect::<Vec<_>>();
    let mut idx = 0usize;
    while idx < chars.len() {
        let ch = chars[idx];
        if in_quotes {
            if ch == '"' {
                if idx + 1 < chars.len() && chars[idx + 1] == '"' {
                    current.push('"');
                    idx += 2;
                    continue;
                }
                in_quotes = false;
                idx += 1;
                continue;
            }
            current.push(ch);
            idx += 1;
            continue;
        }
        if ch == delimiter {
            fields.push(ParsedCopyTextField {
                value: std::mem::take(&mut current),
                quoted,
            });
            quoted = false;
            idx += 1;
            continue;
        }
        if ch == '"' && current.is_empty() {
            in_quotes = true;
            quoted = true;
            idx += 1;
            continue;
        }
        current.push(ch);
        idx += 1;
    }
    if in_quotes {
        return Err(SessionError {
            message: "COPY CSV payload has unterminated quoted field".to_string(),
        });
    }
    fields.push(ParsedCopyTextField {
        value: current,
        quoted,
    });
    Ok(fields)
}

fn parse_copy_text_field(
    field: &ParsedCopyTextField,
    type_oid: PgType,
    null_marker: &str,
) -> Result<ScalarValue, SessionError> {
    if !field.quoted && field.value == null_marker {
        return Ok(ScalarValue::Null);
    }
    match type_oid {
        16 => match field.value.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "1" => Ok(ScalarValue::Bool(true)),
            "false" | "f" | "0" => Ok(ScalarValue::Bool(false)),
            _ => Err(SessionError {
                message: "COPY boolean field is invalid".to_string(),
            }),
        },
        20 => field
            .value
            .trim()
            .parse::<i64>()
            .map(ScalarValue::Int)
            .map_err(|_| SessionError {
                message: "COPY integer field is invalid".to_string(),
            }),
        701 => field
            .value
            .trim()
            .parse::<f64>()
            .map(ScalarValue::Float)
            .map_err(|_| SessionError {
                message: "COPY float field is invalid".to_string(),
            }),
        25 => Ok(ScalarValue::Text(field.value.clone())),
        1082 => {
            let days = parse_pg_date_days(field.value.trim())?;
            Ok(ScalarValue::Text(format_pg_date_from_days(days)))
        }
        1114 => {
            let micros = parse_pg_timestamp_micros(field.value.trim())?;
            Ok(ScalarValue::Text(format_pg_timestamp_from_micros(micros)))
        }
        other => Err(SessionError {
            message: format!("unsupported COPY type oid {other}"),
        }),
    }
}

pub(super) fn encode_copy_binary_stream(
    columns: &[crate::tcop::engine::CopyBinaryColumn],
    rows: &[Vec<ScalarValue>],
) -> Result<Vec<u8>, SessionError> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());

    for row in rows {
        if row.len() != columns.len() {
            return Err(SessionError {
                message: "COPY row width does not match relation column count".to_string(),
            });
        }
        out.extend_from_slice(&(columns.len() as i16).to_be_bytes());
        for (value, column) in row.iter().zip(columns.iter()) {
            encode_copy_binary_field(&mut out, value, column.type_oid)?;
        }
    }
    out.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(out)
}

fn encode_copy_binary_field(
    out: &mut Vec<u8>,
    value: &ScalarValue,
    type_oid: PgType,
) -> Result<(), SessionError> {
    if matches!(value, ScalarValue::Null) {
        out.extend_from_slice(&(-1i32).to_be_bytes());
        return Ok(());
    }

    let bytes = encode_binary_scalar(value, type_oid, "COPY")?;
    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    out.extend_from_slice(&bytes);
    Ok(())
}

fn parse_copy_binary_stream(
    payload: &[u8],
    column_type_oids: &[PgType],
) -> Result<Vec<Vec<ScalarValue>>, SessionError> {
    let mut idx = 0usize;
    let read_bytes = |idx: &mut usize, n: usize| -> Result<&[u8], SessionError> {
        if *idx + n > payload.len() {
            return Err(SessionError {
                message: "COPY binary payload is truncated".to_string(),
            });
        }
        let out = &payload[*idx..*idx + n];
        *idx += n;
        Ok(out)
    };
    let read_i16 = |idx: &mut usize| -> Result<i16, SessionError> {
        let bytes = read_bytes(idx, 2)?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    };
    let read_i32 = |idx: &mut usize| -> Result<i32, SessionError> {
        let bytes = read_bytes(idx, 4)?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    };

    let signature = read_bytes(&mut idx, 11)?;
    if signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(SessionError {
            message: "invalid COPY binary signature".to_string(),
        });
    }
    let _flags = read_i32(&mut idx)?;
    let extension_len = read_i32(&mut idx)?;
    if extension_len < 0 {
        return Err(SessionError {
            message: "invalid COPY extension length".to_string(),
        });
    }
    let _ = read_bytes(&mut idx, extension_len as usize)?;

    let mut rows = Vec::new();
    loop {
        let field_count = read_i16(&mut idx)?;
        if field_count == -1 {
            break;
        }
        if field_count < 0 {
            return Err(SessionError {
                message: "invalid COPY row field count".to_string(),
            });
        }
        if field_count as usize != column_type_oids.len() {
            return Err(SessionError {
                message: format!(
                    "COPY row has {} columns but relation expects {}",
                    field_count,
                    column_type_oids.len()
                ),
            });
        }

        let mut row = Vec::with_capacity(field_count as usize);
        for type_oid in column_type_oids {
            let len = read_i32(&mut idx)?;
            if len == -1 {
                row.push(ScalarValue::Null);
                continue;
            }
            if len < -1 {
                return Err(SessionError {
                    message: "invalid COPY field length".to_string(),
                });
            }
            let raw = read_bytes(&mut idx, len as usize)?;
            let value = decode_binary_scalar(raw, *type_oid, "COPY")?;
            row.push(value);
        }
        rows.push(row);
    }

    if idx != payload.len() {
        return Err(SessionError {
            message: "COPY payload has trailing bytes".to_string(),
        });
    }
    Ok(rows)
}
