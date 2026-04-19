#[allow(clippy::wildcard_imports)]
use super::*;
use crate::parser::sql_parser::parse_statement;
use crate::tcop::pquery::infer_statement_parameter_types;

impl PostgresSession {
    pub(super) fn exec_parse_message(
        &mut self,
        statement_name: &str,
        query_string: &str,
        parameter_types: Vec<PgType>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let operation = self.plan_query_string(query_string)?;
        if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let parameter_types = resolve_parse_parameter_types(query_string, parameter_types)?;
        // Fill in any slots still marked unknown (0) by walking the AST and
        // inferring from context (casts, comparisons, DML targets). Without
        // this, drivers like tokio-postgres recurse trying to resolve OID 0
        // through pg_type.
        let parameter_types = if parameter_types.contains(&0) {
            match parse_statement(query_string) {
                Ok(stmt) => infer_statement_parameter_types(&stmt, parameter_types),
                Err(_) => parameter_types,
            }
        } else {
            parameter_types
        };
        let prepared = PreparedStatement {
            operation,
            parameter_types,
        };

        if statement_name.is_empty() {
            self.drop_unnamed_stmt();
            self.prepared_statements
                .insert(UNNAMED.to_string(), prepared);
        } else {
            self.prepared_statements
                .insert(statement_name.to_string(), prepared);
        }

        out.push(BackendMessage::ParseComplete);
        Ok(())
    }

    pub(super) fn exec_bind_message(
        &mut self,
        portal_name: &str,
        statement_name: &str,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let prepared = self.fetch_prepared_statement(statement_name)?.clone();
        if params.len() != prepared.parameter_types.len() {
            return Err(SessionError {
                message: format!(
                    "bind message supplies {} parameters, but prepared statement \"{}\" requires {}",
                    params.len(),
                    if statement_name.is_empty() {
                        "<unnamed>"
                    } else {
                        statement_name
                    },
                    prepared.parameter_types.len()
                ),
            });
        }

        let normalized_param_formats =
            normalize_format_codes(&param_formats, params.len(), "bind parameter format codes")?;
        let params = params
            .into_iter()
            .enumerate()
            .map(|(idx, param)| {
                decode_bind_parameter(
                    idx,
                    param,
                    normalized_param_formats[idx],
                    prepared.parameter_types[idx],
                )
            })
            .collect::<Result<Vec<_>, _>>()?;

        let result_column_count = match &prepared.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => plan.columns().len(),
            _ => 0,
        };
        let normalized_result_formats = normalize_format_codes(
            &result_formats,
            result_column_count,
            "bind result format codes",
        )?;

        if self.is_aborted_transaction_block()
            && !prepared.operation.allowed_in_failed_transaction()
        {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let portal = Portal {
            operation: prepared.operation,
            params,
            result_format_codes: normalized_result_formats,
            result_cache: None,
            cursor: 0,
        };

        let key = portal_key(portal_name);
        self.portals.insert(key, portal);

        out.push(BackendMessage::BindComplete);
        Ok(())
    }

    pub(super) async fn exec_execute_message(
        &mut self,
        portal_name: &str,
        max_rows: i64,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();

        let key = portal_key(portal_name);
        let (operation, params, result_formats, cached_result, cursor) = {
            let portal = self.portals.get(&key).ok_or_else(|| SessionError {
                message: format!("portal \"{portal_name}\" does not exist"),
            })?;
            (
                portal.operation.clone(),
                portal.params.clone(),
                portal.result_format_codes.clone(),
                portal.result_cache.clone(),
                portal.cursor,
            )
        };

        if matches!(operation, PlannedOperation::Empty) {
            out.push(BackendMessage::EmptyQueryResponse);
            return Ok(());
        }

        if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        let outcome = if let Some(result) = cached_result {
            ExecutionOutcome::Query(result)
        } else {
            self.execute_operation(&operation, &params).await?
        };
        let row_description = operation_row_description_fields(&operation, &result_formats)?;

        let portal = self.portals.get_mut(&key).ok_or_else(|| SessionError {
            message: format!("portal \"{portal_name}\" does not exist"),
        })?;

        if portal.result_cache.is_none()
            && let ExecutionOutcome::Query(result) = &outcome
        {
            portal.result_cache = Some(result.clone());
        }

        Self::emit_outcome(
            out,
            outcome,
            max_rows,
            Some((portal, cursor)),
            row_description.as_deref(),
        )?;

        if operation.is_transaction_exit() {
            self.finish_xact_command();
        }

        Ok(())
    }

    pub(super) fn exec_describe_statement_message(
        &mut self,
        statement_name: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        let prepared = self.fetch_prepared_statement(statement_name)?;

        if self.is_aborted_transaction_block() && prepared.operation.returns_data() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        out.push(BackendMessage::ParameterDescription {
            parameter_types: prepared.parameter_types.clone(),
        });

        match &prepared.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
                out.push(BackendMessage::RowDescription {
                    fields: describe_fields_for_plan(plan, &[])?,
                });
            }
            _ => out.push(BackendMessage::NoData),
        }

        Ok(())
    }

    pub(super) fn exec_describe_portal_message(
        &mut self,
        portal_name: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        let key = portal_key(portal_name);
        let portal = self.portals.get(&key).ok_or_else(|| SessionError {
            message: format!("portal \"{portal_name}\" does not exist"),
        })?;

        if self.is_aborted_transaction_block() && portal.operation.returns_data() {
            return Err(SessionError {
                message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
            });
        }

        match &portal.operation {
            PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
                out.push(BackendMessage::RowDescription {
                    fields: describe_fields_for_plan(plan, &portal.result_format_codes)?,
                });
            }
            _ => out.push(BackendMessage::NoData),
        }

        Ok(())
    }

    pub(super) fn exec_close_statement(
        &mut self,
        statement_name: &str,
        out: &mut Vec<BackendMessage>,
    ) {
        if statement_name.is_empty() {
            self.drop_unnamed_stmt();
        } else {
            self.prepared_statements.remove(statement_name);
        }
        out.push(BackendMessage::CloseComplete);
    }

    pub(super) fn exec_close_portal(&mut self, portal_name: &str, out: &mut Vec<BackendMessage>) {
        let key = portal_key(portal_name);
        self.portals.remove(&key);
        out.push(BackendMessage::CloseComplete);
    }
}

fn decode_bind_parameter(
    index: usize,
    param: Option<Vec<u8>>,
    format_code: i16,
    type_oid: PgType,
) -> Result<Option<ScalarValue>, SessionError> {
    // Phase 2.2: return a typed `ScalarValue` instead of the rendered text.
    // Binary-format binds decode straight through `decode_binary_scalar`;
    // text-format binds are parsed once here using the declared type OID so
    // the executor sees the full typed value — no more render → reparse.
    let Some(raw) = param else {
        return Ok(None);
    };

    let decoded = match format_code {
        0 => {
            let text = String::from_utf8(raw).map_err(|_| SessionError {
                message: format!("bind parameter ${} contains invalid UTF-8 text", index + 1),
            })?;
            parse_text_bind_parameter(index, text, type_oid)?
        }
        1 => decode_binary_bind_parameter(index, &raw, type_oid)?,
        other => {
            return Err(SessionError {
                message: format!("unsupported bind parameter format code {other}"),
            });
        }
    };
    Ok(Some(decoded))
}

fn decode_binary_bind_parameter(
    index: usize,
    raw: &[u8],
    type_oid: PgType,
) -> Result<ScalarValue, SessionError> {
    if type_oid == 0 {
        return String::from_utf8(raw.to_vec())
            .map(ScalarValue::Text)
            .map_err(|_| SessionError {
                message: format!(
                    "bind parameter ${} has binary format but unknown type",
                    index + 1
                ),
            });
    }
    decode_binary_scalar(raw, type_oid, "bind parameter")
}

/// Parse a text-format bind parameter into a typed `ScalarValue`. The
/// declared parameter type OID drives which PG text form to expect; when
/// the OID is 0 (unknown — client skipped declaring parameter types) the
/// value falls back to the earlier bool/int/float/text heuristic so that
/// untyped `$1` placeholders keep working.
fn parse_text_bind_parameter(
    index: usize,
    text: String,
    type_oid: PgType,
) -> Result<ScalarValue, SessionError> {
    let err = |ctx: &str| SessionError {
        message: format!("bind parameter ${} {ctx}", index + 1),
    };
    let trimmed = text.trim();
    match type_oid {
        // Integer family. Text form is the decimal string.
        20 | 21 | 23 | 24 | 26 => trimmed
            .parse::<i64>()
            .map(ScalarValue::Int)
            .map_err(|_| err("integer text is invalid")),
        // Float family.
        700 | 701 => trimmed
            .parse::<f64>()
            .map(ScalarValue::Float)
            .map_err(|_| err("float text is invalid")),
        // Booleans accept t/f, true/false (any case), 1/0.
        16 => match trimmed.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => Ok(ScalarValue::Bool(true)),
            "f" | "false" | "0" => Ok(ScalarValue::Bool(false)),
            _ => Err(err("boolean text is invalid")),
        },
        // Numeric: keep precision via rust_decimal.
        1700 => trimmed
            .parse::<rust_decimal::Decimal>()
            .map(ScalarValue::Numeric)
            .map_err(|_| err("numeric text is invalid")),
        // Text-like types: the wire form is already the text. Keep the raw
        // string (not the trimmed form) so whitespace semantics survive.
        25 | 1042 | 1043 | 19 | 114 | 17 | 2950 | 1082 | 1083 | 1114 | 1184 | 1186 | 1266
        | 3802 => Ok(ScalarValue::Text(text)),
        // Array types: the text form is `{a,b,c}`. Preserve as Text so the
        // engine's array-literal path handles parsing — mirrors pre-2.2
        // behaviour.
        other if crate::types::element_oid_from_array_oid(other).is_some() => {
            Ok(ScalarValue::Text(text))
        }
        // Unknown or pseudo types: fall back to the heuristic parse.
        _ => {
            if trimmed.eq_ignore_ascii_case("true") {
                return Ok(ScalarValue::Bool(true));
            }
            if trimmed.eq_ignore_ascii_case("false") {
                return Ok(ScalarValue::Bool(false));
            }
            if let Ok(v) = trimmed.parse::<i64>() {
                return Ok(ScalarValue::Int(v));
            }
            if let Ok(v) = trimmed.parse::<f64>() {
                return Ok(ScalarValue::Float(v));
            }
            Ok(ScalarValue::Text(text))
        }
    }
}
