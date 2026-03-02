#[allow(clippy::wildcard_imports)]
use super::*;

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
            row_description_sent: false,
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
        let (operation, params, result_formats, cached_result, cursor, row_desc_sent) = {
            let portal = self.portals.get(&key).ok_or_else(|| SessionError {
                message: format!("portal \"{portal_name}\" does not exist"),
            })?;
            (
                portal.operation.clone(),
                portal.params.clone(),
                portal.result_format_codes.clone(),
                portal.result_cache.clone(),
                portal.cursor,
                portal.row_description_sent,
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
            Some((portal, cursor, row_desc_sent)),
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
) -> Result<Option<String>, SessionError> {
    let Some(raw) = param else {
        return Ok(None);
    };

    let decoded = match format_code {
        0 => String::from_utf8(raw).map_err(|_| SessionError {
            message: format!("bind parameter ${} contains invalid UTF-8 text", index + 1),
        })?,
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
) -> Result<String, SessionError> {
    if type_oid == 0 {
        return String::from_utf8(raw.to_vec()).map_err(|_| SessionError {
            message: format!(
                "bind parameter ${} has binary format but unknown type",
                index + 1
            ),
        });
    }
    Ok(decode_binary_scalar(raw, type_oid, "bind parameter")?.render())
}
