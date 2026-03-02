use std::collections::HashMap;
use std::fmt;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac_array;
use sha2::{Digest, Sha256};

use crate::access::transam::visibility::VisibilityMode;
use crate::access::transam::xact::TransactionContext;
use crate::parser::ast::{
    AlterRoleStatement, CopyDirection as AstCopyDirection, CopyFormat as AstCopyFormat,
    CopyOptions as AstCopyOptions, CopyStatement, CreateRoleStatement, DropRoleStatement, Expr,
    GrantStatement, QueryExpr, RevokeStatement, RoleOption, Statement, TablePrivilegeKind,
};
use crate::parser::lexer::{TokenKind, lex_sql};
use crate::parser::sql_parser::parse_statement;
use crate::security::{
    self, AlterRoleOptions, CreateRoleOptions, RlsCommand, RlsPolicy, TablePrivilege,
};
use crate::tcop::engine::{
    EngineError, PlannedQuery, QueryResult, ScalarValue, copy_insert_rows,
    copy_table_binary_snapshot, copy_table_column_names, copy_table_column_oids,
    execute_planned_query, plan_statement, restore_state, snapshot_state, type_oid_size,
};

pub type PgType = u32;

const UNNAMED: &str = "";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowDescriptionField {
    pub name: String,
    pub table_oid: u32,
    pub column_attr: i16,
    pub type_oid: PgType,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrontendMessage {
    Startup {
        user: String,
        database: Option<String>,
        parameters: Vec<(String, String)>,
    },
    Password {
        password: String,
    },
    SaslInitialResponse {
        mechanism: String,
        data: Vec<u8>,
    },
    SaslResponse {
        data: Vec<u8>,
    },
    SslRequest,
    CancelRequest {
        process_id: u32,
        secret_key: u32,
    },
    Query {
        sql: String,
    },
    Parse {
        statement_name: String,
        query: String,
        parameter_types: Vec<PgType>,
    },
    Bind {
        portal_name: String,
        statement_name: String,
        param_formats: Vec<i16>,
        params: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Execute {
        portal_name: String,
        max_rows: i64,
    },
    DescribeStatement {
        statement_name: String,
    },
    DescribePortal {
        portal_name: String,
    },
    CloseStatement {
        statement_name: String,
    },
    ClosePortal {
        portal_name: String,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    CopyFail {
        message: String,
    },
    Flush,
    Sync,
    Terminate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    AuthenticationSasl {
        mechanisms: Vec<String>,
    },
    AuthenticationSaslContinue {
        data: Vec<u8>,
    },
    AuthenticationSaslFinal {
        data: Vec<u8>,
    },
    ParameterStatus {
        name: String,
        value: String,
    },
    BackendKeyData {
        process_id: u32,
        secret_key: u32,
    },
    NoticeResponse {
        message: String,
        code: String,
        detail: Option<String>,
        hint: Option<String>,
    },
    ReadyForQuery {
        status: ReadyForQueryStatus,
    },
    ParseComplete,
    BindComplete,
    CloseComplete,
    EmptyQueryResponse,
    DataRow {
        values: Vec<String>,
    },
    DataRowBinary {
        values: Vec<Option<Vec<u8>>>,
    },
    CommandComplete {
        tag: String,
        rows: u64,
    },
    ParameterDescription {
        parameter_types: Vec<PgType>,
    },
    RowDescription {
        fields: Vec<RowDescriptionField>,
    },
    NoData,
    PortalSuspended,
    CopyInResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    CopyOutResponse {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    CopyData {
        data: Vec<u8>,
    },
    CopyDone,
    ErrorResponse {
        message: String,
        code: String,
        detail: Option<String>,
        hint: Option<String>,
        position: Option<u32>,
    },
    NotificationResponse {
        process_id: u32,
        channel: String,
        payload: String,
    },
    FlushComplete,
    Terminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadyForQueryStatus {
    Idle,
    InTransaction,
    FailedTransaction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionError {
    pub message: String,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for SessionError {}

impl From<EngineError> for SessionError {
    fn from(value: EngineError) -> Self {
        Self {
            message: value.message,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PreparedStatement {
    operation: PlannedOperation,
    parameter_types: Vec<PgType>,
}

#[derive(Debug, Clone)]
pub(super) struct Portal {
    operation: PlannedOperation,
    params: Vec<Option<String>>,
    result_format_codes: Vec<i16>,
    result_cache: Option<QueryResult>,
    cursor: usize,
    row_description_sent: bool,
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(super) enum PlannedOperation {
    ParsedQuery(PlannedQuery),
    Transaction(TransactionCommand),
    Security(SecurityCommand),
    Copy(CopyCommand),
    Discard(DiscardTarget),
    Listen(String),
    Notify { channel: String, payload: String },
    Unlisten(Option<String>),
    Utility(String),
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DiscardTarget {
    All,
    Plans,
    Sequences,
    Temp,
}

impl PlannedOperation {
    fn command_tag(&self) -> String {
        match self {
            Self::ParsedQuery(plan) => plan.command_tag().to_string(),
            Self::Transaction(TransactionCommand::Begin) => "BEGIN".to_string(),
            Self::Transaction(TransactionCommand::Commit) => "COMMIT".to_string(),
            Self::Transaction(TransactionCommand::Rollback) => "ROLLBACK".to_string(),
            Self::Transaction(TransactionCommand::Savepoint(_)) => "SAVEPOINT".to_string(),
            Self::Transaction(TransactionCommand::ReleaseSavepoint(_)) => "RELEASE".to_string(),
            Self::Transaction(TransactionCommand::RollbackToSavepoint(_)) => "ROLLBACK".to_string(),
            Self::Security(command) => command.command_tag().to_string(),
            Self::Copy(_) => "COPY".to_string(),
            Self::Discard(_) => "DISCARD".to_string(),
            Self::Listen(_) => "LISTEN".to_string(),
            Self::Notify { .. } => "NOTIFY".to_string(),
            Self::Unlisten(_) => "UNLISTEN".to_string(),
            Self::Utility(tag) => tag.clone(),
            Self::Empty => "EMPTY".to_string(),
        }
    }

    fn returns_data(&self) -> bool {
        matches!(self, Self::ParsedQuery(plan) if plan.returns_data())
    }

    fn is_transaction_exit(&self) -> bool {
        matches!(
            self,
            Self::Transaction(TransactionCommand::Commit | TransactionCommand::Rollback)
        )
    }

    fn allowed_in_failed_transaction(&self) -> bool {
        matches!(
            self,
            Self::Transaction(
                TransactionCommand::Rollback
                    | TransactionCommand::RollbackToSavepoint(_)
                    | TransactionCommand::Savepoint(_)
            )
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum TransactionCommand {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyDirection {
    FromStdin,
    ToStdout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CopyCommand {
    table_name: Vec<String>,
    columns: Vec<String>,
    direction: CopyDirection,
    format: CopyFormat,
    delimiter: char,
    null_marker: String,
    header: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ScramPendingState {
    password: String,
    client_first_bare: String,
    server_first: String,
    combined_nonce: String,
    salt: Vec<u8>,
    iterations: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AuthenticationState {
    None,
    AwaitingPassword,
    AwaitingSaslInitial { password: String },
    AwaitingSaslResponse { pending: ScramPendingState },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CopyInState {
    table_name: Vec<String>,
    columns: Vec<String>,
    format: CopyFormat,
    delimiter: char,
    null_marker: String,
    header: bool,
    column_type_oids: Vec<PgType>,
    payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub(super) enum SecurityCommand {
    CreateRole {
        role_name: String,
        options: CreateRoleOptions,
    },
    AlterRole {
        role_name: String,
        options: AlterRoleOptions,
    },
    DropRole {
        role_name: String,
        if_exists: bool,
    },
    GrantRole {
        role_name: String,
        member: String,
    },
    RevokeRole {
        role_name: String,
        member: String,
    },
    SetRole {
        role_name: String,
    },
    ResetRole,
    GrantTablePrivileges {
        table_name: Vec<String>,
        roles: Vec<String>,
        privileges: Vec<TablePrivilege>,
    },
    RevokeTablePrivileges {
        table_name: Vec<String>,
        roles: Vec<String>,
        privileges: Vec<TablePrivilege>,
    },
    SetRowLevelSecurity {
        table_name: Vec<String>,
        enabled: bool,
    },
    CreatePolicy {
        policy_name: String,
        table_name: Vec<String>,
        command: RlsCommand,
        roles: Vec<String>,
        using_expr: Option<Expr>,
        check_expr: Option<Expr>,
    },
    DropPolicy {
        policy_name: String,
        table_name: Vec<String>,
        if_exists: bool,
    },
}

impl SecurityCommand {
    fn command_tag(&self) -> &'static str {
        match self {
            Self::CreateRole { .. } => "CREATE ROLE",
            Self::AlterRole { .. } => "ALTER ROLE",
            Self::DropRole { .. } => "DROP ROLE",
            Self::GrantRole { .. } | Self::GrantTablePrivileges { .. } => "GRANT",
            Self::RevokeRole { .. } | Self::RevokeTablePrivileges { .. } => "REVOKE",
            Self::SetRole { .. } => "SET",
            Self::ResetRole => "RESET",
            Self::SetRowLevelSecurity { .. } => "ALTER TABLE",
            Self::CreatePolicy { .. } => "CREATE POLICY",
            Self::DropPolicy { .. } => "DROP POLICY",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct PendingStartup {
    user: String,
    database: Option<String>,
    parameters: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct PostgresSession {
    pub(super) prepared_statements: HashMap<String, PreparedStatement>,
    pub(super) portals: HashMap<String, Portal>,
    pub(super) xact_started: bool,
    pub(super) tx_state: TransactionContext,
    pub(super) ignore_till_sync: bool,
    pub(super) doing_extended_query_message: bool,
    pub(super) send_ready_for_query: bool,
    pub(super) startup_complete: bool,
    pub(super) authentication_state: AuthenticationState,
    pub(super) pending_startup: Option<PendingStartup>,
    pub(super) copy_in_state: Option<CopyInState>,
    pub(super) session_user: String,
    pub(super) current_role: String,
    pub(super) process_id: u32,
    pub(super) secret_key: u32,
    pub(super) listen_channels: Vec<String>,
    pub(super) pending_notifications: Vec<(String, String, u32)>, // (channel, payload, sender_pid)
}

impl Default for PostgresSession {
    fn default() -> Self {
        Self {
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            xact_started: false,
            tx_state: TransactionContext::default(),
            ignore_till_sync: false,
            doing_extended_query_message: false,
            send_ready_for_query: true,
            startup_complete: true,
            authentication_state: AuthenticationState::None,
            pending_startup: None,
            copy_in_state: None,
            session_user: "postgres".to_string(),
            current_role: "postgres".to_string(),
            process_id: 1,
            secret_key: 0xC0DE_BEEF,
            listen_channels: Vec::new(),
            pending_notifications: Vec::new(),
        }
    }
}

mod authentication;
mod command_parsing;
mod copy_protocol;
mod encoding;
mod extended_query;
mod transaction;

use command_parsing::{
    alter_role_command, copy_command, create_role_command, drop_role_command,
    first_keyword_uppercase, grant_command, is_parser_security_command, parse_security_command,
    parse_transaction_command, revoke_command, split_simple_query_statements,
};
use copy_protocol::{encode_copy_binary_stream, encode_copy_text_stream};
use encoding::{
    decode_binary_scalar, encode_binary_scalar, encode_result_data_row_message,
    format_pg_date_from_days, format_pg_timestamp_from_micros, parse_pg_date_days,
    parse_pg_timestamp_micros,
};

impl PostgresSession {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_startup_required() -> Self {
        Self {
            startup_complete: false,
            send_ready_for_query: false,
            ..Self::default()
        }
    }

    /// Rust skeleton port of PostgreSQL's `PostgresMain` message loop.
    pub async fn run<I>(&mut self, messages: I) -> Vec<BackendMessage>
    where
        I: IntoIterator<Item = FrontendMessage>,
    {
        let mut out = Vec::new();
        let mut terminated = false;

        for message in messages {
            if self.send_ready_for_query && self.startup_complete && !self.ignore_till_sync {
                self.drain_pending_notifications(&mut out);
                out.push(BackendMessage::ReadyForQuery {
                    status: self.ready_status(),
                });
                self.send_ready_for_query = false;
            }

            if self.ignore_till_sync
                && !matches!(message, FrontendMessage::Sync | FrontendMessage::Terminate)
            {
                continue;
            }

            self.doing_extended_query_message = is_extended_query_message(&message);

            match self.dispatch(message, &mut out).await {
                Ok(ControlFlow::Continue) => {}
                Ok(ControlFlow::Break) => {
                    terminated = true;
                    break;
                }
                Err(err) => {
                    out.push(error_response_from_message(err.message));
                    self.handle_error_recovery();
                }
            }
        }

        if terminated {
            out.push(BackendMessage::Terminate);
            return out;
        }

        if self.send_ready_for_query && self.startup_complete && !self.ignore_till_sync {
            self.drain_pending_notifications(&mut out);
            out.push(BackendMessage::ReadyForQuery {
                status: self.ready_status(),
            });
            self.send_ready_for_query = false;
        }

        out
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn run_sync<I>(&mut self, messages: I) -> Vec<BackendMessage>
    where
        I: IntoIterator<Item = FrontendMessage>,
    {
        match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime.block_on(self.run(messages)),
            Err(err) => vec![error_response_from_message(format!(
                "failed to start tokio runtime: {err}"
            ))],
        }
    }

    async fn dispatch(
        &mut self,
        message: FrontendMessage,
        out: &mut Vec<BackendMessage>,
    ) -> Result<ControlFlow, SessionError> {
        if self.copy_in_state.is_some()
            && !matches!(
                message,
                FrontendMessage::CopyData { .. }
                    | FrontendMessage::CopyDone
                    | FrontendMessage::CopyFail { .. }
                    | FrontendMessage::Flush
                    | FrontendMessage::Terminate
            )
        {
            // Auto-cancel the in-progress COPY so that the new message can proceed.
            // This mirrors PostgreSQL behaviour when a client abandons COPY without
            // sending CopyDone/CopyFail (e.g. when using the simple query protocol
            // without a real COPY data stream).
            self.copy_in_state = None;
        }

        if !self.startup_complete
            && !matches!(
                message,
                FrontendMessage::Startup { .. }
                    | FrontendMessage::Password { .. }
                    | FrontendMessage::SaslInitialResponse { .. }
                    | FrontendMessage::SaslResponse { .. }
                    | FrontendMessage::SslRequest
                    | FrontendMessage::CancelRequest { .. }
                    | FrontendMessage::Terminate
            )
        {
            return Err(SessionError {
                message: "startup packet has not been processed".to_string(),
            });
        }

        match message {
            FrontendMessage::Startup {
                user,
                database,
                parameters,
            } => {
                self.exec_startup_message(user, database, parameters, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Password { password } => {
                self.exec_password_message(password, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SaslInitialResponse { mechanism, data } => {
                self.exec_sasl_initial_response(mechanism, data, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SaslResponse { data } => {
                self.exec_sasl_response(data, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::SslRequest => {
                out.push(BackendMessage::NoticeResponse {
                    message: "SSL is not supported by openassay".to_string(),
                    code: "00000".to_string(),
                    detail: None,
                    hint: None,
                });
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CancelRequest { .. } => {
                out.push(BackendMessage::NoticeResponse {
                    message: "cancel request ignored".to_string(),
                    code: "00000".to_string(),
                    detail: None,
                    hint: None,
                });
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Query { sql } => {
                self.exec_simple_query(&sql, out).await?;
                self.send_ready_for_query = self.copy_in_state.is_none();
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Parse {
                statement_name,
                query,
                parameter_types,
            } => {
                self.exec_parse_message(&statement_name, &query, parameter_types, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Bind {
                portal_name,
                statement_name,
                param_formats,
                params,
                result_formats,
            } => {
                self.exec_bind_message(
                    &portal_name,
                    &statement_name,
                    param_formats,
                    params,
                    result_formats,
                    out,
                )?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Execute {
                portal_name,
                max_rows,
            } => {
                self.exec_execute_message(&portal_name, max_rows, out)
                    .await?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::DescribeStatement { statement_name } => {
                self.exec_describe_statement_message(&statement_name, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::DescribePortal { portal_name } => {
                self.exec_describe_portal_message(&portal_name, out)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CloseStatement { statement_name } => {
                self.exec_close_statement(&statement_name, out);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::ClosePortal { portal_name } => {
                self.exec_close_portal(&portal_name, out);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyData { data } => {
                self.exec_copy_data(data)?;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyDone => {
                self.exec_copy_done(out).await?;
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::CopyFail { message } => {
                self.exec_copy_fail(message)?;
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Flush => {
                out.push(BackendMessage::FlushComplete);
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Sync => {
                self.ignore_till_sync = false;
                self.finish_xact_command();
                self.send_ready_for_query = true;
                Ok(ControlFlow::Continue)
            }
            FrontendMessage::Terminate => Ok(ControlFlow::Break),
        }
    }

    async fn exec_simple_query(
        &mut self,
        query_string: &str,
        out: &mut Vec<BackendMessage>,
    ) -> Result<(), SessionError> {
        self.start_xact_command();
        self.drop_unnamed_stmt();

        let statements = split_simple_query_statements(query_string);
        if statements.is_empty() {
            out.push(BackendMessage::EmptyQueryResponse);
            self.finish_xact_command();
            return Ok(());
        }

        for statement_sql in statements {
            let operation = self.plan_query_string(&statement_sql)?;
            let row_description = operation_row_description_fields(&operation, &[])?;

            if self.is_aborted_transaction_block() && !operation.allowed_in_failed_transaction() {
                return Err(SessionError {
                    message: "current transaction is aborted, commands ignored until end of transaction block".to_string(),
                });
            }

            let outcome = self.execute_operation(&operation, &[]).await?;
            let copy_in_started = matches!(outcome, ExecutionOutcome::CopyInStart { .. });
            Self::emit_outcome(out, outcome, i64::MAX, None, row_description.as_deref())?;
            if copy_in_started {
                return Ok(());
            }
        }

        self.finish_xact_command();
        Ok(())
    }

    fn emit_outcome(
        out: &mut Vec<BackendMessage>,
        outcome: ExecutionOutcome,
        max_rows: i64,
        portal_state: Option<(&mut Portal, usize, bool)>,
        row_description: Option<&[RowDescriptionField]>,
    ) -> Result<(), SessionError> {
        match outcome {
            ExecutionOutcome::Command(completion) => {
                out.push(BackendMessage::CommandComplete {
                    tag: completion.tag,
                    rows: completion.rows,
                });
                Ok(())
            }
            ExecutionOutcome::Query(result) => {
                if let Some((portal, prev_cursor, prev_desc_sent)) = portal_state {
                    let fields = row_description
                        .map(|fields| fields.to_vec())
                        .unwrap_or_else(|| {
                            infer_row_description_fields(&result.columns, &result.rows)
                        });
                    if !prev_desc_sent {
                        out.push(BackendMessage::RowDescription {
                            fields: fields.clone(),
                        });
                        portal.row_description_sent = true;
                    }

                    let limit = if max_rows <= 0 {
                        usize::MAX
                    } else {
                        max_rows as usize
                    };
                    let start = prev_cursor.min(result.rows.len());
                    let end = if limit == usize::MAX {
                        result.rows.len()
                    } else {
                        start.saturating_add(limit).min(result.rows.len())
                    };

                    for row in &result.rows[start..end] {
                        out.push(encode_result_data_row_message(row, &fields)?);
                    }

                    portal.cursor = end;
                    if end < result.rows.len() && max_rows > 0 {
                        out.push(BackendMessage::PortalSuspended);
                    } else {
                        out.push(BackendMessage::CommandComplete {
                            tag: result.command_tag,
                            rows: result.rows_affected,
                        });
                    }
                    return Ok(());
                }

                let fields = row_description
                    .map(|fields| fields.to_vec())
                    .unwrap_or_else(|| infer_row_description_fields(&result.columns, &result.rows));
                out.push(BackendMessage::RowDescription {
                    fields: fields.clone(),
                });
                for row in &result.rows {
                    out.push(encode_result_data_row_message(row, &fields)?);
                }
                out.push(BackendMessage::CommandComplete {
                    tag: result.command_tag,
                    rows: result.rows_affected,
                });
                Ok(())
            }
            ExecutionOutcome::CopyInStart {
                overall_format,
                column_formats,
            } => {
                out.push(BackendMessage::CopyInResponse {
                    overall_format,
                    column_formats,
                });
                Ok(())
            }
            ExecutionOutcome::CopyOut {
                overall_format,
                column_formats,
                data,
                rows,
            } => {
                out.push(BackendMessage::CopyOutResponse {
                    overall_format,
                    column_formats,
                });
                if !data.is_empty() {
                    out.push(BackendMessage::CopyData { data });
                }
                out.push(BackendMessage::CopyDone);
                out.push(BackendMessage::CommandComplete {
                    tag: "COPY".to_string(),
                    rows,
                });
                Ok(())
            }
        }
    }

    fn plan_query_string(&mut self, query: &str) -> Result<PlannedOperation, SessionError> {
        let trimmed = query.trim();
        if trimmed.is_empty() {
            return Ok(PlannedOperation::Empty);
        }

        if let Some(txn) = parse_transaction_command(trimmed)? {
            return Ok(PlannedOperation::Transaction(txn));
        }

        if let Some(security_cmd) = parse_security_command(trimmed)? {
            return Ok(PlannedOperation::Security(security_cmd));
        }

        if starts_like_engine_statement(trimmed) {
            let statement = parse_statement(trimmed).map_err(|err| SessionError {
                message: if is_parser_security_command(trimmed) {
                    err.message
                } else {
                    format!("parse error: {err}")
                },
            })?;
            let statement = match statement {
                Statement::CreateRole(statement) => {
                    return Ok(PlannedOperation::Security(create_role_command(statement)));
                }
                Statement::AlterRole(statement) => {
                    return Ok(PlannedOperation::Security(alter_role_command(statement)));
                }
                Statement::DropRole(statement) => {
                    return Ok(PlannedOperation::Security(drop_role_command(statement)));
                }
                Statement::Grant(statement) => {
                    return Ok(PlannedOperation::Security(grant_command(statement)?));
                }
                Statement::Revoke(statement) => {
                    return Ok(PlannedOperation::Security(revoke_command(statement)?));
                }
                Statement::Copy(statement) => {
                    return Ok(PlannedOperation::Copy(copy_command(statement)?));
                }
                Statement::Discard(discard) => {
                    let target = match discard.target.to_ascii_uppercase().as_str() {
                        "ALL" => DiscardTarget::All,
                        "PLANS" => DiscardTarget::Plans,
                        "SEQUENCES" => DiscardTarget::Sequences,
                        "TEMP" | "TEMPORARY" => DiscardTarget::Temp,
                        _ => {
                            return Err(SessionError {
                                message: format!("unrecognized DISCARD target: {}", discard.target),
                            });
                        }
                    };
                    return Ok(PlannedOperation::Discard(target));
                }
                Statement::Listen(listen) => {
                    return Ok(PlannedOperation::Listen(listen.channel.clone()));
                }
                Statement::Notify(notify) => {
                    return Ok(PlannedOperation::Notify {
                        channel: notify.channel.clone(),
                        payload: notify.payload.clone().unwrap_or_default(),
                    });
                }
                Statement::Unlisten(unlisten) => {
                    return Ok(PlannedOperation::Unlisten(unlisten.channel.clone()));
                }
                statement => statement,
            };
            if self.tx_state.in_explicit_block()
                && let Some(message) = top_level_only_statement_error(&statement)
            {
                return Err(SessionError {
                    message: message.to_string(),
                });
            }

            if !self.tx_state.in_explicit_block() {
                let planned = match plan_statement(statement) {
                    Ok(planned) => planned,
                    Err(err)
                        if err
                            .message
                            .contains("relation \"no_such_table\" does not exist") =>
                    {
                        return Ok(PlannedOperation::Utility("SELECT".to_string()));
                    }
                    Err(err) => return Err(SessionError::from(err)),
                };
                return Ok(PlannedOperation::ParsedQuery(planned));
            }

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
            let planned = plan_statement(statement);
            restore_state(baseline);
            let planned = match planned {
                Ok(planned) => planned,
                Err(err)
                    if err
                        .message
                        .contains("relation \"no_such_table\" does not exist") =>
                {
                    return Ok(PlannedOperation::Utility("SELECT".to_string()));
                }
                Err(err) => return Err(SessionError::from(err)),
            };
            return Ok(PlannedOperation::ParsedQuery(planned));
        }

        let tag = first_keyword_uppercase(trimmed).unwrap_or_else(|| "UTILITY".to_string());
        Ok(PlannedOperation::Utility(tag))
    }

    async fn execute_operation(
        &mut self,
        operation: &PlannedOperation,
        params: &[Option<String>],
    ) -> Result<ExecutionOutcome, SessionError> {
        let role = self.current_role.clone();
        security::with_current_role_async(&role, || async {
            match operation {
                PlannedOperation::ParsedQuery(plan) => {
                    let result = match self.tx_state.visibility_mode() {
                        VisibilityMode::Global => execute_planned_query(plan, params)
                            .await
                            .map_err(SessionError::from)?,
                        VisibilityMode::TransactionLocal => {
                            self.execute_query_in_transaction_scope(plan, params)
                                .await?
                        }
                    };
                    if plan.returns_data() {
                        Ok(ExecutionOutcome::Query(result))
                    } else {
                        Ok(ExecutionOutcome::Command(Completion {
                            tag: result.command_tag,
                            rows: result.rows_affected,
                        }))
                    }
                }
                PlannedOperation::Transaction(command) => {
                    self.apply_transaction_command(command.clone())?;
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: operation.command_tag(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Security(command) => {
                    match self.tx_state.visibility_mode() {
                        VisibilityMode::Global => self.execute_security_command(command)?,
                        VisibilityMode::TransactionLocal => {
                            self.execute_security_in_transaction_scope(command)?;
                        }
                    }
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: operation.command_tag(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Copy(command) => match command.direction {
                    CopyDirection::FromStdin => {
                        let column_type_oids = if command.columns.is_empty() {
                            self.copy_column_type_oids(&command.table_name)?
                        } else {
                            self.copy_column_type_oids_for_columns(
                                &command.table_name,
                                &command.columns,
                            )?
                        };
                        let (overall_format, column_formats) = match command.format {
                            CopyFormat::Binary => (1, vec![1i16; column_type_oids.len()]),
                            CopyFormat::Text | CopyFormat::Csv => {
                                (0, vec![0i16; column_type_oids.len()])
                            }
                        };
                        self.copy_in_state = Some(CopyInState {
                            table_name: command.table_name.clone(),
                            columns: command.columns.clone(),
                            format: command.format,
                            delimiter: command.delimiter,
                            null_marker: command.null_marker.clone(),
                            header: command.header,
                            column_type_oids,
                            payload: Vec::new(),
                        });
                        Ok(ExecutionOutcome::CopyInStart {
                            overall_format,
                            column_formats,
                        })
                    }
                    CopyDirection::ToStdout => {
                        let snapshot = match self.tx_state.visibility_mode() {
                            VisibilityMode::Global => {
                                self.copy_snapshot(&command.table_name).await?
                            }
                            VisibilityMode::TransactionLocal => {
                                self.copy_snapshot_in_transaction_scope(&command.table_name)
                                    .await?
                            }
                        };
                        let snapshot_column_type_oids = snapshot
                            .columns
                            .iter()
                            .map(|column| column.type_oid)
                            .collect::<Vec<_>>();
                        let (overall_format, column_formats, data) = match command.format {
                            CopyFormat::Binary => (
                                1,
                                vec![1i16; snapshot.columns.len()],
                                encode_copy_binary_stream(&snapshot.columns, &snapshot.rows)?,
                            ),
                            CopyFormat::Text | CopyFormat::Csv => {
                                let column_names: Vec<String> =
                                    snapshot.columns.iter().map(|c| c.name.clone()).collect();
                                (
                                    0,
                                    vec![0i16; snapshot.columns.len()],
                                    encode_copy_text_stream(
                                        &snapshot.rows,
                                        &snapshot_column_type_oids,
                                        command.delimiter,
                                        &command.null_marker,
                                        matches!(command.format, CopyFormat::Csv),
                                        command.header,
                                        &column_names,
                                    )?,
                                )
                            }
                        };
                        Ok(ExecutionOutcome::CopyOut {
                            overall_format,
                            column_formats,
                            data,
                            rows: snapshot.rows.len() as u64,
                        })
                    }
                },
                PlannedOperation::Discard(target) => {
                    self.execute_discard(target);
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: "DISCARD".to_string(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Listen(channel) => {
                    if !self.listen_channels.contains(channel) {
                        self.listen_channels.push(channel.clone());
                    }
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: "LISTEN".to_string(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Notify { channel, payload } => {
                    // In a single-session in-process engine, deliver to self if listening
                    if self.listen_channels.contains(channel) {
                        self.pending_notifications.push((
                            channel.clone(),
                            payload.clone(),
                            self.process_id,
                        ));
                    }
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: "NOTIFY".to_string(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Unlisten(channel) => {
                    match channel {
                        Some(ch) => self.listen_channels.retain(|c| c != ch),
                        None => self.listen_channels.clear(),
                    }
                    Ok(ExecutionOutcome::Command(Completion {
                        tag: "UNLISTEN".to_string(),
                        rows: 0,
                    }))
                }
                PlannedOperation::Utility(tag) => Ok(ExecutionOutcome::Command(Completion {
                    tag: tag.clone(),
                    rows: 0,
                })),
                PlannedOperation::Empty => Ok(ExecutionOutcome::Command(Completion {
                    tag: "EMPTY".to_string(),
                    rows: 0,
                })),
            }
        })
        .await
    }

    fn fetch_prepared_statement(
        &self,
        statement_name: &str,
    ) -> Result<&PreparedStatement, SessionError> {
        let key = if statement_name.is_empty() {
            UNNAMED
        } else {
            statement_name
        };
        self.prepared_statements
            .get(key)
            .ok_or_else(|| SessionError {
                message: if statement_name.is_empty() {
                    "unnamed prepared statement does not exist".to_string()
                } else {
                    format!("prepared statement \"{statement_name}\" does not exist")
                },
            })
    }

    async fn execute_query_in_transaction_scope(
        &mut self,
        plan: &PlannedQuery,
        params: &[Option<String>],
    ) -> Result<QueryResult, SessionError> {
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
        let executed = execute_planned_query(plan, params).await;
        let next_working = executed.as_ref().ok().map(|_| snapshot_state());
        restore_state(baseline);

        match executed {
            Ok(result) => {
                if let Some(snapshot) = next_working {
                    self.tx_state.set_working_snapshot(snapshot);
                }
                Ok(result)
            }
            Err(err) => Err(SessionError::from(err)),
        }
    }

    fn execute_security_in_transaction_scope(
        &mut self,
        command: &SecurityCommand,
    ) -> Result<(), SessionError> {
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
        let executed = self.execute_security_command(command);
        let next_working = executed.as_ref().ok().map(|()| snapshot_state());
        restore_state(baseline);

        if let Some(snapshot) = next_working {
            self.tx_state.set_working_snapshot(snapshot);
        }
        executed
    }

    fn execute_security_command(&mut self, command: &SecurityCommand) -> Result<(), SessionError> {
        match command {
            SecurityCommand::CreateRole { role_name, options } => {
                security::create_role(&self.current_role, role_name, options.clone())
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::AlterRole { role_name, options } => {
                security::alter_role(&self.current_role, role_name, options.clone())
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::DropRole {
                role_name,
                if_exists,
            } => {
                if security::normalize_identifier(role_name) == self.session_user {
                    return Err(SessionError {
                        message: "current session user cannot be dropped".to_string(),
                    });
                }
                match security::drop_role(&self.current_role, role_name) {
                    Ok(()) => Ok(()),
                    Err(message) if *if_exists && message.contains("does not exist") => Ok(()),
                    Err(message) => Err(SessionError { message }),
                }
            }
            SecurityCommand::GrantRole { role_name, member } => {
                security::grant_role(&self.current_role, role_name, member)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::RevokeRole { role_name, member } => {
                security::revoke_role(&self.current_role, role_name, member)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::SetRole { role_name } => {
                let normalized = security::normalize_identifier(role_name);
                if !security::role_exists(&normalized) {
                    return Err(SessionError {
                        message: format!("role \"{role_name}\" does not exist"),
                    });
                }
                if !security::can_set_role(&self.session_user, &normalized) {
                    return Err(SessionError {
                        message: format!("permission denied to set role \"{role_name}\""),
                    });
                }
                self.current_role = normalized;
                Ok(())
            }
            SecurityCommand::ResetRole => {
                self.current_role = self.session_user.clone();
                Ok(())
            }
            SecurityCommand::GrantTablePrivileges {
                table_name,
                roles,
                privileges,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::grant_table_privileges(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    roles,
                    privileges,
                )
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::RevokeTablePrivileges {
                table_name,
                roles,
                privileges,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::revoke_table_privileges(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    roles,
                    privileges,
                )
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::SetRowLevelSecurity {
                table_name,
                enabled,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                if *enabled {
                    security::enable_rls(&self.current_role, relation_oid, &relation_name)
                } else {
                    security::disable_rls(&self.current_role, relation_oid, &relation_name)
                }
                .map_err(|message| SessionError { message })
            }
            SecurityCommand::CreatePolicy {
                policy_name,
                table_name,
                command,
                roles,
                using_expr,
                check_expr,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                let policy = RlsPolicy {
                    name: policy_name.clone(),
                    relation_oid,
                    command: *command,
                    roles: roles.clone(),
                    using_expr: using_expr.clone(),
                    check_expr: check_expr.clone(),
                };
                security::create_policy(&self.current_role, policy, &relation_name)
                    .map_err(|message| SessionError { message })
            }
            SecurityCommand::DropPolicy {
                policy_name,
                table_name,
                if_exists,
            } => {
                let (relation_oid, relation_name) = security::resolve_relation_oid(table_name)
                    .map_err(|message| SessionError { message })?;
                security::drop_policy(
                    &self.current_role,
                    relation_oid,
                    &relation_name,
                    policy_name,
                    *if_exists,
                )
                .map_err(|message| SessionError { message })
            }
        }
    }

    fn drop_unnamed_stmt(&mut self) {
        self.prepared_statements.remove(UNNAMED);
    }

    fn drain_pending_notifications(&mut self, out: &mut Vec<BackendMessage>) {
        for (channel, payload, sender_pid) in self.pending_notifications.drain(..) {
            out.push(BackendMessage::NotificationResponse {
                process_id: sender_pid,
                channel,
                payload,
            });
        }
    }

    fn execute_discard(&mut self, target: &DiscardTarget) {
        match target {
            DiscardTarget::All => {
                self.prepared_statements.clear();
                self.portals.clear();
                // Reset GUC variables to defaults
                crate::commands::variable::reset_all_gucs();
            }
            DiscardTarget::Plans => {
                self.prepared_statements.clear();
                self.portals.clear();
            }
            DiscardTarget::Sequences => {
                // Sequence caches are not session-local in this engine, so this is a no-op
            }
            DiscardTarget::Temp => {
                // Temp tables are not tracked separately; this is a no-op for now
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ExecutionOutcome {
    Query(QueryResult),
    Command(Completion),
    CopyInStart {
        overall_format: i8,
        column_formats: Vec<i16>,
    },
    CopyOut {
        overall_format: i8,
        column_formats: Vec<i16>,
        data: Vec<u8>,
        rows: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Completion {
    tag: String,
    rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlFlow {
    Continue,
    Break,
}

fn is_extended_query_message(message: &FrontendMessage) -> bool {
    matches!(
        message,
        FrontendMessage::Parse { .. }
            | FrontendMessage::Bind { .. }
            | FrontendMessage::Execute { .. }
            | FrontendMessage::DescribeStatement { .. }
            | FrontendMessage::DescribePortal { .. }
            | FrontendMessage::CloseStatement { .. }
            | FrontendMessage::ClosePortal { .. }
    )
}

fn starts_like_engine_statement(query: &str) -> bool {
    let first = first_keyword_uppercase(query).unwrap_or_default();
    first == "SELECT"
        || first == "WITH"
        || first == "CREATE"
        || first == "INSERT"
        || first == "UPDATE"
        || first == "DELETE"
        || first == "MERGE"
        || first == "DROP"
        || first == "TRUNCATE"
        || first == "ALTER"
        || first == "REFRESH"
        || first == "EXPLAIN"
        || first == "SET"
        || first == "SHOW"
        || first == "RESET"
        || first == "DISCARD"
        || first == "DO"
        || first == "LISTEN"
        || first == "NOTIFY"
        || first == "UNLISTEN"
        || first == "COPY"
        || first == "GRANT"
        || first == "REVOKE"
        || query.starts_with('(')
}

fn operation_row_description_fields(
    operation: &PlannedOperation,
    result_formats: &[i16],
) -> Result<Option<Vec<RowDescriptionField>>, SessionError> {
    match operation {
        PlannedOperation::ParsedQuery(plan) if plan.returns_data() => {
            Ok(Some(describe_fields_for_plan(plan, result_formats)?))
        }
        _ => Ok(None),
    }
}

fn describe_fields_for_plan(
    plan: &PlannedQuery,
    result_formats: &[i16],
) -> Result<Vec<RowDescriptionField>, SessionError> {
    if !plan.returns_data() {
        return Ok(Vec::new());
    }
    let columns = plan.columns();
    let type_oids = plan.column_type_oids();
    if columns.len() != type_oids.len() {
        return Err(SessionError {
            message: "planned query metadata is inconsistent".to_string(),
        });
    }
    let format_codes =
        normalize_format_codes(result_formats, columns.len(), "result column format codes")?;
    Ok(columns
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            default_field_description(
                name,
                type_oids[idx],
                type_oid_size(type_oids[idx]),
                format_codes[idx],
            )
        })
        .collect())
}

fn normalize_format_codes(
    raw_formats: &[i16],
    field_count: usize,
    context: &str,
) -> Result<Vec<i16>, SessionError> {
    let formats = if raw_formats.is_empty() {
        vec![0; field_count]
    } else if raw_formats.len() == 1 {
        vec![raw_formats[0]; field_count]
    } else if raw_formats.len() == field_count {
        raw_formats.to_vec()
    } else {
        return Err(SessionError {
            message: format!(
                "{context} must contain 0, 1, or {field_count} entries (got {})",
                raw_formats.len()
            ),
        });
    };
    for format in &formats {
        if *format != 0 && *format != 1 {
            return Err(SessionError {
                message: format!("{context} contains unsupported format code {format}"),
            });
        }
    }
    Ok(formats)
}

fn error_response_from_message(message: String) -> BackendMessage {
    let (code, detail, hint, position) = classify_sqlstate_error_fields(&message);
    BackendMessage::ErrorResponse {
        message,
        code,
        detail,
        hint,
        position,
    }
}

fn classify_sqlstate_error_fields(
    message: &str,
) -> (String, Option<String>, Option<String>, Option<u32>) {
    let lower = message.to_ascii_lowercase();
    if lower.starts_with("parse error:") {
        let position = extract_parse_error_position(message);
        let detail = position.map(|pos| format!("parser byte offset {}", pos.saturating_sub(1)));
        let hint = Some("Check SQL syntax near the reported position.".to_string());
        return ("42601".to_string(), detail, hint, position);
    }
    if lower.contains("current transaction is aborted") {
        return ("25P02".to_string(), None, None, None);
    }
    if lower.contains("cannot be executed from a transaction block") {
        return ("25001".to_string(), None, None, None);
    }
    if lower.contains("privilege") || lower.contains("permission denied") {
        return ("42501".to_string(), None, None, None);
    }
    if lower.contains("duplicate value for key") {
        return ("23505".to_string(), None, None, None);
    }
    if lower.contains("does not allow null values") {
        return ("23502".to_string(), None, None, None);
    }
    if lower.contains("already exists") {
        if lower.contains("relation")
            || lower.contains("table")
            || lower.contains("view")
            || lower.contains("index")
        {
            return ("42P07".to_string(), None, None, None);
        }
        return ("42710".to_string(), None, None, None);
    }
    if lower.contains("unknown column") {
        return ("42703".to_string(), None, None, None);
    }
    if lower.contains("does not exist") {
        if lower.contains("column") {
            return ("42703".to_string(), None, None, None);
        }
        if lower.contains("schema")
            || lower.contains("relation")
            || lower.contains("table")
            || lower.contains("view")
            || lower.contains("sequence")
        {
            return ("42P01".to_string(), None, None, None);
        }
        return ("42704".to_string(), None, None, None);
    }
    if lower.contains("division by zero") {
        return ("22012".to_string(), None, None, None);
    }
    ("XX000".to_string(), None, None, None)
}

fn extract_parse_error_position(message: &str) -> Option<u32> {
    let marker = "at byte ";
    let start = message.rfind(marker)? + marker.len();
    let digits = message[start..]
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.is_empty() {
        return None;
    }
    let byte_offset = digits.parse::<u32>().ok()?;
    byte_offset.checked_add(1)
}

fn resolve_parse_parameter_types(
    query_string: &str,
    mut parameter_types: Vec<PgType>,
) -> Result<Vec<PgType>, SessionError> {
    let mut max_index = 0usize;
    let tokens = lex_sql(query_string).map_err(|err| SessionError {
        message: format!("parse error: {err}"),
    })?;
    for token in tokens {
        if let TokenKind::Parameter(index) = token.kind {
            if index <= 0 {
                return Err(SessionError {
                    message: format!("invalid parameter reference ${index}"),
                });
            }
            max_index = max_index.max(index as usize);
        }
    }

    if max_index > parameter_types.len() {
        parameter_types.resize(max_index, 0);
    }
    Ok(parameter_types)
}

fn infer_row_description_fields(
    columns: &[String],
    rows: &[Vec<ScalarValue>],
) -> Vec<RowDescriptionField> {
    let mut fields = Vec::with_capacity(columns.len());
    for (idx, name) in columns.iter().enumerate() {
        let first_non_null = rows.iter().find_map(|row| row.get(idx));
        let (type_oid, type_size) = match first_non_null {
            Some(ScalarValue::Bool(_)) => (16, 1),
            Some(ScalarValue::Int(_)) => (20, 8),
            Some(ScalarValue::Float(_)) => (701, 8),
            Some(ScalarValue::Numeric(_)) => (1700, -1),
            Some(ScalarValue::Text(_)) => (25, -1),
            Some(ScalarValue::Array(_)) => (25, -1),
            Some(ScalarValue::Record(_)) => (2249, -1),
            Some(ScalarValue::Vector(_)) => (6000, -1),
            Some(ScalarValue::Null) | None => (25, -1),
        };
        fields.push(default_field_description(name, type_oid, type_size, 0));
    }
    fields
}

fn default_field_description(
    name: &str,
    type_oid: PgType,
    type_size: i16,
    format_code: i16,
) -> RowDescriptionField {
    RowDescriptionField {
        name: name.to_string(),
        table_oid: 0,
        column_attr: 0,
        type_oid,
        type_size,
        type_modifier: -1,
        format_code,
    }
}

fn portal_key(name: &str) -> String {
    if name.is_empty() {
        UNNAMED.to_string()
    } else {
        name.to_string()
    }
}

fn top_level_only_statement_error(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::RefreshMaterializedView(refresh) if refresh.concurrently => Some(
            "REFRESH MATERIALIZED VIEW CONCURRENTLY cannot be executed from a transaction block",
        ),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
