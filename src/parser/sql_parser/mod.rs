use std::fmt;

use crate::parser::ast::{
    AlterRoleStatement, AlterSequenceAction, AlterSequenceStatement, AlterTableAction,
    AlterTableStatement, AlterViewAction, AlterViewStatement, Assignment, AssignmentSubscript,
    BinaryOp, BooleanTestType, ColumnDefinition, CommonTableExpr, ComparisonQuantifier,
    ConflictTarget, CopyDirection, CopyFormat, CopyOptions, CopyStatement, CreateCastStatement,
    CreateDomainStatement, CreateExtensionStatement, CreateFunctionStatement, CreateIndexStatement,
    CreateRoleStatement, CreateSchemaStatement, CreateSequenceStatement,
    CreateSubscriptionStatement, CreateTableStatement, CreateTriggerStatement, CreateTypeStatement,
    CreateViewStatement, CycleClause, DeleteStatement, DiscardStatement, DoStatement, DropBehavior,
    DropDomainStatement, DropExtensionStatement, DropFunctionStatement, DropIndexStatement,
    DropRoleStatement, DropSchemaStatement, DropSequenceStatement, DropSubscriptionStatement,
    DropTableStatement, DropTriggerStatement, DropTypeStatement, DropViewStatement,
    ExplainStatement, Expr, ForeignKeyAction, ForeignKeyReference, FunctionParam,
    FunctionParamMode, FunctionReturnType, GrantRoleStatement, GrantStatement,
    GrantTablePrivilegesStatement, GroupByExpr, InsertSource, InsertStatement, JoinCondition,
    JoinExpr, JoinType, ListenStatement, MergeStatement, MergeWhenClause, NotifyStatement,
    OnConflictClause, OrderByExpr, Query, QueryExpr, RefreshMaterializedViewStatement,
    RevokeRoleStatement, RevokeStatement, RevokeTablePrivilegesStatement, RoleOption, SearchClause,
    SelectItem, SelectQuantifier, SelectStatement, SetOperator, SetQuantifier, SetStatement,
    ShowStatement, Statement, SubqueryRef, SubscriptionOptions, TableConstraint, TableExpression,
    TableFunctionRef, TablePrivilegeKind, TableRef, TransactionStatement, TriggerEvent,
    TriggerTiming, TruncateStatement, TypeName, UnaryOp, UnlistenStatement, UpdateStatement,
    WindowDefinition, WindowFrame, WindowFrameBound, WindowFrameExclusion, WindowFrameUnits,
    WindowSpec, WithClause,
};
use crate::parser::lexer::{Keyword, LexError, Token, TokenKind, lex_sql};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.position)
    }
}

impl std::error::Error for ParseError {}

pub fn parse_statement(sql: &str) -> Result<Statement, ParseError> {
    let tokens = lex_sql(sql).map_err(ParseError::from)?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_top_level_statement()?;
    while parser.consume_if(|k| matches!(k, TokenKind::Semicolon)) {}
    parser.expect_eof()?;
    Ok(stmt)
}

impl From<LexError> for ParseError {
    fn from(value: LexError) -> Self {
        Self {
            message: value.message,
            position: value.position,
        }
    }
}

struct Parser {
    pub(super) tokens: Vec<Token>,
    pub(super) idx: usize,
}

mod admin;
mod ddl;
mod dml;
mod expr;
mod helpers;
mod query;

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, idx: 0 }
    }

    fn parse_top_level_statement(&mut self) -> Result<Statement, ParseError> {
        if self.peek_keyword(Keyword::Create) {
            self.advance();
            return self.parse_create_statement();
        }
        if self.peek_keyword(Keyword::Insert) {
            self.advance();
            return self.parse_insert_statement();
        }
        if self.peek_keyword(Keyword::Update) {
            self.advance();
            return self.parse_update_statement();
        }
        if self.peek_keyword(Keyword::Delete) {
            self.advance();
            return self.parse_delete_statement();
        }
        if self.peek_keyword(Keyword::Merge) {
            self.advance();
            return self.parse_merge_statement();
        }
        if self.peek_keyword(Keyword::Refresh) {
            self.advance();
            return self.parse_refresh_statement();
        }
        if self.peek_keyword(Keyword::Drop) {
            self.advance();
            return self.parse_drop_statement();
        }
        if self.peek_keyword(Keyword::Truncate) {
            self.advance();
            return self.parse_truncate_statement();
        }
        if self.peek_keyword(Keyword::Alter) {
            self.advance();
            return self.parse_alter_statement();
        }
        if self.peek_keyword(Keyword::Explain) {
            self.advance();
            return self.parse_explain_statement();
        }
        if self.peek_keyword(Keyword::Set) {
            self.advance();
            return self.parse_set_statement();
        }
        if self.peek_keyword(Keyword::Show) {
            self.advance();
            return self.parse_show_statement();
        }
        if self.peek_keyword(Keyword::Reset) {
            self.advance();
            return self.parse_reset_statement();
        }
        if self.peek_keyword(Keyword::Discard) {
            self.advance();
            return self.parse_discard_statement();
        }
        if self.peek_keyword(Keyword::Do) {
            self.advance();
            return self.parse_do_statement();
        }
        if self.peek_keyword(Keyword::Listen) {
            self.advance();
            return self.parse_listen_statement();
        }
        if self.peek_keyword(Keyword::Notify) {
            self.advance();
            return self.parse_notify_statement();
        }
        if self.peek_keyword(Keyword::Unlisten) {
            self.advance();
            return self.parse_unlisten_statement();
        }
        if self.peek_keyword(Keyword::Begin)
            || self.peek_keyword(Keyword::Start)
            || self.peek_keyword(Keyword::Commit)
            || self.peek_keyword(Keyword::End)
            || self.peek_keyword(Keyword::Rollback)
            || self.peek_keyword(Keyword::Savepoint)
            || self.peek_keyword(Keyword::Release)
        {
            return self.parse_transaction_statement();
        }
        if self.peek_ident("copy") {
            self.advance();
            return self.parse_copy_statement();
        }
        if self.peek_ident("grant") {
            self.advance();
            return self.parse_grant_statement();
        }
        if self.peek_ident("revoke") {
            self.advance();
            return self.parse_revoke_statement();
        }

        let query = self.parse_query()?;
        Ok(Statement::Query(query))
    }
}

#[cfg(test)]
mod tests;
