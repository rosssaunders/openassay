use std::fmt;

use crate::parser::ast::{
    AlterSequenceAction, AlterSequenceStatement, AlterTableAction, AlterTableStatement,
    AlterViewAction, AlterViewStatement, Assignment, BinaryOp, ComparisonQuantifier,
    ColumnDefinition, CommonTableExpr,
    ConflictTarget, CreateIndexStatement, CreateSchemaStatement, CreateSequenceStatement,
    CreateTableStatement, CreateViewStatement, DeleteStatement, DropBehavior, DropIndexStatement,
    DropSchemaStatement, DropSequenceStatement, DropTableStatement, DropViewStatement, Expr,
    ForeignKeyAction, ForeignKeyReference, InsertSource, InsertStatement, JoinCondition, JoinExpr,
    JoinType, MergeStatement, MergeWhenClause, OnConflictClause, OrderByExpr, Query, QueryExpr,
    RefreshMaterializedViewStatement, SelectItem, SelectQuantifier, SelectStatement, SetOperator,
    SetQuantifier, Statement, SubqueryRef, TableConstraint, TableExpression, TableFunctionRef,
    TableRef, TruncateStatement, TypeName, UnaryOp, UpdateStatement, WindowFrame,
    WindowFrameBound, WindowFrameUnits, WindowSpec, WithClause,
    ExplainStatement, SetStatement, ShowStatement, DiscardStatement, DoStatement,
    ListenStatement, NotifyStatement, UnlistenStatement,
    CreateExtensionStatement, DropExtensionStatement,
    CreateFunctionStatement, FunctionParam, FunctionReturnType, GroupByExpr,
    TransactionStatement,
    CopyDirection, CopyFormat, CopyOptions, CopyStatement,
    CreateRoleStatement, AlterRoleStatement, DropRoleStatement, RoleOption,
    GrantRoleStatement, GrantStatement, GrantTablePrivilegesStatement,
    RevokeRoleStatement, RevokeStatement, RevokeTablePrivilegesStatement,
    TablePrivilegeKind,
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
    tokens: Vec<Token>,
    idx: usize,
}

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

    fn parse_refresh_statement(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Materialized, "expected MATERIALIZED after REFRESH")?;
        self.expect_keyword(Keyword::View, "expected VIEW after REFRESH MATERIALIZED")?;
        let concurrently = self.consume_keyword(Keyword::Concurrently);
        let name = self.parse_qualified_name()?;
        let with_data = if self.consume_keyword(Keyword::With) {
            if self.consume_keyword(Keyword::No) {
                self.expect_keyword(Keyword::Data, "expected DATA after WITH NO")?;
                false
            } else {
                self.expect_keyword(Keyword::Data, "expected DATA after WITH")?;
                true
            }
        } else {
            true
        };
        Ok(Statement::RefreshMaterializedView(
            RefreshMaterializedViewStatement {
                name,
                concurrently,
                with_data,
            },
        ))
    }

    fn parse_create_statement(&mut self) -> Result<Statement, ParseError> {
        let or_replace = if self.consume_keyword(Keyword::Or) {
            self.expect_keyword(
                Keyword::Replace,
                "expected REPLACE after OR in CREATE statement",
            )?;
            true
        } else {
            false
        };
        let unique = self.consume_keyword(Keyword::Unique);
        let materialized = self.consume_keyword(Keyword::Materialized);
        if self.consume_keyword(Keyword::Extension) {
            if or_replace || unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE EXTENSION"));
            }
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not, "expected NOT after IF")?;
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF NOT")?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            return Ok(Statement::CreateExtension(CreateExtensionStatement { name, if_not_exists }));
        }
        if self.consume_keyword(Keyword::Function) {
            if unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE FUNCTION"));
            }
            return self.parse_create_function(or_replace);
        }
        if self.consume_keyword(Keyword::Index) {
            if or_replace {
                return Err(self.error_at_current("OR REPLACE is only supported for CREATE VIEW"));
            }
            if materialized {
                return Err(self.error_at_current("unexpected MATERIALIZED before CREATE INDEX"));
            }
            let name = self.parse_identifier()?;
            self.expect_keyword(Keyword::On, "expected ON after CREATE INDEX name")?;
            let table_name = self.parse_qualified_name()?;
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CREATE INDEX table name",
            )?;
            let mut columns = vec![self.parse_identifier()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                columns.push(self.parse_identifier()?);
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after CREATE INDEX column list",
            )?;
            return Ok(Statement::CreateIndex(CreateIndexStatement {
                name,
                table_name,
                columns,
                unique,
            }));
        }
        if unique {
            return Err(self.error_at_current("expected INDEX after CREATE UNIQUE"));
        }
        if self.consume_keyword(Keyword::View) {
            let name = self.parse_qualified_name()?;
            self.expect_keyword(Keyword::As, "expected AS in CREATE VIEW statement")?;
            let query = self.parse_query()?;
            let with_data = if materialized {
                if self.consume_keyword(Keyword::With) {
                    if self.consume_keyword(Keyword::No) {
                        self.expect_keyword(Keyword::Data, "expected DATA after WITH NO")?;
                        false
                    } else {
                        self.expect_keyword(Keyword::Data, "expected DATA after WITH")?;
                        true
                    }
                } else {
                    true
                }
            } else {
                true
            };
            return Ok(Statement::CreateView(CreateViewStatement {
                name,
                or_replace,
                materialized,
                with_data,
                query,
            }));
        }
        if or_replace {
            return Err(self.error_at_current("OR REPLACE is only supported for CREATE VIEW"));
        }
        if materialized {
            return Err(self.error_at_current("expected VIEW after CREATE MATERIALIZED"));
        }
        if self.consume_ident("role") {
            let name = self.parse_role_identifier_with_message("CREATE ROLE requires a role name")?;
            let options = self.parse_role_options("CREATE ROLE")?;
            return Ok(Statement::CreateRole(CreateRoleStatement { name, options }));
        }
        if self.consume_keyword(Keyword::Schema) {
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not, "expected NOT after IF in CREATE SCHEMA")?;
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF NOT in CREATE SCHEMA",
                )?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            return Ok(Statement::CreateSchema(CreateSchemaStatement {
                name,
                if_not_exists,
            }));
        }
        if self.consume_keyword(Keyword::Sequence) {
            let name = self.parse_qualified_name()?;
            let (start, increment, min_value, max_value, cycle, cache) =
                self.parse_create_sequence_options()?;
            return Ok(Statement::CreateSequence(CreateSequenceStatement {
                name,
                start,
                increment,
                min_value,
                max_value,
                cycle,
                cache,
            }));
        }
        self.expect_keyword(
            Keyword::Table,
            "expected TABLE, SCHEMA, INDEX, SEQUENCE, or VIEW after CREATE",
        )?;
        let name = self.parse_qualified_name()?;
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after CREATE TABLE name",
        )?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        loop {
            if self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Foreign)
                || self.peek_keyword(Keyword::Constraint)
            {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_definition()?);
            }

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }

        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after column definitions",
        )?;
        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            constraints,
        }))
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
        let name = if self.consume_keyword(Keyword::Constraint) {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        if self.consume_keyword(Keyword::Primary) {
            self.expect_keyword(Keyword::Key, "expected KEY after PRIMARY")?;
            let columns = self.parse_identifier_list_in_parens()?;
            return Ok(TableConstraint::PrimaryKey { name, columns });
        }
        if self.consume_keyword(Keyword::Unique) {
            let columns = self.parse_identifier_list_in_parens()?;
            return Ok(TableConstraint::Unique { name, columns });
        }
        if self.consume_keyword(Keyword::Foreign) {
            self.expect_keyword(Keyword::Key, "expected KEY after FOREIGN")?;
            let columns = self.parse_identifier_list_in_parens()?;
            self.expect_keyword(
                Keyword::References,
                "expected REFERENCES in FOREIGN KEY clause",
            )?;
            let referenced_table = self.parse_qualified_name()?;
            let referenced_columns = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                let mut cols = vec![self.parse_identifier()?];
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    cols.push(self.parse_identifier()?);
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after REFERENCES column list",
                )?;
                cols
            } else {
                Vec::new()
            };
            let (on_delete, on_update) = self.parse_optional_fk_actions()?;
            return Ok(TableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            });
        }

        Err(self.error_at_current("expected PRIMARY KEY, UNIQUE, or FOREIGN KEY table constraint"))
    }

    fn parse_insert_statement(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Into, "expected INTO after INSERT")?;
        let table_name = self.parse_qualified_name()?;
        let table_alias = self.parse_insert_table_alias()?;
        let columns = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut out = vec![self.parse_identifier()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                out.push(self.parse_identifier()?);
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after INSERT column list",
            )?;
            out
        } else {
            Vec::new()
        };

        let source = if self.consume_keyword(Keyword::Values) {
            let mut values = vec![self.parse_insert_values_row()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                values.push(self.parse_insert_values_row()?);
            }
            InsertSource::Values(values)
        } else {
            let query = self.parse_query()?;
            InsertSource::Query(query)
        };
        let on_conflict = if self.consume_keyword(Keyword::On) {
            self.expect_keyword(Keyword::Conflict, "expected CONFLICT after ON")?;
            let conflict_target = self.parse_conflict_target()?;
            self.expect_keyword(Keyword::Do, "expected DO in ON CONFLICT clause")?;
            if self.consume_keyword(Keyword::Nothing) {
                Some(OnConflictClause::DoNothing { conflict_target })
            } else if self.consume_keyword(Keyword::Update) {
                self.expect_keyword(Keyword::Set, "expected SET after ON CONFLICT DO UPDATE")?;
                let mut assignments = vec![self.parse_assignment()?];
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    assignments.push(self.parse_assignment()?);
                }
                let where_clause = if self.consume_keyword(Keyword::Where) {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                Some(OnConflictClause::DoUpdate {
                    conflict_target,
                    assignments,
                    where_clause,
                })
            } else {
                return Err(
                    self.error_at_current("expected NOTHING or UPDATE after ON CONFLICT DO")
                );
            }
        } else {
            None
        };
        let returning = if self.consume_keyword(Keyword::Returning) {
            self.parse_target_list()?
        } else {
            Vec::new()
        };

        Ok(Statement::Insert(InsertStatement {
            table_name,
            table_alias,
            columns,
            source,
            on_conflict,
            returning,
        }))
    }

    fn parse_update_statement(&mut self) -> Result<Statement, ParseError> {
        let table_name = self.parse_qualified_name()?;
        self.expect_keyword(Keyword::Set, "expected SET in UPDATE statement")?;

        let mut assignments = vec![self.parse_assignment()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            assignments.push(self.parse_assignment()?);
        }
        let from = if self.consume_keyword(Keyword::From) {
            self.parse_from_list()?
        } else {
            Vec::new()
        };

        let where_clause = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = if self.consume_keyword(Keyword::Returning) {
            self.parse_target_list()?
        } else {
            Vec::new()
        };

        Ok(Statement::Update(UpdateStatement {
            table_name,
            assignments,
            from,
            where_clause,
            returning,
        }))
    }

    fn parse_delete_statement(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::From, "expected FROM after DELETE")?;
        let table_name = self.parse_qualified_name()?;
        let using = if self.consume_keyword(Keyword::Using) {
            self.parse_from_list()?
        } else {
            Vec::new()
        };
        let where_clause = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };
        let returning = if self.consume_keyword(Keyword::Returning) {
            self.parse_target_list()?
        } else {
            Vec::new()
        };

        Ok(Statement::Delete(DeleteStatement {
            table_name,
            using,
            where_clause,
            returning,
        }))
    }

    fn parse_merge_statement(&mut self) -> Result<Statement, ParseError> {
        self.expect_keyword(Keyword::Into, "expected INTO after MERGE")?;
        let target_table = self.parse_qualified_name()?;
        let target_alias = self.parse_optional_alias()?;
        self.expect_keyword(Keyword::Using, "expected USING in MERGE statement")?;
        let source = self.parse_table_expression()?;
        self.expect_keyword(Keyword::On, "expected ON in MERGE statement")?;
        let on = self.parse_expr()?;

        let mut when_clauses = Vec::new();
        while self.consume_keyword(Keyword::When) {
            let mut not = false;
            if self.consume_keyword(Keyword::Not) {
                not = true;
            }
            self.expect_keyword(Keyword::Matched, "expected MATCHED in MERGE WHEN clause")?;
            let mut not_matched_by_source = false;
            if not && self.consume_keyword(Keyword::By) {
                if self.consume_keyword(Keyword::Source) {
                    not_matched_by_source = true;
                } else if self.consume_keyword(Keyword::Target) {
                    not_matched_by_source = false;
                } else {
                    return Err(self
                        .error_at_current("expected SOURCE or TARGET after WHEN NOT MATCHED BY"));
                }
            }
            let condition = if self.consume_keyword(Keyword::And) {
                Some(self.parse_expr()?)
            } else {
                None
            };
            self.expect_keyword(Keyword::Then, "expected THEN in MERGE WHEN clause")?;

            if not {
                if not_matched_by_source {
                    if self.consume_keyword(Keyword::Update) {
                        self.expect_keyword(Keyword::Set, "expected SET in MERGE UPDATE clause")?;
                        let mut assignments = vec![self.parse_assignment()?];
                        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                            assignments.push(self.parse_assignment()?);
                        }
                        when_clauses.push(MergeWhenClause::NotMatchedBySourceUpdate {
                            condition,
                            assignments,
                        });
                    } else if self.consume_keyword(Keyword::Delete) {
                        when_clauses.push(MergeWhenClause::NotMatchedBySourceDelete { condition });
                    } else if self.consume_keyword(Keyword::Do) {
                        self.expect_keyword(
                            Keyword::Nothing,
                            "expected NOTHING after DO in MERGE clause",
                        )?;
                        when_clauses
                            .push(MergeWhenClause::NotMatchedBySourceDoNothing { condition });
                    } else {
                        return Err(self.error_at_current(
                            "expected UPDATE, DELETE, or DO NOTHING for WHEN NOT MATCHED BY SOURCE",
                        ));
                    }
                } else {
                    if self.consume_keyword(Keyword::Insert) {
                        let columns = if matches!(self.current_kind(), TokenKind::LParen) {
                            self.parse_identifier_list_in_parens()?
                        } else {
                            Vec::new()
                        };
                        self.expect_keyword(
                            Keyword::Values,
                            "expected VALUES in MERGE INSERT clause",
                        )?;
                        let values = self.parse_insert_values_row()?;
                        when_clauses.push(MergeWhenClause::NotMatchedInsert {
                            condition,
                            columns,
                            values,
                        });
                    } else if self.consume_keyword(Keyword::Do) {
                        self.expect_keyword(
                            Keyword::Nothing,
                            "expected NOTHING after DO in MERGE clause",
                        )?;
                        when_clauses.push(MergeWhenClause::NotMatchedDoNothing { condition });
                    } else {
                        return Err(self.error_at_current(
                            "expected INSERT or DO NOTHING for WHEN NOT MATCHED",
                        ));
                    }
                }
                continue;
            }

            if self.consume_keyword(Keyword::Update) {
                self.expect_keyword(Keyword::Set, "expected SET in MERGE UPDATE clause")?;
                let mut assignments = vec![self.parse_assignment()?];
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    assignments.push(self.parse_assignment()?);
                }
                when_clauses.push(MergeWhenClause::MatchedUpdate {
                    condition,
                    assignments,
                });
            } else if self.consume_keyword(Keyword::Delete) {
                when_clauses.push(MergeWhenClause::MatchedDelete { condition });
            } else if self.consume_keyword(Keyword::Do) {
                self.expect_keyword(
                    Keyword::Nothing,
                    "expected NOTHING after DO in MERGE clause",
                )?;
                when_clauses.push(MergeWhenClause::MatchedDoNothing { condition });
            } else {
                return Err(self
                    .error_at_current("expected UPDATE, DELETE, or DO NOTHING for WHEN MATCHED"));
            }
        }

        if when_clauses.is_empty() {
            return Err(self.error_at_current("MERGE requires at least one WHEN clause"));
        }
        self.validate_merge_clause_reachability(&when_clauses)?;

        let returning = if self.consume_keyword(Keyword::Returning) {
            self.parse_target_list()?
        } else {
            Vec::new()
        };

        Ok(Statement::Merge(MergeStatement {
            target_table,
            target_alias,
            source,
            on,
            when_clauses,
            returning,
        }))
    }

    fn validate_merge_clause_reachability(
        &self,
        when_clauses: &[MergeWhenClause],
    ) -> Result<(), ParseError> {
        let mut unconditional_matched = false;
        let mut unconditional_not_matched = false;
        let mut unconditional_not_matched_by_source = false;
        for clause in when_clauses {
            match clause {
                MergeWhenClause::MatchedUpdate { condition, .. }
                | MergeWhenClause::MatchedDelete { condition }
                | MergeWhenClause::MatchedDoNothing { condition } => {
                    if unconditional_matched {
                        return Err(self.error_at_current(
                            "unreachable MERGE WHEN MATCHED clause after unconditional clause",
                        ));
                    }
                    if condition.is_none() {
                        unconditional_matched = true;
                    }
                }
                MergeWhenClause::NotMatchedInsert { condition, .. }
                | MergeWhenClause::NotMatchedDoNothing { condition } => {
                    if unconditional_not_matched {
                        return Err(self.error_at_current(
                            "unreachable MERGE WHEN NOT MATCHED clause after unconditional clause",
                        ));
                    }
                    if condition.is_none() {
                        unconditional_not_matched = true;
                    }
                }
                MergeWhenClause::NotMatchedBySourceUpdate { condition, .. }
                | MergeWhenClause::NotMatchedBySourceDelete { condition }
                | MergeWhenClause::NotMatchedBySourceDoNothing { condition } => {
                    if unconditional_not_matched_by_source {
                        return Err(self.error_at_current(
                            "unreachable MERGE WHEN NOT MATCHED BY SOURCE clause after unconditional clause",
                        ));
                    }
                    if condition.is_none() {
                        unconditional_not_matched_by_source = true;
                    }
                }
            }
        }
        Ok(())
    }

    fn parse_drop_statement(&mut self) -> Result<Statement, ParseError> {
        let materialized = self.consume_keyword(Keyword::Materialized);
        if !materialized && self.consume_ident("role") {
            return self.parse_drop_role_statement();
        }
        if self.consume_keyword(Keyword::View) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP VIEW")?;
                true
            } else {
                false
            };
            let mut names = vec![self.parse_qualified_name()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                names.push(self.parse_qualified_name()?);
            }
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropView(DropViewStatement {
                names,
                materialized,
                if_exists,
                behavior,
            }));
        }
        if materialized {
            return Err(self.error_at_current("expected VIEW after DROP MATERIALIZED"));
        }
        if self.consume_keyword(Keyword::Table) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP TABLE")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropTable(DropTableStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Schema) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP SCHEMA")?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropSchema(DropSchemaStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Index) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP INDEX")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropIndex(DropIndexStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Sequence) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP SEQUENCE")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropSequence(DropSequenceStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Extension) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP EXTENSION")?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            return Ok(Statement::DropExtension(DropExtensionStatement { name, if_exists }));
        }
        if self.consume_keyword(Keyword::Function) {
            // DROP FUNCTION name - simple form only
            let _name = self.parse_qualified_name()?;
            // consume optional parameter list
            if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                while !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                    self.advance();
                }
            }
            // We don't actually implement DROP FUNCTION yet, just parse it
            return Err(self.error_at_current("DROP FUNCTION is not yet supported"));
        }
        Err(self.error_at_current("expected TABLE, SCHEMA, INDEX, SEQUENCE, VIEW, or EXTENSION after DROP"))
    }

    fn parse_copy_statement(&mut self) -> Result<Statement, ParseError> {
        if self.peek_keyword(Keyword::To) || self.peek_keyword(Keyword::From) {
            return Err(self.error_at_current(
                "unsupported COPY command (expected COPY <table> TO/FROM STDOUT/STDIN ...)",
            ));
        }
        let table_name = self.parse_qualified_name()?;
        let direction = if self.consume_keyword(Keyword::To) {
            CopyDirection::To
        } else if self.consume_keyword(Keyword::From) {
            CopyDirection::From
        } else {
            return Err(self.error_at_current(
                "unsupported COPY command (expected COPY <table> TO/FROM STDOUT/STDIN ...)",
            ));
        };
        let target = self
            .take_keyword_or_identifier()
            .ok_or_else(|| {
                let message = match direction {
                    CopyDirection::To => "COPY TO requires STDOUT",
                    CopyDirection::From => "COPY FROM requires STDIN",
                };
                self.error_at_current(message)
            })?;
        let target_lower = target.to_ascii_lowercase();
        match direction {
            CopyDirection::To if target_lower != "stdout" => {
                return Err(self.error_at_current("COPY TO requires STDOUT"));
            }
            CopyDirection::From if target_lower != "stdin" => {
                return Err(self.error_at_current("COPY FROM requires STDIN"));
            }
            _ => {}
        }
        let mut options = CopyOptions {
            format: None,
            delimiter: None,
            null_marker: None,
        };
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                loop {
                    self.parse_copy_option_item(&mut options)?;
                    if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        continue;
                    }
                    self.expect_token(|k| matches!(k, TokenKind::RParen), "expected ')' after COPY options")?;
                    break;
                }
            }
        } else if !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            let format = self.parse_copy_format_value("unsupported COPY target option")?;
            options.format = Some(format);
        }
        if !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            return Err(self.error_at_current("unsupported COPY target syntax"));
        }
        Ok(Statement::Copy(CopyStatement {
            table_name,
            direction,
            options,
        }))
    }

    fn parse_grant_statement(&mut self) -> Result<Statement, ParseError> {
        let mut on_pos = None;
        let mut to_pos = None;
        for (idx, token) in self.tokens[self.idx..].iter().enumerate() {
            match token.kind {
                TokenKind::Keyword(Keyword::On) => {
                    if on_pos.is_none() {
                        on_pos = Some(idx);
                    }
                }
                TokenKind::Keyword(Keyword::To) => {
                    to_pos = Some(idx);
                }
                TokenKind::Eof | TokenKind::Semicolon => break,
                _ => {}
            }
        }
        if let (Some(on_idx), Some(to_idx)) = (on_pos, to_pos)
            && to_idx <= on_idx
        {
            return Err(self.error_at_current("GRANT clause order is invalid"));
        }
        let stmt = if on_pos.is_some() {
            self.parse_grant_table_privileges_statement()?
        } else {
            self.parse_grant_role_statement()?
        };
        Ok(Statement::Grant(stmt))
    }

    fn parse_revoke_statement(&mut self) -> Result<Statement, ParseError> {
        let mut on_pos = None;
        let mut from_pos = None;
        for (idx, token) in self.tokens[self.idx..].iter().enumerate() {
            match token.kind {
                TokenKind::Keyword(Keyword::On) => {
                    if on_pos.is_none() {
                        on_pos = Some(idx);
                    }
                }
                TokenKind::Keyword(Keyword::From) => {
                    from_pos = Some(idx);
                }
                TokenKind::Eof | TokenKind::Semicolon => break,
                _ => {}
            }
        }
        if let (Some(on_idx), Some(from_idx)) = (on_pos, from_pos)
            && from_idx <= on_idx
        {
            return Err(self.error_at_current("REVOKE clause order is invalid"));
        }
        let stmt = if on_pos.is_some() {
            self.parse_revoke_table_privileges_statement()?
        } else {
            self.parse_revoke_role_statement()?
        };
        Ok(Statement::Revoke(stmt))
    }

    fn parse_grant_role_statement(&mut self) -> Result<GrantStatement, ParseError> {
        let role_name =
            self.parse_role_identifier_with_message("GRANT role requires role and member names")?;
        if !self.consume_keyword(Keyword::To) {
            return Err(self.error_at_current("GRANT role requires TO clause"));
        }
        let member =
            self.parse_role_identifier_with_message("GRANT role requires role and member names")?;
        Ok(GrantStatement::Role(GrantRoleStatement { role_name, member }))
    }

    fn parse_grant_table_privileges_statement(&mut self) -> Result<GrantStatement, ParseError> {
        let privileges = self.parse_privilege_list("GRANT")?;
        if !self.consume_keyword(Keyword::On) {
            return Err(self.error_at_current("GRANT requires ON TABLE clause"));
        }
        self.consume_keyword(Keyword::Table);
        let table_name = self.parse_qualified_name()?;
        if !self.consume_keyword(Keyword::To) {
            return Err(self.error_at_current("GRANT requires TO clause"));
        }
        let roles = self.parse_role_list("GRANT requires at least one target role")?;
        Ok(GrantStatement::TablePrivileges(GrantTablePrivilegesStatement {
            privileges,
            table_name,
            roles,
        }))
    }

    fn parse_revoke_role_statement(&mut self) -> Result<RevokeStatement, ParseError> {
        let role_name =
            self.parse_role_identifier_with_message("REVOKE role requires role and member names")?;
        if !self.consume_keyword(Keyword::From) {
            return Err(self.error_at_current("REVOKE role requires FROM clause"));
        }
        let member =
            self.parse_role_identifier_with_message("REVOKE role requires role and member names")?;
        Ok(RevokeStatement::Role(RevokeRoleStatement { role_name, member }))
    }

    fn parse_revoke_table_privileges_statement(&mut self) -> Result<RevokeStatement, ParseError> {
        let privileges = self.parse_privilege_list("REVOKE")?;
        if !self.consume_keyword(Keyword::On) {
            return Err(self.error_at_current("REVOKE requires ON TABLE clause"));
        }
        self.consume_keyword(Keyword::Table);
        let table_name = self.parse_qualified_name()?;
        if !self.consume_keyword(Keyword::From) {
            return Err(self.error_at_current("REVOKE requires FROM clause"));
        }
        let roles = self.parse_role_list("REVOKE requires at least one target role")?;
        Ok(RevokeStatement::TablePrivileges(RevokeTablePrivilegesStatement {
            privileges,
            table_name,
            roles,
        }))
    }

    fn parse_alter_role_statement(&mut self) -> Result<Statement, ParseError> {
        let name = self.parse_role_identifier_with_message("ALTER ROLE requires a role name")?;
        let options = self.parse_role_options("ALTER ROLE")?;
        Ok(Statement::AlterRole(AlterRoleStatement { name, options }))
    }

    fn parse_drop_role_statement(&mut self) -> Result<Statement, ParseError> {
        let if_exists = if self.consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP ROLE")?;
            true
        } else {
            false
        };
        let name = self.parse_role_identifier_with_message("DROP ROLE requires a role name")?;
        Ok(Statement::DropRole(DropRoleStatement { name, if_exists }))
    }

    fn parse_truncate_statement(&mut self) -> Result<Statement, ParseError> {
        self.consume_keyword(Keyword::Table);
        let mut table_names = vec![self.parse_qualified_name()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            table_names.push(self.parse_qualified_name()?);
        }
        let behavior = self.parse_drop_behavior()?;
        Ok(Statement::Truncate(TruncateStatement {
            table_names,
            behavior,
        }))
    }

    fn parse_drop_behavior(&mut self) -> Result<DropBehavior, ParseError> {
        if self.consume_keyword(Keyword::Cascade) {
            return Ok(DropBehavior::Cascade);
        }
        if self.consume_keyword(Keyword::Restrict) {
            return Ok(DropBehavior::Restrict);
        }
        Ok(DropBehavior::Restrict)
    }

    fn parse_alter_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_ident("role") {
            return self.parse_alter_role_statement();
        }
        if self.consume_keyword(Keyword::Table) {
            return self.parse_alter_table_statement();
        }
        let materialized = self.consume_keyword(Keyword::Materialized);
        if self.consume_keyword(Keyword::View) {
            return self.parse_alter_view_statement(materialized);
        }
        if materialized {
            return Err(self.error_at_current("expected VIEW after ALTER MATERIALIZED"));
        }
        if self.consume_keyword(Keyword::Sequence) {
            return self.parse_alter_sequence_statement();
        }
        Err(self.error_at_current("expected TABLE, VIEW, or SEQUENCE after ALTER"))
    }

    fn parse_alter_table_statement(&mut self) -> Result<Statement, ParseError> {
        let table_name = self.parse_qualified_name()?;
        let action = if self.consume_keyword(Keyword::Add) {
            if self.consume_keyword(Keyword::Column) {
                AlterTableAction::AddColumn(self.parse_column_definition()?)
            } else if self.peek_keyword(Keyword::Constraint)
                || self.peek_keyword(Keyword::Primary)
                || self.peek_keyword(Keyword::Unique)
                || self.peek_keyword(Keyword::Foreign)
            {
                AlterTableAction::AddConstraint(self.parse_table_constraint()?)
            } else {
                AlterTableAction::AddColumn(self.parse_column_definition()?)
            }
        } else if self.consume_keyword(Keyword::Drop) {
            if self.consume_keyword(Keyword::Constraint) {
                AlterTableAction::DropConstraint {
                    name: self.parse_identifier()?,
                }
            } else {
                self.consume_keyword(Keyword::Column);
                AlterTableAction::DropColumn {
                    name: self.parse_identifier()?,
                }
            }
        } else if self.consume_keyword(Keyword::Rename) {
            self.consume_keyword(Keyword::Column);
            let old_name = self.parse_identifier()?;
            self.expect_keyword(Keyword::To, "expected TO in RENAME COLUMN clause")?;
            let new_name = self.parse_identifier()?;
            AlterTableAction::RenameColumn { old_name, new_name }
        } else if self.consume_keyword(Keyword::Alter) {
            self.expect_keyword(
                Keyword::Column,
                "expected COLUMN after ALTER TABLE ... ALTER",
            )?;
            let name = self.parse_identifier()?;
            if self.consume_keyword(Keyword::Set) {
                if self.consume_keyword(Keyword::Default) {
                    AlterTableAction::SetColumnDefault {
                        name,
                        default: Some(self.parse_expr()?),
                    }
                } else {
                    self.expect_keyword(
                        Keyword::Not,
                        "expected NOT after SET in ALTER COLUMN clause",
                    )?;
                    self.expect_keyword(
                        Keyword::Null,
                        "expected NULL after SET NOT in ALTER COLUMN clause",
                    )?;
                    AlterTableAction::SetColumnNullable {
                        name,
                        nullable: false,
                    }
                }
            } else if self.consume_keyword(Keyword::Drop) {
                if self.consume_keyword(Keyword::Default) {
                    AlterTableAction::SetColumnDefault {
                        name,
                        default: None,
                    }
                } else {
                    self.expect_keyword(
                        Keyword::Not,
                        "expected NOT after DROP in ALTER COLUMN clause",
                    )?;
                    self.expect_keyword(
                        Keyword::Null,
                        "expected NULL after DROP NOT in ALTER COLUMN clause",
                    )?;
                    AlterTableAction::SetColumnNullable {
                        name,
                        nullable: true,
                    }
                }
            } else {
                return Err(self.error_at_current(
                    "expected SET/DROP NOT NULL or SET/DROP DEFAULT in ALTER COLUMN clause",
                ));
            }
        } else {
            return Err(
                self.error_at_current("expected ADD, DROP, RENAME, or ALTER action in ALTER TABLE")
            );
        };

        Ok(Statement::AlterTable(AlterTableStatement {
            table_name,
            action,
        }))
    }

    fn parse_alter_view_statement(&mut self, materialized: bool) -> Result<Statement, ParseError> {
        let name = self.parse_qualified_name()?;
        let action = if self.consume_keyword(Keyword::Rename) {
            if self.consume_keyword(Keyword::Column) {
                let old_name = self.parse_identifier()?;
                self.expect_keyword(
                    Keyword::To,
                    "expected TO after RENAME COLUMN in ALTER VIEW statement",
                )?;
                AlterViewAction::RenameColumn {
                    old_name,
                    new_name: self.parse_identifier()?,
                }
            } else {
                self.expect_keyword(
                    Keyword::To,
                    "expected TO after RENAME in ALTER VIEW statement",
                )?;
                AlterViewAction::RenameTo {
                    new_name: self.parse_identifier()?,
                }
            }
        } else if self.consume_keyword(Keyword::Set) {
            self.expect_keyword(
                Keyword::Schema,
                "expected SCHEMA after SET in ALTER VIEW statement",
            )?;
            AlterViewAction::SetSchema {
                schema_name: self.parse_identifier()?,
            }
        } else {
            return Err(self.error_at_current(
                "expected RENAME TO, RENAME COLUMN, or SET SCHEMA in ALTER VIEW statement",
            ));
        };
        Ok(Statement::AlterView(AlterViewStatement {
            name,
            materialized,
            action,
        }))
    }

    fn parse_alter_sequence_statement(&mut self) -> Result<Statement, ParseError> {
        let name = self.parse_qualified_name()?;
        let mut actions = Vec::new();
        loop {
            if self.consume_keyword(Keyword::Restart) {
                let with = if self.consume_keyword(Keyword::With)
                    || matches!(
                        self.current_kind(),
                        TokenKind::Integer(_) | TokenKind::Plus | TokenKind::Minus
                    ) {
                    Some(self.parse_signed_integer_literal()?)
                } else {
                    None
                };
                actions.push(AlterSequenceAction::Restart { with });
                continue;
            }
            if self.consume_keyword(Keyword::Start) {
                self.consume_keyword(Keyword::With);
                let start = self.parse_signed_integer_literal()?;
                actions.push(AlterSequenceAction::SetStart { start });
                continue;
            }
            if self.consume_keyword(Keyword::Increment) {
                self.consume_keyword(Keyword::By);
                let increment = self.parse_signed_integer_literal()?;
                actions.push(AlterSequenceAction::SetIncrement { increment });
                continue;
            }
            if self.consume_keyword(Keyword::MinValue) {
                let min = self.parse_signed_integer_literal()?;
                actions.push(AlterSequenceAction::SetMinValue { min: Some(min) });
                continue;
            }
            if self.consume_keyword(Keyword::MaxValue) {
                let max = self.parse_signed_integer_literal()?;
                actions.push(AlterSequenceAction::SetMaxValue { max: Some(max) });
                continue;
            }
            if self.consume_keyword(Keyword::No) {
                if self.consume_keyword(Keyword::MinValue) {
                    actions.push(AlterSequenceAction::SetMinValue { min: None });
                    continue;
                }
                if self.consume_keyword(Keyword::MaxValue) {
                    actions.push(AlterSequenceAction::SetMaxValue { max: None });
                    continue;
                }
                if self.consume_keyword(Keyword::Cycle) {
                    actions.push(AlterSequenceAction::SetCycle { cycle: false });
                    continue;
                }
                return Err(self.error_at_current("expected MINVALUE, MAXVALUE, or CYCLE after NO"));
            }
            if self.consume_keyword(Keyword::Cycle) {
                actions.push(AlterSequenceAction::SetCycle { cycle: true });
                continue;
            }
            if self.consume_keyword(Keyword::Cache) {
                let cache = self.parse_signed_integer_literal()?;
                actions.push(AlterSequenceAction::SetCache { cache });
                continue;
            }
            break;
        }
        if actions.is_empty() {
            return Err(
                self.error_at_current("expected sequence options in ALTER SEQUENCE statement")
            );
        }
        Ok(Statement::AlterSequence(AlterSequenceStatement {
            name,
            actions,
        }))
    }

    fn parse_assignment(&mut self) -> Result<Assignment, ParseError> {
        let column = self.parse_identifier()?;
        self.expect_token(
            |k| matches!(k, TokenKind::Equal),
            "expected '=' in assignment",
        )?;
        let value = self.parse_expr()?;
        Ok(Assignment { column, value })
    }

    fn parse_insert_values_row(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' to start VALUES row",
        )?;
        let mut row = vec![self.parse_expr()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            row.push(self.parse_expr()?);
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after VALUES row",
        )?;
        Ok(row)
    }

    #[allow(clippy::type_complexity)]
    fn parse_create_sequence_options(
        &mut self,
    ) -> Result<
        (
            Option<i64>,
            Option<i64>,
            Option<Option<i64>>,
            Option<Option<i64>>,
            Option<bool>,
            Option<i64>,
        ),
        ParseError,
    > {
        let mut start = None;
        let mut increment = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut cycle = None;
        let mut cache = None;

        loop {
            if self.consume_keyword(Keyword::Start) {
                if start.is_some() {
                    return Err(self.error_at_current("duplicate START option in CREATE SEQUENCE"));
                }
                self.consume_keyword(Keyword::With);
                start = Some(self.parse_signed_integer_literal()?);
                continue;
            }
            if self.consume_keyword(Keyword::Increment) {
                if increment.is_some() {
                    return Err(
                        self.error_at_current("duplicate INCREMENT option in CREATE SEQUENCE")
                    );
                }
                self.consume_keyword(Keyword::By);
                increment = Some(self.parse_signed_integer_literal()?);
                continue;
            }
            if self.consume_keyword(Keyword::MinValue) {
                if min_value.is_some() {
                    return Err(
                        self.error_at_current("duplicate MINVALUE option in CREATE SEQUENCE")
                    );
                }
                min_value = Some(Some(self.parse_signed_integer_literal()?));
                continue;
            }
            if self.consume_keyword(Keyword::MaxValue) {
                if max_value.is_some() {
                    return Err(
                        self.error_at_current("duplicate MAXVALUE option in CREATE SEQUENCE")
                    );
                }
                max_value = Some(Some(self.parse_signed_integer_literal()?));
                continue;
            }
            if self.consume_keyword(Keyword::No) {
                if self.consume_keyword(Keyword::MinValue) {
                    if min_value.is_some() {
                        return Err(
                            self.error_at_current("duplicate MINVALUE option in CREATE SEQUENCE")
                        );
                    }
                    min_value = Some(None);
                    continue;
                }
                if self.consume_keyword(Keyword::MaxValue) {
                    if max_value.is_some() {
                        return Err(
                            self.error_at_current("duplicate MAXVALUE option in CREATE SEQUENCE")
                        );
                    }
                    max_value = Some(None);
                    continue;
                }
                if self.consume_keyword(Keyword::Cycle) {
                    if cycle.is_some() {
                        return Err(
                            self.error_at_current("duplicate CYCLE option in CREATE SEQUENCE")
                        );
                    }
                    cycle = Some(false);
                    continue;
                }
                return Err(self.error_at_current("expected MINVALUE, MAXVALUE, or CYCLE after NO"));
            }
            if self.consume_keyword(Keyword::Cycle) {
                if cycle.is_some() {
                    return Err(self.error_at_current("duplicate CYCLE option in CREATE SEQUENCE"));
                }
                cycle = Some(true);
                continue;
            }
            if self.consume_keyword(Keyword::Cache) {
                if cache.is_some() {
                    return Err(self.error_at_current("duplicate CACHE option in CREATE SEQUENCE"));
                }
                cache = Some(self.parse_signed_integer_literal()?);
                continue;
            }
            break;
        }

        Ok((start, increment, min_value, max_value, cycle, cache))
    }

    fn parse_insert_table_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.consume_keyword(Keyword::As) {
            return Ok(Some(self.parse_identifier()?));
        }
        if matches!(self.current_kind(), TokenKind::Identifier(_)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    fn parse_conflict_target(&mut self) -> Result<Option<ConflictTarget>, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut cols = vec![self.parse_identifier()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                cols.push(self.parse_identifier()?);
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after ON CONFLICT target",
            )?;
            return Ok(Some(ConflictTarget::Columns(cols)));
        }
        if self.consume_keyword(Keyword::On) {
            self.expect_keyword(
                Keyword::Constraint,
                "expected CONSTRAINT after ON in ON CONFLICT clause",
            )?;
            let name = self.parse_identifier()?;
            return Ok(Some(ConflictTarget::Constraint(name)));
        }
        Ok(None)
    }

    fn parse_signed_integer_literal(&mut self) -> Result<i64, ParseError> {
        let sign = if self.consume_if(|k| matches!(k, TokenKind::Minus)) {
            -1i64
        } else {
            self.consume_if(|k| matches!(k, TokenKind::Plus));
            1i64
        };
        match self.current_kind() {
            TokenKind::Integer(v) => {
                let value = *v;
                self.advance();
                Ok(sign.saturating_mul(value))
            }
            _ => Err(self.error_at_current("expected integer literal")),
        }
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDefinition, ParseError> {
        let name = self.parse_identifier()?;
        let data_type = self.parse_type_name()?;
        let mut nullable = true;
        let mut identity = false;
        let mut primary_key = false;
        let mut unique = false;
        let mut references = None;
        let mut check = None;
        let mut default = None;

        loop {
            if self.consume_keyword(Keyword::Not) {
                self.expect_keyword(Keyword::Null, "expected NULL after NOT")?;
                nullable = false;
                continue;
            }
            if self.consume_keyword(Keyword::Null) {
                nullable = true;
                continue;
            }
            if self.consume_keyword(Keyword::Primary) {
                self.expect_keyword(Keyword::Key, "expected KEY after PRIMARY")?;
                primary_key = true;
                unique = true;
                nullable = false;
                continue;
            }
            if self.consume_keyword(Keyword::Generated) {
                if self.consume_keyword(Keyword::Always) {
                    // Accepted for parser parity; treated like BY DEFAULT currently.
                } else if self.consume_keyword(Keyword::By) {
                    self.expect_keyword(Keyword::Default, "expected DEFAULT after GENERATED BY")?;
                } else {
                    return Err(
                        self.error_at_current("expected ALWAYS or BY DEFAULT after GENERATED")
                    );
                }
                self.expect_keyword(Keyword::As, "expected AS in GENERATED ... AS IDENTITY")?;
                self.expect_keyword(
                    Keyword::Identity,
                    "expected IDENTITY in GENERATED ... AS IDENTITY",
                )?;
                identity = true;
                nullable = false;
                continue;
            }
            if self.consume_keyword(Keyword::Unique) {
                unique = true;
                continue;
            }
            if self.consume_keyword(Keyword::References) {
                references = Some(self.parse_references_clause()?);
                continue;
            }
            if self.consume_keyword(Keyword::Default) {
                default = Some(self.parse_expr()?);
                continue;
            }
            if self.consume_keyword(Keyword::Check) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after CHECK",
                )?;
                check = Some(self.parse_expr()?);
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after CHECK expression",
                )?;
                continue;
            }
            break;
        }

        Ok(ColumnDefinition {
            name,
            data_type,
            nullable,
            identity,
            primary_key,
            unique,
            references,
            check,
            default,
        })
    }

    fn parse_references_clause(&mut self) -> Result<ForeignKeyReference, ParseError> {
        let table_name = self.parse_qualified_name()?;
        let column_name = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let column = self.parse_identifier()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after REFERENCES column",
            )?;
            Some(column)
        } else {
            None
        };
        let (on_delete, on_update) = self.parse_optional_fk_actions()?;

        Ok(ForeignKeyReference {
            table_name,
            column_name,
            on_delete,
            on_update,
        })
    }

    fn parse_fk_action(&mut self) -> Result<ForeignKeyAction, ParseError> {
        if self.consume_keyword(Keyword::Cascade) {
            return Ok(ForeignKeyAction::Cascade);
        }
        if self.consume_keyword(Keyword::Restrict) {
            return Ok(ForeignKeyAction::Restrict);
        }
        if self.consume_keyword(Keyword::Set) {
            self.expect_keyword(
                Keyword::Null,
                "expected NULL after SET in foreign key ON DELETE action",
            )?;
            return Ok(ForeignKeyAction::SetNull);
        }
        Err(self.error_at_current("expected CASCADE, RESTRICT, or SET NULL"))
    }

    fn parse_optional_fk_actions(
        &mut self,
    ) -> Result<(ForeignKeyAction, ForeignKeyAction), ParseError> {
        let mut on_delete = ForeignKeyAction::Restrict;
        let mut on_update = ForeignKeyAction::Restrict;
        let mut saw_delete = false;
        let mut saw_update = false;

        while self.consume_keyword(Keyword::On) {
            if self.consume_keyword(Keyword::Delete) {
                if saw_delete {
                    return Err(
                        self.error_at_current("duplicate ON DELETE action in foreign key clause")
                    );
                }
                on_delete = self.parse_fk_action()?;
                saw_delete = true;
                continue;
            }
            if self.consume_keyword(Keyword::Update) {
                if saw_update {
                    return Err(
                        self.error_at_current("duplicate ON UPDATE action in foreign key clause")
                    );
                }
                on_update = self.parse_fk_action()?;
                saw_update = true;
                continue;
            }
            return Err(
                self.error_at_current("expected DELETE or UPDATE after ON in foreign key clause")
            );
        }

        Ok((on_delete, on_update))
    }

    fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let base = self.parse_identifier()?.to_ascii_lowercase();
        let ty = match base.as_str() {
            "bool" | "boolean" => TypeName::Bool,
            "smallint" | "int2" => TypeName::Int2,
            "int" | "integer" | "int4" => TypeName::Int4,
            "bigint" | "int8" => TypeName::Int8,
            "real" | "float4" => TypeName::Float4,
            "float" | "float8" => TypeName::Float8,
            "double" => {
                if let TokenKind::Identifier(next) = self.current_kind()
                    && next.eq_ignore_ascii_case("precision")
                {
                    self.advance();
                }
                TypeName::Float8
            }
            "text" => TypeName::Text,
            "varchar" => TypeName::Varchar,
            "character" => {
                if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("varying"))
                {
                    self.advance();
                    TypeName::Varchar
                } else {
                    TypeName::Char
                }
            }
            "char" => TypeName::Char,
            "bytea" => TypeName::Bytea,
            "uuid" => TypeName::Uuid,
            "json" => TypeName::Json,
            "jsonb" => TypeName::Jsonb,
            "date" => TypeName::Date,
            "timestamp" => {
                // Check for WITH TIME ZONE / WITHOUT TIME ZONE
                if let TokenKind::Keyword(Keyword::With) = self.current_kind() {
                    // peek ahead for TIME ZONE
                    // For now, just treat as TimestampTz if WITH follows
                    TypeName::Timestamp
                } else {
                    TypeName::Timestamp
                }
            }
            "timestamptz" => TypeName::TimestampTz,
            "interval" => TypeName::Interval,
            "serial" => TypeName::Serial,
            "bigserial" | "serial8" => TypeName::BigSerial,
            "numeric" | "decimal" => TypeName::Numeric,
            "money" => TypeName::Numeric,  // treat money as numeric for now
            other => {
                return Err(self.error_at_current(&format!("unsupported type name \"{other}\"")));
            }
        };

        // Ignore type modifiers like varchar(255).
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut depth = 1usize;
            while depth > 0 {
                match self.current_kind() {
                    TokenKind::LParen => {
                        depth += 1;
                        self.advance();
                    }
                    TokenKind::RParen => {
                        depth -= 1;
                        self.advance();
                    }
                    TokenKind::Eof => {
                        return Err(self.error_at_current("unterminated type modifier list"));
                    }
                    _ => self.advance(),
                }
            }
        }

        Ok(ty)
    }

    fn parse_query(&mut self) -> Result<Query, ParseError> {
        let with = if self.consume_keyword(Keyword::With) {
            let recursive = self.consume_keyword(Keyword::Recursive);
            let mut ctes = Vec::new();
            loop {
                let name = self.parse_identifier()?;
                self.expect_keyword(Keyword::As, "expected AS in common table expression")?;
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' to open common table expression",
                )?;
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close common table expression",
                )?;
                ctes.push(CommonTableExpr { name, query });
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    break;
                }
            }
            Some(WithClause { recursive, ctes })
        } else {
            None
        };

        let body = self.parse_query_expr_bp(0)?;

        let order_by = if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By, "expected BY after ORDER")?;
            self.parse_order_by_list()?
        } else {
            Vec::new()
        };

        let limit = if self.consume_keyword(Keyword::Limit) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let offset = if self.consume_keyword(Keyword::Offset) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Query {
            with,
            body,
            order_by,
            limit,
            offset,
        })
    }

    fn parse_query_expr_bp(&mut self, min_bp: u8) -> Result<QueryExpr, ParseError> {
        let mut lhs = self.parse_query_term()?;

        while let Some((op, l_bp, r_bp)) = self.current_set_op() {
            if l_bp < min_bp {
                break;
            }

            self.advance();
            let quantifier = if self.consume_keyword(Keyword::All) {
                SetQuantifier::All
            } else {
                self.consume_keyword(Keyword::Distinct);
                SetQuantifier::Distinct
            };

            let rhs = self.parse_query_expr_bp(r_bp)?;
            lhs = QueryExpr::SetOperation {
                left: Box::new(lhs),
                op,
                quantifier,
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_query_term(&mut self) -> Result<QueryExpr, ParseError> {
        if self.consume_keyword(Keyword::Select) {
            return Ok(QueryExpr::Select(self.parse_select_after_select_keyword()?));
        }

        if self.consume_keyword(Keyword::Values) {
            // VALUES (expr, ...), (expr, ...) -> converted to UNION ALL of SELECTs
            let mut all_rows = Vec::new();
            loop {
                self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' in VALUES")?;
                let values = self.parse_expr_list()?;
                self.expect_token(|k| matches!(k, TokenKind::RParen), "expected ')' in VALUES")?;
                all_rows.push(values);
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    break;
                }
            }
            // Generate column names: column1, column2, ...
            let ncols = all_rows.first().map(|r| r.len()).unwrap_or(0);
            let _targets: Vec<SelectItem> = (0..ncols).map(|i| SelectItem {
                expr: Expr::Identifier(vec![format!("column{}", i + 1)]),
                alias: Some(format!("column{}", i + 1)),
            }).collect();
            // For first row, create a SELECT with literal values
            let mut result: Option<QueryExpr> = None;
            for row_values in all_rows {
                let row_targets: Vec<SelectItem> = row_values.into_iter().enumerate().map(|(i, expr)| SelectItem {
                    expr,
                    alias: Some(format!("column{}", i + 1)),
                }).collect();
                let select = QueryExpr::Select(SelectStatement {
                    quantifier: None,
                    distinct_on: Vec::new(),
                    targets: row_targets,
                    from: Vec::new(),
                    where_clause: None,
                    group_by: Vec::new(),
                    having: None,
                });
                result = Some(match result {
                    None => select,
                    Some(left) => QueryExpr::SetOperation {
                        left: Box::new(left),
                        op: SetOperator::Union,
                        quantifier: SetQuantifier::All,
                        right: Box::new(select),
                    },
                });
            }
            return Ok(result.unwrap_or(QueryExpr::Select(SelectStatement {
                quantifier: None,
                distinct_on: Vec::new(),
                targets: Vec::new(),
                from: Vec::new(),
                where_clause: None,
                group_by: Vec::new(),
                having: None,
            })));
        }

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let nested = self.parse_query()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close subquery",
            )?;
            return Ok(QueryExpr::Nested(Box::new(nested)));
        }

        Err(self.error_at_current("expected query term (SELECT, VALUES, or parenthesized query)"))
    }

    fn parse_select_after_select_keyword(&mut self) -> Result<SelectStatement, ParseError> {
        let mut distinct_on = Vec::new();
        let quantifier = if self.consume_keyword(Keyword::Distinct) {
            // Check for DISTINCT ON (expr, ...)
            if self.consume_keyword(Keyword::On) {
                self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' after DISTINCT ON")?;
                distinct_on = self.parse_expr_list()?;
                self.expect_token(|k| matches!(k, TokenKind::RParen), "expected ')' after DISTINCT ON expressions")?;
            }
            Some(SelectQuantifier::Distinct)
        } else if self.consume_keyword(Keyword::All) {
            Some(SelectQuantifier::All)
        } else {
            None
        };

        let targets = self.parse_target_list()?;

        let from = if self.consume_keyword(Keyword::From) {
            self.parse_from_list()?
        } else {
            Vec::new()
        };

        let where_clause = if self.consume_keyword(Keyword::Where) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.consume_keyword(Keyword::Group) {
            self.expect_keyword(Keyword::By, "expected BY after GROUP")?;
            self.parse_group_by_list()?
        } else {
            Vec::new()
        };

        let having = if self.consume_keyword(Keyword::Having) {
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(SelectStatement {
            quantifier,
            distinct_on,
            targets,
            from,
            where_clause,
            group_by,
            having,
        })
    }

    fn parse_target_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut targets = Vec::new();
        targets.push(self.parse_target_item()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            targets.push(self.parse_target_item()?);
        }
        Ok(targets)
    }

    fn parse_target_item(&mut self) -> Result<SelectItem, ParseError> {
        let expr = if self.consume_if(|k| matches!(k, TokenKind::Star)) {
            Expr::Wildcard
        } else {
            self.parse_expr()?
        };

        let alias = self.parse_optional_alias()?;
        Ok(SelectItem { expr, alias })
    }

    fn parse_from_list(&mut self) -> Result<Vec<TableExpression>, ParseError> {
        let mut tables = Vec::new();
        tables.push(self.parse_table_expression()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            tables.push(self.parse_table_expression()?);
        }
        Ok(tables)
    }

    fn parse_table_expression(&mut self) -> Result<TableExpression, ParseError> {
        let mut left = self.parse_table_factor()?;

        loop {
            if self.consume_keyword(Keyword::Cross) {
                self.expect_keyword(Keyword::Join, "expected JOIN after CROSS")?;
                let right = self.parse_table_factor()?;
                left = TableExpression::Join(JoinExpr {
                    left: Box::new(left),
                    kind: JoinType::Cross,
                    right: Box::new(right),
                    condition: None,
                    natural: false,
                    alias: None,
                });
                continue;
            }

            let natural = self.consume_keyword(Keyword::Natural);
            let kind = if self.consume_keyword(Keyword::Left) {
                self.consume_keyword(Keyword::Outer);
                Some(JoinType::Left)
            } else if self.consume_keyword(Keyword::Right) {
                self.consume_keyword(Keyword::Outer);
                Some(JoinType::Right)
            } else if self.consume_keyword(Keyword::Full) {
                self.consume_keyword(Keyword::Outer);
                Some(JoinType::Full)
            } else if self.consume_keyword(Keyword::Inner) || self.peek_keyword(Keyword::Join) {
                Some(JoinType::Inner)
            } else {
                None
            };

            if natural && kind.is_none() && !self.peek_keyword(Keyword::Join) {
                return Err(self.error_at_current("expected JOIN after NATURAL"));
            }

            let Some(kind) = kind else {
                break;
            };

            self.expect_keyword(Keyword::Join, "expected JOIN in join clause")?;
            let right = self.parse_table_factor()?;
            let condition = if natural || matches!(kind, JoinType::Cross) {
                None
            } else if self.consume_keyword(Keyword::On) {
                Some(JoinCondition::On(self.parse_expr()?))
            } else if self.consume_keyword(Keyword::Using) {
                Some(JoinCondition::Using(
                    self.parse_identifier_list_in_parens()?,
                ))
            } else {
                None
            };

            left = TableExpression::Join(JoinExpr {
                left: Box::new(left),
                kind,
                right: Box::new(right),
                condition,
                natural,
                alias: None,
            });
        }

        Ok(left)
    }

    fn parse_table_factor(&mut self) -> Result<TableExpression, ParseError> {
        let lateral = self.consume_keyword(Keyword::Lateral);
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if self.peek_keyword(Keyword::Select)
                || self.peek_keyword(Keyword::Values)
                || matches!(self.current_kind(), TokenKind::LParen)
            {
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close subquery in FROM",
                )?;
                let alias = self.parse_optional_alias()?;
                return Ok(TableExpression::Subquery(SubqueryRef {
                    query,
                    alias,
                    lateral,
                }));
            }

            let mut inner = self.parse_table_expression()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close table expression",
            )?;
            if let Some(alias) = self.parse_optional_alias()? {
                self.apply_table_alias(&mut inner, alias);
            }
            return Ok(inner);
        }
        if lateral {
            return Err(self.error_at_current("expected subquery after LATERAL"));
        }

        let name = self.parse_qualified_name()?;
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut args = Vec::new();
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                args.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    args.push(self.parse_expr()?);
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after function arguments",
                )?;
            }
            let alias = self.parse_optional_alias()?;
            let (column_aliases, column_alias_types) = self.parse_optional_column_aliases()?;
            return Ok(TableExpression::Function(TableFunctionRef {
                name,
                args,
                alias,
                column_aliases,
                column_alias_types,
            }));
        }
        let alias = self.parse_optional_alias()?;
        Ok(TableExpression::Relation(TableRef { name, alias }))
    }

    fn apply_table_alias(&self, table: &mut TableExpression, alias: String) {
        match table {
            TableExpression::Relation(rel) => rel.alias = Some(alias),
            TableExpression::Function(function) => function.alias = Some(alias),
            TableExpression::Subquery(sub) => sub.alias = Some(alias),
            TableExpression::Join(join) => join.alias = Some(alias),
        }
    }

    fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.consume_keyword(Keyword::As) {
            return Ok(Some(self.parse_identifier()?));
        }
        if matches!(self.current_kind(), TokenKind::Identifier(_)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    fn parse_identifier_list_in_parens(&mut self) -> Result<Vec<String>, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after USING",
        )?;
        let mut cols = vec![self.parse_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            cols.push(self.parse_identifier()?);
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after USING column list",
        )?;
        Ok(cols)
    }

    fn parse_optional_column_aliases(
        &mut self,
    ) -> Result<(Vec<String>, Vec<Option<String>>), ParseError> {
        if !self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            return Ok((Vec::new(), Vec::new()));
        }

        let mut cols = Vec::new();
        let mut types = Vec::new();
        loop {
            cols.push(self.parse_identifier()?);
            types.push(self.parse_optional_column_alias_type()?);
            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after column alias list",
        )?;
        Ok((cols, types))
    }

    fn parse_optional_column_alias_type(&mut self) -> Result<Option<String>, ParseError> {
        match self.current_kind() {
            TokenKind::Comma | TokenKind::RParen => Ok(None),
            _ => self.parse_expr_type_name().map(Some),
        }
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, ParseError> {
        let mut out = Vec::new();
        loop {
            let expr = self.parse_expr()?;
            let ascending = if self.consume_keyword(Keyword::Asc) {
                Some(true)
            } else if self.consume_keyword(Keyword::Desc) {
                Some(false)
            } else {
                None
            };
            out.push(OrderByExpr { expr, ascending });

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        Ok(out)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut out = Vec::new();
        out.push(self.parse_expr()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            out.push(self.parse_expr()?);
        }
        Ok(out)
    }

    fn parse_group_by_list(&mut self) -> Result<Vec<GroupByExpr>, ParseError> {
        let mut items = Vec::new();
        loop {
            if self.consume_keyword(Keyword::Grouping) {
                self.expect_keyword(Keyword::Sets, "expected SETS after GROUPING")?;
                items.push(GroupByExpr::GroupingSets(self.parse_grouping_sets()?));
            } else if self.consume_keyword(Keyword::Rollup) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after ROLLUP",
                )?;
                let exprs = if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                    Vec::new()
                } else {
                    let exprs = self.parse_expr_list()?;
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after ROLLUP list",
                    )?;
                    exprs
                };
                items.push(GroupByExpr::Rollup(exprs));
            } else if self.consume_keyword(Keyword::Cube) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after CUBE",
                )?;
                let exprs = if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                    Vec::new()
                } else {
                    let exprs = self.parse_expr_list()?;
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after CUBE list",
                    )?;
                    exprs
                };
                items.push(GroupByExpr::Cube(exprs));
            } else {
                items.push(GroupByExpr::Expr(self.parse_expr()?));
            }

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        Ok(items)
    }

    fn parse_grouping_sets(&mut self) -> Result<Vec<Vec<Expr>>, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after GROUPING SETS",
        )?;
        let mut sets = Vec::new();
        loop {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' to start grouping set",
            )?;
            if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                sets.push(Vec::new());
            } else {
                let exprs = self.parse_expr_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close grouping set",
                )?;
                sets.push(exprs);
            }

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after GROUPING SETS",
        )?;
        Ok(sets)
    }

    fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_bp(0)
    }

    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        let mut lhs = self.parse_prefix_expr()?;

        loop {
            if self
                .peek_nth_kind(0)
                .is_some_and(|k| matches!(k, TokenKind::Typecast))
            {
                let l_bp = 12;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                let type_name = self.parse_expr_type_name()?;
                lhs = Expr::Cast {
                    expr: Box::new(lhs),
                    type_name,
                };
                continue;
            }
            if self.peek_keyword(Keyword::Not) && self.peek_nth_keyword(1, Keyword::In) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                self.advance();
                lhs = self.parse_in_expr(lhs, true)?;
                continue;
            }
            if self.peek_keyword(Keyword::In) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                lhs = self.parse_in_expr(lhs, false)?;
                continue;
            }
            if self.peek_keyword(Keyword::Not) && self.peek_nth_keyword(1, Keyword::Between) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                self.advance();
                lhs = self.parse_between_expr(lhs, true)?;
                continue;
            }
            if self.peek_keyword(Keyword::Between) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                lhs = self.parse_between_expr(lhs, false)?;
                continue;
            }
            if self.peek_keyword(Keyword::Not)
                && (self.peek_nth_keyword(1, Keyword::Like)
                    || self.peek_nth_keyword(1, Keyword::ILike))
            {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                let case_insensitive = if self.consume_keyword(Keyword::Like) {
                    false
                } else {
                    self.expect_keyword(Keyword::ILike, "expected LIKE or ILIKE after NOT")?;
                    true
                };
                lhs = self.parse_like_expr(lhs, true, case_insensitive)?;
                continue;
            }
            if self.peek_keyword(Keyword::Like) || self.peek_keyword(Keyword::ILike) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                let case_insensitive = if self.consume_keyword(Keyword::Like) {
                    false
                } else {
                    self.expect_keyword(Keyword::ILike, "expected LIKE or ILIKE")?;
                    true
                };
                lhs = self.parse_like_expr(lhs, false, case_insensitive)?;
                continue;
            }
            if self.peek_keyword(Keyword::Is) {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                self.advance();
                let negated = self.consume_keyword(Keyword::Not);
                if self.consume_keyword(Keyword::Null) {
                    lhs = Expr::IsNull {
                        expr: Box::new(lhs),
                        negated,
                    };
                    continue;
                }
                self.expect_keyword(Keyword::Distinct, "expected NULL or DISTINCT after IS")?;
                self.expect_keyword(Keyword::From, "expected FROM after IS DISTINCT")?;
                let rhs = self.parse_expr_bp(6)?;
                lhs = Expr::IsDistinctFrom {
                    left: Box::new(lhs),
                    right: Box::new(rhs),
                    negated,
                };
                continue;
            }

            let Some((op, l_bp, r_bp)) = self.current_binary_op() else {
                break;
            };
            if l_bp < min_bp {
                break;
            }

            self.advance();
            if matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::Lte
                    | BinaryOp::Gt
                    | BinaryOp::Gte
            ) && (self.peek_keyword(Keyword::Any) || self.peek_keyword(Keyword::All))
            {
                let quantifier = if self.consume_keyword(Keyword::Any) {
                    ComparisonQuantifier::Any
                } else {
                    self.expect_keyword(Keyword::All, "expected ANY or ALL")?;
                    ComparisonQuantifier::All
                };
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after ANY/ALL",
                )?;
                let rhs = self.parse_expr()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after ANY/ALL expression",
                )?;
                lhs = Expr::AnyAll {
                    left: Box::new(lhs),
                    op,
                    right: Box::new(rhs),
                    quantifier,
                };
                continue;
            }

            let rhs = self.parse_expr_bp(r_bp)?;
            lhs = Expr::Binary {
                left: Box::new(lhs),
                op,
                right: Box::new(rhs),
            };
        }

        Ok(lhs)
    }

    fn parse_prefix_expr(&mut self) -> Result<Expr, ParseError> {
        if self.consume_keyword(Keyword::Array) {
            if self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
                if self.consume_if(|k| matches!(k, TokenKind::RBracket)) {
                    return Ok(Expr::ArrayConstructor(Vec::new()));
                }
                let mut items = Vec::new();
                items.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    items.push(self.parse_expr()?);
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RBracket),
                    "expected ']' to close ARRAY constructor",
                )?;
                return Ok(Expr::ArrayConstructor(items));
            }
            if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                if !self.current_starts_query() {
                    return Err(self.error_at_current("expected subquery after ARRAY("));
                }
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after ARRAY subquery",
                )?;
                return Ok(Expr::ArraySubquery(Box::new(query)));
            }
            return Err(self.error_at_current("expected ARRAY[ or ARRAY("));
        }
        if self.consume_keyword(Keyword::Cast) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CAST",
            )?;
            let expr = self.parse_expr()?;
            self.expect_keyword(Keyword::As, "expected AS in CAST expression")?;
            let type_name = self.parse_expr_type_name()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close CAST expression",
            )?;
            return Ok(Expr::Cast {
                expr: Box::new(expr),
                type_name,
            });
        }
        if self.consume_keyword(Keyword::Exists) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after EXISTS",
            )?;
            let subquery = self.parse_query()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after EXISTS subquery",
            )?;
            return Ok(Expr::Exists(Box::new(subquery)));
        }
        if self.consume_keyword(Keyword::Case) {
            return self.parse_case_expr();
        }
        if self.consume_keyword(Keyword::Not) {
            let expr = self.parse_expr_bp(11)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            });
        }
        if self.consume_if(|k| matches!(k, TokenKind::Plus)) {
            let expr = self.parse_expr_bp(11)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Plus,
                expr: Box::new(expr),
            });
        }
        if self.consume_if(|k| matches!(k, TokenKind::Minus)) {
            let expr = self.parse_expr_bp(11)?;
            return Ok(Expr::Unary {
                op: UnaryOp::Minus,
                expr: Box::new(expr),
            });
        }

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if self.current_starts_query() {
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after scalar subquery",
                )?;
                return Ok(Expr::ScalarSubquery(Box::new(query)));
            }

            let expr = self.parse_expr()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close expression",
            )?;
            return Ok(expr);
        }

        match self.current_kind() {
            TokenKind::Integer(v) => {
                let value = *v;
                self.advance();
                Ok(Expr::Integer(value))
            }
            TokenKind::Float(v) => {
                let value = v.clone();
                self.advance();
                Ok(Expr::Float(value))
            }
            TokenKind::String(v) => {
                let value = v.clone();
                self.advance();
                Ok(Expr::String(value))
            }
            TokenKind::Parameter(v) => {
                let value = *v;
                self.advance();
                Ok(Expr::Parameter(value))
            }
            TokenKind::Keyword(Keyword::True) => {
                self.advance();
                Ok(Expr::Boolean(true))
            }
            TokenKind::Keyword(Keyword::False) => {
                self.advance();
                Ok(Expr::Boolean(false))
            }
            TokenKind::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Expr::Null)
            }
            TokenKind::Star => {
                self.advance();
                Ok(Expr::Wildcard)
            }
            TokenKind::Identifier(_)
            | TokenKind::Keyword(
                Keyword::Left
                    | Keyword::Right
                    | Keyword::Replace
                    | Keyword::Filter
                    | Keyword::Grouping,
            ) => self.parse_identifier_expr(),
            _ => Err(self.error_at_current("expected expression")),
        }
    }

    fn parse_identifier_expr(&mut self) -> Result<Expr, ParseError> {
        let mut name = vec![self.parse_expr_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            name.push(self.parse_expr_identifier()?);
        }

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let distinct = self.consume_keyword(Keyword::Distinct);
            let mut args = Vec::new();
            let mut order_by = Vec::new();
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                let fn_name = name
                    .last()
                    .map(|part| part.to_ascii_lowercase())
                    .unwrap_or_default();
                let args_start = self.idx;
                if fn_name == "position" {
                    let left = self.parse_expr_bp(6)?;
                    if self.consume_keyword(Keyword::In) {
                        let right = self.parse_expr_bp(6)?;
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after position arguments",
                        )?;
                        args = vec![left, right];
                        return Ok(Expr::FunctionCall {
                            name,
                            args,
                            distinct,
                            order_by,
                            within_group: Vec::new(),
                            filter: None,
                            over: None,
                        });
                    }
                    self.idx = args_start;
                } else if fn_name == "overlay" {
                    let input = self.parse_expr_bp(6)?;
                    if self.consume_keyword(Keyword::Placing) {
                        let replacement = self.parse_expr_bp(6)?;
                        self.expect_keyword(Keyword::From, "expected FROM in overlay")?;
                        let start = self.parse_expr_bp(6)?;
                        let count = if self.consume_keyword(Keyword::For) {
                            Some(self.parse_expr_bp(6)?)
                        } else {
                            None
                        };
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after overlay arguments",
                        )?;
                        args = vec![input, replacement, start];
                        if let Some(count) = count {
                            args.push(count);
                        }
                        return Ok(Expr::FunctionCall {
                            name,
                            args,
                            distinct,
                            order_by,
                            within_group: Vec::new(),
                            filter: None,
                            over: None,
                        });
                    }
                    self.idx = args_start;
                }

                args.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    args.push(self.parse_expr()?);
                }
                if self.consume_keyword(Keyword::Order) {
                    self.expect_keyword(
                        Keyword::By,
                        "expected BY after ORDER in function argument list",
                    )?;
                    order_by = self.parse_order_by_list()?;
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after function arguments",
                )?;
            }
            let within_group = if self.consume_keyword(Keyword::Within) {
                self.expect_keyword(Keyword::Group, "expected GROUP after WITHIN")?;
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after WITHIN GROUP",
                )?;
                self.expect_keyword(
                    Keyword::Order,
                    "expected ORDER after WITHIN GROUP (",
                )?;
                self.expect_keyword(
                    Keyword::By,
                    "expected BY after WITHIN GROUP ORDER",
                )?;
                let order_by = self.parse_order_by_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after WITHIN GROUP ORDER BY",
                )?;
                order_by
            } else {
                Vec::new()
            };
            let filter = if self.consume_keyword(Keyword::Filter) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after FILTER",
                )?;
                self.expect_keyword(Keyword::Where, "expected WHERE in FILTER clause")?;
                let predicate = self.parse_expr()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after FILTER clause",
                )?;
                Some(Box::new(predicate))
            } else {
                None
            };
            let over = if self.consume_keyword(Keyword::Over) {
                Some(Box::new(self.parse_window_spec()?))
            } else {
                None
            };
            return Ok(Expr::FunctionCall {
                name,
                args,
                distinct,
                order_by,
                within_group,
                filter,
                over,
            });
        }

        Ok(Expr::Identifier(name))
    }

    fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after OVER",
        )?;
        let mut partition_by = Vec::new();
        let mut order_by = Vec::new();
        let mut frame = None;

        if self.consume_keyword(Keyword::Partition) {
            self.expect_keyword(Keyword::By, "expected BY after PARTITION")?;
            partition_by = self.parse_expr_list()?;
        }

        if self.consume_keyword(Keyword::Order) {
            self.expect_keyword(Keyword::By, "expected BY after ORDER in window clause")?;
            order_by = self.parse_order_by_list()?;
        }

        if self.peek_keyword(Keyword::Rows) || self.peek_keyword(Keyword::Range) {
            frame = Some(self.parse_window_frame()?);
        }

        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' to close OVER clause",
        )?;

        Ok(WindowSpec {
            partition_by,
            order_by,
            frame,
        })
    }

    fn parse_window_frame(&mut self) -> Result<WindowFrame, ParseError> {
        let units = if self.consume_keyword(Keyword::Rows) {
            WindowFrameUnits::Rows
        } else {
            self.expect_keyword(
                Keyword::Range,
                "expected ROWS or RANGE in window frame clause",
            )?;
            WindowFrameUnits::Range
        };

        self.expect_keyword(
            Keyword::Between,
            "expected BETWEEN in window frame clause",
        )?;
        let start = self.parse_window_frame_bound()?;
        self.expect_keyword(Keyword::And, "expected AND in window frame clause")?;
        let end = self.parse_window_frame_bound()?;

        Ok(WindowFrame { units, start, end })
    }

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParseError> {
        if self.consume_keyword(Keyword::Unbounded) {
            if self.consume_keyword(Keyword::Preceding) {
                return Ok(WindowFrameBound::UnboundedPreceding);
            }
            if self.consume_keyword(Keyword::Following) {
                return Ok(WindowFrameBound::UnboundedFollowing);
            }
            return Err(self.error_at_current("expected PRECEDING or FOLLOWING after UNBOUNDED"));
        }

        if self.consume_keyword(Keyword::Current) {
            self.expect_window_row_keyword("expected ROW after CURRENT in frame bound")?;
            return Ok(WindowFrameBound::CurrentRow);
        }

        let offset = self.parse_expr()?;
        if self.consume_keyword(Keyword::Preceding) {
            return Ok(WindowFrameBound::OffsetPreceding(offset));
        }
        if self.consume_keyword(Keyword::Following) {
            return Ok(WindowFrameBound::OffsetFollowing(offset));
        }

        Err(self.error_at_current("expected PRECEDING or FOLLOWING in frame bound"))
    }

    fn expect_window_row_keyword(&mut self, message: &'static str) -> Result<(), ParseError> {
        match self.current_kind() {
            TokenKind::Identifier(ident) if ident.eq_ignore_ascii_case("row") => {
                self.advance();
                Ok(())
            }
            _ => Err(self.error_at_current(message)),
        }
    }

    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        let searched = self.peek_keyword(Keyword::When);
        let operand = if searched {
            None
        } else {
            Some(self.parse_expr()?)
        };

        let mut when_then = Vec::new();
        loop {
            self.expect_keyword(Keyword::When, "expected WHEN in CASE expression")?;
            let when_expr = self.parse_expr()?;
            self.expect_keyword(Keyword::Then, "expected THEN in CASE expression")?;
            let then_expr = self.parse_expr()?;
            when_then.push((when_expr, then_expr));
            if !self.peek_keyword(Keyword::When) {
                break;
            }
        }

        let else_expr = if self.consume_keyword(Keyword::Else) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };
        self.expect_keyword(Keyword::End, "expected END to close CASE expression")?;

        if let Some(operand) = operand {
            Ok(Expr::CaseSimple {
                operand: Box::new(operand),
                when_then,
                else_expr,
            })
        } else {
            Ok(Expr::CaseSearched {
                when_then,
                else_expr,
            })
        }
    }

    fn parse_expr_type_name(&mut self) -> Result<String, ParseError> {
        let base = self.parse_expr_type_word()?.to_ascii_lowercase();
        let normalized = match base.as_str() {
            "bool" | "boolean" => "boolean".to_string(),
            "int" | "integer" | "int8" | "bigint" | "int4" | "smallint" => "int8".to_string(),
            "float" | "float8" | "real" | "numeric" | "decimal" => "float8".to_string(),
            "double" => {
                if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("precision"))
                {
                    self.advance();
                }
                "float8".to_string()
            }
            "text" | "varchar" | "char" => "text".to_string(),
            "character" => {
                if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("varying"))
                {
                    self.advance();
                }
                "text".to_string()
            }
            "date" => "date".to_string(),
            "timestamp" | "timestamptz" => {
                if self.consume_keyword(Keyword::With) {
                    if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("time"))
                    {
                        self.advance();
                    }
                    if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("zone"))
                    {
                        self.advance();
                    }
                } else if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("without"))
                {
                    self.advance();
                    if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("time"))
                    {
                        self.advance();
                    }
                    if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("zone"))
                    {
                        self.advance();
                    }
                }
                "timestamp".to_string()
            }
            other => {
                return Err(
                    self.error_at_current(&format!("unsupported cast type name \"{other}\""))
                );
            }
        };

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut depth = 1usize;
            while depth > 0 {
                match self.current_kind() {
                    TokenKind::LParen => {
                        depth += 1;
                        self.advance();
                    }
                    TokenKind::RParen => {
                        depth -= 1;
                        self.advance();
                    }
                    TokenKind::Eof => {
                        return Err(self.error_at_current("unterminated cast type modifier list"));
                    }
                    _ => self.advance(),
                }
            }
        }

        Ok(normalized)
    }

    fn parse_expr_type_word(&mut self) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.clone();
                self.advance();
                Ok(out)
            }
            TokenKind::Keyword(Keyword::With) => {
                self.advance();
                Ok("with".to_string())
            }
            _ => Err(self.error_at_current("expected type name")),
        }
    }

    fn parse_expr_identifier(&mut self) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.clone();
                self.advance();
                Ok(out)
            }
            TokenKind::Keyword(Keyword::Left) => {
                self.advance();
                Ok("left".to_string())
            }
            TokenKind::Keyword(Keyword::Right) => {
                self.advance();
                Ok("right".to_string())
            }
            TokenKind::Keyword(Keyword::Replace) => {
                self.advance();
                Ok("replace".to_string())
            }
            TokenKind::Keyword(Keyword::Filter) => {
                self.advance();
                Ok("filter".to_string())
            }
            TokenKind::Keyword(Keyword::Grouping) => {
                self.advance();
                Ok("grouping".to_string())
            }
            _ => Err(self.error_at_current("expected identifier")),
        }
    }

    fn parse_in_expr(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' after IN")?;
        if self.current_starts_query() {
            let subquery = self.parse_query()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after IN subquery",
            )?;
            return Ok(Expr::InSubquery {
                expr: Box::new(lhs),
                subquery: Box::new(subquery),
                negated,
            });
        }

        let mut list = vec![self.parse_expr()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            list.push(self.parse_expr()?);
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after IN value list",
        )?;
        Ok(Expr::InList {
            expr: Box::new(lhs),
            list,
            negated,
        })
    }

    fn parse_between_expr(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
        let low = self.parse_expr_bp(6)?;
        self.expect_keyword(Keyword::And, "expected AND in BETWEEN predicate")?;
        let high = self.parse_expr_bp(6)?;
        Ok(Expr::Between {
            expr: Box::new(lhs),
            low: Box::new(low),
            high: Box::new(high),
            negated,
        })
    }

    fn parse_like_expr(
        &mut self,
        lhs: Expr,
        negated: bool,
        case_insensitive: bool,
    ) -> Result<Expr, ParseError> {
        let pattern = self.parse_expr_bp(6)?;
        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            case_insensitive,
            negated,
        })
    }

    fn parse_qualified_name(&mut self) -> Result<Vec<String>, ParseError> {
        let mut out = vec![self.parse_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            out.push(self.parse_identifier()?);
        }
        Ok(out)
    }

    fn parse_identifier(&mut self) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.clone();
                self.advance();
                Ok(out)
            }
            TokenKind::Keyword(Keyword::Filter) => {
                self.advance();
                Ok("filter".to_string())
            }
            _ => Err(self.error_at_current("expected identifier")),
        }
    }

    fn take_keyword_or_identifier(&mut self) -> Option<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.clone();
                self.advance();
                Some(out)
            }
            TokenKind::Keyword(kw) => {
                let out = format!("{:?}", kw).to_lowercase();
                self.advance();
                Some(out)
            }
            _ => None,
        }
    }

    fn take_keyword_or_identifier_upper(&mut self) -> Option<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.to_ascii_uppercase();
                self.advance();
                Some(out)
            }
            TokenKind::Keyword(kw) => {
                let out = format!("{:?}", kw).to_ascii_uppercase();
                self.advance();
                Some(out)
            }
            _ => None,
        }
    }

    fn parse_role_identifier_with_message(
        &mut self,
        message: &'static str,
    ) -> Result<String, ParseError> {
        let Some(name) = self.take_keyword_or_identifier() else {
            return Err(self.error_at_current(message));
        };
        Ok(name)
    }

    fn parse_role_list(&mut self, message: &'static str) -> Result<Vec<String>, ParseError> {
        let mut roles = Vec::new();
        roles.push(self.parse_role_identifier_with_message(message)?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            roles.push(self.parse_role_identifier_with_message(message)?);
        }
        Ok(roles)
    }

    fn parse_privilege_list(
        &mut self,
        command: &'static str,
    ) -> Result<Vec<TablePrivilegeKind>, ParseError> {
        let mut privileges = Vec::new();
        loop {
            if self.peek_keyword(Keyword::On) {
                break;
            }
            if matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
                return Err(self.error_at_current(&format!(
                    "{command} requires ON TABLE clause"
                )));
            }
            let Some(token) = self.take_keyword_or_identifier_upper() else {
                return Err(self.error_at_current("expected privilege name"));
            };
            if token == "ALL" {
                self.consume_ident("privileges");
                privileges.extend(TablePrivilegeKind::all());
            } else if let Some(privilege) = TablePrivilegeKind::from_keyword(&token) {
                privileges.push(privilege);
            } else {
                return Err(self.error_at_current(&format!("unsupported privilege {}", token)));
            }
            if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                continue;
            }
            if self.peek_keyword(Keyword::On) {
                break;
            }
        }
        if privileges.is_empty() {
            return Err(self.error_at_current("no privileges specified"));
        }
        Ok(privileges)
    }

    fn parse_role_options(&mut self, command: &'static str) -> Result<Vec<RoleOption>, ParseError> {
        let mut options = Vec::new();
        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            let token = match self.current_kind() {
                TokenKind::Identifier(value) => value.clone(),
                TokenKind::Keyword(kw) => format!("{:?}", kw).to_lowercase(),
                _ => {
                    let raw = format!("{:?}", self.current_kind());
                    return Err(self.error_at_current(&format!(
                        "unsupported {command} option {raw}"
                    )));
                }
            };
            self.advance();
            match token.as_str() {
                "superuser" => options.push(RoleOption::Superuser(true)),
                "nosuperuser" => options.push(RoleOption::Superuser(false)),
                "login" => options.push(RoleOption::Login(true)),
                "nologin" => options.push(RoleOption::Login(false)),
                "password" => match self.current_kind() {
                    TokenKind::String(value) => {
                        let password = value.clone();
                        self.advance();
                        options.push(RoleOption::Password(password));
                    }
                    _ => {
                        return Err(self.error_at_current(&format!(
                            "{command} PASSWORD requires a value"
                        )));
                    }
                },
                _ => {
                    return Err(self.error_at_current(&format!(
                        "unsupported {command} option {}",
                        token.to_ascii_uppercase()
                    )));
                }
            }
        }
        Ok(options)
    }

    fn parse_copy_option_item(&mut self, options: &mut CopyOptions) -> Result<(), ParseError> {
        let Some(token) = self.take_keyword_or_identifier_upper() else {
            return Err(self.error_at_current("unsupported COPY option"));
        };
        match token.as_str() {
            "BINARY" => options.format = Some(CopyFormat::Binary),
            "CSV" => options.format = Some(CopyFormat::Csv),
            "TEXT" => options.format = Some(CopyFormat::Text),
            "FORMAT" => {
                let format = self.parse_copy_format_value("unsupported COPY FORMAT option")?;
                options.format = Some(format);
            }
            "DELIMITER" => {
                let delimiter = self.parse_copy_string_literal("DELIMITER")?;
                let mut chars = delimiter.chars();
                let ch = chars.next().ok_or_else(|| {
                    self.error_at_current("COPY DELIMITER cannot be empty")
                })?;
                if chars.next().is_some() {
                    return Err(self.error_at_current(
                        "COPY DELIMITER must be a single character",
                    ));
                }
                options.delimiter = Some(ch.to_string());
            }
            "NULL" => {
                let null_marker = self.parse_copy_string_literal("NULL")?;
                options.null_marker = Some(null_marker);
            }
            other => {
                return Err(self.error_at_current(&format!("unsupported COPY option {}", other)));
            }
        }
        Ok(())
    }

    fn parse_copy_format_value(
        &mut self,
        message_prefix: &'static str,
    ) -> Result<CopyFormat, ParseError> {
        let Some(token) = self.take_keyword_or_identifier_upper() else {
            return Err(self.error_at_current(&format!("{message_prefix} ")));
        };
        match token.as_str() {
            "BINARY" => Ok(CopyFormat::Binary),
            "CSV" => Ok(CopyFormat::Csv),
            "TEXT" => Ok(CopyFormat::Text),
            _ => Err(self.error_at_current(&format!(
                "{message_prefix} {}",
                token
            ))),
        }
    }

    fn parse_copy_string_literal(
        &mut self,
        option_name: &'static str,
    ) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::String(value) => {
                let out = value.clone();
                self.advance();
                Ok(out)
            }
            _ => Err(self.error_at_current(&format!(
                "COPY {option_name} requires a single-quoted string"
            ))),
        }
    }

    fn current_set_op(&self) -> Option<(SetOperator, u8, u8)> {
        match self.current_kind() {
            TokenKind::Keyword(Keyword::Union) => Some((SetOperator::Union, 1, 2)),
            TokenKind::Keyword(Keyword::Except) => Some((SetOperator::Except, 1, 2)),
            TokenKind::Keyword(Keyword::Intersect) => Some((SetOperator::Intersect, 3, 4)),
            _ => None,
        }
    }

    fn current_binary_op(&self) -> Option<(BinaryOp, u8, u8)> {
        match self.current_kind() {
            TokenKind::Keyword(Keyword::Or) => Some((BinaryOp::Or, 1, 2)),
            TokenKind::Keyword(Keyword::And) => Some((BinaryOp::And, 3, 4)),
            TokenKind::Equal => Some((BinaryOp::Eq, 5, 6)),
            TokenKind::NotEquals => Some((BinaryOp::NotEq, 5, 6)),
            TokenKind::Less => Some((BinaryOp::Lt, 5, 6)),
            TokenKind::LessEquals => Some((BinaryOp::Lte, 5, 6)),
            TokenKind::Greater => Some((BinaryOp::Gt, 5, 6)),
            TokenKind::GreaterEquals => Some((BinaryOp::Gte, 5, 6)),
            TokenKind::Plus => Some((BinaryOp::Add, 7, 8)),
            TokenKind::Minus => Some((BinaryOp::Sub, 7, 8)),
            TokenKind::Star => Some((BinaryOp::Mul, 9, 10)),
            TokenKind::Slash => Some((BinaryOp::Div, 9, 10)),
            TokenKind::Percent => Some((BinaryOp::Mod, 9, 10)),
            TokenKind::Operator(op) if op == "->" => Some((BinaryOp::JsonGet, 11, 12)),
            TokenKind::Operator(op) if op == "->>" => Some((BinaryOp::JsonGetText, 11, 12)),
            TokenKind::Operator(op) if op == "#>" => Some((BinaryOp::JsonPath, 11, 12)),
            TokenKind::Operator(op) if op == "#>>" => Some((BinaryOp::JsonPathText, 11, 12)),
            TokenKind::Operator(op) if op == "||" => Some((BinaryOp::JsonConcat, 7, 8)),
            TokenKind::Operator(op) if op == "@>" => Some((BinaryOp::JsonContains, 5, 6)),
            TokenKind::Operator(op) if op == "<@" => Some((BinaryOp::JsonContainedBy, 5, 6)),
            TokenKind::Operator(op) if op == "@?" => Some((BinaryOp::JsonPathExists, 5, 6)),
            TokenKind::Operator(op) if op == "@@" => Some((BinaryOp::JsonPathMatch, 5, 6)),
            TokenKind::Operator(op) if op == "?" => Some((BinaryOp::JsonHasKey, 5, 6)),
            TokenKind::Operator(op) if op == "?|" => Some((BinaryOp::JsonHasAny, 5, 6)),
            TokenKind::Operator(op) if op == "?&" => Some((BinaryOp::JsonHasAll, 5, 6)),
            TokenKind::Operator(op) if op == "#-" => Some((BinaryOp::JsonDeletePath, 11, 12)),
            _ => None,
        }
    }

    fn expect_keyword(
        &mut self,
        keyword: Keyword,
        message: &'static str,
    ) -> Result<(), ParseError> {
        if self.consume_keyword(keyword) {
            return Ok(());
        }
        Err(self.error_at_current(message))
    }

    fn expect_token<F>(&mut self, predicate: F, message: &'static str) -> Result<(), ParseError>
    where
        F: Fn(&TokenKind) -> bool,
    {
        if self.consume_if(predicate) {
            return Ok(());
        }
        Err(self.error_at_current(message))
    }

    fn expect_eof(&self) -> Result<(), ParseError> {
        if matches!(self.current_kind(), TokenKind::Eof) {
            return Ok(());
        }
        Err(self.error_at_current("unexpected token after end of statement"))
    }

    fn consume_keyword(&mut self, keyword: Keyword) -> bool {
        self.consume_if(|k| matches!(k, TokenKind::Keyword(kv) if *kv == keyword))
    }

    fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current_kind(), TokenKind::Keyword(kv) if *kv == keyword)
    }

    fn consume_ident(&mut self, value: &str) -> bool {
        self.consume_if(|k| matches!(k, TokenKind::Identifier(ident) if ident.eq_ignore_ascii_case(value)))
    }

    fn peek_ident(&self, value: &str) -> bool {
        matches!(self.current_kind(), TokenKind::Identifier(ident) if ident.eq_ignore_ascii_case(value))
    }

    fn consume_if<F>(&mut self, predicate: F) -> bool
    where
        F: Fn(&TokenKind) -> bool,
    {
        if predicate(self.current_kind()) {
            self.advance();
            return true;
        }
        false
    }

    fn current_kind(&self) -> &TokenKind {
        &self.tokens[self.idx].kind
    }

    fn peek_nth_kind(&self, n: usize) -> Option<&TokenKind> {
        self.tokens.get(self.idx + n).map(|token| &token.kind)
    }

    fn peek_nth_keyword(&self, n: usize, keyword: Keyword) -> bool {
        matches!(self.peek_nth_kind(n), Some(TokenKind::Keyword(kv)) if *kv == keyword)
    }

    fn current_starts_query(&self) -> bool {
        matches!(
            self.current_kind(),
            TokenKind::Keyword(Keyword::Select | Keyword::With)
        )
    }

    fn advance(&mut self) {
        if self.idx + 1 < self.tokens.len() {
            self.idx += 1;
        }
    }

    fn error_at_current(&self, message: &str) -> ParseError {
        ParseError {
            message: message.to_string(),
            position: self.tokens[self.idx].start,
        }
    }

    fn parse_explain_statement(&mut self) -> Result<Statement, ParseError> {
        let mut analyze = false;
        let mut verbose = false;
        // Check for EXPLAIN (options) or EXPLAIN ANALYZE VERBOSE
        if self.consume_keyword(Keyword::Analyze) {
            analyze = true;
        }
        if self.consume_keyword(Keyword::Verbose) {
            verbose = true;
        }
        let inner = self.parse_top_level_statement()?;
        Ok(Statement::Explain(ExplainStatement {
            analyze,
            verbose,
            statement: Box::new(inner),
        }))
    }

    fn parse_set_statement(&mut self) -> Result<Statement, ParseError> {
        let is_local = self.consume_keyword(Keyword::Local);
        let name = self.parse_identifier()?;
        // Accept = or TO
        if !self.consume_if(|k| matches!(k, TokenKind::Equal))
            && !self.consume_keyword(Keyword::To)
        {
            return Err(self.error_at_current("expected = or TO after SET variable name"));
        }
        // Collect the rest as value
        let mut value_parts = Vec::new();
        while !matches!(self.current_kind(), TokenKind::Eof) && !matches!(self.current_kind(), TokenKind::Semicolon) {
            let token = &self.tokens[self.idx];
            match &token.kind {
                TokenKind::Keyword(kw) => value_parts.push(format!("{:?}", kw).to_lowercase()),
                TokenKind::Identifier(s) => value_parts.push(s.clone()),
                TokenKind::String(s) => value_parts.push(s.clone()),
                TokenKind::Integer(i) => value_parts.push(i.to_string()),
                TokenKind::Float(s) => value_parts.push(s.clone()),
                TokenKind::Comma => value_parts.push(",".to_string()),
                _ => value_parts.push(format!("{:?}", token.kind)),
            }
            self.advance();
        }
        Ok(Statement::Set(SetStatement {
            name,
            value: value_parts.join(" "),
            is_local,
        }))
    }

    fn parse_show_statement(&mut self) -> Result<Statement, ParseError> {
        let name = if self.consume_keyword(Keyword::All) {
            "all".to_string()
        } else {
            self.parse_identifier()?
        };
        Ok(Statement::Show(ShowStatement { name }))
    }

    fn parse_reset_statement(&mut self) -> Result<Statement, ParseError> {
        let name = if self.consume_keyword(Keyword::All) {
            "all".to_string()
        } else {
            self.parse_identifier()?
        };
        Ok(Statement::Set(SetStatement {
            name,
            value: "DEFAULT".to_string(),
            is_local: false,
        }))
    }

    fn parse_create_function(&mut self, or_replace: bool) -> Result<Statement, ParseError> {
        let name = self.parse_qualified_name()?;
        // Parse parameter list
        self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' after function name")?;
        let mut params = Vec::new();
        if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
            loop {
                // Try name TYPE or just TYPE
                let first_ident = self.parse_identifier()?;
                let (param_name, data_type) = if let Ok(dt) = self.try_parse_type_name(&first_ident) {
                    (None, dt)
                } else {
                    // first_ident is param name, next is type
                    let type_ident = self.parse_identifier()?;
                    let dt = self.try_parse_type_name(&type_ident).map_err(|_| {
                        self.error_at_current(&format!("unknown type: {}", type_ident))
                    })?;
                    (Some(first_ident), dt)
                };
                // Check for DEFAULT
                if self.consume_keyword(Keyword::Default) {
                    // Skip the default expression (simple: just consume until , or ))
                    while !matches!(self.current_kind(), TokenKind::Comma | TokenKind::RParen | TokenKind::Eof) {
                        self.advance();
                    }
                }
                params.push(FunctionParam { name: param_name, data_type });
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    self.expect_token(|k| matches!(k, TokenKind::RParen), "expected ')' or ',' in parameter list")?;
                    break;
                }
            }
        }
        // Parse RETURNS
        let return_type = if self.consume_keyword(Keyword::Returns) {
            if self.consume_keyword(Keyword::Table) {
                // RETURNS TABLE(col type, ...)
                self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' after TABLE")?;
                let mut cols = Vec::new();
                loop {
                    let col_name = self.parse_identifier()?;
                    let type_ident = self.parse_identifier()?;
                    let dt = self.try_parse_type_name(&type_ident).map_err(|_| {
                        self.error_at_current(&format!("unknown type: {}", type_ident))
                    })?;
                    cols.push(ColumnDefinition {
                        name: col_name,
                        data_type: dt,
                        nullable: true,
                        identity: false,
                        primary_key: false,
                        unique: false,
                        references: None,
                        check: None,
                        default: None,
                    });
                    if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        self.expect_token(|k| matches!(k, TokenKind::RParen), "expected ')'")?;
                        break;
                    }
                }
                Some(FunctionReturnType::Table(cols))
            } else {
                let type_ident = self.parse_identifier()?;
                let dt = self.try_parse_type_name(&type_ident).map_err(|_| {
                    self.error_at_current(&format!("unknown return type: {}", type_ident))
                })?;
                Some(FunctionReturnType::Type(dt))
            }
        } else {
            None
        };
        // Parse AS $$ body $$
        self.expect_keyword(Keyword::As, "expected AS before function body")?;
        let body = match &self.tokens[self.idx].kind {
            TokenKind::String(s) => {
                let b = s.clone();
                self.advance();
                b
            }
            _ => return Err(self.error_at_current("expected dollar-quoted or string function body")),
        };
        // Optional LANGUAGE
        let language = if self.consume_keyword(Keyword::Language) {
            self.parse_identifier()?
        } else {
            "sql".to_string()
        };
        Ok(Statement::CreateFunction(CreateFunctionStatement {
            name,
            params,
            return_type,
            body,
            language,
            or_replace,
        }))
    }

    fn try_parse_type_name(&self, ident: &str) -> Result<TypeName, ()> {
        match ident.to_ascii_lowercase().as_str() {
            "bool" | "boolean" => Ok(TypeName::Bool),
            "int2" | "smallint" => Ok(TypeName::Int2),
            "int4" | "integer" | "int" => Ok(TypeName::Int4),
            "int8" | "bigint" => Ok(TypeName::Int8),
            "float4" | "real" => Ok(TypeName::Float4),
            "float8" | "double" => Ok(TypeName::Float8),
            "text" => Ok(TypeName::Text),
            "varchar" => Ok(TypeName::Varchar),
            "char" => Ok(TypeName::Char),
            "bytea" => Ok(TypeName::Bytea),
            "uuid" => Ok(TypeName::Uuid),
            "json" => Ok(TypeName::Json),
            "jsonb" => Ok(TypeName::Jsonb),
            "date" => Ok(TypeName::Date),
            "timestamp" => Ok(TypeName::Timestamp),
            "timestamptz" => Ok(TypeName::TimestampTz),
            "interval" => Ok(TypeName::Interval),
            "serial" => Ok(TypeName::Serial),
            "bigserial" => Ok(TypeName::BigSerial),
            "numeric" | "decimal" => Ok(TypeName::Numeric),
            _ => Err(()),
        }
    }

    fn parse_discard_statement(&mut self) -> Result<Statement, ParseError> {
        let target = if self.consume_keyword(Keyword::All) {
            "ALL".to_string()
        } else {
            self.parse_identifier()?.to_uppercase()
        };
        Ok(Statement::Discard(DiscardStatement { target }))
    }

    fn parse_do_statement(&mut self) -> Result<Statement, ParseError> {
        // DO 'body' [LANGUAGE lang]
        let body = match &self.tokens[self.idx].kind {
            TokenKind::String(s) => {
                let b = s.clone();
                self.advance();
                b
            }
            _ => return Err(self.error_at_current("expected string body after DO")),
        };
        // Optional LANGUAGE clause
        let language = if !matches!(self.current_kind(), TokenKind::Eof) && !matches!(self.current_kind(), TokenKind::Semicolon) {
            if let Some(token) = self.tokens.get(self.idx) {
                if let TokenKind::Identifier(id) = &token.kind {
                    if id.eq_ignore_ascii_case("language") {
                        self.advance();
                        self.parse_identifier()?
                    } else {
                        "plpgsql".to_string()
                    }
                } else {
                    "plpgsql".to_string()
                }
            } else {
                "plpgsql".to_string()
            }
        } else {
            "plpgsql".to_string()
        };
        Ok(Statement::Do(DoStatement { body, language }))
    }

    fn parse_listen_statement(&mut self) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        Ok(Statement::Listen(ListenStatement { channel }))
    }

    fn parse_notify_statement(&mut self) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        let payload = if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            match &self.tokens[self.idx].kind {
                TokenKind::String(s) => {
                    let p = s.clone();
                    self.advance();
                    Some(p)
                }
                _ => return Err(self.error_at_current("expected string payload after NOTIFY channel,")),
            }
        } else {
            None
        };
        Ok(Statement::Notify(NotifyStatement { channel, payload }))
    }

    fn parse_unlisten_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::Star)) {
            return Ok(Statement::Unlisten(UnlistenStatement { channel: None }));
        }
        let channel = self.parse_identifier()?;
        Ok(Statement::Unlisten(UnlistenStatement { channel: Some(channel) }))
    }

    fn parse_transaction_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_keyword(Keyword::Begin) || self.consume_keyword(Keyword::Start) {
            self.consume_keyword(Keyword::Transaction);
            return Ok(Statement::Transaction(TransactionStatement::Begin));
        }
        if self.consume_keyword(Keyword::Commit) || self.consume_keyword(Keyword::End) {
            self.consume_keyword(Keyword::Transaction);
            return Ok(Statement::Transaction(TransactionStatement::Commit));
        }
        if self.consume_keyword(Keyword::Rollback) {
            if self.consume_keyword(Keyword::To) {
                self.consume_keyword(Keyword::Savepoint);
                let name = self.parse_identifier()?;
                return Ok(Statement::Transaction(
                    TransactionStatement::RollbackToSavepoint(name),
                ));
            }
            return Ok(Statement::Transaction(TransactionStatement::Rollback));
        }
        if self.consume_keyword(Keyword::Savepoint) {
            let name = self.parse_identifier()?;
            return Ok(Statement::Transaction(TransactionStatement::Savepoint(name)));
        }
        if self.consume_keyword(Keyword::Release) {
            self.consume_keyword(Keyword::Savepoint);
            let name = self.parse_identifier()?;
            return Ok(Statement::Transaction(
                TransactionStatement::ReleaseSavepoint(name),
            ));
        }
        Err(self.error_at_current("expected transaction statement"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn as_select(query: &Query) -> &SelectStatement {
        match &query.body {
            QueryExpr::Select(select) => select,
            other => panic!("expected simple SELECT query body, got {other:?}"),
        }
    }

    #[test]
    fn parses_simple_select() {
        let stmt = parse_statement("SELECT 1;").expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.targets.len(), 1);
        assert_eq!(select.targets[0].expr, Expr::Integer(1));
    }

    #[test]
    fn parses_with_clause_query() {
        let stmt = parse_statement("WITH t AS (SELECT 1 AS id) SELECT id FROM t")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let with = query.with.as_ref().expect("with clause should exist");
        assert!(!with.recursive);
        assert_eq!(with.ctes.len(), 1);
        assert_eq!(with.ctes[0].name, "t");
    }

    #[test]
    fn parses_with_recursive_clause_query() {
        let stmt = parse_statement(
            "WITH RECURSIVE t AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM t WHERE id < 3) SELECT id FROM t",
        )
        .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let with = query.with.as_ref().expect("with clause should exist");
        assert!(with.recursive);
        assert_eq!(with.ctes.len(), 1);
    }

    #[test]
    fn parses_select_with_clauses() {
        let stmt = parse_statement(
            "SELECT DISTINCT foo AS bar, count(*) \
             FROM public.users u \
             WHERE id >= $1 AND active = true \
             GROUP BY foo \
             HAVING count(*) > 1 \
             ORDER BY foo DESC \
             LIMIT 10 OFFSET 20;",
        )
        .expect("parse should succeed");

        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.quantifier, Some(SelectQuantifier::Distinct));
        assert_eq!(select.targets.len(), 2);
        assert_eq!(select.from.len(), 1);
        assert!(select.where_clause.is_some());
        assert_eq!(select.group_by.len(), 1);
        assert!(select.having.is_some());
        assert_eq!(query.order_by.len(), 1);
        assert!(query.limit.is_some());
        assert!(query.offset.is_some());
    }

    #[test]
    fn parses_joins_in_from_clause() {
        let stmt =
            parse_statement("SELECT u.id FROM users u INNER JOIN accounts a ON u.id = a.user_id")
                .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.from.len(), 1);
        match &select.from[0] {
            TableExpression::Join(join) => {
                assert_eq!(join.kind, JoinType::Inner);
                assert!(matches!(join.condition, Some(JoinCondition::On(_))));
            }
            other => panic!("expected join table expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_subquery_in_from_clause() {
        let stmt =
            parse_statement("SELECT sq.id FROM (SELECT id FROM users WHERE active = true) sq")
                .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.from.len(), 1);
        match &select.from[0] {
            TableExpression::Subquery(sub) => {
                assert_eq!(sub.alias.as_deref(), Some("sq"));
                match &sub.query.body {
                    QueryExpr::Select(inner) => assert_eq!(inner.targets.len(), 1),
                    other => panic!("expected inner SELECT, got {other:?}"),
                }
            }
            other => panic!("expected subquery table expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_lateral_subquery_in_from_clause() {
        let stmt = parse_statement(
            "SELECT t.id FROM test_table t, LATERAL (SELECT t.id AS id) l",
        )
        .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.from.len(), 2);
        match &select.from[1] {
            TableExpression::Subquery(sub) => {
                assert_eq!(sub.alias.as_deref(), Some("l"));
                assert!(sub.lateral);
            }
            other => panic!("expected lateral subquery table expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_function_call_in_from_clause() {
        let stmt = parse_statement("SELECT elem FROM json_array_elements('[1,2,3]') AS elem")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.from.len(), 1);
        match &select.from[0] {
            TableExpression::Function(function) => {
                assert_eq!(function.name, vec!["json_array_elements".to_string()]);
                assert_eq!(function.args.len(), 1);
                assert_eq!(function.alias.as_deref(), Some("elem"));
                assert!(function.column_aliases.is_empty());
                assert!(function.column_alias_types.is_empty());
            }
            other => panic!("expected function table expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_function_call_with_column_aliases_in_from_clause() {
        let stmt = parse_statement("SELECT x FROM json_array_elements('[1,2,3]') AS t(x)")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.from.len(), 1);
        match &select.from[0] {
            TableExpression::Function(function) => {
                assert_eq!(function.alias.as_deref(), Some("t"));
                assert_eq!(function.column_aliases, vec!["x".to_string()]);
                assert_eq!(function.column_alias_types, vec![None]);
            }
            other => panic!("expected function table expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_exists_predicate_subquery() {
        let stmt =
            parse_statement("SELECT 1 WHERE EXISTS (SELECT 1 FROM users)").expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("where clause should exist");
        assert!(matches!(where_clause, Expr::Exists(_)));
    }

    #[test]
    fn parses_array_constructors() {
        let stmt = parse_statement("SELECT ARRAY[1, 2, 3]").expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.targets.len(), 1);
        assert!(matches!(select.targets[0].expr, Expr::ArrayConstructor(_)));

        let stmt = parse_statement("SELECT ARRAY(SELECT id FROM users)")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.targets.len(), 1);
        assert!(matches!(select.targets[0].expr, Expr::ArraySubquery(_)));
    }

    #[test]
    fn parses_in_subquery_predicate() {
        let stmt =
            parse_statement("SELECT 1 WHERE id NOT IN (SELECT id FROM users)").expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("where clause should exist");
        match where_clause {
            Expr::InSubquery { negated, .. } => assert!(*negated),
            other => panic!("expected IN subquery predicate, got {other:?}"),
        }
    }

    #[test]
    fn parses_scalar_subquery_expression() {
        let stmt = parse_statement("SELECT (SELECT 42)").expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert!(matches!(select.targets[0].expr, Expr::ScalarSubquery(_)));
    }

    #[test]
    fn parses_is_null_predicates() {
        let stmt = parse_statement("SELECT 1 WHERE a IS NULL OR b IS NOT NULL").expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("where clause should exist");
        match where_clause {
            Expr::Binary { left, op, right } => {
                assert_eq!(*op, BinaryOp::Or);
                assert!(matches!(left.as_ref(), Expr::IsNull { negated: false, .. }));
                assert!(matches!(right.as_ref(), Expr::IsNull { negated: true, .. }));
            }
            other => panic!("expected OR predicate, got {other:?}"),
        }
    }

    #[test]
    fn parses_between_and_like_predicates() {
        let stmt = parse_statement(
            "SELECT 1 WHERE score BETWEEN 10 AND 20 AND name NOT LIKE 'a%' AND email ILIKE '%@x.com'",
        )
        .expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("where clause should exist");
        let Expr::Binary { left, op, right } = where_clause else {
            panic!("expected AND tree");
        };
        assert_eq!(*op, BinaryOp::And);
        assert!(matches!(
            right.as_ref(),
            Expr::Like {
                case_insensitive: true,
                negated: false,
                ..
            }
        ));
        let Expr::Binary {
            left: inner_left,
            op: inner_op,
            right: inner_right,
        } = left.as_ref()
        else {
            panic!("expected inner AND");
        };
        assert_eq!(*inner_op, BinaryOp::And);
        assert!(matches!(
            inner_left.as_ref(),
            Expr::Between { negated: false, .. }
        ));
        assert!(matches!(
            inner_right.as_ref(),
            Expr::Like {
                case_insensitive: false,
                negated: true,
                ..
            }
        ));
    }

    #[test]
    fn parses_is_distinct_from_predicates() {
        let stmt =
            parse_statement("SELECT 1 WHERE a IS DISTINCT FROM b OR a IS NOT DISTINCT FROM c")
                .expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let where_clause = select
            .where_clause
            .as_ref()
            .expect("where clause should exist");
        let Expr::Binary { left, op, right } = where_clause else {
            panic!("expected OR expression");
        };
        assert_eq!(*op, BinaryOp::Or);
        assert!(matches!(
            left.as_ref(),
            Expr::IsDistinctFrom { negated: false, .. }
        ));
        assert!(matches!(
            right.as_ref(),
            Expr::IsDistinctFrom { negated: true, .. }
        ));
    }

    #[test]
    fn parses_simple_case_expression() {
        let stmt = parse_statement(
            "SELECT CASE level WHEN 1 THEN 'low' WHEN 2 THEN 'mid' ELSE 'high' END",
        )
        .expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } = &select.targets[0].expr
        else {
            panic!("expected simple CASE expression");
        };
        assert!(
            matches!(operand.as_ref(), Expr::Identifier(parts) if parts == &vec!["level".to_string()])
        );
        assert_eq!(when_then.len(), 2);
        assert!(matches!(when_then[0].0, Expr::Integer(1)));
        assert!(matches!(when_then[0].1, Expr::String(ref value) if value == "low"));
        assert!(matches!(
            else_expr.as_deref(),
            Some(Expr::String(value)) if value == "high"
        ));
    }

    #[test]
    fn parses_searched_and_nested_case_expression() {
        let stmt = parse_statement(
            "SELECT CASE WHEN score >= 90 THEN CASE WHEN bonus THEN 'A+' ELSE 'A' END WHEN score >= 80 THEN 'B' END",
        )
        .expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        let Expr::CaseSearched {
            when_then,
            else_expr,
        } = &select.targets[0].expr
        else {
            panic!("expected searched CASE expression");
        };
        assert_eq!(when_then.len(), 2);
        assert!(matches!(
            when_then[0].1,
            Expr::CaseSearched {
                when_then: _,
                else_expr: _
            }
        ));
        assert!(else_expr.is_none());
    }

    #[test]
    fn parses_cast_and_typecast_expressions() {
        let stmt = parse_statement(
            "SELECT CAST('1' AS int8), '2024-02-29'::date, CAST('2024-02-29 10:30:40' AS timestamp)",
        )
        .expect("parse ok");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.targets.len(), 3);
        assert!(matches!(
            select.targets[0].expr,
            Expr::Cast { ref type_name, .. } if type_name == "int8"
        ));
        assert!(matches!(
            select.targets[1].expr,
            Expr::Cast { ref type_name, .. } if type_name == "date"
        ));
        assert!(matches!(
            select.targets[2].expr,
            Expr::Cast { ref type_name, .. } if type_name == "timestamp"
        ));
    }

    #[test]
    fn parses_set_operations_with_precedence() {
        let stmt = parse_statement("SELECT 1 UNION SELECT 2 INTERSECT SELECT 3")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        match &query.body {
            QueryExpr::SetOperation { op, right, .. } => {
                assert_eq!(*op, SetOperator::Union);
                match right.as_ref() {
                    QueryExpr::SetOperation { op, .. } => assert_eq!(*op, SetOperator::Intersect),
                    other => panic!("expected INTERSECT on right side, got {other:?}"),
                }
            }
            other => panic!("expected set operation body, got {other:?}"),
        }
    }

    #[test]
    fn parses_set_operation_with_query_modifiers() {
        let stmt = parse_statement("SELECT 1 UNION SELECT 2 ORDER BY 1 LIMIT 5 OFFSET 2")
            .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        assert_eq!(query.order_by.len(), 1);
        assert!(query.limit.is_some());
        assert!(query.offset.is_some());
    }

    #[test]
    fn expression_precedence_matches_sql_expectation() {
        let stmt = parse_statement("SELECT 1 + 2 * 3 = 7 OR false;").expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        match &select.targets[0].expr {
            Expr::Binary { op, .. } => assert_eq!(*op, BinaryOp::Or),
            other => panic!("expected OR expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_keyword_named_function_calls_in_expressions() {
        let stmt =
            parse_statement("SELECT left('abc', 2), right('abc', 1), replace('abc', 'a', 'x')")
                .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let select = as_select(&query);
        assert_eq!(select.targets.len(), 3);
        match &select.targets[0].expr {
            Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["left".to_string()]),
            other => panic!("expected function call, got {other:?}"),
        }
        match &select.targets[1].expr {
            Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["right".to_string()]),
            other => panic!("expected function call, got {other:?}"),
        }
        match &select.targets[2].expr {
            Expr::FunctionCall { name, .. } => assert_eq!(name, &vec!["replace".to_string()]),
            other => panic!("expected function call, got {other:?}"),
        }
    }

    #[test]
    fn parse_fails_on_missing_target() {
        let err = parse_statement("SELECT FROM t").expect_err("parse should fail");
        assert!(err.message.contains("expected expression"));
    }

    #[test]
    fn parses_create_table_statement() {
        let stmt = parse_statement(
            "CREATE TABLE public.users (id int8 NOT NULL, name text, active boolean)",
        )
        .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert_eq!(create.name, vec!["public".to_string(), "users".to_string()]);
        assert_eq!(create.columns.len(), 3);
        assert_eq!(create.columns[0].name, "id");
        assert_eq!(create.columns[0].data_type, TypeName::Int8);
        assert!(!create.columns[0].nullable);
        assert!(!create.columns[0].identity);
        assert!(!create.columns[0].primary_key);
        assert!(!create.columns[0].unique);
        assert!(create.columns[0].references.is_none());
        assert!(create.columns[0].check.is_none());
        assert!(create.columns[0].default.is_none());
        assert!(create.constraints.is_empty());
    }

    #[test]
    fn parses_create_table_with_date_and_timestamp_types() {
        let stmt = parse_statement("CREATE TABLE events (event_day date, created_at timestamp)")
            .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert_eq!(create.columns.len(), 2);
        assert_eq!(create.columns[0].data_type, TypeName::Date);
        assert_eq!(create.columns[1].data_type, TypeName::Timestamp);
    }

    #[test]
    fn parses_insert_values_statement() {
        let stmt = parse_statement("INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b')")
            .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };

        assert_eq!(insert.table_name, vec!["users".to_string()]);
        assert!(insert.table_alias.is_none());
        assert_eq!(insert.columns, vec!["id".to_string(), "name".to_string()]);
        let InsertSource::Values(values) = insert.source else {
            panic!("expected VALUES source");
        };
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].len(), 2);
        assert!(insert.on_conflict.is_none());
        assert!(insert.returning.is_empty());
    }

    #[test]
    fn parses_insert_with_returning() {
        let stmt =
            parse_statement("INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name")
                .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        assert!(insert.table_alias.is_none());
        assert_eq!(insert.returning.len(), 2);
        assert!(insert.on_conflict.is_none());
    }

    #[test]
    fn parses_insert_with_on_conflict_do_nothing() {
        let stmt = parse_statement(
            "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id",
        )
        .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        assert!(matches!(
            insert.on_conflict,
            Some(OnConflictClause::DoNothing {
                conflict_target: Some(ConflictTarget::Columns(_))
            })
        ));
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parses_insert_with_on_conflict_do_update() {
        let stmt = parse_statement(
            "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = 1 RETURNING id",
        )
        .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        match insert.on_conflict {
            Some(OnConflictClause::DoUpdate {
                conflict_target,
                assignments,
                where_clause,
            }) => {
                assert_eq!(
                    conflict_target,
                    Some(ConflictTarget::Columns(vec!["id".to_string()]))
                );
                assert_eq!(assignments.len(), 1);
                assert!(where_clause.is_some());
            }
            other => panic!("expected ON CONFLICT DO UPDATE clause, got {other:?}"),
        }
        assert_eq!(insert.returning.len(), 1);
    }

    #[test]
    fn parses_insert_with_on_conflict_on_constraint() {
        let stmt = parse_statement(
            "INSERT INTO users AS u (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE u.id = 1 RETURNING id",
        )
        .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        assert_eq!(insert.table_alias.as_deref(), Some("u"));
        match insert.on_conflict {
            Some(OnConflictClause::DoUpdate {
                conflict_target,
                assignments,
                where_clause,
            }) => {
                assert_eq!(
                    conflict_target,
                    Some(ConflictTarget::Constraint("users_pkey".to_string()))
                );
                assert_eq!(assignments.len(), 1);
                assert!(where_clause.is_some());
            }
            other => panic!("expected ON CONFLICT DO UPDATE clause, got {other:?}"),
        }
    }

    #[test]
    fn parses_insert_select_source() {
        let stmt = parse_statement("INSERT INTO users (id, name) SELECT id, name FROM staging")
            .expect("parse should succeed");
        let Statement::Insert(insert) = stmt else {
            panic!("expected insert statement");
        };
        match insert.source {
            InsertSource::Query(query) => match query.body {
                QueryExpr::Select(_) => {}
                other => panic!("expected select query source, got {other:?}"),
            },
            other => panic!("expected query source, got {other:?}"),
        }
    }

    #[test]
    fn parses_column_level_constraints() {
        let stmt = parse_statement(
            "CREATE TABLE child (id int8 PRIMARY KEY, email text UNIQUE, parent_id int8 REFERENCES parent(id) ON DELETE SET NULL, score int8 CHECK (score >= 0))",
        )
        .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert!(create.columns[0].primary_key);
        assert!(create.columns[0].unique);
        assert!(!create.columns[0].nullable);
        assert!(!create.columns[0].identity);
        assert!(create.columns[1].unique);
        let references = create.columns[2]
            .references
            .as_ref()
            .expect("references should parse");
        assert_eq!(references.table_name, vec!["parent".to_string()]);
        assert_eq!(references.column_name.as_deref(), Some("id"));
        assert_eq!(references.on_delete, ForeignKeyAction::SetNull);
        assert_eq!(references.on_update, ForeignKeyAction::Restrict);
        assert!(create.columns[3].check.is_some());
        assert!(create.columns[3].default.is_none());
        assert!(create.constraints.is_empty());
    }

    #[test]
    fn parses_column_default_expression() {
        let stmt = parse_statement(
            "CREATE TABLE t (id int8 PRIMARY KEY, score int8 DEFAULT 7, tag text DEFAULT 'x')",
        )
        .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };
        assert!(create.columns[1].default.is_some());
        assert!(create.columns[2].default.is_some());
    }

    #[test]
    fn parses_identity_column_definition() {
        let stmt =
            parse_statement("CREATE TABLE t (id int8 GENERATED BY DEFAULT AS IDENTITY, name text)")
                .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };
        assert!(create.columns[0].identity);
        assert!(!create.columns[0].nullable);
    }

    #[test]
    fn parses_table_level_key_constraints() {
        let stmt = parse_statement(
            "CREATE TABLE t (a int8, b int8, c text, PRIMARY KEY (a, b), CONSTRAINT uq_c UNIQUE (c))",
        )
        .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert_eq!(create.columns.len(), 3);
        assert_eq!(create.constraints.len(), 2);
        match &create.constraints[0] {
            TableConstraint::PrimaryKey { name, columns } => {
                assert!(name.is_none());
                assert_eq!(columns, &vec!["a".to_string(), "b".to_string()])
            }
            other => panic!("expected primary key constraint, got {other:?}"),
        }
        match &create.constraints[1] {
            TableConstraint::Unique { name, columns } => {
                assert_eq!(name.as_deref(), Some("uq_c"));
                assert_eq!(columns, &vec!["c".to_string()]);
            }
            other => panic!("expected unique constraint, got {other:?}"),
        }
    }

    #[test]
    fn parses_table_level_composite_foreign_key_with_actions() {
        let stmt = parse_statement(
            "CREATE TABLE child (a int8, b int8, CONSTRAINT fk_ab FOREIGN KEY (a, b) REFERENCES parent (x, y) ON DELETE CASCADE ON UPDATE SET NULL)",
        )
        .expect("parse should succeed");
        let Statement::CreateTable(create) = stmt else {
            panic!("expected create table statement");
        };

        assert_eq!(create.constraints.len(), 1);
        match &create.constraints[0] {
            TableConstraint::ForeignKey {
                name,
                columns,
                referenced_table,
                referenced_columns,
                on_delete,
                on_update,
            } => {
                assert_eq!(name.as_deref(), Some("fk_ab"));
                assert_eq!(columns, &vec!["a".to_string(), "b".to_string()]);
                assert_eq!(referenced_table, &vec!["parent".to_string()]);
                assert_eq!(referenced_columns, &vec!["x".to_string(), "y".to_string()]);
                assert_eq!(*on_delete, ForeignKeyAction::Cascade);
                assert_eq!(*on_update, ForeignKeyAction::SetNull);
            }
            other => panic!("expected foreign key constraint, got {other:?}"),
        }
    }

    #[test]
    fn parses_update_statement() {
        let stmt = parse_statement(
            "UPDATE users SET name = 'z', active = true FROM teams t WHERE users.id = t.id",
        )
        .expect("parse should succeed");
        let Statement::Update(update) = stmt else {
            panic!("expected update statement");
        };

        assert_eq!(update.table_name, vec!["users".to_string()]);
        assert_eq!(update.assignments.len(), 2);
        assert_eq!(update.from.len(), 1);
        assert!(update.where_clause.is_some());
        assert!(update.returning.is_empty());
    }

    #[test]
    fn parses_update_with_returning() {
        let stmt = parse_statement("UPDATE users SET name = 'z' WHERE id = 1 RETURNING *")
            .expect("parse should succeed");
        let Statement::Update(update) = stmt else {
            panic!("expected update statement");
        };
        assert!(update.from.is_empty());
        assert_eq!(update.returning.len(), 1);
    }

    #[test]
    fn parses_delete_statement() {
        let stmt = parse_statement("DELETE FROM public.users USING teams WHERE active = false")
            .expect("parse should succeed");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete statement");
        };

        assert_eq!(
            delete.table_name,
            vec!["public".to_string(), "users".to_string()]
        );
        assert_eq!(delete.using.len(), 1);
        assert!(delete.where_clause.is_some());
        assert!(delete.returning.is_empty());
    }

    #[test]
    fn parses_delete_with_returning() {
        let stmt = parse_statement("DELETE FROM users WHERE active = false RETURNING id")
            .expect("parse should succeed");
        let Statement::Delete(delete) = stmt else {
            panic!("expected delete statement");
        };
        assert!(delete.using.is_empty());
        assert_eq!(delete.returning.len(), 1);
    }

    #[test]
    fn parses_merge_statement() {
        let stmt = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)",
        )
        .expect("parse should succeed");
        let Statement::Merge(merge) = stmt else {
            panic!("expected merge statement");
        };
        assert_eq!(merge.target_table, vec!["users".to_string()]);
        assert_eq!(merge.target_alias.as_deref(), Some("u"));
        assert_eq!(merge.when_clauses.len(), 2);
        assert!(merge.returning.is_empty());
    }

    #[test]
    fn parses_merge_with_matched_do_nothing_clause() {
        let stmt = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN MATCHED AND s.skip = true THEN DO NOTHING \
             WHEN NOT MATCHED THEN INSERT (id) VALUES (s.id)",
        )
        .expect("parse should succeed");
        let Statement::Merge(merge) = stmt else {
            panic!("expected merge statement");
        };
        assert_eq!(merge.when_clauses.len(), 2);
        assert!(matches!(
            merge.when_clauses[0],
            MergeWhenClause::MatchedDoNothing { .. }
        ));
    }

    #[test]
    fn parses_merge_with_not_matched_by_source_clauses() {
        let stmt = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN NOT MATCHED BY SOURCE AND u.active = false THEN DELETE \
             WHEN NOT MATCHED BY SOURCE THEN UPDATE SET active = false",
        )
        .expect("parse should succeed");
        let Statement::Merge(merge) = stmt else {
            panic!("expected merge statement");
        };
        assert_eq!(merge.when_clauses.len(), 2);
        assert!(matches!(
            merge.when_clauses[0],
            MergeWhenClause::NotMatchedBySourceDelete { .. }
        ));
        assert!(matches!(
            merge.when_clauses[1],
            MergeWhenClause::NotMatchedBySourceUpdate { .. }
        ));
    }

    #[test]
    fn parses_merge_with_not_matched_by_target_clause() {
        let stmt = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN NOT MATCHED BY TARGET THEN INSERT (id, name) VALUES (s.id, s.name)",
        )
        .expect("parse should succeed");
        let Statement::Merge(merge) = stmt else {
            panic!("expected merge statement");
        };
        assert_eq!(merge.when_clauses.len(), 1);
        assert!(matches!(
            merge.when_clauses[0],
            MergeWhenClause::NotMatchedInsert { .. }
        ));
    }

    #[test]
    fn parses_merge_with_returning() {
        let stmt = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             RETURNING u.id, u.name",
        )
        .expect("parse should succeed");
        let Statement::Merge(merge) = stmt else {
            panic!("expected merge statement");
        };
        assert_eq!(merge.returning.len(), 2);
    }

    #[test]
    fn rejects_unreachable_merge_when_clause_after_unconditional() {
        let err = parse_statement(
            "MERGE INTO users u USING staging s ON u.id = s.id \
             WHEN MATCHED THEN UPDATE SET name = s.name \
             WHEN MATCHED AND s.id > 0 THEN DELETE",
        )
        .expect_err("parse should fail");
        assert!(err.message.contains("unreachable"));
    }

    #[test]
    fn parses_drop_table_statement() {
        let stmt = parse_statement("DROP TABLE IF EXISTS users").expect("parse should succeed");
        let Statement::DropTable(drop_table) = stmt else {
            panic!("expected drop table statement");
        };

        assert_eq!(drop_table.name, vec!["users".to_string()]);
        assert!(drop_table.if_exists);
        assert_eq!(drop_table.behavior, DropBehavior::Restrict);
    }

    #[test]
    fn parses_create_and_drop_schema_statements() {
        let create =
            parse_statement("CREATE SCHEMA IF NOT EXISTS app").expect("parse should succeed");
        let Statement::CreateSchema(create_schema) = create else {
            panic!("expected create schema statement");
        };
        assert_eq!(create_schema.name, "app");
        assert!(create_schema.if_not_exists);

        let drop =
            parse_statement("DROP SCHEMA IF EXISTS app CASCADE").expect("parse should succeed");
        let Statement::DropSchema(drop_schema) = drop else {
            panic!("expected drop schema statement");
        };
        assert_eq!(drop_schema.name, "app");
        assert!(drop_schema.if_exists);
        assert_eq!(drop_schema.behavior, DropBehavior::Cascade);
    }

    #[test]
    fn parses_create_and_drop_view_statements() {
        let create_view = parse_statement("CREATE VIEW app.v_users AS SELECT id FROM users")
            .expect("parse should succeed");
        let Statement::CreateView(view) = create_view else {
            panic!("expected create view statement");
        };
        assert_eq!(view.name, vec!["app".to_string(), "v_users".to_string()]);
        assert!(!view.or_replace);
        assert!(!view.materialized);
        assert!(view.with_data);

        let create_mat =
            parse_statement("CREATE MATERIALIZED VIEW app.mv_users AS SELECT id FROM users")
                .expect("parse should succeed");
        let Statement::CreateView(mat) = create_mat else {
            panic!("expected create materialized view statement");
        };
        assert!(!mat.or_replace);
        assert!(mat.materialized);
        assert!(mat.with_data);

        let drop_view = parse_statement("DROP VIEW IF EXISTS app.v_users CASCADE")
            .expect("parse should succeed");
        let Statement::DropView(drop_view) = drop_view else {
            panic!("expected drop view statement");
        };
        assert_eq!(drop_view.names.len(), 1);
        assert!(!drop_view.materialized);
        assert!(drop_view.if_exists);
        assert_eq!(drop_view.behavior, DropBehavior::Cascade);

        let drop_mat = parse_statement("DROP MATERIALIZED VIEW app.mv_users RESTRICT")
            .expect("parse should succeed");
        let Statement::DropView(drop_mat) = drop_mat else {
            panic!("expected drop materialized view statement");
        };
        assert_eq!(drop_mat.names.len(), 1);
        assert!(drop_mat.materialized);
        assert_eq!(drop_mat.behavior, DropBehavior::Restrict);
    }

    #[test]
    fn parses_drop_view_multiple_names() {
        let stmt = parse_statement("DROP VIEW v1, app.v2 CASCADE").expect("parse should succeed");
        let Statement::DropView(drop) = stmt else {
            panic!("expected drop view statement");
        };
        assert_eq!(
            drop.names,
            vec![
                vec!["v1".to_string()],
                vec!["app".to_string(), "v2".to_string()]
            ]
        );
        assert_eq!(drop.behavior, DropBehavior::Cascade);
    }

    #[test]
    fn parses_create_or_replace_view_statement() {
        let stmt = parse_statement("CREATE OR REPLACE VIEW app.v_users AS SELECT id FROM users")
            .expect("parse should succeed");
        let Statement::CreateView(view) = stmt else {
            panic!("expected create view statement");
        };
        assert!(view.or_replace);
        assert!(!view.materialized);
        assert!(view.with_data);
    }

    #[test]
    fn parses_create_or_replace_materialized_view_statement() {
        let stmt = parse_statement("CREATE OR REPLACE MATERIALIZED VIEW app.mv AS SELECT 1")
            .expect("parse should succeed");
        let Statement::CreateView(view) = stmt else {
            panic!("expected create view statement");
        };
        assert!(view.or_replace);
        assert!(view.materialized);
        assert!(view.with_data);
    }

    #[test]
    fn parses_create_materialized_view_with_no_data_option() {
        let stmt = parse_statement("CREATE MATERIALIZED VIEW app.mv AS SELECT 1 WITH NO DATA")
            .expect("parse should succeed");
        let Statement::CreateView(view) = stmt else {
            panic!("expected create view statement");
        };
        assert!(view.materialized);
        assert!(!view.with_data);
    }

    #[test]
    fn parses_create_materialized_view_with_data_option() {
        let stmt = parse_statement("CREATE MATERIALIZED VIEW app.mv AS SELECT 1 WITH DATA")
            .expect("parse should succeed");
        let Statement::CreateView(view) = stmt else {
            panic!("expected create view statement");
        };
        assert!(view.materialized);
        assert!(view.with_data);
    }

    #[test]
    fn parses_refresh_materialized_view_statement() {
        let stmt = parse_statement("REFRESH MATERIALIZED VIEW app.mv_users")
            .expect("parse should succeed");
        let Statement::RefreshMaterializedView(refresh) = stmt else {
            panic!("expected refresh materialized view statement");
        };
        assert_eq!(
            refresh.name,
            vec!["app".to_string(), "mv_users".to_string()]
        );
        assert!(!refresh.concurrently);
        assert!(refresh.with_data);
    }

    #[test]
    fn parses_refresh_materialized_view_options() {
        let stmt =
            parse_statement("REFRESH MATERIALIZED VIEW CONCURRENTLY app.mv_users WITH NO DATA")
                .expect("parse should succeed");
        let Statement::RefreshMaterializedView(refresh) = stmt else {
            panic!("expected refresh materialized view statement");
        };
        assert!(refresh.concurrently);
        assert!(!refresh.with_data);
    }

    #[test]
    fn parses_drop_index_drop_sequence_and_truncate() {
        let drop_index = parse_statement("DROP INDEX IF EXISTS public.uq_users_email RESTRICT")
            .expect("parse should succeed");
        let Statement::DropIndex(drop_index) = drop_index else {
            panic!("expected drop index statement");
        };
        assert_eq!(
            drop_index.name,
            vec!["public".to_string(), "uq_users_email".to_string()]
        );
        assert!(drop_index.if_exists);
        assert_eq!(drop_index.behavior, DropBehavior::Restrict);

        let drop_sequence =
            parse_statement("DROP SEQUENCE user_id_seq CASCADE").expect("parse should succeed");
        let Statement::DropSequence(drop_sequence) = drop_sequence else {
            panic!("expected drop sequence statement");
        };
        assert_eq!(drop_sequence.name, vec!["user_id_seq".to_string()]);
        assert!(!drop_sequence.if_exists);
        assert_eq!(drop_sequence.behavior, DropBehavior::Cascade);

        let truncate = parse_statement("TRUNCATE TABLE users, sessions CASCADE")
            .expect("parse should succeed");
        let Statement::Truncate(truncate) = truncate else {
            panic!("expected truncate statement");
        };
        assert_eq!(truncate.table_names.len(), 2);
        assert_eq!(truncate.behavior, DropBehavior::Cascade);
    }

    #[test]
    fn parses_create_sequence_statement() {
        let stmt =
            parse_statement("CREATE SEQUENCE public.user_id_seq START WITH 7 INCREMENT BY 3")
                .expect("parse should succeed");
        let Statement::CreateSequence(create) = stmt else {
            panic!("expected create sequence statement");
        };
        assert_eq!(
            create.name,
            vec!["public".to_string(), "user_id_seq".to_string()]
        );
        assert_eq!(create.start, Some(7));
        assert_eq!(create.increment, Some(3));
        assert!(create.min_value.is_none());
        assert!(create.max_value.is_none());
        assert!(create.cycle.is_none());
        assert!(create.cache.is_none());
    }

    #[test]
    fn parses_create_sequence_extended_options() {
        let stmt = parse_statement(
            "CREATE SEQUENCE s START 5 INCREMENT -2 MINVALUE -10 MAXVALUE 100 CYCLE CACHE 8",
        )
        .expect("parse should succeed");
        let Statement::CreateSequence(create) = stmt else {
            panic!("expected create sequence statement");
        };
        assert_eq!(create.start, Some(5));
        assert_eq!(create.increment, Some(-2));
        assert_eq!(create.min_value, Some(Some(-10)));
        assert_eq!(create.max_value, Some(Some(100)));
        assert_eq!(create.cycle, Some(true));
        assert_eq!(create.cache, Some(8));
    }

    #[test]
    fn parses_alter_sequence_restart_statement() {
        let stmt = parse_statement("ALTER SEQUENCE public.user_id_seq RESTART WITH 42")
            .expect("parse should succeed");
        let Statement::AlterSequence(alter) = stmt else {
            panic!("expected alter sequence statement");
        };
        assert_eq!(
            alter.name,
            vec!["public".to_string(), "user_id_seq".to_string()]
        );
        assert_eq!(
            alter.actions,
            vec![AlterSequenceAction::Restart { with: Some(42) }]
        );
    }

    #[test]
    fn parses_alter_sequence_multiple_options() {
        let stmt = parse_statement(
            "ALTER SEQUENCE s RESTART WITH 9 INCREMENT BY -3 NO MINVALUE MAXVALUE 30 NO CYCLE CACHE 12",
        )
        .expect("parse should succeed");
        let Statement::AlterSequence(alter) = stmt else {
            panic!("expected alter sequence statement");
        };
        assert_eq!(
            alter.actions,
            vec![
                AlterSequenceAction::Restart { with: Some(9) },
                AlterSequenceAction::SetIncrement { increment: -3 },
                AlterSequenceAction::SetMinValue { min: None },
                AlterSequenceAction::SetMaxValue { max: Some(30) },
                AlterSequenceAction::SetCycle { cycle: false },
                AlterSequenceAction::SetCache { cache: 12 }
            ]
        );
    }

    #[test]
    fn parses_create_unique_index_statement() {
        let stmt = parse_statement("CREATE UNIQUE INDEX uq_users_email ON users (email)")
            .expect("parse should succeed");
        let Statement::CreateIndex(create) = stmt else {
            panic!("expected create index statement");
        };
        assert_eq!(create.name, "uq_users_email");
        assert_eq!(create.table_name, vec!["users".to_string()]);
        assert_eq!(create.columns, vec!["email".to_string()]);
        assert!(create.unique);
    }

    #[test]
    fn parses_alter_table_add_column_statement() {
        let stmt = parse_statement("ALTER TABLE users ADD COLUMN note text")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        assert_eq!(alter.table_name, vec!["users".to_string()]);
        match alter.action {
            AlterTableAction::AddColumn(column) => {
                assert_eq!(column.name, "note");
                assert_eq!(column.data_type, TypeName::Text);
                assert!(column.nullable);
            }
            other => panic!("expected add column action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_add_constraint_statement() {
        let stmt = parse_statement("ALTER TABLE users ADD CONSTRAINT uq_email UNIQUE (email)")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::AddConstraint(TableConstraint::Unique { name, columns }) => {
                assert_eq!(name.as_deref(), Some("uq_email"));
                assert_eq!(columns, vec!["email".to_string()]);
            }
            other => panic!("expected add constraint action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_drop_column_statement() {
        let stmt =
            parse_statement("ALTER TABLE users DROP COLUMN note").expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::DropColumn { name } => assert_eq!(name, "note"),
            other => panic!("expected drop column action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_drop_constraint_statement() {
        let stmt = parse_statement("ALTER TABLE users DROP CONSTRAINT users_pkey")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::DropConstraint { name } => assert_eq!(name, "users_pkey"),
            other => panic!("expected drop constraint action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_rename_column_statement() {
        let stmt = parse_statement("ALTER TABLE users RENAME COLUMN note TO details")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::RenameColumn { old_name, new_name } => {
                assert_eq!(old_name, "note");
                assert_eq!(new_name, "details");
            }
            other => panic!("expected rename column action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_set_not_null_statement() {
        let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note SET NOT NULL")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::SetColumnNullable { name, nullable } => {
                assert_eq!(name, "note");
                assert!(!nullable);
            }
            other => panic!("expected set column nullable action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_drop_not_null_statement() {
        let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note DROP NOT NULL")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::SetColumnNullable { name, nullable } => {
                assert_eq!(name, "note");
                assert!(nullable);
            }
            other => panic!("expected set column nullable action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_set_default_statement() {
        let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note SET DEFAULT 'x'")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::SetColumnDefault { name, default } => {
                assert_eq!(name, "note");
                assert_eq!(default, Some(Expr::String("x".to_string())));
            }
            other => panic!("expected set column default action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_table_drop_default_statement() {
        let stmt = parse_statement("ALTER TABLE users ALTER COLUMN note DROP DEFAULT")
            .expect("parse should succeed");
        let Statement::AlterTable(alter) = stmt else {
            panic!("expected alter table statement");
        };

        match alter.action {
            AlterTableAction::SetColumnDefault { name, default } => {
                assert_eq!(name, "note");
                assert_eq!(default, None);
            }
            other => panic!("expected set column default action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_view_rename_statement() {
        let stmt = parse_statement("ALTER VIEW users_v RENAME TO users_view")
            .expect("parse should succeed");
        let Statement::AlterView(alter) = stmt else {
            panic!("expected alter view statement");
        };
        assert_eq!(alter.name, vec!["users_v".to_string()]);
        assert!(!alter.materialized);
        match alter.action {
            AlterViewAction::RenameTo { new_name } => assert_eq!(new_name, "users_view"),
            other => panic!("expected rename action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_view_rename_column_statement() {
        let stmt = parse_statement("ALTER VIEW users_v RENAME COLUMN old_col TO new_col")
            .expect("parse should succeed");
        let Statement::AlterView(alter) = stmt else {
            panic!("expected alter view statement");
        };
        assert_eq!(alter.name, vec!["users_v".to_string()]);
        assert!(!alter.materialized);
        match alter.action {
            AlterViewAction::RenameColumn { old_name, new_name } => {
                assert_eq!(old_name, "old_col");
                assert_eq!(new_name, "new_col");
            }
            other => panic!("expected rename column action, got {other:?}"),
        }
    }

    #[test]
    fn parses_alter_materialized_view_set_schema_statement() {
        let stmt = parse_statement("ALTER MATERIALIZED VIEW mv_users SET SCHEMA app")
            .expect("parse should succeed");
        let Statement::AlterView(alter) = stmt else {
            panic!("expected alter view statement");
        };
        assert_eq!(alter.name, vec!["mv_users".to_string()]);
        assert!(alter.materialized);
        match alter.action {
            AlterViewAction::SetSchema { schema_name } => assert_eq!(schema_name, "app"),
            other => panic!("expected set schema action, got {other:?}"),
        }
    }

    #[test]
    fn parses_json_binary_operators() {
        let stmt = parse_statement(
            "SELECT \
             doc -> 'a', \
             doc ->> 'a', \
             doc #> '{a,b}', \
             doc #>> '{a,b}', \
             doc || '{\"z\":1}', \
             doc @> '{\"a\":1}', \
             doc <@ '{\"a\":1,\"b\":2}', \
             doc @? '$.a', \
             doc @@ '$.a', \
             doc ? 'a', \
             doc ?| '{a,b}', \
             doc ?| array['a','c'], \
             doc ?& '{a,b}', \
             doc ?& array['a','b'], \
             doc #- '{a,b}' \
             FROM t",
        )
        .expect("parse should succeed");

        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query body");
        };

        let expected = [
            BinaryOp::JsonGet,
            BinaryOp::JsonGetText,
            BinaryOp::JsonPath,
            BinaryOp::JsonPathText,
            BinaryOp::JsonConcat,
            BinaryOp::JsonContains,
            BinaryOp::JsonContainedBy,
            BinaryOp::JsonPathExists,
            BinaryOp::JsonPathMatch,
            BinaryOp::JsonHasKey,
            BinaryOp::JsonHasAny,
            BinaryOp::JsonHasAny,
            BinaryOp::JsonHasAll,
            BinaryOp::JsonHasAll,
            BinaryOp::JsonDeletePath,
        ];

        assert_eq!(select.targets.len(), expected.len());
        for (target, op) in select.targets.iter().zip(expected) {
            match &target.expr {
                Expr::Binary { op: parsed, .. } => assert_eq!(parsed, &op),
                other => panic!("expected binary expression target, got {other:?}"),
            }
        }
    }

    #[test]
    fn parses_aggregate_function_modifiers() {
        let stmt = parse_statement(
            "SELECT json_agg(DISTINCT payload ORDER BY id DESC) FILTER (WHERE keep = true) FROM t",
        )
        .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query body");
        };
        let Expr::FunctionCall {
            name,
            distinct,
            order_by,
            filter,
            ..
        } = &select.targets[0].expr
        else {
            panic!("expected function call");
        };
        assert_eq!(name, &vec!["json_agg".to_string()]);
        assert!(*distinct);
        assert_eq!(order_by.len(), 1);
        assert!(filter.is_some());
    }

    #[test]
    fn parses_window_function_with_partition_and_order_by() {
        let stmt =
            parse_statement("SELECT row_number() OVER (PARTITION BY team ORDER BY score DESC) FROM t")
                .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query body");
        };
        let Expr::FunctionCall {
            name,
            args,
            over,
            ..
        } = &select.targets[0].expr
        else {
            panic!("expected function call");
        };
        assert_eq!(name, &vec!["row_number".to_string()]);
        assert!(args.is_empty());
        let over = over.as_deref().expect("expected OVER clause");
        assert_eq!(over.partition_by.len(), 1);
        assert_eq!(over.order_by.len(), 1);
        assert!(over.frame.is_none());
    }

    #[test]
    fn parses_window_frame_rows_and_range_between() {
        let stmt = parse_statement(
            "SELECT \
             sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), \
             avg(v) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) \
             FROM t",
        )
        .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query body");
        };

        let Expr::FunctionCall {
            over: over_rows, ..
        } = &select.targets[0].expr
        else {
            panic!("expected function call");
        };
        let over_rows = over_rows.as_deref().expect("expected OVER clause");
        let frame_rows = over_rows.frame.as_ref().expect("expected frame");
        assert_eq!(frame_rows.units, WindowFrameUnits::Rows);
        assert!(matches!(
            frame_rows.start,
            WindowFrameBound::OffsetPreceding(Expr::Integer(1))
        ));
        assert!(matches!(frame_rows.end, WindowFrameBound::CurrentRow));

        let Expr::FunctionCall {
            over: over_range, ..
        } = &select.targets[1].expr
        else {
            panic!("expected function call");
        };
        let over_range = over_range.as_deref().expect("expected OVER clause");
        let frame_range = over_range.frame.as_ref().expect("expected frame");
        assert_eq!(frame_range.units, WindowFrameUnits::Range);
        assert!(matches!(
            frame_range.start,
            WindowFrameBound::OffsetPreceding(Expr::Integer(2))
        ));
        assert!(matches!(
            frame_range.end,
            WindowFrameBound::OffsetFollowing(Expr::Integer(1))
        ));
    }

    #[test]
    fn parses_typed_column_aliases_for_table_functions() {
        let stmt = parse_statement(
            "SELECT * FROM json_to_record('{\"a\":1,\"b\":\"x\"}') AS r(a int8, b text)",
        )
        .expect("parse should succeed");
        let Statement::Query(query) = stmt else {
            panic!("expected query statement");
        };
        let QueryExpr::Select(select) = query.body else {
            panic!("expected select query body");
        };
        let TableExpression::Function(function) = &select.from[0] else {
            panic!("expected function table expression");
        };
        assert_eq!(
            function.column_aliases,
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(
            function.column_alias_types,
            vec![Some("int8".to_string()), Some("text".to_string())]
        );
    }

    #[test]
    fn parses_explain_statement() {
        let stmt = parse_statement("EXPLAIN SELECT 1").unwrap();
        assert!(matches!(stmt, Statement::Explain(_)));
    }

    #[test]
    fn parses_explain_analyze() {
        let stmt = parse_statement("EXPLAIN ANALYZE SELECT 1").unwrap();
        match stmt {
            Statement::Explain(e) => assert!(e.analyze),
            _ => panic!("expected EXPLAIN"),
        }
    }

    #[test]
    fn parses_set_statement() {
        let stmt = parse_statement("SET search_path = public").unwrap();
        match stmt {
            Statement::Set(s) => {
                assert_eq!(s.name, "search_path");
                assert_eq!(s.value, "public");
            }
            _ => panic!("expected SET"),
        }
    }

    #[test]
    fn parses_set_to_syntax() {
        let stmt = parse_statement("SET timezone TO 'UTC'").unwrap();
        match stmt {
            Statement::Set(s) => {
                assert_eq!(s.name, "timezone");
                assert_eq!(s.value, "UTC");
            }
            _ => panic!("expected SET"),
        }
    }

    #[test]
    fn parses_show_statement() {
        let stmt = parse_statement("SHOW search_path").unwrap();
        match stmt {
            Statement::Show(s) => assert_eq!(s.name, "search_path"),
            _ => panic!("expected SHOW"),
        }
    }

    #[test]
    fn parses_listen_notify_unlisten() {
        let stmt = parse_statement("LISTEN my_channel").unwrap();
        assert!(matches!(stmt, Statement::Listen(_)));

        let stmt = parse_statement("NOTIFY my_channel, 'payload'").unwrap();
        match stmt {
            Statement::Notify(n) => {
                assert_eq!(n.channel, "my_channel");
                assert_eq!(n.payload, Some("payload".to_string()));
            }
            _ => panic!("expected NOTIFY"),
        }

        let stmt = parse_statement("UNLISTEN *").unwrap();
        match stmt {
            Statement::Unlisten(u) => assert!(u.channel.is_none()),
            _ => panic!("expected UNLISTEN"),
        }
    }

    #[test]
    fn parses_do_block() {
        let stmt = parse_statement("DO 'BEGIN NULL; END'").unwrap();
        match stmt {
            Statement::Do(d) => assert_eq!(d.body, "BEGIN NULL; END"),
            _ => panic!("expected DO"),
        }
    }

    #[test]
    fn parses_discard_all() {
        let stmt = parse_statement("DISCARD ALL").unwrap();
        assert!(matches!(stmt, Statement::Discard(_)));
    }

    #[test]
    fn parses_transaction_statements() {
        let stmt = parse_statement("BEGIN").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Begin)
        ));

        let stmt = parse_statement("START TRANSACTION").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Begin)
        ));

        let stmt = parse_statement("COMMIT").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Commit)
        ));

        let stmt = parse_statement("END").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Commit)
        ));

        let stmt = parse_statement("ROLLBACK").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Rollback)
        ));

        let stmt = parse_statement("SAVEPOINT s1").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::Savepoint(name)) if name == "s1"
        ));

        let stmt = parse_statement("RELEASE SAVEPOINT s1").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::ReleaseSavepoint(name)) if name == "s1"
        ));

        let stmt = parse_statement("ROLLBACK TO SAVEPOINT s1").unwrap();
        assert!(matches!(
            stmt,
            Statement::Transaction(TransactionStatement::RollbackToSavepoint(name)) if name == "s1"
        ));
    }
}
