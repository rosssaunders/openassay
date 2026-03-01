#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn parse_insert_statement(&mut self) -> Result<Statement, ParseError> {
        let stmt = self.parse_insert_statement_after_keyword()?;
        Ok(Statement::Insert(stmt))
    }

    pub(super) fn parse_insert_statement_after_keyword(
        &mut self,
    ) -> Result<InsertStatement, ParseError> {
        self.expect_keyword(Keyword::Into, "expected INTO after INSERT")?;
        let table_name = self.parse_qualified_name()?;
        let table_alias = self.parse_insert_table_alias()?;
        let columns = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut out = vec![self.parse_identifier()?];
            self.skip_array_subscripts();
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                out.push(self.parse_identifier()?);
                self.skip_array_subscripts();
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after INSERT column list",
            )?;
            out
        } else {
            Vec::new()
        };

        let source = if self.consume_keyword(Keyword::Default) {
            self.expect_keyword(
                Keyword::Values,
                "expected VALUES after DEFAULT in INSERT statement",
            )?;
            InsertSource::Values(vec![Vec::new()])
        } else if self.consume_keyword(Keyword::Values) {
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
            if self.consume_keyword(Keyword::Where) {
                let _ = self.parse_expr()?;
            }
            self.expect_keyword(Keyword::Do, "expected DO in ON CONFLICT clause")?;
            if self.consume_keyword(Keyword::Nothing) {
                Some(OnConflictClause::DoNothing { conflict_target })
            } else if self.consume_keyword(Keyword::Update) {
                self.expect_keyword(Keyword::Set, "expected SET after ON CONFLICT DO UPDATE")?;
                let mut assignments = self.parse_update_set_clause()?;
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    assignments.extend(self.parse_update_set_clause()?);
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

        Ok(InsertStatement {
            table_name,
            table_alias,
            columns,
            source,
            on_conflict,
            returning,
        })
    }

    pub(super) fn parse_insert_value_expr(&mut self) -> Result<Expr, ParseError> {
        if self.consume_keyword(Keyword::Default) {
            Ok(Expr::Default)
        } else {
            self.parse_expr()
        }
    }

    pub(super) fn parse_insert_values_row(&mut self) -> Result<Vec<Expr>, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' to start VALUES row",
        )?;
        let mut row = vec![self.parse_insert_value_expr()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            row.push(self.parse_insert_value_expr()?);
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after VALUES row",
        )?;
        Ok(row)
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn parse_insert_table_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.consume_keyword(Keyword::As) {
            return Ok(Some(self.parse_identifier()?));
        }
        if matches!(self.current_kind(), TokenKind::Identifier(_)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    pub(super) fn parse_conflict_target(&mut self) -> Result<Option<ConflictTarget>, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut cols = Vec::new();
            loop {
                let expr = self.parse_expr()?;
                let fallback = format!("expr{}", cols.len() + 1);
                cols.push(Self::extract_identifier_from_expr(&expr).unwrap_or(fallback));
                self.parse_optional_collation_clause()?;
                self.parse_optional_index_operator_class()?;
                self.skip_optional_parenthesized_tokens();
                let _ = self.consume_keyword(Keyword::Asc) || self.consume_keyword(Keyword::Desc);
                if self.consume_ident("nulls")
                    && !(self.consume_keyword(Keyword::First)
                        || self.consume_keyword(Keyword::Last))
                {
                    return Err(self.error_at_current("expected FIRST or LAST after NULLS"));
                }
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    break;
                }
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

    pub(super) fn parse_update_statement(&mut self) -> Result<Statement, ParseError> {
        let stmt = self.parse_update_statement_after_keyword()?;
        Ok(Statement::Update(stmt))
    }

    pub(super) fn parse_update_statement_after_keyword(
        &mut self,
    ) -> Result<UpdateStatement, ParseError> {
        let table_name = self.parse_qualified_name()?;

        // Optional alias: UPDATE t AS alias SET ... or UPDATE t alias SET ...
        let alias = if self.consume_keyword(Keyword::As)
            || (!self.peek_keyword(Keyword::Set)
                && matches!(self.current_kind(), TokenKind::Identifier(_)))
        {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        self.expect_keyword(Keyword::Set, "expected SET in UPDATE statement")?;

        let mut assignments = self.parse_update_set_clause()?;
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            assignments.extend(self.parse_update_set_clause()?);
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

        Ok(UpdateStatement {
            table_name,
            alias,
            assignments,
            from,
            where_clause,
            returning,
        })
    }

    /// Parse a single SET clause, which can be either:
    /// - `column = expr` (single column)
    /// - `(col1, col2, ...) = (expr1, expr2, ...)` (multi-column)
    /// - `(col1, col2, ...) = (SELECT ...)` (multi-column from subquery)
    ///
    /// Returns one or more assignments.
    pub(super) fn parse_update_set_clause(&mut self) -> Result<Vec<Assignment>, ParseError> {
        // Multi-column SET: (col1, col2) = (expr1, expr2) or (col1, col2) = (SELECT ...)
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut columns = vec![self.parse_identifier()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                columns.push(self.parse_identifier()?);
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after column list in SET clause",
            )?;
            self.expect_token(
                |k| matches!(k, TokenKind::Equal),
                "expected '=' after column list in SET clause",
            )?;

            // Check if it's a ROW(...) constructor
            if self.peek_keyword(Keyword::Row) {
                self.advance(); // consume ROW
                self.expect_token(|k| matches!(k, TokenKind::LParen), "expected '(' after ROW")?;
                let values = self.parse_update_value_expr_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after ROW values",
                )?;
                if values.len() != columns.len() {
                    return Err(self.error_at_current(&format!(
                        "number of columns ({}) does not match number of values ({})",
                        columns.len(),
                        values.len()
                    )));
                }
                return Ok(columns
                    .into_iter()
                    .zip(values)
                    .map(|(column, value)| Assignment {
                        column,
                        subscripts: Vec::new(),
                        value,
                    })
                    .collect());
            }

            // Must be ( ... ) — could be subquery or value list
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after '=' in multi-column SET clause",
            )?;

            // Check if it starts with SELECT or WITH (subquery)
            if self.peek_keyword(Keyword::Select) || self.peek_keyword(Keyword::With) {
                // Subquery: (col1, col2) = (SELECT a, b FROM ...)
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after subquery in SET clause",
                )?;
                // Create a MultiColumnSubquery expression for each column
                let total = columns.len();
                return Ok(columns
                    .into_iter()
                    .enumerate()
                    .map(|(i, column)| Assignment {
                        column,
                        subscripts: Vec::new(),
                        value: Expr::MultiColumnSubqueryRef {
                            subquery: Box::new(query.clone()),
                            index: i,
                            total,
                        },
                    })
                    .collect());
            }

            // Value list: (col1, col2) = (expr1, expr2)
            let values = self.parse_update_value_expr_list()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after value list in SET clause",
            )?;
            if values.len() != columns.len() {
                return Err(self.error_at_current(&format!(
                    "number of columns ({}) does not match number of values ({})",
                    columns.len(),
                    values.len()
                )));
            }
            return Ok(columns
                .into_iter()
                .zip(values)
                .map(|(column, value)| Assignment {
                    column,
                    subscripts: Vec::new(),
                    value,
                })
                .collect());
        }

        // Single column assignment
        Ok(vec![self.parse_assignment()?])
    }

    /// Parse a comma-separated list of expressions that may include DEFAULT
    pub(super) fn parse_update_value_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut values = vec![self.parse_update_value_expr()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            values.push(self.parse_update_value_expr()?);
        }
        Ok(values)
    }

    /// Parse an expression in UPDATE SET value position (allows DEFAULT)
    pub(super) fn parse_update_value_expr(&mut self) -> Result<Expr, ParseError> {
        if self.consume_keyword(Keyword::Default) {
            Ok(Expr::Default)
        } else {
            self.parse_expr()
        }
    }

    pub(super) fn parse_delete_statement(&mut self) -> Result<Statement, ParseError> {
        let stmt = self.parse_delete_statement_after_keyword()?;
        Ok(Statement::Delete(stmt))
    }

    pub(super) fn parse_delete_statement_after_keyword(
        &mut self,
    ) -> Result<DeleteStatement, ParseError> {
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

        Ok(DeleteStatement {
            table_name,
            using,
            where_clause,
            returning,
        })
    }

    pub(super) fn parse_merge_statement(&mut self) -> Result<Statement, ParseError> {
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
                } else if self.consume_keyword(Keyword::Insert) {
                    // INSERT DEFAULT VALUES — insert all defaults
                    if self.consume_keyword(Keyword::Default) {
                        self.expect_keyword(
                            Keyword::Values,
                            "expected VALUES after DEFAULT in MERGE INSERT clause",
                        )?;
                        when_clauses.push(MergeWhenClause::NotMatchedInsert {
                            condition,
                            columns: Vec::new(),
                            values: Vec::new(),
                        });
                    } else {
                        let columns = if matches!(self.current_kind(), TokenKind::LParen) {
                            // Peek ahead: if this is a column list, VALUES must follow
                            let saved = self.idx;
                            match self.parse_identifier_list_in_parens() {
                                Ok(cols) if self.peek_keyword(Keyword::Values) => cols,
                                _ => {
                                    self.idx = saved;
                                    Vec::new()
                                }
                            }
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
                    }
                } else if self.consume_keyword(Keyword::Do) {
                    self.expect_keyword(
                        Keyword::Nothing,
                        "expected NOTHING after DO in MERGE clause",
                    )?;
                    when_clauses.push(MergeWhenClause::NotMatchedDoNothing { condition });
                } else {
                    return Err(
                        self.error_at_current("expected INSERT or DO NOTHING for WHEN NOT MATCHED")
                    );
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

    pub(super) fn validate_merge_clause_reachability(
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

    pub(super) fn parse_assignment(&mut self) -> Result<Assignment, ParseError> {
        let mut column = self.parse_identifier()?;
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            column = self.parse_identifier()?;
        }
        let mut subscripts = Vec::new();
        // Parse optional array subscripts: col[idx], col[start:end], etc.
        while self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
            // Check for empty-start slice [:end]
            if self
                .peek_nth_kind(0)
                .is_some_and(|k| matches!(k, TokenKind::Colon))
            {
                self.advance(); // consume ':'
                let end_expr = if self
                    .peek_nth_kind(0)
                    .is_some_and(|k| matches!(k, TokenKind::RBracket))
                {
                    None
                } else {
                    Some(self.parse_expr()?)
                };
                self.expect_token(|k| matches!(k, TokenKind::RBracket), "expected ']'")?;
                subscripts.push(AssignmentSubscript::Slice(None, end_expr));
            } else {
                let first = self.parse_expr()?;
                if self.consume_if(|k| matches!(k, TokenKind::Colon)) {
                    let end_expr = if self
                        .peek_nth_kind(0)
                        .is_some_and(|k| matches!(k, TokenKind::RBracket))
                    {
                        None
                    } else {
                        Some(self.parse_expr()?)
                    };
                    self.expect_token(|k| matches!(k, TokenKind::RBracket), "expected ']'")?;
                    subscripts.push(AssignmentSubscript::Slice(Some(first), end_expr));
                } else {
                    self.expect_token(|k| matches!(k, TokenKind::RBracket), "expected ']'")?;
                    subscripts.push(AssignmentSubscript::Index(first));
                }
            }
        }
        self.expect_token(
            |k| matches!(k, TokenKind::Equal),
            "expected '=' in assignment",
        )?;
        let value = self.parse_update_value_expr()?;
        Ok(Assignment {
            column,
            subscripts,
            value,
        })
    }
}
