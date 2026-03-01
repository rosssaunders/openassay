#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn parse_query(&mut self) -> Result<Query, ParseError> {
        let with = if self.consume_keyword(Keyword::With) {
            let recursive = self.consume_keyword(Keyword::Recursive);
            let mut ctes = Vec::new();
            loop {
                let name = self.parse_identifier()?;

                // Optional column list: WITH cte(a, b) AS (...)
                let column_names = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                    let mut cols = vec![self.parse_identifier()?];
                    while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        cols.push(self.parse_identifier()?);
                    }
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after CTE column list",
                    )?;
                    cols
                } else {
                    Vec::new()
                };

                self.expect_keyword(Keyword::As, "expected AS in common table expression")?;

                // Optional MATERIALIZED / NOT MATERIALIZED hint
                let materialized = if self.consume_keyword(Keyword::Materialized) {
                    Some(true)
                } else if self.consume_keyword(Keyword::Not) {
                    self.expect_keyword(Keyword::Materialized, "expected MATERIALIZED after NOT")?;
                    Some(false)
                } else {
                    None
                };

                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' to open common table expression",
                )?;
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close common table expression",
                )?;

                // Optional SEARCH clause
                let search_clause = if self.consume_keyword(Keyword::Search) {
                    Some(self.parse_search_clause()?)
                } else {
                    None
                };

                // Optional CYCLE clause
                let cycle_clause = if self.consume_keyword(Keyword::Cycle) {
                    Some(self.parse_cycle_clause()?)
                } else {
                    None
                };

                ctes.push(CommonTableExpr {
                    name,
                    column_names,
                    materialized,
                    query,
                    search_clause,
                    cycle_clause,
                });
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

        // Parse and ignore row-locking clauses such as:
        //   FOR UPDATE
        //   FOR UPDATE OF alias[, ...]
        if self.consume_keyword(Keyword::For) {
            self.expect_keyword(Keyword::Update, "expected UPDATE after FOR")?;
            if self.consume_ident("of") {
                let _ = self.parse_identifier()?;
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    let _ = self.parse_identifier()?;
                }
            }
            // Optional postfixes accepted by PostgreSQL lock clauses.
            let _ = self.consume_ident("nowait");
            if self.consume_ident("skip") {
                let _ = self.consume_ident("locked");
            }
        }

        Ok(Query {
            with,
            body,
            order_by,
            limit,
            offset,
        })
    }

    pub(super) fn parse_search_clause(&mut self) -> Result<SearchClause, ParseError> {
        // SEARCH { DEPTH | BREADTH } FIRST BY column_list SET search_seq_col_name
        let depth_first = if self.consume_keyword(Keyword::Depth) {
            true
        } else if self.consume_keyword(Keyword::Breadth) {
            false
        } else {
            return Err(self.error_at_current("expected DEPTH or BREADTH after SEARCH"));
        };

        self.expect_keyword(Keyword::First, "expected FIRST after DEPTH/BREADTH")?;
        self.expect_keyword(Keyword::By, "expected BY in SEARCH clause")?;

        let mut by_columns = vec![self.parse_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            by_columns.push(self.parse_identifier()?);
        }

        self.expect_keyword(Keyword::Set, "expected SET in SEARCH clause")?;
        let set_column = self.parse_identifier()?;

        Ok(SearchClause {
            depth_first,
            by_columns,
            set_column,
        })
    }

    pub(super) fn parse_cycle_clause(&mut self) -> Result<CycleClause, ParseError> {
        // CYCLE col_list SET cycle_mark_col_name [ TO cycle_mark_value DEFAULT non_cycle_mark_value ] USING cycle_path_col_name
        let mut columns = vec![self.parse_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            columns.push(self.parse_identifier()?);
        }

        self.expect_keyword(Keyword::Set, "expected SET in CYCLE clause")?;
        let set_column = self.parse_identifier()?;

        let (mark_value, default_value) = if self.consume_keyword(Keyword::To) {
            let mark = self.parse_literal_string()?;
            self.expect_keyword(
                Keyword::Default,
                "expected DEFAULT after TO value in CYCLE clause",
            )?;
            let default = self.parse_literal_string()?;
            (Some(mark), Some(default))
        } else {
            (None, None)
        };

        self.expect_keyword(Keyword::Using, "expected USING in CYCLE clause")?;
        let using_column = self.parse_identifier()?;

        Ok(CycleClause {
            columns,
            set_column,
            using_column,
            mark_value,
            default_value,
        })
    }

    pub(super) fn parse_query_expr_bp(&mut self, min_bp: u8) -> Result<QueryExpr, ParseError> {
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

    pub(super) fn parse_query_term(&mut self) -> Result<QueryExpr, ParseError> {
        if self.consume_keyword(Keyword::Select) {
            return Ok(QueryExpr::Select(self.parse_select_after_select_keyword()?));
        }

        if self.consume_keyword(Keyword::Values) {
            // VALUES (expr, ...), (expr, ...) as a standalone query
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
            return Ok(QueryExpr::Values(all_rows));
        }

        if self.consume_keyword(Keyword::Table) {
            let name = self.parse_qualified_name()?;
            return Ok(QueryExpr::Select(SelectStatement {
                quantifier: Some(SelectQuantifier::All),
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![TableExpression::Relation(TableRef { name, alias: None })],
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                window_definitions: Vec::new(),
                distinct_on: Vec::new(),
            }));
        }

        // Support DML statements (INSERT/UPDATE/DELETE) in CTEs
        if self.consume_keyword(Keyword::Insert) {
            let stmt = self.parse_insert_statement_after_keyword()?;
            return Ok(QueryExpr::Insert(Box::new(stmt)));
        }

        if self.consume_keyword(Keyword::Update) {
            let stmt = self.parse_update_statement_after_keyword()?;
            return Ok(QueryExpr::Update(Box::new(stmt)));
        }

        if self.consume_keyword(Keyword::Delete) {
            let stmt = self.parse_delete_statement_after_keyword()?;
            return Ok(QueryExpr::Delete(Box::new(stmt)));
        }

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let nested = self.parse_query()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close subquery",
            )?;
            return Ok(QueryExpr::Nested(Box::new(nested)));
        }

        Err(self.error_at_current(
            "expected query term (SELECT, VALUES, INSERT, UPDATE, DELETE, or parenthesized query)",
        ))
    }

    pub(super) fn parse_select_after_select_keyword(
        &mut self,
    ) -> Result<SelectStatement, ParseError> {
        let mut distinct_on = Vec::new();
        let quantifier = if self.consume_keyword(Keyword::Distinct) {
            // Check for DISTINCT ON (expr, ...)
            if self.consume_keyword(Keyword::On) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after DISTINCT ON",
                )?;
                distinct_on = self.parse_expr_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after DISTINCT ON expressions",
                )?;
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

        let window_definitions = if self.consume_keyword(Keyword::Window) {
            self.parse_window_definitions()?
        } else {
            Vec::new()
        };

        Ok(SelectStatement {
            quantifier,
            distinct_on,
            targets,
            from,
            where_clause,
            group_by,
            having,
            window_definitions,
        })
    }

    pub(super) fn parse_target_list(&mut self) -> Result<Vec<SelectItem>, ParseError> {
        let mut targets = Vec::new();
        targets.push(self.parse_target_item()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            targets.push(self.parse_target_item()?);
        }
        Ok(targets)
    }

    pub(super) fn parse_target_item(&mut self) -> Result<SelectItem, ParseError> {
        let expr = if self.consume_if(|k| matches!(k, TokenKind::Star)) {
            Expr::Wildcard
        } else {
            self.parse_expr()?
        };

        let alias = self.parse_optional_alias()?;
        Ok(SelectItem { expr, alias })
    }

    pub(super) fn parse_from_list(&mut self) -> Result<Vec<TableExpression>, ParseError> {
        let mut tables = Vec::new();
        tables.push(self.parse_table_expression()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            tables.push(self.parse_table_expression()?);
        }
        Ok(tables)
    }

    pub(super) fn parse_table_expression(&mut self) -> Result<TableExpression, ParseError> {
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

        if matches!(left, TableExpression::Join(_)) {
            let alias = self.parse_optional_alias()?;
            let (column_aliases, _) = self.parse_optional_column_aliases()?;
            if alias.is_some() || !column_aliases.is_empty() {
                if alias.is_none() {
                    return Err(self.error_at_current(
                        "expected table alias before column alias list in FROM clause",
                    ));
                }
                left = self.wrap_table_expression_with_alias(left, alias, column_aliases);
            }
        }

        Ok(left)
    }

    pub(super) fn parse_table_factor(&mut self) -> Result<TableExpression, ParseError> {
        let lateral = self.consume_keyword(Keyword::Lateral);
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if self.peek_keyword(Keyword::Select)
                || self.peek_keyword(Keyword::Values)
                || self.peek_keyword(Keyword::With)
                || matches!(self.current_kind(), TokenKind::LParen)
            {
                let query = self.parse_query()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close subquery in FROM",
                )?;
                let alias = self.parse_optional_alias()?;
                let (column_aliases, _) = self.parse_optional_column_aliases()?;
                return Ok(TableExpression::Subquery(SubqueryRef {
                    query,
                    alias,
                    column_aliases,
                    lateral,
                }));
            }

            let mut inner = self.parse_table_expression()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close table expression",
            )?;
            let alias = self.parse_optional_alias()?;
            let (column_aliases, _) = self.parse_optional_column_aliases()?;
            if alias.is_some() || !column_aliases.is_empty() {
                if alias.is_none() {
                    return Err(self.error_at_current(
                        "expected table alias before column alias list in FROM clause",
                    ));
                }
                if !column_aliases.is_empty() || matches!(inner, TableExpression::Join(_)) {
                    return Ok(self.wrap_table_expression_with_alias(inner, alias, column_aliases));
                }
                if let Some(alias) = alias {
                    self.apply_table_alias(&mut inner, alias);
                }
            }
            return Ok(inner);
        }

        let name = self.parse_qualified_name()?;
        let _inherit = self.consume_if(|k| matches!(k, TokenKind::Star));
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut args = Vec::new();
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                self.consume_ident("variadic");
                args.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    self.consume_ident("variadic");
                    args.push(self.parse_expr()?);
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after function arguments",
                )?;
            }
            let alias = self.parse_optional_function_alias()?;
            let (column_aliases, column_alias_types) = self.parse_optional_column_aliases()?;
            return Ok(TableExpression::Function(TableFunctionRef {
                name,
                args,
                alias,
                column_aliases,
                column_alias_types,
                lateral,
            }));
        }
        if lateral {
            return Err(self.error_at_current("expected subquery or function after LATERAL"));
        }
        let alias = self.parse_optional_alias()?;
        let (column_aliases, _) = self.parse_optional_column_aliases()?;
        if !column_aliases.is_empty() {
            if alias.is_none() {
                return Err(self.error_at_current(
                    "expected table alias before column alias list in FROM clause",
                ));
            }
            let relation = TableExpression::Relation(TableRef { name, alias: None });
            return Ok(self.wrap_table_expression_with_alias(relation, alias, column_aliases));
        }
        Ok(TableExpression::Relation(TableRef { name, alias }))
    }

    pub(super) fn wrap_table_expression_with_alias(
        &self,
        table: TableExpression,
        alias: Option<String>,
        column_aliases: Vec<String>,
    ) -> TableExpression {
        let query = Query {
            with: None,
            body: QueryExpr::Select(SelectStatement {
                quantifier: Some(SelectQuantifier::All),
                targets: vec![SelectItem {
                    expr: Expr::Wildcard,
                    alias: None,
                }],
                from: vec![table],
                where_clause: None,
                group_by: Vec::new(),
                having: None,
                window_definitions: Vec::new(),
                distinct_on: Vec::new(),
            }),
            order_by: Vec::new(),
            limit: None,
            offset: None,
        };

        TableExpression::Subquery(SubqueryRef {
            query,
            alias,
            column_aliases,
            lateral: false,
        })
    }

    pub(super) fn apply_table_alias(&self, table: &mut TableExpression, alias: String) {
        match table {
            TableExpression::Relation(rel) => rel.alias = Some(alias),
            TableExpression::Function(function) => function.alias = Some(alias),
            TableExpression::Subquery(sub) => sub.alias = Some(alias),
            TableExpression::Join(join) => join.alias = Some(alias),
        }
    }

    pub(super) fn parse_group_by_list(&mut self) -> Result<Vec<GroupByExpr>, ParseError> {
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

    pub(super) fn parse_grouping_sets(&mut self) -> Result<Vec<Vec<Expr>>, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after GROUPING SETS",
        )?;
        let mut sets = Vec::new();
        loop {
            let mut item_sets = self.parse_grouping_set_item()?;
            sets.append(&mut item_sets);
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

    pub(super) fn parse_grouping_set_item(&mut self) -> Result<Vec<Vec<Expr>>, ParseError> {
        if self.consume_keyword(Keyword::Grouping) {
            self.expect_keyword(Keyword::Sets, "expected SETS after GROUPING")?;
            return self.parse_grouping_sets();
        }
        if self.consume_keyword(Keyword::Rollup) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after ROLLUP",
            )?;
            let exprs = if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                Vec::new()
            } else {
                let out = self.parse_expr_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after ROLLUP list",
                )?;
                out
            };
            return Ok(vec![exprs]);
        }
        if self.consume_keyword(Keyword::Cube) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CUBE",
            )?;
            let exprs = if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                Vec::new()
            } else {
                let out = self.parse_expr_list()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after CUBE list",
                )?;
                out
            };
            return Ok(vec![exprs]);
        }

        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                return Ok(vec![Vec::new()]);
            }
            let exprs = self.parse_expr_list()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close grouping set",
            )?;
            return Ok(vec![exprs]);
        }

        Ok(vec![vec![self.parse_expr()?]])
    }

    pub(super) fn parse_order_by_list(&mut self) -> Result<Vec<OrderByExpr>, ParseError> {
        let mut out = Vec::new();
        loop {
            let expr = self.parse_expr()?;

            // Check for USING operator before ASC/DESC
            let (using_operator, ascending) = if self.consume_keyword(Keyword::Using) {
                // Parse the operator after USING
                let operator = match self.current_kind() {
                    TokenKind::Less => {
                        self.advance();
                        "<".to_string()
                    }
                    TokenKind::Greater => {
                        self.advance();
                        ">".to_string()
                    }
                    TokenKind::LessEquals => {
                        self.advance();
                        "<=".to_string()
                    }
                    TokenKind::GreaterEquals => {
                        self.advance();
                        ">=".to_string()
                    }
                    TokenKind::Operator(op) => {
                        let op_str = op.clone();
                        self.advance();
                        op_str
                    }
                    _ => {
                        return Err(self.error_at_current("expected operator after USING"));
                    }
                };
                // Map common operators to ascending/descending
                let asc = match operator.as_str() {
                    "<" | "<=" => Some(true),  // USING < means ascending
                    ">" | ">=" => Some(false), // USING > means descending
                    _ => None,                 // Other operators don't have clear mapping
                };
                (Some(operator), asc)
            } else if self.consume_keyword(Keyword::Asc) {
                (None, Some(true))
            } else if self.consume_keyword(Keyword::Desc) {
                (None, Some(false))
            } else {
                (None, None)
            };
            if self.consume_ident("nulls")
                && !(self.consume_keyword(Keyword::First) || self.consume_keyword(Keyword::Last))
            {
                return Err(self.error_at_current("expected FIRST or LAST after NULLS"));
            }

            out.push(OrderByExpr {
                expr,
                ascending,
                using_operator,
            });

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        Ok(out)
    }

    pub(super) fn parse_expr_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut out = Vec::new();
        out.push(self.parse_expr()?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            out.push(self.parse_expr()?);
        }
        Ok(out)
    }
}
