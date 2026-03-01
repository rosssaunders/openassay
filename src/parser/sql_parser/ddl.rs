#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn parse_create_statement(&mut self) -> Result<Statement, ParseError> {
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
            return Ok(Statement::CreateExtension(CreateExtensionStatement {
                name,
                if_not_exists,
            }));
        }
        if self.consume_keyword(Keyword::Function) {
            if unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE FUNCTION"));
            }
            return self.parse_create_function(or_replace);
        }
        if self.consume_keyword(Keyword::Cast) {
            if or_replace || unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE CAST"));
            }
            return self.parse_create_cast();
        }
        if self.consume_ident("trigger") {
            if or_replace || unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE TRIGGER"));
            }
            return self.parse_create_trigger();
        }
        if self.consume_ident("subscription") {
            if or_replace || unique || materialized {
                return Err(self.error_at_current("unexpected modifier before CREATE SUBSCRIPTION"));
            }
            return self.parse_create_subscription();
        }
        if self.consume_keyword(Keyword::Index) {
            if or_replace {
                return Err(self.error_at_current("OR REPLACE is only supported for CREATE VIEW"));
            }
            if materialized {
                return Err(self.error_at_current("unexpected MATERIALIZED before CREATE INDEX"));
            }
            let _concurrently = self.consume_keyword(Keyword::Concurrently);
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not, "expected NOT after IF in CREATE INDEX")?;
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF NOT in CREATE INDEX",
                )?;
                true
            } else {
                false
            };
            let (mut name, generated_name) = if self.peek_keyword(Keyword::On) {
                (String::new(), true)
            } else {
                (self.parse_identifier()?, false)
            };
            self.expect_keyword(Keyword::On, "expected ON after CREATE INDEX name")?;
            let _only = self.consume_ident("only");
            let table_name = self.parse_qualified_name()?;
            if self.consume_keyword(Keyword::Using) {
                let _access_method = self.parse_identifier()?;
            }
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CREATE INDEX table name",
            )?;
            let mut columns = Vec::new();
            loop {
                let expr = self.parse_expr()?;
                let fallback = format!("expr{}", columns.len() + 1);
                columns.push(Self::extract_identifier_from_expr(&expr).unwrap_or(fallback));
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
                "expected ')' after CREATE INDEX column list",
            )?;
            if self.consume_ident("include") {
                return Err(self.error_at_current("CREATE INDEX INCLUDE clause is not supported"));
            }
            if self.consume_ident("nulls") {
                return Err(self.error_at_current(
                    "CREATE INDEX NULLS [NOT] DISTINCT clause is not supported",
                ));
            }
            if self.consume_keyword(Keyword::With) {
                return Err(self.error_at_current(
                    "CREATE INDEX WITH (...) storage parameters are not supported",
                ));
            }
            if self.consume_ident("tablespace") {
                return Err(
                    self.error_at_current("CREATE INDEX TABLESPACE clause is not supported")
                );
            }
            if generated_name {
                name = Self::default_index_name(&table_name, &columns);
            }
            if self.consume_keyword(Keyword::Where) {
                let _ = self.parse_expr()?;
            }
            return Ok(Statement::CreateIndex(CreateIndexStatement {
                name,
                generated_name,
                table_name,
                columns,
                unique,
                if_not_exists,
            }));
        }
        if unique {
            return Err(self.error_at_current("expected INDEX after CREATE UNIQUE"));
        }
        // CREATE RECURSIVE VIEW name (columns) AS ...
        // Syntactic sugar for: CREATE VIEW name AS WITH RECURSIVE name(columns) AS (...) SELECT * FROM name
        if self.consume_keyword(Keyword::Recursive) {
            self.expect_keyword(Keyword::View, "expected VIEW after CREATE RECURSIVE")?;
            let name = self.parse_qualified_name()?;
            let column_aliases = self.parse_identifier_list_in_parens()?;
            self.expect_keyword(Keyword::As, "expected AS in CREATE RECURSIVE VIEW")?;
            // Parse the CTE body (non-recursive-term UNION ALL recursive-term)
            let cte_body = self.parse_query()?;
            let view_name_str = name.last().cloned().unwrap_or_default();
            // Build: WITH RECURSIVE name(columns) AS (cte_body) SELECT * FROM name
            let with_query = Query {
                with: Some(WithClause {
                    recursive: true,
                    ctes: vec![CommonTableExpr {
                        name: view_name_str.clone(),
                        column_names: column_aliases,
                        materialized: None,
                        query: cte_body,
                        search_clause: None,
                        cycle_clause: None,
                    }],
                }),
                body: QueryExpr::Select(SelectStatement {
                    quantifier: Some(SelectQuantifier::All),
                    targets: vec![SelectItem {
                        expr: Expr::Wildcard,
                        alias: None,
                    }],
                    from: vec![TableExpression::Relation(TableRef {
                        name: vec![view_name_str],
                        alias: None,
                    })],
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
            return Ok(Statement::CreateView(CreateViewStatement {
                name,
                or_replace,
                materialized: false,
                with_data: true,
                query: with_query,
                if_not_exists: false,
                column_aliases: Vec::new(), // columns are in the CTE itself
            }));
        }
        // Parse optional TEMP/TEMPORARY early so it's available for both VIEW and TABLE
        let temporary_early =
            self.consume_keyword(Keyword::Temporary) || self.consume_keyword(Keyword::Temp);
        if self.consume_keyword(Keyword::View) {
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not, "expected NOT after IF in CREATE VIEW")?;
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF NOT in CREATE VIEW",
                )?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            // Optional column aliases: CREATE VIEW v(a, b, c) AS ...
            let column_aliases = if matches!(self.current_kind(), TokenKind::LParen)
                && !self.peek_keyword(Keyword::As)
            {
                self.parse_identifier_list_in_parens()?
            } else {
                Vec::new()
            };
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
                if_not_exists,
                column_aliases,
            }));
        }
        if or_replace {
            return Err(self.error_at_current("OR REPLACE is only supported for CREATE VIEW"));
        }
        if materialized {
            return Err(self.error_at_current("expected VIEW after CREATE MATERIALIZED"));
        }

        // Parse optional TEMP/TEMPORARY or UNLOGGED before TABLE
        let temporary = temporary_early
            || self.consume_keyword(Keyword::Temporary)
            || self.consume_keyword(Keyword::Temp);
        let unlogged = self.consume_ident("unlogged");

        if self.consume_ident("role") || self.consume_ident("user") {
            if temporary || unlogged {
                return Err(self.error_at_current("unexpected modifier before CREATE ROLE"));
            }
            let name =
                self.parse_role_identifier_with_message("CREATE ROLE requires a role name")?;
            let options = self.parse_role_options("CREATE ROLE")?;
            return Ok(Statement::CreateRole(CreateRoleStatement { name, options }));
        }
        if self.consume_keyword(Keyword::Schema) {
            if temporary || unlogged {
                return Err(self.error_at_current("unexpected modifier before CREATE SCHEMA"));
            }
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
        if self.consume_ident("tablespace") {
            let name = self.parse_identifier()?;
            if self.consume_ident("location")
                && !matches!(self.current_kind(), TokenKind::String(_))
            {
                return Err(self.error_at_current("CREATE TABLESPACE LOCATION requires a string"));
            }
            if matches!(self.current_kind(), TokenKind::String(_)) {
                self.advance();
            }
            // OpenAssay has no durable tablespaces; accept as no-op via schema creation.
            return Ok(Statement::CreateSchema(CreateSchemaStatement {
                name,
                if_not_exists: true,
            }));
        }
        if self.consume_keyword(Keyword::Sequence) {
            let if_not_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Not, "expected NOT after IF in CREATE SEQUENCE")?;
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF NOT in CREATE SEQUENCE",
                )?;
                true
            } else {
                false
            };
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
                if_not_exists,
            }));
        }
        if self.consume_keyword(Keyword::Type) {
            if temporary || unlogged {
                return Err(self.error_at_current("unexpected modifier before CREATE TYPE"));
            }
            let name = self.parse_qualified_name()?;
            self.expect_keyword(Keyword::As, "expected AS after CREATE TYPE name")?;
            self.expect_keyword(Keyword::Enum, "expected ENUM after CREATE TYPE ... AS")?;
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CREATE TYPE ... AS ENUM",
            )?;

            // Parse first enum value
            let first_value = match self.current_kind() {
                TokenKind::String(value) => {
                    let out = value.clone();
                    self.advance();
                    out
                }
                _ => return Err(self.error_at_current("expected string literal for enum value")),
            };
            let mut enum_values = vec![first_value];

            // Parse remaining enum values
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                let value = match self.current_kind() {
                    TokenKind::String(value) => {
                        let out = value.clone();
                        self.advance();
                        out
                    }
                    _ => {
                        return Err(self.error_at_current("expected string literal for enum value"));
                    }
                };
                enum_values.push(value);
            }

            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after enum values",
            )?;
            return Ok(Statement::CreateType(CreateTypeStatement {
                name,
                as_enum: enum_values,
            }));
        }
        if self.consume_keyword(Keyword::Domain) {
            if temporary || unlogged {
                return Err(self.error_at_current("unexpected modifier before CREATE DOMAIN"));
            }
            let name = self.parse_qualified_name()?;
            self.expect_keyword(Keyword::As, "expected AS after CREATE DOMAIN name")?;
            let base_type = self.parse_type_name()?;
            if self.consume_keyword(Keyword::Constraint) || self.consume_ident("constraint") {
                let _ = self.parse_identifier()?;
            }
            let check_constraint = if self.consume_keyword(Keyword::Check) {
                self.expect_token(
                    |k| matches!(k, TokenKind::LParen),
                    "expected '(' after CHECK",
                )?;
                let constraint = self.parse_expr()?;
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after CHECK constraint",
                )?;
                Some(constraint)
            } else {
                None
            };
            return Ok(Statement::CreateDomain(CreateDomainStatement {
                name,
                base_type,
                check_constraint,
            }));
        }
        self.expect_keyword(
            Keyword::Table,
            "expected TABLE, SCHEMA, INDEX, SEQUENCE, VIEW, FUNCTION, TRIGGER, CAST, TYPE, DOMAIN, or SUBSCRIPTION after CREATE",
        )?;

        // Parse optional IF NOT EXISTS clause
        let if_not_exists = if self.consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Not, "expected NOT after IF in CREATE TABLE")?;
            self.expect_keyword(
                Keyword::Exists,
                "expected EXISTS after IF NOT in CREATE TABLE",
            )?;
            true
        } else {
            false
        };

        let name = self.parse_qualified_name()?;

        if self.consume_keyword(Keyword::Partition) {
            if !self.consume_ident("of") {
                return Err(self.error_at_current("expected OF after PARTITION in CREATE TABLE"));
            }
            let parent_name = self.parse_qualified_name()?;
            self.skip_to_statement_end();
            let query = Query {
                with: None,
                body: QueryExpr::Select(SelectStatement {
                    quantifier: Some(SelectQuantifier::All),
                    distinct_on: Vec::new(),
                    targets: vec![SelectItem {
                        expr: Expr::Wildcard,
                        alias: None,
                    }],
                    from: vec![TableExpression::Relation(TableRef {
                        name: parent_name,
                        alias: None,
                    })],
                    where_clause: None,
                    group_by: Vec::new(),
                    having: None,
                    window_definitions: Vec::new(),
                }),
                order_by: Vec::new(),
                limit: Some(Expr::Integer(0)),
                offset: None,
            };
            return Ok(Statement::CreateTable(CreateTableStatement {
                name,
                columns: Vec::new(),
                constraints: Vec::new(),
                if_not_exists,
                temporary,
                unlogged,
                as_select: Some(Box::new(query)),
            }));
        }

        // Check for CREATE TABLE AS SELECT (CTAS)
        if self.consume_keyword(Keyword::As) {
            let query = self.parse_query()?;
            return Ok(Statement::CreateTable(CreateTableStatement {
                name,
                columns: Vec::new(),
                constraints: Vec::new(),
                if_not_exists,
                temporary,
                unlogged,
                as_select: Some(Box::new(query)),
            }));
        }

        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' or AS after CREATE TABLE name",
        )?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
            loop {
                if self.parse_like_table_element()? || self.parse_ignored_table_constraint()? {
                    // handled above
                } else if self.peek_keyword(Keyword::Primary)
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
        }

        // Parse optional WITH (...) storage parameters — ignore
        if self.consume_keyword(Keyword::With)
            && self.consume_if(|k| matches!(k, TokenKind::LParen))
        {
            let mut depth = 1i32;
            while depth > 0 {
                if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                    depth += 1;
                } else if self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                    depth -= 1;
                } else {
                    self.advance();
                }
            }
        }
        self.skip_to_statement_end();

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
            constraints,
            if_not_exists,
            temporary,
            unlogged,
            as_select: None,
        }))
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn parse_create_sequence_options(
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
            if self.consume_keyword(Keyword::As) {
                let _ = self.parse_type_name()?;
                continue;
            }
            if self.consume_ident("owned") {
                self.expect_keyword(Keyword::By, "expected BY after OWNED in CREATE SEQUENCE")?;
                if self.consume_ident("none") {
                    continue;
                }
                let _ = self.parse_qualified_name()?;
                continue;
            }
            break;
        }

        Ok((start, increment, min_value, max_value, cycle, cache))
    }

    pub(super) fn parse_create_function(
        &mut self,
        or_replace: bool,
    ) -> Result<Statement, ParseError> {
        let name = self.parse_qualified_name()?;
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after function name",
        )?;

        let mut params = Vec::new();
        if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
            loop {
                let mut param_tokens = Vec::new();
                let mut paren_depth = 0usize;
                loop {
                    match self.current_kind() {
                        TokenKind::Eof => {
                            return Err(self.error_at_current(
                                "unterminated parameter list in CREATE FUNCTION",
                            ));
                        }
                        TokenKind::Comma if paren_depth == 0 => break,
                        TokenKind::RParen if paren_depth == 0 => break,
                        TokenKind::LParen => {
                            paren_depth += 1;
                            param_tokens.push(self.current_kind().clone());
                            self.advance();
                        }
                        TokenKind::RParen => {
                            paren_depth = paren_depth.saturating_sub(1);
                            param_tokens.push(self.current_kind().clone());
                            self.advance();
                        }
                        _ => {
                            param_tokens.push(self.current_kind().clone());
                            self.advance();
                        }
                    }
                }
                params.push(self.parse_function_param_tokens(&param_tokens));
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' or ',' in parameter list",
                    )?;
                    break;
                }
            }
        }

        let mut return_type = None;
        let mut is_trigger = false;
        let mut body = None;
        let mut language = "sql".to_string();

        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            if self.consume_keyword(Keyword::Returns) {
                let (parsed, trigger_return) = self.parse_function_return_clause()?;
                return_type = parsed;
                if trigger_return {
                    is_trigger = true;
                }
                continue;
            }
            if self.consume_keyword(Keyword::As) {
                let text = match self.current_kind() {
                    TokenKind::String(s) => {
                        let out = s.clone();
                        self.advance();
                        out
                    }
                    _ => {
                        return Err(
                            self.error_at_current("expected dollar-quoted or string function body")
                        );
                    }
                };
                body = Some(text);
                if self.consume_if(|k| matches!(k, TokenKind::Comma))
                    && matches!(self.current_kind(), TokenKind::String(_))
                {
                    self.advance();
                }
                continue;
            }
            if self.consume_keyword(Keyword::Language) || self.consume_ident("language") {
                language = self.parse_identifier_or_string()?;
                continue;
            }
            self.advance();
        }

        let Some(body) = body else {
            return Err(self.error_at_current("expected AS before function body"));
        };

        Ok(Statement::CreateFunction(CreateFunctionStatement {
            name,
            params,
            return_type,
            is_trigger,
            body,
            language,
            or_replace,
        }))
    }

    pub(super) fn parse_create_cast(&mut self) -> Result<Statement, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after CREATE CAST",
        )?;
        let source_type = self.parse_type_name()?;
        self.expect_keyword(Keyword::As, "expected AS in CREATE CAST")?;
        let target_type = self.parse_type_name()?;
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after CREATE CAST type pair",
        )?;

        let mut function_name = None;
        if self.consume_keyword(Keyword::With) {
            if self.consume_keyword(Keyword::Function) || self.consume_ident("function") {
                function_name = Some(self.parse_qualified_name()?);
                self.skip_optional_parenthesized_tokens();
            } else if self.consume_ident("inout") {
                // CREATE CAST ... WITH INOUT is accepted as a no-op.
            } else {
                return Err(
                    self.error_at_current("expected FUNCTION or INOUT after WITH in CREATE CAST")
                );
            }
        } else if self.consume_ident("without")
            && !(self.consume_keyword(Keyword::Function) || self.consume_ident("function"))
        {
            return Err(self.error_at_current("expected FUNCTION after WITHOUT in CREATE CAST"));
        }

        let mut as_assignment = false;
        let mut as_implicit = false;
        if self.consume_keyword(Keyword::As) {
            if self.consume_ident("assignment") {
                as_assignment = true;
            } else if self.consume_ident("implicit") {
                as_implicit = true;
            } else {
                return Err(self
                    .error_at_current("expected ASSIGNMENT or IMPLICIT after AS in CREATE CAST"));
            }
        }

        Ok(Statement::CreateCast(CreateCastStatement {
            source_type,
            target_type,
            function_name,
            as_assignment,
            as_implicit,
        }))
    }

    pub(super) fn parse_create_trigger(&mut self) -> Result<Statement, ParseError> {
        let name = self.parse_identifier()?;
        let timing = if self.consume_ident("before") {
            TriggerTiming::Before
        } else if self.consume_ident("after") {
            TriggerTiming::After
        } else {
            return Err(self.error_at_current("expected BEFORE or AFTER in CREATE TRIGGER"));
        };

        let mut events = Vec::new();
        loop {
            if self.consume_keyword(Keyword::Insert) {
                events.push(TriggerEvent::Insert);
            } else if self.consume_keyword(Keyword::Update) {
                events.push(TriggerEvent::Update);
            } else if self.consume_keyword(Keyword::Delete) {
                events.push(TriggerEvent::Delete);
            } else if self.consume_keyword(Keyword::Truncate) {
                events.push(TriggerEvent::Truncate);
            } else {
                return Err(self.error_at_current(
                    "expected INSERT, UPDATE, DELETE, or TRUNCATE in CREATE TRIGGER event list",
                ));
            }

            if self.consume_keyword(Keyword::Or) || self.consume_ident("or") {
                continue;
            }
            break;
        }

        if !(self.consume_keyword(Keyword::On) || self.consume_ident("on")) {
            return Err(self.error_at_current("expected ON in CREATE TRIGGER"));
        }
        let table_name = self.parse_qualified_name()?;

        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            if self.consume_ident("execute") {
                break;
            }
            self.advance();
        }
        if !(self.consume_keyword(Keyword::Function)
            || self.consume_ident("function")
            || self.consume_ident("procedure"))
        {
            return Err(self.error_at_current("expected FUNCTION or PROCEDURE in CREATE TRIGGER"));
        }
        let function_name = self.parse_qualified_name()?;
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
                        return Err(self.error_at_current(
                            "unterminated trigger argument list in CREATE TRIGGER",
                        ));
                    }
                    _ => self.advance(),
                }
            }
        }

        Ok(Statement::CreateTrigger(CreateTriggerStatement {
            name,
            table_name,
            timing,
            events,
            function_name,
        }))
    }

    pub(super) fn parse_create_subscription(&mut self) -> Result<Statement, ParseError> {
        let name = self.parse_identifier()?;
        if !self.consume_ident("connection") {
            return Err(self.error_at_current("expected CONNECTION after CREATE SUBSCRIPTION name"));
        }
        let connection = match self.current_kind() {
            TokenKind::String(value) => {
                let out = value.clone();
                self.advance();
                out
            }
            _ => {
                return Err(self.error_at_current(
                    "CREATE SUBSCRIPTION CONNECTION requires a single-quoted string",
                ));
            }
        };
        if !self.consume_ident("publication") {
            return Err(
                self.error_at_current("expected PUBLICATION after CREATE SUBSCRIPTION CONNECTION")
            );
        }
        let publication = self.parse_identifier()?;
        let options = if self.consume_keyword(Keyword::With) {
            self.parse_subscription_options()?
        } else {
            SubscriptionOptions {
                copy_data: true,
                slot_name: None,
            }
        };
        Ok(Statement::CreateSubscription(CreateSubscriptionStatement {
            name,
            connection,
            publication,
            options,
        }))
    }

    pub(super) fn parse_subscription_options(&mut self) -> Result<SubscriptionOptions, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after WITH in CREATE SUBSCRIPTION",
        )?;
        let mut options = SubscriptionOptions {
            copy_data: true,
            slot_name: None,
        };
        loop {
            let Some(option) = self.take_keyword_or_identifier_upper() else {
                return Err(self.error_at_current("expected subscription option"));
            };
            if self.consume_if(|k| matches!(k, TokenKind::Equal)) {
                // optional '='
            }
            match option.as_str() {
                "COPY_DATA" => {
                    let Some(value) = self.take_keyword_or_identifier_upper() else {
                        return Err(self.error_at_current("COPY_DATA requires TRUE or FALSE"));
                    };
                    match value.as_str() {
                        "TRUE" => options.copy_data = true,
                        "FALSE" => options.copy_data = false,
                        _ => {
                            return Err(self.error_at_current("COPY_DATA requires TRUE or FALSE"));
                        }
                    }
                }
                "SLOT_NAME" => match self.current_kind() {
                    TokenKind::String(value) => {
                        let out = value.clone();
                        self.advance();
                        options.slot_name = Some(out);
                    }
                    _ => {
                        return Err(
                            self.error_at_current("SLOT_NAME requires a single-quoted string")
                        );
                    }
                },
                _ => {
                    return Err(
                        self.error_at_current(&format!("unsupported subscription option {option}"))
                    );
                }
            }
            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after subscription options",
        )?;
        Ok(options)
    }

    pub(super) fn parse_table_constraint(&mut self) -> Result<TableConstraint, ParseError> {
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

    pub(super) fn parse_column_definition(&mut self) -> Result<ColumnDefinition, ParseError> {
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

    pub(super) fn parse_like_table_element(&mut self) -> Result<bool, ParseError> {
        if !self.consume_keyword(Keyword::Like) {
            return Ok(false);
        }
        let _ = self.parse_qualified_name()?;
        while !matches!(
            self.current_kind(),
            TokenKind::Comma | TokenKind::RParen | TokenKind::Eof
        ) {
            self.advance();
        }
        Ok(true)
    }

    pub(super) fn parse_ignored_table_constraint(&mut self) -> Result<bool, ParseError> {
        let save = self.idx;
        if self.consume_keyword(Keyword::Constraint) {
            let _ = self.parse_identifier()?;
        }

        if self.consume_keyword(Keyword::Check) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after CHECK",
            )?;
            let _ = self.parse_expr()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after CHECK expression",
            )?;
            if self.consume_keyword(Keyword::No) {
                let _ = self.consume_ident("inherit");
            }
            return Ok(true);
        }

        if self.consume_ident("exclude") {
            let mut depth = 0usize;
            while !matches!(self.current_kind(), TokenKind::Eof) {
                match self.current_kind() {
                    TokenKind::LParen => {
                        depth += 1;
                        self.advance();
                    }
                    TokenKind::RParen if depth == 0 => break,
                    TokenKind::RParen => {
                        depth -= 1;
                        self.advance();
                    }
                    TokenKind::Comma if depth == 0 => break,
                    _ => self.advance(),
                }
            }
            return Ok(true);
        }

        self.idx = save;
        Ok(false)
    }

    pub(super) fn default_index_name(table_name: &[String], columns: &[String]) -> String {
        let relation = table_name
            .last()
            .cloned()
            .unwrap_or_else(|| "index".to_string());
        if columns.is_empty() {
            return format!("{relation}_idx");
        }
        format!("{relation}_{}_idx", columns.join("_"))
    }

    pub(super) fn parse_references_clause(&mut self) -> Result<ForeignKeyReference, ParseError> {
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

    pub(super) fn parse_fk_action(&mut self) -> Result<ForeignKeyAction, ParseError> {
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

    pub(super) fn parse_optional_fk_actions(
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

    pub(super) fn parse_drop_statement(&mut self) -> Result<Statement, ParseError> {
        let materialized = self.consume_keyword(Keyword::Materialized);
        if !materialized && (self.consume_ident("role") || self.consume_ident("user")) {
            return self.parse_drop_role_statement();
        }
        if self.consume_ident("subscription") {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF in DROP SUBSCRIPTION",
                )?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            return Ok(Statement::DropSubscription(DropSubscriptionStatement {
                name,
                if_exists,
            }));
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
            let mut names = vec![self.parse_qualified_name()?];
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                names.push(self.parse_qualified_name()?);
            }
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropTable(DropTableStatement {
                names,
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
            let _concurrently = self.consume_keyword(Keyword::Concurrently);
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP INDEX")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                let _ = self.parse_qualified_name()?;
            }
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
        if self.consume_keyword(Keyword::Type) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP TYPE")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropType(DropTypeStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Domain) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP DOMAIN")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropDomain(DropDomainStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_keyword(Keyword::Extension) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(
                    Keyword::Exists,
                    "expected EXISTS after IF in DROP EXTENSION",
                )?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            return Ok(Statement::DropExtension(DropExtensionStatement {
                name,
                if_exists,
            }));
        }
        if self.consume_keyword(Keyword::Function) {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF")?;
                true
            } else {
                false
            };
            let name = self.parse_qualified_name()?;
            self.consume_optional_drop_function_signature()?;
            while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                let _ = self.parse_qualified_name()?;
                self.consume_optional_drop_function_signature()?;
            }
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropFunction(DropFunctionStatement {
                name,
                if_exists,
                behavior,
            }));
        }
        if self.consume_ident("trigger") {
            let if_exists = if self.consume_keyword(Keyword::If) {
                self.expect_keyword(Keyword::Exists, "expected EXISTS after IF")?;
                true
            } else {
                false
            };
            let name = self.parse_identifier()?;
            if !(self.consume_keyword(Keyword::On) || self.consume_ident("on")) {
                return Err(self.error_at_current("expected ON in DROP TRIGGER"));
            }
            let table_name = self.parse_qualified_name()?;
            let behavior = self.parse_drop_behavior()?;
            return Ok(Statement::DropTrigger(DropTriggerStatement {
                name,
                table_name,
                if_exists,
                behavior,
            }));
        }
        Err(self.error_at_current(
            "expected TABLE, SCHEMA, INDEX, SEQUENCE, VIEW, FUNCTION, TRIGGER, SUBSCRIPTION, or EXTENSION after DROP",
        ))
    }

    pub(super) fn parse_drop_behavior(&mut self) -> Result<DropBehavior, ParseError> {
        if self.consume_keyword(Keyword::Cascade) {
            return Ok(DropBehavior::Cascade);
        }
        if self.consume_keyword(Keyword::Restrict) {
            return Ok(DropBehavior::Restrict);
        }
        Ok(DropBehavior::Restrict)
    }

    pub(super) fn parse_drop_role_statement(&mut self) -> Result<Statement, ParseError> {
        let if_exists = if self.consume_keyword(Keyword::If) {
            self.expect_keyword(Keyword::Exists, "expected EXISTS after IF in DROP ROLE")?;
            true
        } else {
            false
        };
        let name = self.parse_role_identifier_with_message("DROP ROLE requires a role name")?;
        Ok(Statement::DropRole(DropRoleStatement { name, if_exists }))
    }

    pub(super) fn consume_optional_drop_function_signature(&mut self) -> Result<(), ParseError> {
        if !self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            return Ok(());
        }

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
                    return Err(
                        self.error_at_current("unterminated function signature in DROP FUNCTION")
                    );
                }
                _ => self.advance(),
            }
        }
        Ok(())
    }

    pub(super) fn parse_alter_statement(&mut self) -> Result<Statement, ParseError> {
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

    pub(super) fn parse_alter_table_statement(&mut self) -> Result<Statement, ParseError> {
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
            if self.consume_keyword(Keyword::Type) {
                let data_type = self.parse_type_name()?;
                let using = if self.consume_keyword(Keyword::Using) || self.consume_ident("using") {
                    Some(self.parse_expr()?)
                } else {
                    None
                };
                AlterTableAction::SetColumnType {
                    name,
                    data_type,
                    using,
                }
            } else if self.consume_keyword(Keyword::Set) {
                if self.consume_keyword(Keyword::Default) {
                    AlterTableAction::SetColumnDefault {
                        name,
                        default: Some(self.parse_expr()?),
                    }
                } else if self.consume_keyword(Keyword::Type) {
                    let data_type = self.parse_type_name()?;
                    let using =
                        if self.consume_keyword(Keyword::Using) || self.consume_ident("using") {
                            Some(self.parse_expr()?)
                        } else {
                            None
                        };
                    AlterTableAction::SetColumnType {
                        name,
                        data_type,
                        using,
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

    pub(super) fn parse_alter_sequence_statement(&mut self) -> Result<Statement, ParseError> {
        let if_exists = if self.consume_keyword(Keyword::If) {
            self.expect_keyword(
                Keyword::Exists,
                "expected EXISTS after IF in ALTER SEQUENCE",
            )?;
            true
        } else {
            false
        };
        let name = self.parse_qualified_name()?;
        let mut actions = Vec::new();
        loop {
            if self.consume_keyword(Keyword::As) {
                let _ = self.parse_type_name()?;
                actions.push(AlterSequenceAction::NoOp);
                continue;
            }
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
            if self.consume_keyword(Keyword::Set) {
                if self.consume_ident("logged") || self.consume_ident("unlogged") {
                    actions.push(AlterSequenceAction::NoOp);
                    continue;
                }
                return Err(self
                    .error_at_current("expected LOGGED or UNLOGGED after SET in ALTER SEQUENCE"));
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
            if_exists,
            actions,
        }))
    }

    pub(super) fn parse_alter_view_statement(
        &mut self,
        materialized: bool,
    ) -> Result<Statement, ParseError> {
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

    pub(super) fn parse_alter_role_statement(&mut self) -> Result<Statement, ParseError> {
        let name = self.parse_role_identifier_with_message("ALTER ROLE requires a role name")?;
        let options = self.parse_role_options("ALTER ROLE")?;
        Ok(Statement::AlterRole(AlterRoleStatement { name, options }))
    }

    pub(super) fn parse_type_name(&mut self) -> Result<TypeName, ParseError> {
        let base = self.parse_identifier()?.to_ascii_lowercase();
        let mut ty = match base.as_str() {
            "bool" | "boolean" => TypeName::Bool,
            "smallint" | "int2" => TypeName::Int2,
            "int" | "integer" | "int4" => TypeName::Int4,
            "bigint" | "int8" => TypeName::Int8,
            "xid" => TypeName::Int8,
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
            "bit" | "varbit" => TypeName::Text,
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
            "time" => TypeName::Time,
            "timestamp" => TypeName::Timestamp,
            "timestamptz" => TypeName::TimestampTz,
            "interval" => TypeName::Interval,
            "serial" => TypeName::Serial,
            "smallserial" | "serial2" => TypeName::Serial,
            "bigserial" | "serial8" => TypeName::BigSerial,
            "numeric" | "decimal" => TypeName::Numeric,
            "money" => TypeName::Numeric, // treat money as numeric for now
            "vector" => TypeName::Vector(None),
            "name" => TypeName::Name,
            // PostgreSQL underscore-prefixed array type aliases
            other if other.starts_with('_') => {
                // _int2 => int2[], _text => text[], etc.
                let inner = &other[1..];
                let inner_ty = match inner {
                    "bool" | "boolean" => TypeName::Bool,
                    "int2" | "smallint" => TypeName::Int2,
                    "int4" | "integer" | "int" => TypeName::Int4,
                    "int8" | "bigint" => TypeName::Int8,
                    "float4" | "real" => TypeName::Float4,
                    "float8" => TypeName::Float8,
                    "text" | "varchar" | "char" => TypeName::Text,
                    "name" => TypeName::Name,
                    "numeric" | "decimal" => TypeName::Numeric,
                    _ => {
                        return Err(
                            self.error_at_current(&format!("unsupported type name \"{other}\""))
                        );
                    }
                };
                return Ok(TypeName::Array(Box::new(inner_ty)));
            }
            _other => TypeName::Text,
        };

        // Parse vector(dim) modifier; ignore other type modifiers like varchar(255).
        if let TypeName::Vector(_) = ty {
            if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
                let dim = match self.current_kind() {
                    TokenKind::Integer(v) => {
                        let value = *v;
                        self.advance();
                        if value <= 0 {
                            return Err(self.error_at_current("vector dimension must be positive"));
                        }
                        Some(value as usize)
                    }
                    _ => {
                        return Err(
                            self.error_at_current("expected integer dimension in vector(...)")
                        );
                    }
                };
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' after vector dimension",
                )?;
                ty = TypeName::Vector(dim);
            }
        } else if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
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

        if matches!(ty, TypeName::Timestamp | TypeName::Time) {
            if self.consume_keyword(Keyword::With) || self.consume_ident("with") {
                self.expect_token(
                    |k| {
                        matches!(k, TokenKind::Keyword(Keyword::Time))
                            || matches!(
                                k,
                                TokenKind::Identifier(word) if word.eq_ignore_ascii_case("time")
                            )
                    },
                    "expected TIME after WITH",
                )?;
                self.expect_token(
                    |k| matches!(k, TokenKind::Identifier(word) if word.eq_ignore_ascii_case("zone")),
                    "expected ZONE after WITH TIME",
                )?;
                if matches!(ty, TypeName::Timestamp) {
                    ty = TypeName::TimestampTz;
                }
            } else if self.consume_ident("without") {
                self.expect_token(
                    |k| {
                        matches!(k, TokenKind::Keyword(Keyword::Time))
                            || matches!(
                                k,
                                TokenKind::Identifier(word) if word.eq_ignore_ascii_case("time")
                            )
                    },
                    "expected TIME after WITHOUT",
                )?;
                self.expect_token(
                    |k| matches!(k, TokenKind::Identifier(word) if word.eq_ignore_ascii_case("zone")),
                    "expected ZONE after WITHOUT TIME",
                )?;
            }
        }

        // Handle array type suffix: int4[], text[][], etc.
        let mut result_ty = ty;
        while self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
            if !self.consume_if(|k| matches!(k, TokenKind::RBracket)) {
                if matches!(self.current_kind(), TokenKind::Integer(_)) {
                    self.advance();
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RBracket),
                    "expected ']' after '[' in array type",
                )?;
            }
            result_ty = TypeName::Array(Box::new(result_ty));
        }

        Ok(result_ty)
    }

    pub(super) fn try_parse_type_name(&self, ident: &str) -> Result<TypeName, ()> {
        match ident.to_ascii_lowercase().as_str() {
            "bool" | "boolean" => Ok(TypeName::Bool),
            "int2" | "smallint" => Ok(TypeName::Int2),
            "int4" | "integer" | "int" => Ok(TypeName::Int4),
            "int8" | "bigint" => Ok(TypeName::Int8),
            "xid" => Ok(TypeName::Int8),
            "float4" | "real" => Ok(TypeName::Float4),
            "float8" | "double" => Ok(TypeName::Float8),
            "text" => Ok(TypeName::Text),
            "bit" | "varbit" => Ok(TypeName::Text),
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
            "serial2" | "smallserial" => Ok(TypeName::Serial),
            "bigserial" => Ok(TypeName::BigSerial),
            "numeric" | "decimal" => Ok(TypeName::Numeric),
            "vector" => Ok(TypeName::Vector(None)),
            "name" => Ok(TypeName::Name),
            _ => Err(()),
        }
    }

    pub(super) fn looks_like_function_type_name(&self, word: &str) -> bool {
        self.try_parse_type_name(word).is_ok()
            || matches!(
                word,
                "void"
                    | "record"
                    | "trigger"
                    | "refcursor"
                    | "any"
                    | "anyelement"
                    | "anyarray"
                    | "anyrange"
                    | "anycompatible"
                    | "anycompatiblearray"
                    | "anycompatiblerange"
                    | "cstring"
                    | "internal"
                    | "bpchar"
                    | "oid"
            )
            || word.starts_with('_')
    }

    pub(super) fn infer_function_type_name(&self, tokens: &[TokenKind]) -> TypeName {
        let mut words = Vec::new();
        let mut array_depth = 0usize;
        let mut saw_percent = false;
        let mut saw_setof = false;

        for token in tokens {
            match token {
                TokenKind::LBracket => array_depth += 1,
                TokenKind::Operator(op) if op == "%" => saw_percent = true,
                _ => {
                    if let Some(word) = self.token_word(token) {
                        if word == "setof" {
                            saw_setof = true;
                            continue;
                        }
                        words.push(word);
                    }
                }
            }
        }

        if saw_percent {
            return TypeName::Text;
        }

        let mut base_word = if words.is_empty() {
            "text".to_string()
        } else if words.len() >= 2 && words[0] == "double" && words[1] == "precision" {
            "float8".to_string()
        } else if saw_setof {
            words.last().cloned().unwrap_or_else(|| "text".to_string())
        } else {
            words.last().cloned().unwrap_or_else(|| "text".to_string())
        };

        let mut explicit_array = 0usize;
        while let Some(stripped) = base_word.strip_suffix("[]") {
            explicit_array += 1;
            base_word = stripped.to_string();
        }
        if let Some(inner) = base_word.strip_prefix('_') {
            explicit_array += 1;
            base_word = inner.to_string();
        }

        let mut ty = if let Ok(found) = self.try_parse_type_name(&base_word) {
            found
        } else {
            match base_word.as_str() {
                "name" => TypeName::Name,
                "bpchar" => TypeName::Char,
                "oid" => TypeName::Int8,
                "record" | "trigger" | "refcursor" | "void" | "any" | "anyelement" | "anyarray"
                | "anyrange" | "anycompatible" | "anycompatiblearray" | "anycompatiblerange"
                | "cstring" | "internal" => TypeName::Text,
                _ => TypeName::Text,
            }
        };

        for _ in 0..(array_depth + explicit_array) {
            ty = TypeName::Array(Box::new(ty));
        }
        ty
    }

    pub(super) fn parse_function_param_tokens(&self, tokens: &[TokenKind]) -> FunctionParam {
        let mut trimmed = Vec::with_capacity(tokens.len());
        let mut depth = 0usize;
        for token in tokens {
            match token {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => depth = depth.saturating_sub(1),
                _ => {}
            }
            if depth == 0
                && (matches!(token, TokenKind::Keyword(Keyword::Default))
                    || matches!(token, TokenKind::Equal | TokenKind::ColonEquals))
            {
                break;
            }
            trimmed.push(token.clone());
        }

        let mut idx = 0usize;
        let mut mode = FunctionParamMode::In;
        if trimmed
            .get(idx)
            .and_then(|t| self.token_word(t))
            .is_some_and(|w| w == "variadic")
        {
            idx += 1;
        }
        if let Some(word) = trimmed.get(idx).and_then(|t| self.token_word(t)) {
            if word == "in" {
                mode = FunctionParamMode::In;
                idx += 1;
            } else if word == "out" {
                mode = FunctionParamMode::Out;
                idx += 1;
            } else if word == "inout" {
                mode = FunctionParamMode::InOut;
                idx += 1;
            }
        }

        let remaining = if idx < trimmed.len() {
            &trimmed[idx..]
        } else {
            &[]
        };
        if remaining.is_empty() {
            return FunctionParam {
                name: None,
                data_type: TypeName::Text,
                mode,
            };
        }

        let (name, type_tokens): (Option<String>, &[TokenKind]) = if remaining.len() == 1 {
            (None, remaining)
        } else if let Some(first_word) = self.token_word(&remaining[0]) {
            if self.looks_like_function_type_name(&first_word) {
                (None, remaining)
            } else {
                (Some(first_word), &remaining[1..])
            }
        } else {
            (None, remaining)
        };

        FunctionParam {
            name,
            data_type: self.infer_function_type_name(type_tokens),
            mode,
        }
    }

    pub(super) fn parse_function_return_clause(
        &mut self,
    ) -> Result<(Option<FunctionReturnType>, bool), ParseError> {
        if self.consume_keyword(Keyword::Table) {
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after TABLE",
            )?;
            let mut cols = Vec::new();
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                loop {
                    let mut col_tokens = Vec::new();
                    let mut depth = 0usize;
                    loop {
                        match self.current_kind() {
                            TokenKind::Eof => {
                                return Err(
                                    self.error_at_current("unterminated RETURNS TABLE column list")
                                );
                            }
                            TokenKind::Comma if depth == 0 => break,
                            TokenKind::RParen if depth == 0 => break,
                            TokenKind::LParen => {
                                depth += 1;
                                col_tokens.push(self.current_kind().clone());
                                self.advance();
                            }
                            TokenKind::RParen => {
                                depth = depth.saturating_sub(1);
                                col_tokens.push(self.current_kind().clone());
                                self.advance();
                            }
                            _ => {
                                col_tokens.push(self.current_kind().clone());
                                self.advance();
                            }
                        }
                    }
                    let (col_name, col_type_tokens): (String, &[TokenKind]) =
                        if let Some(first) = col_tokens.first().and_then(|t| self.token_word(t)) {
                            if col_tokens.len() > 1 {
                                (first, &col_tokens[1..])
                            } else {
                                (format!("column{}", cols.len() + 1), col_tokens.as_slice())
                            }
                        } else {
                            (format!("column{}", cols.len() + 1), col_tokens.as_slice())
                        };
                    cols.push(ColumnDefinition {
                        name: col_name,
                        data_type: self.infer_function_type_name(col_type_tokens),
                        nullable: true,
                        identity: false,
                        primary_key: false,
                        unique: false,
                        references: None,
                        check: None,
                        default: None,
                    });
                    if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        continue;
                    }
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after RETURNS TABLE column list",
                    )?;
                    break;
                }
            }
            return Ok((Some(FunctionReturnType::Table(cols)), false));
        }

        let mut return_tokens = Vec::new();
        let mut depth = 0usize;
        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            if depth == 0
                && (matches!(
                    self.current_kind(),
                    TokenKind::Keyword(Keyword::As | Keyword::Language)
                ) || matches!(self.current_kind(), TokenKind::Identifier(id) if id.eq_ignore_ascii_case("as") || id.eq_ignore_ascii_case("language") || id.eq_ignore_ascii_case("immutable") || id.eq_ignore_ascii_case("stable") || id.eq_ignore_ascii_case("volatile") || id.eq_ignore_ascii_case("strict") || id.eq_ignore_ascii_case("parallel") || id.eq_ignore_ascii_case("cost") || id.eq_ignore_ascii_case("rows") || id.eq_ignore_ascii_case("security")))
            {
                break;
            }
            match self.current_kind() {
                TokenKind::LParen => depth += 1,
                TokenKind::RParen => depth = depth.saturating_sub(1),
                _ => {}
            }
            return_tokens.push(self.current_kind().clone());
            self.advance();
        }

        let is_trigger = return_tokens.iter().any(|token| {
            self.token_word(token)
                .is_some_and(|word| word.eq_ignore_ascii_case("trigger"))
        });

        if return_tokens.is_empty() {
            return Ok((None, is_trigger));
        }
        Ok((
            Some(FunctionReturnType::Type(
                self.infer_function_type_name(&return_tokens),
            )),
            is_trigger,
        ))
    }
}
