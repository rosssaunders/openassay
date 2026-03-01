#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_expr_bp(0)
    }

    pub(super) fn parse_expr_bp(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
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
            // Array subscript: arr[1] or arr[1:3]
            if self
                .peek_nth_kind(0)
                .is_some_and(|k| matches!(k, TokenKind::LBracket))
            {
                let l_bp = 12; // Same precedence as typecast
                if l_bp < min_bp {
                    break;
                }
                self.advance(); // consume '['

                // Check for empty-start slice [:end] or [:]
                if self
                    .peek_nth_kind(0)
                    .is_some_and(|k| matches!(k, TokenKind::Colon))
                {
                    self.advance(); // consume ':'
                    let end_expr = if self
                        .peek_nth_kind(0)
                        .is_some_and(|k| matches!(k, TokenKind::RBracket))
                    {
                        None // [:]
                    } else {
                        Some(Box::new(self.parse_expr()?)) // [:end]
                    };
                    self.expect_token(
                        |k| matches!(k, TokenKind::RBracket),
                        "expected ']' after array slice",
                    )?;
                    lhs = Expr::ArraySlice {
                        expr: Box::new(lhs),
                        start: None,
                        end: end_expr,
                    };
                } else {
                    // Parse first expression
                    let first_expr = self.parse_expr()?;

                    // Check for slice syntax ':'
                    if self
                        .peek_nth_kind(0)
                        .is_some_and(|k| matches!(k, TokenKind::Colon))
                    {
                        self.advance(); // consume ':'

                        // Check for end expression
                        if self
                            .peek_nth_kind(0)
                            .is_some_and(|k| matches!(k, TokenKind::RBracket))
                        {
                            // arr[start:]
                            self.expect_token(
                                |k| matches!(k, TokenKind::RBracket),
                                "expected ']' after array slice",
                            )?;
                            lhs = Expr::ArraySlice {
                                expr: Box::new(lhs),
                                start: Some(Box::new(first_expr)),
                                end: None,
                            };
                        } else {
                            // arr[start:end]
                            let end_expr = self.parse_expr()?;
                            self.expect_token(
                                |k| matches!(k, TokenKind::RBracket),
                                "expected ']' after array slice",
                            )?;
                            lhs = Expr::ArraySlice {
                                expr: Box::new(lhs),
                                start: Some(Box::new(first_expr)),
                                end: Some(Box::new(end_expr)),
                            };
                        }
                    } else {
                        // Simple subscript: arr[index]
                        self.expect_token(
                            |k| matches!(k, TokenKind::RBracket),
                            "expected ']' after array subscript",
                        )?;
                        lhs = Expr::ArraySubscript {
                            expr: Box::new(lhs),
                            index: Box::new(first_expr),
                        };
                    }
                }
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
            if let TokenKind::Operator(op) = self.current_kind()
                && matches!(op.as_str(), "~" | "~*" | "!~" | "!~*")
            {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                let operator = op.clone();
                self.advance();
                lhs = self.parse_regex_match_expr(lhs, &operator)?;
                continue;
            }
            if self.peek_ident("operator")
                && self
                    .peek_nth_kind(1)
                    .is_some_and(|k| matches!(k, TokenKind::LParen))
            {
                let l_bp = 5;
                if l_bp < min_bp {
                    break;
                }
                let operator = self.parse_operator_wrapper_symbol()?;
                if matches!(operator.as_str(), "~" | "~*" | "!~" | "!~*") {
                    lhs = self.parse_regex_match_expr(lhs, &operator)?;
                    continue;
                }
                return Err(self.error_at_current("unsupported OPERATOR() expression"));
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
                if self.consume_keyword(Keyword::True) {
                    lhs = Expr::BooleanTest {
                        expr: Box::new(lhs),
                        test_type: BooleanTestType::True,
                        negated,
                    };
                    continue;
                }
                if self.consume_keyword(Keyword::False) {
                    lhs = Expr::BooleanTest {
                        expr: Box::new(lhs),
                        test_type: BooleanTestType::False,
                        negated,
                    };
                    continue;
                }
                // IS [NOT] UNKNOWN
                if matches!(self.current_kind(), TokenKind::Identifier(id) if id.eq_ignore_ascii_case("unknown"))
                {
                    self.advance();
                    lhs = Expr::BooleanTest {
                        expr: Box::new(lhs),
                        test_type: BooleanTestType::Unknown,
                        negated,
                    };
                    continue;
                }
                self.expect_keyword(
                    Keyword::Distinct,
                    "expected NULL, TRUE, FALSE, UNKNOWN, or DISTINCT after IS",
                )?;
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
                // Check if this is a subquery: = ANY (SELECT ...)
                let rhs = if self.peek_keyword(Keyword::Select)
                    || self.peek_keyword(Keyword::With)
                    || self.peek_keyword(Keyword::Values)
                {
                    let query = self.parse_query()?;
                    Expr::ArraySubquery(Box::new(query))
                } else {
                    self.parse_expr()?
                };
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

    pub(super) fn parse_prefix_expr(&mut self) -> Result<Expr, ParseError> {
        if self.consume_keyword(Keyword::Array) {
            if self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
                return Ok(Expr::ArrayConstructor(
                    self.parse_array_constructor_elements()?,
                ));
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
        // ROW constructor: ROW(expr, expr, ...)
        if self.peek_keyword(Keyword::Row)
            && self
                .peek_nth_kind(1)
                .is_some_and(|k| matches!(k, TokenKind::LParen))
        {
            self.advance(); // consume ROW
            self.advance(); // consume (
            let mut fields = Vec::new();
            if !matches!(self.current_kind(), TokenKind::RParen) {
                fields.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    fields.push(self.parse_expr()?);
                }
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' after ROW constructor",
            )?;
            return Ok(Expr::RowConstructor(fields));
        }
        // INTERVAL typed literals with optional precision and field qualifiers:
        // INTERVAL '1 day', INTERVAL(2) '1.23', INTERVAL '1' YEAR TO MONTH, ...
        if self.peek_keyword(Keyword::Interval) {
            let save = self.idx;
            self.advance(); // INTERVAL keyword
            self.consume_optional_type_modifiers();
            if let Some(TokenKind::String(value)) = self.peek_nth_kind(0) {
                let value_str = value.clone();
                self.advance();
                self.parse_optional_interval_qualifier()?;
                return Ok(Expr::TypedLiteral {
                    type_name: "interval".to_string(),
                    value: value_str,
                });
            }
            self.idx = save;
        }

        // Typed literals with optional precision/timezone modifiers.
        if self.peek_keyword(Keyword::Timestamp) || self.peek_keyword(Keyword::Time) {
            let save = self.idx;
            let type_name = if self.consume_keyword(Keyword::Timestamp) {
                "timestamp".to_string()
            } else {
                self.advance(); // TIME keyword
                "time".to_string()
            };
            self.consume_optional_type_modifiers();
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
                    |k| {
                        matches!(
                            k,
                            TokenKind::Identifier(word) if word.eq_ignore_ascii_case("zone")
                        )
                    },
                    "expected ZONE after WITH TIME",
                )?;
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
                    |k| {
                        matches!(
                            k,
                            TokenKind::Identifier(word) if word.eq_ignore_ascii_case("zone")
                        )
                    },
                    "expected ZONE after WITHOUT TIME",
                )?;
            }
            if let Some(TokenKind::String(value)) = self.peek_nth_kind(0) {
                let value_str = value.clone();
                self.advance();
                return Ok(Expr::TypedLiteral {
                    type_name,
                    value: value_str,
                });
            }
            self.idx = save;
        }
        // Typed literals: DATE 'literal', TIME 'literal', TIMESTAMP 'literal', INTERVAL 'literal'
        // Only match if followed by a string literal (not a parenthesis for function calls)
        if (self.peek_keyword(Keyword::Date)
            || self.peek_keyword(Keyword::Time)
            || self.peek_keyword(Keyword::Timestamp))
            && self
                .peek_nth_kind(1)
                .is_some_and(|k| matches!(k, TokenKind::String(_)))
        {
            let type_name = if self.consume_keyword(Keyword::Date) {
                "date"
            } else if self.consume_keyword(Keyword::Time) {
                "time"
            } else {
                self.advance();
                "timestamp"
            };

            if let Some(TokenKind::String(value)) = self.peek_nth_kind(0) {
                let value_str = value.clone();
                self.advance();
                return Ok(Expr::TypedLiteral {
                    type_name: type_name.to_string(),
                    value: value_str,
                });
            }
            return Err(self.error_at_current("expected string literal after type keyword"));
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
            // Check for comma → row constructor (a, b, c)
            if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                let mut fields = vec![expr];
                fields.push(self.parse_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    fields.push(self.parse_expr()?);
                }
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close row constructor",
                )?;
                return Ok(Expr::RowConstructor(fields));
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close expression",
            )?;
            return Ok(expr);
        }

        if let Some(TokenKind::Identifier(prefix)) = self.peek_nth_kind(0)
            && (prefix.eq_ignore_ascii_case("b") || prefix.eq_ignore_ascii_case("x"))
            && let Some(TokenKind::String(value)) = self.peek_nth_kind(1)
        {
            let out = value.clone();
            self.advance();
            self.advance();
            return Ok(Expr::String(out));
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
                self.consume_optional_unicode_escape_clause()?;
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
                | Keyword::Grouping
                | Keyword::Date
                | Keyword::Time
                | Keyword::Timestamp
                | Keyword::Interval,
            ) => self.parse_identifier_expr(),
            TokenKind::Keyword(kw) if Self::is_unreserved_keyword(kw) => {
                self.parse_identifier_expr()
            }
            _ => Err(self.error_at_current("expected expression")),
        }
    }

    pub(super) fn parse_identifier_expr(&mut self) -> Result<Expr, ParseError> {
        let mut name = vec![self.parse_expr_identifier()?];

        // Handle type-name 'literal' syntax for types like bool, int, etc.
        if name.len() == 1 {
            let type_lower = name[0].to_ascii_lowercase();
            let is_type_literal = matches!(
                type_lower.as_str(),
                "bool"
                    | "boolean"
                    | "int"
                    | "integer"
                    | "int2"
                    | "int4"
                    | "int8"
                    | "smallint"
                    | "bigint"
                    | "float"
                    | "float4"
                    | "float8"
                    | "real"
                    | "numeric"
                    | "decimal"
                    | "text"
                    | "varchar"
                    | "bytea"
                    | "uuid"
                    | "json"
                    | "jsonb"
                    | "date"
                    | "time"
                    | "timetz"
                    | "timestamp"
                    | "timestamptz"
                    | "interval"
                    | "regclass"
                    | "regnamespace"
                    | "oid"
                    | "name"
            );
            if is_type_literal && let Some(TokenKind::String(value)) = self.peek_nth_kind(0) {
                let value_str = value.clone();
                self.advance();
                // Normalize type name for TypedLiteral
                let normalized = match type_lower.as_str() {
                    "bool" | "boolean" => "boolean",
                    "int" | "integer" | "int4" => "integer",
                    "int2" | "smallint" => "smallint",
                    "int8" | "bigint" => "bigint",
                    "float" | "float8" => "double precision",
                    "float4" | "real" => "real",
                    "numeric" | "decimal" => "numeric",
                    "text" | "varchar" => "text",
                    "timetz" => "time",
                    "regnamespace" => "regclass",
                    _ => &type_lower,
                };
                return Ok(Expr::TypedLiteral {
                    type_name: normalized.to_string(),
                    value: value_str,
                });
            }
        }

        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            // Check if this is a qualified wildcard (e.g., table.*)
            if self.consume_if(|k| matches!(k, TokenKind::Star)) {
                return Ok(Expr::QualifiedWildcard(name));
            }
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
                if fn_name == "extract" {
                    // EXTRACT(field FROM source) or extract('field', source)
                    let field = self.parse_expr_bp(6)?;
                    if self.peek_keyword(Keyword::From) {
                        self.expect_keyword(Keyword::From, "expected FROM in EXTRACT")?;
                        let source = self.parse_expr_bp(6)?;
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after EXTRACT arguments",
                        )?;
                        args = vec![field, source];
                    } else {
                        // Comma-separated form: extract('year', ts)
                        self.expect_token(
                            |k| matches!(k, TokenKind::Comma),
                            "expected ',' or FROM in EXTRACT",
                        )?;
                        let source = self.parse_expr_bp(6)?;
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after EXTRACT arguments",
                        )?;
                        args = vec![field, source];
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
                } else if fn_name == "position" {
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
                } else if fn_name == "substring" {
                    // SUBSTRING(string FROM start [FOR length])
                    let string = self.parse_expr_bp(6)?;
                    if self.consume_ident("similar") {
                        let pattern = self.parse_expr_bp(6)?;
                        self.expect_keyword(Keyword::Escape, "expected ESCAPE in SUBSTRING")?;
                        let escape = self.parse_expr_bp(6)?;
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after SUBSTRING arguments",
                        )?;
                        args = vec![string, pattern, escape];
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
                    if self.consume_keyword(Keyword::From) {
                        let start = self.parse_expr_bp(6)?;
                        let length = if self.consume_keyword(Keyword::For) {
                            Some(self.parse_expr_bp(6)?)
                        } else {
                            None
                        };
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after SUBSTRING arguments",
                        )?;
                        args = vec![string, start];
                        if let Some(length) = length {
                            args.push(length);
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
                } else if fn_name == "trim" {
                    // TRIM([LEADING | TRAILING | BOTH] [characters] FROM string)
                    // Check for LEADING/TRAILING/BOTH
                    let trim_mode = match self.current_kind() {
                        TokenKind::Identifier(s) if s.eq_ignore_ascii_case("leading") => {
                            self.advance();
                            Some(Expr::String("leading".to_string()))
                        }
                        TokenKind::Identifier(s) if s.eq_ignore_ascii_case("trailing") => {
                            self.advance();
                            Some(Expr::String("trailing".to_string()))
                        }
                        TokenKind::Identifier(s) if s.eq_ignore_ascii_case("both") => {
                            self.advance();
                            Some(Expr::String("both".to_string()))
                        }
                        _ => None,
                    };

                    // Check if there's a characters expression before FROM
                    let chars_expr = if !self.peek_keyword(Keyword::From) {
                        Some(self.parse_expr_bp(6)?)
                    } else {
                        None
                    };

                    if self.consume_keyword(Keyword::From) {
                        let string = self.parse_expr_bp(6)?;
                        self.expect_token(
                            |k| matches!(k, TokenKind::RParen),
                            "expected ')' after TRIM arguments",
                        )?;

                        // Build args: [mode, chars, string] or subsets
                        args = Vec::new();
                        if let Some(mode) = trim_mode {
                            args.push(mode);
                        }
                        if let Some(chars) = chars_expr {
                            args.push(chars);
                        }
                        args.push(string);

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

                args.push(self.parse_function_argument_expr()?);
                while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    args.push(self.parse_function_argument_expr()?);
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
                self.expect_keyword(Keyword::Order, "expected ORDER after WITHIN GROUP (")?;
                self.expect_keyword(Keyword::By, "expected BY after WITHIN GROUP ORDER")?;
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
            if (self.consume_ident("respect") || self.consume_ident("ignore"))
                && !self.consume_ident("nulls")
            {
                return Err(self.error_at_current("expected NULLS after RESPECT/IGNORE"));
            }
            let over = if self.consume_keyword(Keyword::Over) {
                // OVER can be followed by:
                // 1. Window name: OVER window_name
                // 2. Window spec: OVER (...)
                if matches!(self.current_kind(), TokenKind::Identifier(_)) {
                    // Check if next token is LParen - if not, this is OVER window_name
                    let next_is_lparen = matches!(self.peek_nth_kind(1), Some(TokenKind::LParen));
                    if !next_is_lparen {
                        // OVER window_name (no parentheses)
                        let window_name = self.parse_identifier()?;
                        Some(Box::new(WindowSpec {
                            name: Some(window_name),
                            partition_by: Vec::new(),
                            order_by: Vec::new(),
                            frame: None,
                        }))
                    } else {
                        // OVER (...)
                        Some(Box::new(self.parse_window_spec()?))
                    }
                } else {
                    // OVER (...)
                    Some(Box::new(self.parse_window_spec()?))
                }
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

    pub(super) fn parse_array_constructor_elements(&mut self) -> Result<Vec<Expr>, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::RBracket)) {
            return Ok(Vec::new());
        }

        let mut items = Vec::new();
        loop {
            items.push(self.parse_array_constructor_element()?);
            if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                continue;
            }
            self.expect_token(
                |k| matches!(k, TokenKind::RBracket),
                "expected ']' to close ARRAY constructor",
            )?;
            break;
        }
        Ok(items)
    }

    pub(super) fn parse_array_constructor_element(&mut self) -> Result<Expr, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
            return Ok(Expr::ArrayConstructor(
                self.parse_array_constructor_elements()?,
            ));
        }
        self.parse_expr()
    }

    pub(super) fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
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

    pub(super) fn parse_in_expr(&mut self, lhs: Expr, negated: bool) -> Result<Expr, ParseError> {
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

    pub(super) fn parse_between_expr(
        &mut self,
        lhs: Expr,
        negated: bool,
    ) -> Result<Expr, ParseError> {
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

    pub(super) fn parse_like_expr(
        &mut self,
        lhs: Expr,
        negated: bool,
        case_insensitive: bool,
    ) -> Result<Expr, ParseError> {
        let pattern = self.parse_expr_bp(6)?;

        // Check for optional ESCAPE clause
        let escape = if self.consume_keyword(Keyword::Escape) {
            Some(Box::new(self.parse_expr_bp(6)?))
        } else {
            None
        };

        Ok(Expr::Like {
            expr: Box::new(lhs),
            pattern: Box::new(pattern),
            case_insensitive,
            negated,
            escape,
        })
    }

    pub(super) fn parse_regex_match_expr(
        &mut self,
        lhs: Expr,
        operator: &str,
    ) -> Result<Expr, ParseError> {
        let pattern = self.parse_expr_bp(6)?;
        let case_insensitive = matches!(operator, "~*" | "!~*");
        let negated = matches!(operator, "!~" | "!~*");

        let mut args = vec![lhs, pattern];
        if case_insensitive {
            args.push(Expr::String("i".to_string()));
        }

        let expr = Expr::FunctionCall {
            name: vec!["regexp_like".to_string()],
            args,
            distinct: false,
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter: None,
            over: None,
        };

        if negated {
            Ok(Expr::Unary {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            Ok(expr)
        }
    }

    pub(super) fn parse_operator_wrapper_symbol(&mut self) -> Result<String, ParseError> {
        if !self.consume_ident("operator") {
            return Err(self.error_at_current("expected OPERATOR keyword"));
        }
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after OPERATOR",
        )?;
        let mut symbol: Option<String> = None;
        loop {
            match self.current_kind() {
                TokenKind::Operator(op) => {
                    symbol = Some(op.clone());
                    self.advance();
                }
                TokenKind::Equal => {
                    symbol = Some("=".to_string());
                    self.advance();
                }
                TokenKind::NotEquals => {
                    symbol = Some("<>".to_string());
                    self.advance();
                }
                TokenKind::Less => {
                    symbol = Some("<".to_string());
                    self.advance();
                }
                TokenKind::LessEquals => {
                    symbol = Some("<=".to_string());
                    self.advance();
                }
                TokenKind::Greater => {
                    symbol = Some(">".to_string());
                    self.advance();
                }
                TokenKind::GreaterEquals => {
                    symbol = Some(">=".to_string());
                    self.advance();
                }
                TokenKind::Dot | TokenKind::Identifier(_) | TokenKind::Keyword(_) => {
                    self.advance();
                }
                TokenKind::RParen => break,
                _ => return Err(self.error_at_current("invalid OPERATOR() syntax")),
            }
        }
        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' after OPERATOR(...)",
        )?;
        symbol.ok_or_else(|| self.error_at_current("expected operator symbol in OPERATOR()"))
    }

    /// Returns true if the keyword is unreserved in PostgreSQL and can be used as an identifier.
    pub(super) fn parse_function_argument_expr(&mut self) -> Result<Expr, ParseError> {
        self.consume_ident("variadic");

        let named_prefix = matches!(
            self.current_kind(),
            TokenKind::Identifier(_) | TokenKind::Keyword(_)
        ) && self
            .peek_nth_kind(1)
            .is_some_and(|kind| matches!(kind, TokenKind::ColonEquals | TokenKind::EqualsGreater));
        if named_prefix {
            self.advance(); // argument name
            self.advance(); // := or =>
        }

        self.parse_expr()
    }

    pub(super) fn consume_optional_unicode_escape_clause(&mut self) -> Result<(), ParseError> {
        if self.consume_ident("uescape") {
            match self.current_kind() {
                TokenKind::String(_) => {
                    self.advance();
                    Ok(())
                }
                _ => Err(self.error_at_current("expected string literal after UESCAPE")),
            }
        } else {
            Ok(())
        }
    }

    pub(super) fn parse_window_definitions(&mut self) -> Result<Vec<WindowDefinition>, ParseError> {
        let mut definitions = Vec::new();
        loop {
            let name = self.parse_identifier()?;
            self.expect_keyword(Keyword::As, "expected AS after window name")?;
            self.expect_token(
                |k| matches!(k, TokenKind::LParen),
                "expected '(' after AS in window definition",
            )?;
            let spec = self.parse_window_spec_body()?;
            self.expect_token(
                |k| matches!(k, TokenKind::RParen),
                "expected ')' to close window definition",
            )?;
            definitions.push(WindowDefinition { name, spec });

            if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                break;
            }
        }
        Ok(definitions)
    }

    pub(super) fn parse_window_spec(&mut self) -> Result<WindowSpec, ParseError> {
        self.expect_token(
            |k| matches!(k, TokenKind::LParen),
            "expected '(' after OVER",
        )?;

        // Check for a named window reference: OVER (window_name) or OVER (window_name ORDER BY ...)
        let name = if matches!(self.current_kind(), TokenKind::Identifier(_))
            && !self.peek_keyword(Keyword::Partition)
            && !self.peek_keyword(Keyword::Order)
            && !self.peek_keyword(Keyword::Rows)
            && !self.peek_keyword(Keyword::Range)
            && !self.peek_keyword(Keyword::Groups)
        {
            // Try to parse as window name
            let ident = self.parse_identifier()?;
            // If next is ), this is just a window name reference: OVER (w)
            // If next is ORDER BY or frame clause, this is refinement: OVER (w ORDER BY ...)
            if matches!(self.current_kind(), TokenKind::RParen) {
                self.expect_token(
                    |k| matches!(k, TokenKind::RParen),
                    "expected ')' to close OVER clause",
                )?;
                return Ok(WindowSpec {
                    name: Some(ident),
                    partition_by: Vec::new(),
                    order_by: Vec::new(),
                    frame: None,
                });
            }
            Some(ident)
        } else {
            None
        };

        let spec = self.parse_window_spec_body()?;

        self.expect_token(
            |k| matches!(k, TokenKind::RParen),
            "expected ')' to close OVER clause",
        )?;

        Ok(WindowSpec {
            name,
            partition_by: spec.partition_by,
            order_by: spec.order_by,
            frame: spec.frame,
        })
    }

    pub(super) fn parse_window_spec_body(&mut self) -> Result<WindowSpec, ParseError> {
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

        if self.peek_keyword(Keyword::Rows)
            || self.peek_keyword(Keyword::Range)
            || self.peek_keyword(Keyword::Groups)
        {
            frame = Some(self.parse_window_frame()?);
        }

        Ok(WindowSpec {
            name: None,
            partition_by,
            order_by,
            frame,
        })
    }

    pub(super) fn parse_window_frame(&mut self) -> Result<WindowFrame, ParseError> {
        let units = if self.consume_keyword(Keyword::Rows) {
            WindowFrameUnits::Rows
        } else if self.consume_keyword(Keyword::Range) {
            WindowFrameUnits::Range
        } else if self.consume_keyword(Keyword::Groups) {
            WindowFrameUnits::Groups
        } else {
            return Err(
                self.error_at_current("expected ROWS, RANGE, or GROUPS in window frame clause")
            );
        };

        self.expect_keyword(Keyword::Between, "expected BETWEEN in window frame clause")?;
        let start = self.parse_window_frame_bound()?;
        self.expect_keyword(Keyword::And, "expected AND in window frame clause")?;
        let end = self.parse_window_frame_bound()?;

        // Optional EXCLUDE clause
        let exclusion = if self.consume_keyword(Keyword::Exclude) {
            if self.consume_keyword(Keyword::Current) {
                self.expect_window_row_keyword("expected ROW after EXCLUDE CURRENT")?;
                Some(WindowFrameExclusion::CurrentRow)
            } else if self.consume_keyword(Keyword::Group) {
                Some(WindowFrameExclusion::Group)
            } else if matches!(self.current_kind(), TokenKind::Identifier(id) if id.eq_ignore_ascii_case("ties"))
            {
                self.advance();
                Some(WindowFrameExclusion::Ties)
            } else if self.consume_keyword(Keyword::No) {
                if matches!(self.current_kind(), TokenKind::Identifier(id) if id.eq_ignore_ascii_case("others"))
                {
                    self.advance();
                }
                Some(WindowFrameExclusion::NoOthers)
            } else {
                return Err(self.error_at_current(
                    "expected CURRENT ROW, GROUP, TIES, or NO OTHERS after EXCLUDE",
                ));
            }
        } else {
            None
        };

        Ok(WindowFrame {
            units,
            start,
            end,
            exclusion,
        })
    }

    pub(super) fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, ParseError> {
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

    pub(super) fn expect_window_row_keyword(
        &mut self,
        message: &'static str,
    ) -> Result<(), ParseError> {
        let is_row = match self.current_kind() {
            TokenKind::Keyword(Keyword::Row) => true,
            TokenKind::Identifier(ident) => ident.eq_ignore_ascii_case("row"),
            _ => false,
        };
        if is_row {
            self.advance();
            Ok(())
        } else {
            Err(self.error_at_current(message))
        }
    }

    pub(super) fn parse_expr_type_name(&mut self) -> Result<String, ParseError> {
        let base = self.parse_expr_type_word()?.to_ascii_lowercase();
        let normalized = match base.as_str() {
            "bool" | "boolean" => "boolean".to_string(),
            // Integer types - preserve specific types for overflow checking
            "int2" | "smallint" => "int2".to_string(),
            "int" | "integer" | "int4" => "int4".to_string(),
            "int8" | "bigint" => "int8".to_string(),
            // Float types - normalize to float8
            "float4" | "real" => "float8".to_string(),
            "float" | "float8" => "float8".to_string(),
            "numeric" | "decimal" => "float8".to_string(),
            "double" => {
                if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("precision"))
                {
                    self.advance();
                }
                "float8".to_string()
            }
            // String types
            "text" | "varchar" | "char" => "text".to_string(),
            "character" => {
                if matches!(self.current_kind(), TokenKind::Identifier(next) if next.eq_ignore_ascii_case("varying"))
                {
                    self.advance();
                }
                "text".to_string()
            }
            // Date/time types
            "date" => "date".to_string(),
            "time" => "time".to_string(),
            "interval" => "interval".to_string(),
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
            // Binary and special types
            "bytea" => "bytea".to_string(),
            "uuid" => "uuid".to_string(),
            "vector" => "vector".to_string(),
            "bit" | "varbit" => "text".to_string(),
            // JSON types
            "json" => "json".to_string(),
            "jsonb" => "jsonb".to_string(),
            // System types
            "regclass" => "regclass".to_string(),
            "regnamespace" => "regclass".to_string(),
            "xid" => "int8".to_string(),
            "oid" => "oid".to_string(),
            "name" => "text".to_string(),
            // PostgreSQL underscore-prefixed array type aliases
            other if other.starts_with('_') => {
                let inner = &other[1..];
                let inner_norm = match inner {
                    "bool" | "boolean" => "boolean",
                    "int2" | "smallint" => "int4",
                    "int4" | "integer" | "int" => "int4",
                    "int8" | "bigint" => "int8",
                    "float4" | "real" => "float8",
                    "float8" => "float8",
                    "numeric" | "decimal" => "float8",
                    "text" | "varchar" | "char" | "name" => "text",
                    _ => {
                        return Err(self
                            .error_at_current(&format!("unsupported cast type name \"{other}\"")));
                    }
                };
                format!("{inner_norm}[]")
            }
            other => other.to_string(),
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

        // Handle array types like int[], text[]
        let mut final_type = normalized;
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
            final_type = format!("{final_type}[]");
        }

        Ok(final_type)
    }

    pub(super) fn parse_expr_type_word(&mut self) -> Result<String, ParseError> {
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
            TokenKind::Keyword(Keyword::Date) => {
                self.advance();
                Ok("date".to_string())
            }
            TokenKind::Keyword(Keyword::Time) => {
                self.advance();
                Ok("time".to_string())
            }
            TokenKind::Keyword(Keyword::Timestamp) => {
                self.advance();
                Ok("timestamp".to_string())
            }
            TokenKind::Keyword(Keyword::Interval) => {
                self.advance();
                Ok("interval".to_string())
            }
            _ => Err(self.error_at_current("expected type name")),
        }
    }

    pub(super) fn parse_expr_identifier(&mut self) -> Result<String, ParseError> {
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
            TokenKind::Keyword(Keyword::Date) => {
                self.advance();
                Ok("date".to_string())
            }
            TokenKind::Keyword(Keyword::Time) => {
                self.advance();
                Ok("time".to_string())
            }
            TokenKind::Keyword(Keyword::Timestamp) => {
                self.advance();
                Ok("timestamp".to_string())
            }
            TokenKind::Keyword(Keyword::Interval) => {
                self.advance();
                Ok("interval".to_string())
            }
            TokenKind::Keyword(Keyword::True) => {
                self.advance();
                Ok("true".to_string())
            }
            TokenKind::Keyword(Keyword::False) => {
                self.advance();
                Ok("false".to_string())
            }
            TokenKind::Keyword(Keyword::Null) => {
                self.advance();
                Ok("null".to_string())
            }
            TokenKind::Keyword(kw) if Self::is_unreserved_keyword(kw) => {
                let name = format!("{kw:?}").to_ascii_lowercase();
                self.advance();
                Ok(name)
            }
            _ => Err(self.error_at_current("expected identifier")),
        }
    }

    pub(super) fn current_binary_op(&self) -> Option<(BinaryOp, u8, u8)> {
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
            TokenKind::Caret => Some((BinaryOp::Pow, 13, 14)),
            TokenKind::Operator(op) if op == "<<" => Some((BinaryOp::ShiftLeft, 9, 10)),
            TokenKind::Operator(op) if op == ">>" => Some((BinaryOp::ShiftRight, 9, 10)),
            TokenKind::Operator(op) if op == "->" => Some((BinaryOp::JsonGet, 11, 12)),
            TokenKind::Operator(op) if op == "->>" => Some((BinaryOp::JsonGetText, 11, 12)),
            TokenKind::Operator(op) if op == "#>" => Some((BinaryOp::JsonPath, 11, 12)),
            TokenKind::Operator(op) if op == "#>>" => Some((BinaryOp::JsonPathText, 11, 12)),
            TokenKind::Operator(op) if op == "||" => Some((BinaryOp::JsonConcat, 6, 7)),
            TokenKind::Operator(op) if op == "@>" => Some((BinaryOp::JsonContains, 5, 6)),
            TokenKind::Operator(op) if op == "<@" => Some((BinaryOp::JsonContainedBy, 5, 6)),
            TokenKind::Operator(op) if op == "@?" => Some((BinaryOp::JsonPathExists, 5, 6)),
            TokenKind::Operator(op) if op == "@@" => Some((BinaryOp::JsonPathMatch, 5, 6)),
            TokenKind::Operator(op) if op == "?" => Some((BinaryOp::JsonHasKey, 5, 6)),
            TokenKind::Operator(op) if op == "?|" => Some((BinaryOp::JsonHasAny, 5, 6)),
            TokenKind::Operator(op) if op == "?&" => Some((BinaryOp::JsonHasAll, 5, 6)),
            TokenKind::Operator(op) if op == "#-" => Some((BinaryOp::JsonDeletePath, 11, 12)),
            TokenKind::Operator(op) if op == "&&" => Some((BinaryOp::ArrayOverlap, 5, 6)),
            TokenKind::Operator(op) if op == "<->" => Some((BinaryOp::VectorL2Distance, 7, 8)),
            TokenKind::Operator(op) if op == "<#>" => Some((BinaryOp::VectorInnerProduct, 7, 8)),
            TokenKind::Operator(op) if op == "<=>" => Some((BinaryOp::VectorCosineDistance, 7, 8)),
            _ => None,
        }
    }

    pub(super) fn current_set_op(&self) -> Option<(SetOperator, u8, u8)> {
        match self.current_kind() {
            TokenKind::Keyword(Keyword::Union) => Some((SetOperator::Union, 1, 2)),
            TokenKind::Keyword(Keyword::Except) => Some((SetOperator::Except, 1, 2)),
            TokenKind::Keyword(Keyword::Intersect) => Some((SetOperator::Intersect, 3, 4)),
            _ => None,
        }
    }

    pub(super) fn parse_optional_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.consume_keyword(Keyword::As) {
            return Ok(Some(self.parse_identifier()?));
        }
        if matches!(self.current_kind(), TokenKind::Identifier(_)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    pub(super) fn parse_optional_function_alias(&mut self) -> Result<Option<String>, ParseError> {
        if self.consume_keyword(Keyword::As) {
            if matches!(self.current_kind(), TokenKind::LParen) {
                return Ok(None);
            }
            return Ok(Some(self.parse_identifier()?));
        }
        if matches!(self.current_kind(), TokenKind::Identifier(_)) {
            return Ok(Some(self.parse_identifier()?));
        }
        Ok(None)
    }

    pub(super) fn parse_optional_column_aliases(
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

    pub(super) fn parse_optional_column_alias_type(
        &mut self,
    ) -> Result<Option<String>, ParseError> {
        match self.current_kind() {
            TokenKind::Comma | TokenKind::RParen => Ok(None),
            _ => self.parse_expr_type_name().map(Some),
        }
    }
}
