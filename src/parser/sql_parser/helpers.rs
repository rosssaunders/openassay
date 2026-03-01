#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn advance(&mut self) {
        if self.idx + 1 < self.tokens.len() {
            self.idx += 1;
        }
    }

    pub(super) fn current_kind(&self) -> &TokenKind {
        &self.tokens[self.idx].kind
    }

    pub(super) fn peek_nth_kind(&self, n: usize) -> Option<&TokenKind> {
        self.tokens.get(self.idx + n).map(|token| &token.kind)
    }

    pub(super) fn current_starts_query(&self) -> bool {
        matches!(
            self.current_kind(),
            TokenKind::Keyword(Keyword::Select | Keyword::With | Keyword::Values | Keyword::Table)
        )
    }

    pub(super) fn peek_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current_kind(), TokenKind::Keyword(kv) if *kv == keyword)
    }

    pub(super) fn consume_keyword(&mut self, keyword: Keyword) -> bool {
        self.consume_if(|k| matches!(k, TokenKind::Keyword(kv) if *kv == keyword))
    }

    pub(super) fn peek_ident(&self, value: &str) -> bool {
        matches!(self.current_kind(), TokenKind::Identifier(ident) if ident.eq_ignore_ascii_case(value))
    }

    pub(super) fn consume_ident(&mut self, value: &str) -> bool {
        self.consume_if(
            |k| matches!(k, TokenKind::Identifier(ident) if ident.eq_ignore_ascii_case(value)),
        )
    }

    pub(super) fn peek_nth_keyword(&self, n: usize, keyword: Keyword) -> bool {
        matches!(self.peek_nth_kind(n), Some(TokenKind::Keyword(kv)) if *kv == keyword)
    }

    pub(super) fn consume_if<F>(&mut self, predicate: F) -> bool
    where
        F: Fn(&TokenKind) -> bool,
    {
        if predicate(self.current_kind()) {
            self.advance();
            return true;
        }
        false
    }

    pub(super) fn error_at_current(&self, message: &str) -> ParseError {
        ParseError {
            message: message.to_string(),
            position: self.tokens[self.idx].start,
        }
    }

    pub(super) fn expect_keyword(
        &mut self,
        keyword: Keyword,
        message: &'static str,
    ) -> Result<(), ParseError> {
        if self.consume_keyword(keyword) {
            return Ok(());
        }
        Err(self.error_at_current(message))
    }

    pub(super) fn expect_token<F>(
        &mut self,
        predicate: F,
        message: &'static str,
    ) -> Result<(), ParseError>
    where
        F: Fn(&TokenKind) -> bool,
    {
        if self.consume_if(predicate) {
            return Ok(());
        }
        Err(self.error_at_current(message))
    }

    pub(super) fn expect_eof(&self) -> Result<(), ParseError> {
        if matches!(self.current_kind(), TokenKind::Eof) {
            return Ok(());
        }
        Err(self.error_at_current("unexpected token after end of statement"))
    }

    pub(super) fn parse_identifier(&mut self) -> Result<String, ParseError> {
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

    /// Parse and ignore an optional CREATE INDEX operator class token, e.g. `int4_ops`
    /// or schema-qualified `pg_catalog.int4_ops`.
    pub(super) fn parse_qualified_name(&mut self) -> Result<Vec<String>, ParseError> {
        let mut out = vec![self.parse_identifier()?];
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            out.push(self.parse_identifier()?);
        }
        Ok(out)
    }

    pub(super) fn parse_identifier_or_string(&mut self) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::String(s) => {
                let out = s.clone();
                self.advance();
                Ok(out)
            }
            _ => self.parse_identifier(),
        }
    }

    pub(super) fn token_word(&self, token: &TokenKind) -> Option<String> {
        match token {
            TokenKind::Identifier(word) => Some(word.to_ascii_lowercase()),
            TokenKind::Keyword(keyword) => Some(format!("{keyword:?}").to_ascii_lowercase()),
            _ => None,
        }
    }

    pub(super) fn take_keyword_or_identifier(&mut self) -> Option<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.clone();
                self.advance();
                Some(out)
            }
            TokenKind::Keyword(kw) => {
                let out = format!("{kw:?}").to_lowercase();
                self.advance();
                Some(out)
            }
            _ => None,
        }
    }

    pub(super) fn take_keyword_or_identifier_upper(&mut self) -> Option<String> {
        match self.current_kind() {
            TokenKind::Identifier(value) => {
                let out = value.to_ascii_uppercase();
                self.advance();
                Some(out)
            }
            TokenKind::Keyword(kw) => {
                let out = format!("{kw:?}").to_ascii_uppercase();
                self.advance();
                Some(out)
            }
            _ => None,
        }
    }

    pub(super) fn extract_identifier_from_expr(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(parts) => parts.last().cloned(),
            Expr::Cast { expr, .. }
            | Expr::Unary { expr, .. }
            | Expr::ArraySubscript { expr, .. }
            | Expr::ArraySlice { expr, .. } => Self::extract_identifier_from_expr(expr),
            Expr::FunctionCall { args, .. }
            | Expr::ArrayConstructor(args)
            | Expr::RowConstructor(args) => {
                args.iter().find_map(Self::extract_identifier_from_expr)
            }
            Expr::Binary { left, right, .. } | Expr::AnyAll { left, right, .. } => {
                Self::extract_identifier_from_expr(left)
                    .or_else(|| Self::extract_identifier_from_expr(right))
            }
            _ => None,
        }
    }

    /// Skip optional array subscripts like [1], [1:2], [1:2][3:4], etc.
    /// Used in INSERT column lists where subscripts are accepted but we ignore them.
    pub(super) fn consume_optional_type_modifiers(&mut self) {
        if !self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            return;
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
                TokenKind::Eof => break,
                _ => self.advance(),
            }
        }
    }

    pub(super) fn consume_interval_field_specifier(&mut self) -> bool {
        self.consume_ident("year")
            || self.consume_ident("month")
            || self.consume_ident("day")
            || self.consume_ident("hour")
            || self.consume_ident("minute")
            || self.consume_ident("second")
    }

    pub(super) fn parse_optional_interval_qualifier(&mut self) -> Result<(), ParseError> {
        if !self.consume_interval_field_specifier() {
            return Ok(());
        }
        self.consume_optional_type_modifiers();
        if self.consume_keyword(Keyword::To) {
            if !self.consume_interval_field_specifier() {
                return Err(
                    self.error_at_current("expected interval field after TO in INTERVAL literal")
                );
            }
            self.consume_optional_type_modifiers();
        }
        Ok(())
    }

    pub(super) fn skip_optional_parenthesized_tokens(&mut self) {
        if !self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            return;
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
                TokenKind::Eof => break,
                _ => self.advance(),
            }
        }
    }

    pub(super) fn skip_array_subscripts(&mut self) {
        while self.consume_if(|k| matches!(k, TokenKind::LBracket)) {
            let mut depth = 1usize;
            while depth > 0 {
                match self.current_kind() {
                    TokenKind::LBracket => {
                        depth += 1;
                        self.advance();
                    }
                    TokenKind::RBracket => {
                        depth -= 1;
                        self.advance();
                    }
                    TokenKind::Eof => break,
                    _ => self.advance(),
                }
            }
        }
    }

    pub(super) fn parse_optional_index_operator_class(&mut self) -> Result<(), ParseError> {
        let TokenKind::Identifier(ident) = self.current_kind() else {
            return Ok(());
        };
        if ident.eq_ignore_ascii_case("nulls") {
            return Ok(());
        }

        let _ = self.parse_identifier()?;
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            let _ = self.parse_identifier()?;
        }
        Ok(())
    }

    pub(super) fn parse_optional_collation_clause(&mut self) -> Result<(), ParseError> {
        if self.consume_ident("collate") {
            let _ = self.parse_identifier()?;
        }
        Ok(())
    }

    pub(super) fn is_unreserved_keyword(kw: &Keyword) -> bool {
        matches!(
            kw,
            Keyword::Target
                | Keyword::Source
                | Keyword::Matched
                | Keyword::Nothing
                | Keyword::Filter
                | Keyword::First
                | Keyword::Last
                | Keyword::Data
                | Keyword::Always
                | Keyword::Restart
                | Keyword::Start
                | Keyword::Depth
                | Keyword::Breadth
                | Keyword::Search
                | Keyword::Cycle
                | Keyword::Materialized
                | Keyword::Verbose
                | Keyword::Local
                | Keyword::Reset
                | Keyword::Replace
                | Keyword::Cascade
                | Keyword::Restrict
                | Keyword::Cache
                | Keyword::Increment
                | Keyword::MinValue
                | Keyword::MaxValue
                | Keyword::No
                | Keyword::Identity
                | Keyword::Generated
                | Keyword::Sets
                | Keyword::Cube
                | Keyword::Rollup
                | Keyword::Within
                | Keyword::Groups
                | Keyword::Exclude
                | Keyword::Preceding
                | Keyword::Following
                | Keyword::Unbounded
                | Keyword::Range
                | Keyword::Rows
                | Keyword::Row
                | Keyword::Merge
                | Keyword::Rename
                | Keyword::Column
                | Keyword::Schema
                | Keyword::Index
                | Keyword::Sequence
                | Keyword::View
                | Keyword::Extension
                | Keyword::Function
                | Keyword::Returns
                | Keyword::Language
                | Keyword::Temporary
                | Keyword::Temp
                | Keyword::Type
                | Keyword::Enum
                | Keyword::Domain
                | Keyword::Placing
                | Keyword::Add
                | Keyword::Concurrently
                | Keyword::Do
                | Keyword::Conflict
                | Keyword::Key
                | Keyword::Partition
                | Keyword::Transaction
                | Keyword::Explain
                | Keyword::Analyze
                | Keyword::Show
                | Keyword::Discard
                | Keyword::Listen
                | Keyword::Notify
                | Keyword::Unlisten
                | Keyword::Savepoint
                | Keyword::Release
                | Keyword::Returning
                | Keyword::Recursive
                | Keyword::Refresh
                | Keyword::Window
                | Keyword::Array
        )
    }

    pub(super) fn parse_signed_integer_literal(&mut self) -> Result<i64, ParseError> {
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

    pub(super) fn parse_literal_string(&mut self) -> Result<String, ParseError> {
        match self.current_kind() {
            TokenKind::String(s) => {
                let result = s.clone();
                self.advance();
                Ok(result)
            }
            _ => Err(self.error_at_current("expected string literal")),
        }
    }

    pub(super) fn parse_identifier_list_in_parens(&mut self) -> Result<Vec<String>, ParseError> {
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

    pub(super) fn skip_to_statement_end(&mut self) {
        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            self.advance();
        }
    }
}
