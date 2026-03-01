#[allow(clippy::wildcard_imports)]
use super::*;

impl Parser {
    pub(super) fn parse_grant_statement(&mut self) -> Result<Statement, ParseError> {
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

    pub(super) fn parse_grant_role_statement(&mut self) -> Result<GrantStatement, ParseError> {
        let role_name =
            self.parse_role_identifier_with_message("GRANT role requires role and member names")?;
        if !self.consume_keyword(Keyword::To) {
            return Err(self.error_at_current("GRANT role requires TO clause"));
        }
        let member =
            self.parse_role_identifier_with_message("GRANT role requires role and member names")?;
        Ok(GrantStatement::Role(GrantRoleStatement {
            role_name,
            member,
        }))
    }

    pub(super) fn parse_grant_table_privileges_statement(
        &mut self,
    ) -> Result<GrantStatement, ParseError> {
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
        Ok(GrantStatement::TablePrivileges(
            GrantTablePrivilegesStatement {
                privileges,
                table_name,
                roles,
            },
        ))
    }

    pub(super) fn parse_revoke_statement(&mut self) -> Result<Statement, ParseError> {
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

    pub(super) fn parse_revoke_role_statement(&mut self) -> Result<RevokeStatement, ParseError> {
        let role_name =
            self.parse_role_identifier_with_message("REVOKE role requires role and member names")?;
        if !self.consume_keyword(Keyword::From) {
            return Err(self.error_at_current("REVOKE role requires FROM clause"));
        }
        let member =
            self.parse_role_identifier_with_message("REVOKE role requires role and member names")?;
        Ok(RevokeStatement::Role(RevokeRoleStatement {
            role_name,
            member,
        }))
    }

    pub(super) fn parse_revoke_table_privileges_statement(
        &mut self,
    ) -> Result<RevokeStatement, ParseError> {
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
        Ok(RevokeStatement::TablePrivileges(
            RevokeTablePrivilegesStatement {
                privileges,
                table_name,
                roles,
            },
        ))
    }

    pub(super) fn parse_privilege_list(
        &mut self,
        command: &'static str,
    ) -> Result<Vec<TablePrivilegeKind>, ParseError> {
        let mut privileges = Vec::new();
        loop {
            if self.peek_keyword(Keyword::On) {
                break;
            }
            if matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
                return Err(self.error_at_current(&format!("{command} requires ON TABLE clause")));
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
                return Err(self.error_at_current(&format!("unsupported privilege {token}")));
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

    pub(super) fn parse_role_identifier_with_message(
        &mut self,
        message: &'static str,
    ) -> Result<String, ParseError> {
        let Some(name) = self.take_keyword_or_identifier() else {
            return Err(self.error_at_current(message));
        };
        Ok(name)
    }

    pub(super) fn parse_role_list(
        &mut self,
        message: &'static str,
    ) -> Result<Vec<String>, ParseError> {
        let mut roles = Vec::new();
        roles.push(self.parse_role_identifier_with_message(message)?);
        while self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            roles.push(self.parse_role_identifier_with_message(message)?);
        }
        Ok(roles)
    }

    pub(super) fn parse_role_options(
        &mut self,
        command: &'static str,
    ) -> Result<Vec<RoleOption>, ParseError> {
        let mut options = Vec::new();
        while !matches!(self.current_kind(), TokenKind::Eof | TokenKind::Semicolon) {
            let token = match self.current_kind() {
                TokenKind::Identifier(value) => value.clone(),
                TokenKind::Keyword(kw) => format!("{kw:?}").to_lowercase(),
                _ => {
                    let raw = format!("{:?}", self.current_kind());
                    return Err(
                        self.error_at_current(&format!("unsupported {command} option {raw}"))
                    );
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
                        return Err(
                            self.error_at_current(&format!("{command} PASSWORD requires a value"))
                        );
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

    pub(super) fn parse_copy_statement(&mut self) -> Result<Statement, ParseError> {
        if self.peek_keyword(Keyword::To) || self.peek_keyword(Keyword::From) {
            return Err(self.error_at_current(
                "unsupported COPY command (expected COPY <table> TO/FROM STDOUT/STDIN ...)",
            ));
        }
        let table_name = self.parse_qualified_name()?;
        // Parse optional column list: COPY table (col1, col2, ...) FROM/TO
        let columns = if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            let mut cols = Vec::new();
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                loop {
                    let col = self.take_keyword_or_identifier().ok_or_else(|| {
                        self.error_at_current("expected column name in COPY column list")
                    })?;
                    cols.push(col);
                    if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        continue;
                    }
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after COPY column list",
                    )?;
                    break;
                }
            }
            cols
        } else {
            Vec::new()
        };
        let direction = if self.consume_keyword(Keyword::To) {
            CopyDirection::To
        } else if self.consume_keyword(Keyword::From) {
            CopyDirection::From
        } else {
            return Err(self.error_at_current(
                "unsupported COPY command (expected COPY <table> TO/FROM STDOUT/STDIN ...)",
            ));
        };
        let target = self.take_keyword_or_identifier().ok_or_else(|| {
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
            header: false,
        };
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                loop {
                    self.parse_copy_option_item(&mut options)?;
                    if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                        continue;
                    }
                    self.expect_token(
                        |k| matches!(k, TokenKind::RParen),
                        "expected ')' after COPY options",
                    )?;
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
            columns,
            direction,
            options,
        }))
    }

    pub(super) fn parse_copy_option_item(
        &mut self,
        options: &mut CopyOptions,
    ) -> Result<(), ParseError> {
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
                let ch = chars
                    .next()
                    .ok_or_else(|| self.error_at_current("COPY DELIMITER cannot be empty"))?;
                if chars.next().is_some() {
                    return Err(self.error_at_current("COPY DELIMITER must be a single character"));
                }
                options.delimiter = Some(ch.to_string());
            }
            "NULL" => {
                let null_marker = self.parse_copy_string_literal("NULL")?;
                options.null_marker = Some(null_marker);
            }
            "HEADER" => {
                // HEADER can optionally be followed by TRUE/FALSE, or standalone means TRUE
                let saved = self.idx;
                if let Some(next) = self.take_keyword_or_identifier_upper() {
                    match next.as_str() {
                        "TRUE" | "ON" => options.header = true,
                        "FALSE" | "OFF" => options.header = false,
                        _ => {
                            self.idx = saved;
                            options.header = true;
                        }
                    }
                } else {
                    options.header = true;
                }
            }
            other => {
                return Err(self.error_at_current(&format!("unsupported COPY option {other}")));
            }
        }
        Ok(())
    }

    pub(super) fn parse_copy_format_value(
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
            _ => Err(self.error_at_current(&format!("{message_prefix} {token}"))),
        }
    }

    pub(super) fn parse_copy_string_literal(
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

    pub(super) fn parse_explain_statement(&mut self) -> Result<Statement, ParseError> {
        let mut analyze = false;
        let mut verbose = false;
        // Check for EXPLAIN (options) or EXPLAIN ANALYZE VERBOSE
        if self.consume_if(|k| matches!(k, TokenKind::LParen)) {
            // EXPLAIN (option [, option ...]) statement
            loop {
                if self.consume_keyword(Keyword::Analyze) {
                    analyze = true;
                } else if self.consume_keyword(Keyword::Verbose) {
                    verbose = true;
                } else {
                    // Skip unknown options like COSTS, BUFFERS, TIMING, FORMAT, etc.
                    // Consume the option name
                    if matches!(
                        self.current_kind(),
                        TokenKind::Identifier(_) | TokenKind::Keyword(_)
                    ) {
                        self.advance();
                    }
                    // Optionally consume ON/OFF/TRUE/FALSE or other value
                    if matches!(
                        self.current_kind(),
                        TokenKind::Identifier(_) | TokenKind::Keyword(_)
                    ) {
                        self.advance();
                    }
                }
                if !self.consume_if(|k| matches!(k, TokenKind::Comma)) {
                    break;
                }
            }
            if !self.consume_if(|k| matches!(k, TokenKind::RParen)) {
                return Err(self.error_at_current("expected ')' after EXPLAIN options"));
            }
        } else {
            if self.consume_keyword(Keyword::Analyze) {
                analyze = true;
            }
            if self.consume_keyword(Keyword::Verbose) {
                verbose = true;
            }
        }
        let inner = self.parse_top_level_statement()?;
        Ok(Statement::Explain(ExplainStatement {
            analyze,
            verbose,
            statement: Box::new(inner),
        }))
    }

    pub(super) fn parse_set_statement(&mut self) -> Result<Statement, ParseError> {
        let is_local = self.consume_keyword(Keyword::Local);
        if self.consume_ident("session") {
            if !self.consume_ident("authorization") {
                return Err(self.error_at_current("expected AUTHORIZATION after SET SESSION"));
            }
            let value = self.collect_setting_value_tokens();
            if value.is_empty() {
                return Err(self.error_at_current(
                    "expected role name or DEFAULT after SET SESSION AUTHORIZATION",
                ));
            }
            return Ok(Statement::Set(SetStatement {
                name: "session_authorization".to_string(),
                value,
                is_local,
            }));
        }
        let name = self.parse_setting_name()?;
        let has_assignment =
            self.consume_if(|k| matches!(k, TokenKind::Equal)) || self.consume_keyword(Keyword::To);
        // PostgreSQL allows SET TIME ZONE value without TO/=.
        if !has_assignment && !name.eq_ignore_ascii_case("timezone") {
            return Err(self.error_at_current("expected = or TO after SET variable name"));
        }
        let value = self.collect_setting_value_tokens();
        Ok(Statement::Set(SetStatement {
            name,
            value,
            is_local,
        }))
    }

    pub(super) fn parse_show_statement(&mut self) -> Result<Statement, ParseError> {
        let name = if self.consume_keyword(Keyword::All) {
            "all".to_string()
        } else {
            self.parse_identifier()?
        };
        Ok(Statement::Show(ShowStatement { name }))
    }

    pub(super) fn parse_reset_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_ident("session") {
            if !self.consume_ident("authorization") {
                return Err(self.error_at_current("expected AUTHORIZATION after RESET SESSION"));
            }
            return Ok(Statement::Set(SetStatement {
                name: "session_authorization".to_string(),
                value: "DEFAULT".to_string(),
                is_local: false,
            }));
        }
        let name = if self.consume_keyword(Keyword::All) {
            "all".to_string()
        } else {
            self.parse_setting_name()?
        };
        Ok(Statement::Set(SetStatement {
            name,
            value: "DEFAULT".to_string(),
            is_local: false,
        }))
    }

    pub(super) fn parse_setting_name(&mut self) -> Result<String, ParseError> {
        let first = self.parse_identifier()?;
        // PostgreSQL supports SET TIME ZONE ... spelling.
        if first.eq_ignore_ascii_case("time") && self.consume_ident("zone") {
            return Ok("timezone".to_string());
        }

        let mut parts = vec![first];
        while self.consume_if(|k| matches!(k, TokenKind::Dot)) {
            parts.push(self.parse_identifier()?);
        }
        Ok(parts.join("."))
    }

    pub(super) fn collect_setting_value_tokens(&mut self) -> String {
        let mut value_parts = Vec::new();
        while !matches!(self.current_kind(), TokenKind::Eof)
            && !matches!(self.current_kind(), TokenKind::Semicolon)
        {
            let token = &self.tokens[self.idx];
            match &token.kind {
                TokenKind::Keyword(kw) => value_parts.push(format!("{kw:?}").to_lowercase()),
                TokenKind::Identifier(s) => value_parts.push(s.clone()),
                TokenKind::String(s) => value_parts.push(s.clone()),
                TokenKind::Integer(i) => value_parts.push(i.to_string()),
                TokenKind::Float(s) => value_parts.push(s.clone()),
                TokenKind::Comma => value_parts.push(",".to_string()),
                _ => value_parts.push(format!("{:?}", token.kind)),
            }
            self.advance();
        }
        value_parts.join(" ")
    }

    pub(super) fn parse_discard_statement(&mut self) -> Result<Statement, ParseError> {
        let target = if self.consume_keyword(Keyword::All) {
            "ALL".to_string()
        } else {
            self.parse_identifier()?.to_uppercase()
        };
        Ok(Statement::Discard(DiscardStatement { target }))
    }

    pub(super) fn parse_do_statement(&mut self) -> Result<Statement, ParseError> {
        // Accept both:
        //   DO 'body' [LANGUAGE lang]
        //   DO LANGUAGE lang 'body'
        let mut language = "plpgsql".to_string();
        if self.consume_keyword(Keyword::Language) || self.consume_ident("language") {
            language = self.parse_identifier_or_string()?;
            let body = match &self.tokens[self.idx].kind {
                TokenKind::String(s) => {
                    let b = s.clone();
                    self.advance();
                    b
                }
                _ => return Err(self.error_at_current("expected string body after DO")),
            };
            return Ok(Statement::Do(DoStatement { body, language }));
        }

        let body = match &self.tokens[self.idx].kind {
            TokenKind::String(s) => {
                let b = s.clone();
                self.advance();
                b
            }
            _ => return Err(self.error_at_current("expected string body after DO")),
        };

        if self.consume_keyword(Keyword::Language) || self.consume_ident("language") {
            language = self.parse_identifier_or_string()?;
        }
        Ok(Statement::Do(DoStatement { body, language }))
    }

    pub(super) fn parse_listen_statement(&mut self) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        Ok(Statement::Listen(ListenStatement { channel }))
    }

    pub(super) fn parse_notify_statement(&mut self) -> Result<Statement, ParseError> {
        let channel = self.parse_identifier()?;
        let payload = if self.consume_if(|k| matches!(k, TokenKind::Comma)) {
            match &self.tokens[self.idx].kind {
                TokenKind::String(s) => {
                    let p = s.clone();
                    self.advance();
                    Some(p)
                }
                _ => {
                    return Err(
                        self.error_at_current("expected string payload after NOTIFY channel,")
                    );
                }
            }
        } else {
            None
        };
        Ok(Statement::Notify(NotifyStatement { channel, payload }))
    }

    pub(super) fn parse_unlisten_statement(&mut self) -> Result<Statement, ParseError> {
        if self.consume_if(|k| matches!(k, TokenKind::Star)) {
            return Ok(Statement::Unlisten(UnlistenStatement { channel: None }));
        }
        let channel = self.parse_identifier()?;
        Ok(Statement::Unlisten(UnlistenStatement {
            channel: Some(channel),
        }))
    }

    pub(super) fn parse_transaction_statement(&mut self) -> Result<Statement, ParseError> {
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
            return Ok(Statement::Transaction(TransactionStatement::Savepoint(
                name,
            )));
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

    pub(super) fn parse_truncate_statement(&mut self) -> Result<Statement, ParseError> {
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

    pub(super) fn parse_refresh_statement(&mut self) -> Result<Statement, ParseError> {
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
}
