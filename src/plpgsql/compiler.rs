//! Phase 1 PL/pgSQL compiler.
//!
//! This is a lightweight parser/compiler for PL/pgSQL function bodies and
//! anonymous `DO` blocks. It follows the same high-level compile flow as
//! PostgreSQL `pl_comp.c` (`pl_comp.c:90-318`, `760-902`) but intentionally
//! implements a restricted subset for OpenAssay Phase 1.

use std::collections::HashMap;
use std::fmt;

use crate::parser::ast::{
    CreateFunctionStatement, DoStatement, FunctionParamMode, Statement, TypeName,
};
use crate::parser::sql_parser::{ParseError as SqlParseError, parse_statement};
use crate::plpgsql::scanner::{
    PlPgSqlKeyword, PlPgSqlScanError, PlPgSqlToken, PlPgSqlTokenKind, extract_sql_expression,
    tokenize,
};
use crate::plpgsql::types::{
    PLPGSQL_OTHERS, PlPgSqlCondition, PlPgSqlDatum, PlPgSqlDiagItem, PlPgSqlDtype,
    PlPgSqlException, PlPgSqlExceptionBlock, PlPgSqlExpr, PlPgSqlFunction, PlPgSqlGetdiagKind,
    PlPgSqlIfElsif, PlPgSqlPromiseType, PlPgSqlRaiseOption, PlPgSqlRaiseOptionType,
    PlPgSqlResolveOption, PlPgSqlRow, PlPgSqlStmt, PlPgSqlStmtAssign, PlPgSqlStmtBlock,
    PlPgSqlStmtCall, PlPgSqlStmtClose, PlPgSqlStmtCommit, PlPgSqlStmtDynexecute,
    PlPgSqlStmtExecSql, PlPgSqlStmtFetch, PlPgSqlStmtForc, PlPgSqlStmtFori, PlPgSqlStmtFors,
    PlPgSqlStmtGetdiag, PlPgSqlStmtIf, PlPgSqlStmtOpen, PlPgSqlStmtPerform, PlPgSqlStmtRaise,
    PlPgSqlStmtReturn, PlPgSqlStmtRollback, PlPgSqlStmtType, PlPgSqlTrigtype, PlPgSqlType,
    PlPgSqlTypeType, PlPgSqlValue, PlPgSqlVar, PlPgSqlVariable,
};

/// Compiler error for PL/pgSQL parse/compile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlCompileError {
    pub message: String,
    pub position: usize,
    pub line: usize,
    pub column: usize,
}

impl fmt::Display for PlPgSqlCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at byte {} (line {}, col {})",
            self.message, self.position, self.line, self.column
        )
    }
}

impl std::error::Error for PlPgSqlCompileError {}

impl From<PlPgSqlScanError> for PlPgSqlCompileError {
    fn from(value: PlPgSqlScanError) -> Self {
        Self {
            message: value.message,
            position: value.position,
            line: value.line,
            column: value.column,
        }
    }
}

/// Compiles a PL/pgSQL body (`DECLARE ... BEGIN ... END`) into a function AST.
pub fn compile_function_body(body: &str) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
    let tokens = tokenize(body)?;
    let mut parser = BodyParser::new(body, tokens);
    parser.parse_function_body()
}

/// Compiles a SQL `DO ... LANGUAGE plpgsql` command into a function AST.
pub fn compile_do_block_sql(sql: &str) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
    match parse_statement(sql) {
        Ok(Statement::Do(do_stmt)) => compile_do_statement(&do_stmt),
        Ok(_) => Err(PlPgSqlCompileError {
            message: "expected DO statement".to_string(),
            position: 0,
            line: 1,
            column: 1,
        }),
        Err(_) => {
            let do_stmt = parse_do_sql_fallback(sql)?;
            compile_do_statement(&do_stmt)
        }
    }
}

/// Compiles an already-parsed SQL `DO` statement.
pub fn compile_do_statement(do_stmt: &DoStatement) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
    if !do_stmt.language.eq_ignore_ascii_case("plpgsql") {
        return Err(PlPgSqlCompileError {
            message: format!(
                "DO block language must be plpgsql, got {}",
                do_stmt.language
            ),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let mut compiled = compile_function_body(&do_stmt.body)?;
    compiled.fn_signature = "inline_code_block".to_string();
    Ok(compiled)
}

/// Compiles a SQL `CREATE FUNCTION ... LANGUAGE plpgsql` command body.
pub fn compile_create_function_sql(sql: &str) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
    let stmt = parse_statement(sql).map_err(|e| map_sql_parse_error(sql, e))?;
    match stmt {
        Statement::CreateFunction(create_stmt) => compile_create_function_statement(&create_stmt),
        _ => Err(PlPgSqlCompileError {
            message: "expected CREATE FUNCTION statement".to_string(),
            position: 0,
            line: 1,
            column: 1,
        }),
    }
}

/// Compiles an already-parsed SQL `CREATE FUNCTION` statement body.
pub fn compile_create_function_statement(
    create_stmt: &CreateFunctionStatement,
) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
    if !create_stmt.language.eq_ignore_ascii_case("plpgsql") {
        return Err(PlPgSqlCompileError {
            message: format!(
                "function language must be plpgsql, got {}",
                create_stmt.language
            ),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let tokens = tokenize(&create_stmt.body)?;
    let mut parser = BodyParser::new(&create_stmt.body, tokens);
    let (argvarnos, out_param_varno, new_varno, old_varno) =
        parser.add_argument_datums(create_stmt);
    let mut compiled = parser.parse_function_body()?;
    compiled.fn_signature = create_stmt.name.join(".");
    compiled.fn_nargs = i32::try_from(argvarnos.len()).unwrap_or(i32::MAX);
    compiled.fn_argvarnos = argvarnos;
    compiled.out_param_varno = out_param_varno;
    compiled.fn_is_trigger = if create_stmt.is_trigger {
        PlPgSqlTrigtype::DmlTrigger
    } else {
        PlPgSqlTrigtype::NotTrigger
    };
    compiled.new_varno = new_varno;
    compiled.old_varno = old_varno;
    Ok(compiled)
}

struct BodyParser<'a> {
    source: &'a str,
    tokens: Vec<PlPgSqlToken>,
    idx: usize,
    datums: Vec<PlPgSqlDatum>,
    variables_by_name: HashMap<String, i32>,
    next_dno: i32,
    next_stmtid: u32,
    has_exception_block: bool,
}

struct ParsedSelectInto {
    rewritten_sql: String,
    strict: bool,
    target_dnos: Vec<i32>,
}

impl<'a> BodyParser<'a> {
    fn new(source: &'a str, tokens: Vec<PlPgSqlToken>) -> Self {
        Self {
            source,
            tokens,
            idx: 0,
            datums: Vec::new(),
            variables_by_name: HashMap::new(),
            next_dno: 0,
            next_stmtid: 0,
            has_exception_block: false,
        }
    }

    fn parse_function_body(&mut self) -> Result<PlPgSqlFunction, PlPgSqlCompileError> {
        let found_varno = self.add_found_datum();
        let mut initvarnos = vec![found_varno];

        if self.consume_keyword(PlPgSqlKeyword::Declare) {
            initvarnos.extend(self.parse_declare_section()?);
        }

        let begin_token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Begin, "expected BEGIN")?;
        let block = self.parse_block_after_begin(&begin_token, initvarnos, false)?;

        if !matches!(self.current_kind(), PlPgSqlTokenKind::Eof) {
            return Err(self.error_at_current("unexpected tokens after END"));
        }

        let ndatums = i32::try_from(self.datums.len()).unwrap_or(i32::MAX);

        Ok(PlPgSqlFunction {
            fn_signature: "plpgsql_phase1".to_string(),
            fn_oid: 0,
            fn_is_trigger: PlPgSqlTrigtype::NotTrigger,
            fn_input_collation: 0,
            fn_cxt: None,
            fn_rettype: 2278, // VOIDOID in PostgreSQL.
            fn_rettyplen: 4,
            fn_retbyval: true,
            fn_retistuple: false,
            fn_retisdomain: false,
            fn_retset: false,
            fn_readonly: false,
            fn_prokind: 'f',
            fn_nargs: 0,
            fn_argvarnos: Vec::new(),
            out_param_varno: -1,
            found_varno,
            new_varno: -1,
            old_varno: -1,
            resolve_option: PlPgSqlResolveOption::Error,
            print_strict_params: false,
            extra_warnings: 0,
            extra_errors: 0,
            ndatums,
            datums: self.datums.clone(),
            copiable_size: 0,
            action: Some(block),
            nstatements: self.next_stmtid,
            requires_procedure_resowner: false,
            has_exception_block: self.has_exception_block,
            cur_estate: None,
            cached_function: None,
        })
    }

    fn add_argument_datums(
        &mut self,
        create_stmt: &CreateFunctionStatement,
    ) -> (Vec<i32>, i32, i32, i32) {
        let mut argvarnos = Vec::with_capacity(create_stmt.params.len());
        let mut out_varnos = Vec::new();
        let mut out_fieldnames = Vec::new();

        for (idx, param) in create_stmt.params.iter().enumerate() {
            let dno = self.allocate_dno();
            let type_name = type_name_to_decl_string(&param.data_type);
            let refname = param
                .name
                .clone()
                .unwrap_or_else(|| format!("${}", idx + 1));
            let variable = PlPgSqlVariable {
                dtype: PlPgSqlDtype::Var,
                dno,
                refname: refname.clone(),
                lineno: 1,
                isconst: false,
                notnull: false,
                default_val: None,
            };
            let var = PlPgSqlVar {
                variable,
                datatype: Some(self.make_type(&type_name)),
                cursor_explicit_expr: None,
                cursor_explicit_argrow: -1,
                cursor_options: 0,
                value: Some(PlPgSqlValue::Null),
                isnull: true,
                freeval: false,
                promise: PlPgSqlPromiseType::None,
            };
            if let Some(name) = &param.name {
                self.variables_by_name
                    .insert(name.to_ascii_lowercase(), dno);
            }
            self.datums.push(PlPgSqlDatum::Var(var));
            if matches!(param.mode, FunctionParamMode::In | FunctionParamMode::InOut) {
                argvarnos.push(dno);
            }
            if matches!(
                param.mode,
                FunctionParamMode::Out | FunctionParamMode::InOut
            ) {
                out_varnos.push(dno);
                out_fieldnames.push(refname);
            }
        }

        let out_param_varno = if out_varnos.is_empty() {
            -1
        } else if out_varnos.len() == 1 {
            out_varnos[0]
        } else {
            let dno = self.allocate_dno();
            let variable = PlPgSqlVariable {
                dtype: PlPgSqlDtype::Row,
                dno,
                refname: "__out_params__".to_string(),
                lineno: 1,
                isconst: true,
                notnull: false,
                default_val: None,
            };
            self.datums.push(PlPgSqlDatum::Row(PlPgSqlRow {
                variable,
                rowtupdesc: None,
                nfields: i32::try_from(out_fieldnames.len()).unwrap_or(i32::MAX),
                fieldnames: out_fieldnames,
                varnos: out_varnos,
            }));
            dno
        };

        let (new_varno, old_varno) = if create_stmt.is_trigger {
            (
                self.add_internal_var("new", "record", PlPgSqlValue::Null),
                self.add_internal_var("old", "record", PlPgSqlValue::Null),
            )
        } else {
            (-1, -1)
        };
        if create_stmt.is_trigger {
            self.add_internal_var("tg_op", "text", PlPgSqlValue::Text(String::new()));
            self.add_internal_var("tg_table_name", "text", PlPgSqlValue::Text(String::new()));
        }

        (argvarnos, out_param_varno, new_varno, old_varno)
    }

    fn add_internal_var(&mut self, name: &str, type_name: &str, value: PlPgSqlValue) -> i32 {
        let dno = self.allocate_dno();
        let variable = PlPgSqlVariable {
            dtype: PlPgSqlDtype::Var,
            dno,
            refname: name.to_string(),
            lineno: 1,
            isconst: false,
            notnull: false,
            default_val: None,
        };
        let var = PlPgSqlVar {
            variable,
            datatype: Some(self.make_type(type_name)),
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
            value: Some(value),
            isnull: true,
            freeval: false,
            promise: PlPgSqlPromiseType::None,
        };
        self.variables_by_name
            .insert(name.to_ascii_lowercase(), dno);
        self.datums.push(PlPgSqlDatum::Var(var));
        dno
    }

    fn add_found_datum(&mut self) -> i32 {
        let dno = self.allocate_dno();
        let variable = PlPgSqlVariable {
            dtype: PlPgSqlDtype::Var,
            dno,
            refname: "found".to_string(),
            lineno: 0,
            isconst: false,
            notnull: true,
            default_val: Some(self.make_expr("false".to_string())),
        };
        let var = PlPgSqlVar {
            variable,
            datatype: Some(self.make_type("boolean")),
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
            value: Some(PlPgSqlValue::Null),
            isnull: true,
            freeval: false,
            promise: PlPgSqlPromiseType::None,
        };
        self.variables_by_name.insert("found".to_string(), dno);
        self.datums.push(PlPgSqlDatum::Var(var));
        dno
    }

    fn parse_declare_section(&mut self) -> Result<Vec<i32>, PlPgSqlCompileError> {
        let mut initvarnos = Vec::new();

        while !self.is_keyword(PlPgSqlKeyword::Begin) {
            if matches!(self.current_kind(), PlPgSqlTokenKind::Eof) {
                return Err(self.error_at_current("expected BEGIN after DECLARE section"));
            }
            if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
                self.advance();
                continue;
            }

            let name = self.expect_identifier("expected variable name in DECLARE section")?;
            let decl_line = self.current_token().span.line;
            let dno = self.allocate_dno();

            let type_start_idx = self.idx;
            while !matches!(self.current_kind(), PlPgSqlTokenKind::Assign)
                && !matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon)
                && !matches!(self.current_kind(), PlPgSqlTokenKind::Eof)
            {
                self.advance();
            }

            let type_end = self.current_token().span.start;
            let type_name = self.source[self.tokens[type_start_idx].span.start..type_end]
                .trim()
                .to_string();
            if type_name.is_empty() {
                return Err(self.error_at_current("expected variable type in DECLARE section"));
            }

            let default_val = if matches!(self.current_kind(), PlPgSqlTokenKind::Assign) {
                self.advance();
                let start_idx = self.idx;
                let (expr, end_idx) = extract_sql_expression(&self.tokens, self.source, start_idx)?;
                self.idx = end_idx;
                Some(self.make_expr(expr))
            } else {
                None
            };

            self.expect_semicolon()?;

            let variable = PlPgSqlVariable {
                dtype: PlPgSqlDtype::Var,
                dno,
                refname: name.clone(),
                lineno: i32::try_from(decl_line).unwrap_or(i32::MAX),
                isconst: false,
                notnull: false,
                default_val,
            };
            let datatype = self.make_type(&type_name);
            let var = PlPgSqlVar {
                variable,
                datatype: Some(datatype),
                cursor_explicit_expr: None,
                cursor_explicit_argrow: -1,
                cursor_options: 0,
                value: Some(PlPgSqlValue::Null),
                isnull: true,
                freeval: false,
                promise: PlPgSqlPromiseType::None,
            };

            self.variables_by_name
                .insert(name.to_ascii_lowercase(), dno);
            self.datums.push(PlPgSqlDatum::Var(var));
            initvarnos.push(dno);
        }

        Ok(initvarnos)
    }

    fn parse_block_after_begin(
        &mut self,
        begin_token: &PlPgSqlToken,
        initvarnos: Vec<i32>,
        require_semicolon: bool,
    ) -> Result<PlPgSqlStmtBlock, PlPgSqlCompileError> {
        let body = self.parse_statement_list_until_keywords(&[
            PlPgSqlKeyword::Exception,
            PlPgSqlKeyword::End,
        ])?;
        let exceptions = if self.consume_keyword(PlPgSqlKeyword::Exception) {
            self.parse_exception_section()?
        } else {
            None
        };

        self.expect_keyword(PlPgSqlKeyword::End, "expected END")?;

        // Accept optional END <label>.
        if matches!(self.current_kind(), PlPgSqlTokenKind::Identifier(_)) {
            self.advance();
        }
        if require_semicolon {
            self.expect_semicolon()?;
        } else if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
            self.advance();
        }

        Ok(PlPgSqlStmtBlock {
            cmd_type: PlPgSqlStmtType::Block,
            lineno: i32::try_from(begin_token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            label: None,
            body,
            n_initvars: i32::try_from(initvarnos.len()).unwrap_or(i32::MAX),
            initvarnos,
            exceptions,
        })
    }

    fn parse_exception_section(
        &mut self,
    ) -> Result<Option<PlPgSqlExceptionBlock>, PlPgSqlCompileError> {
        let sqlstate_varno = self.add_exception_magic_var("sqlstate");
        let sqlerrm_varno = self.add_exception_magic_var("sqlerrm");

        let mut exc_list = Vec::new();
        while self.consume_keyword(PlPgSqlKeyword::When) {
            let when_line = self.previous_token().span.line;
            let conditions = self.parse_exception_conditions()?;
            self.expect_keyword(PlPgSqlKeyword::Then, "expected THEN after WHEN conditions")?;
            let action = self.parse_statement_list_until_keywords(&[
                PlPgSqlKeyword::When,
                PlPgSqlKeyword::End,
            ])?;

            exc_list.push(PlPgSqlException {
                lineno: i32::try_from(when_line).unwrap_or(i32::MAX),
                conditions,
                action,
            });
        }

        if exc_list.is_empty() {
            return Err(self.error_at_current("expected WHEN in EXCEPTION block"));
        }

        self.has_exception_block = true;
        Ok(Some(PlPgSqlExceptionBlock {
            sqlstate_varno,
            sqlerrm_varno,
            exc_list,
        }))
    }

    fn parse_exception_conditions(&mut self) -> Result<Vec<PlPgSqlCondition>, PlPgSqlCompileError> {
        let mut conditions = vec![self.parse_exception_condition()?];
        while self.consume_identifier_word("or") {
            conditions.push(self.parse_exception_condition()?);
        }
        Ok(conditions)
    }

    fn parse_exception_condition(&mut self) -> Result<PlPgSqlCondition, PlPgSqlCompileError> {
        if self.consume_identifier_word("sqlstate") {
            let PlPgSqlTokenKind::StringLiteral(code_literal) = self.current_kind().clone() else {
                return Err(self.error_at_current("expected SQLSTATE string literal"));
            };
            self.advance();

            let Some(code) = decode_single_quoted_string(code_literal.as_str()) else {
                return Err(self.error_at_current("invalid SQLSTATE code"));
            };
            if !is_valid_sqlstate_code(code.as_str()) {
                return Err(self.error_at_current("invalid SQLSTATE code"));
            }

            let Some(sqlerrstate) = sqlstate_code_to_int(code.as_str()) else {
                return Err(self.error_at_current("invalid SQLSTATE code"));
            };
            return Ok(PlPgSqlCondition {
                sqlerrstate,
                condname: code,
            });
        }

        let condname = match self.current_kind().clone() {
            PlPgSqlTokenKind::Identifier(name) => {
                self.advance();
                name
            }
            _ => return Err(self.error_at_current("expected exception condition")),
        };

        if condname.eq_ignore_ascii_case("others") {
            return Ok(PlPgSqlCondition {
                sqlerrstate: PLPGSQL_OTHERS,
                condname,
            });
        }

        let Some(sqlerrstate) = exception_condition_name_to_sqlstate(condname.as_str()) else {
            return Err(PlPgSqlCompileError {
                message: format!("unrecognized exception condition \"{condname}\""),
                position: self.previous_token().span.start,
                line: self.previous_token().span.line,
                column: self.previous_token().span.column,
            });
        };

        Ok(PlPgSqlCondition {
            sqlerrstate,
            condname,
        })
    }

    fn add_exception_magic_var(&mut self, name: &str) -> i32 {
        let dno = self.allocate_dno();
        let variable = PlPgSqlVariable {
            dtype: PlPgSqlDtype::Var,
            dno,
            refname: name.to_string(),
            lineno: i32::try_from(self.previous_token().span.line).unwrap_or(i32::MAX),
            isconst: true,
            notnull: false,
            default_val: None,
        };
        let var = PlPgSqlVar {
            variable,
            datatype: Some(self.make_type("text")),
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
            value: Some(PlPgSqlValue::Null),
            isnull: true,
            freeval: false,
            promise: PlPgSqlPromiseType::None,
        };
        self.variables_by_name
            .insert(name.to_ascii_lowercase(), dno);
        self.datums.push(PlPgSqlDatum::Var(var));
        dno
    }

    fn parse_statement_list_until_keywords(
        &mut self,
        stop_keywords: &[PlPgSqlKeyword],
    ) -> Result<Vec<PlPgSqlStmt>, PlPgSqlCompileError> {
        let mut stmts = Vec::new();

        while !matches!(self.current_kind(), PlPgSqlTokenKind::Eof)
            && !self.current_keyword_in(stop_keywords)
        {
            if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
                self.advance();
                continue;
            }
            stmts.push(self.parse_statement()?);
        }

        Ok(stmts)
    }

    fn parse_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        if self.is_keyword(PlPgSqlKeyword::Begin) {
            return self.parse_begin_block_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Declare) {
            return self.parse_declared_block_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::If) {
            return self.parse_if_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::For) {
            return self.parse_for_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Return) {
            return self.parse_return_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Raise) {
            return self.parse_raise_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Assert) {
            return self.parse_assert_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Execute) {
            return self.parse_execute_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Open) {
            return self.parse_open_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Fetch) {
            return self.parse_fetch_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Close) {
            return self.parse_close_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Perform) {
            return self.parse_perform_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Commit) {
            return self.parse_commit_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Rollback) {
            return self.parse_rollback_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Call) {
            return self.parse_call_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Get) {
            return self.parse_getdiag_statement();
        }
        if self.is_keyword(PlPgSqlKeyword::Null) {
            return self.parse_null_statement();
        }

        if let PlPgSqlTokenKind::Identifier(name) = self.current_kind().clone() {
            if matches!(self.peek_kind(1), Some(PlPgSqlTokenKind::Assign)) {
                return self.parse_assignment_statement(name, None);
            }
            if matches!(self.peek_kind(1), Some(PlPgSqlTokenKind::Dot))
                && matches!(self.peek_kind(2), Some(PlPgSqlTokenKind::Identifier(_)))
                && matches!(self.peek_kind(3), Some(PlPgSqlTokenKind::Assign))
            {
                let Some(PlPgSqlTokenKind::Identifier(fieldname)) = self.peek_kind(2).cloned()
                else {
                    return Err(self.error_at_current("expected record field name"));
                };
                return self.parse_assignment_statement(name, Some(fieldname));
            }
        }

        self.parse_execsql_statement()
    }

    fn parse_begin_block_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let begin_token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Begin, "expected BEGIN")?;
        let block = self.parse_block_after_begin(&begin_token, Vec::new(), true)?;
        Ok(PlPgSqlStmt::Block(block))
    }

    fn parse_declared_block_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        self.expect_keyword(PlPgSqlKeyword::Declare, "expected DECLARE")?;
        let initvarnos = self.parse_declare_section()?;
        let begin_token = self.current_token().clone();
        self.expect_keyword(
            PlPgSqlKeyword::Begin,
            "expected BEGIN after DECLARE section",
        )?;
        let block = self.parse_block_after_begin(&begin_token, initvarnos, true)?;
        Ok(PlPgSqlStmt::Block(block))
    }

    fn parse_assignment_statement(
        &mut self,
        name: String,
        fieldname: Option<String>,
    ) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.advance();
        if fieldname.is_some() {
            if !matches!(self.current_kind(), PlPgSqlTokenKind::Dot) {
                return Err(self.error_at_current("expected '.' in record field assignment"));
            }
            self.advance();
            let _ = self.expect_identifier("expected record field name in assignment target")?;
        }
        self.expect_assign()?;

        let start_idx = self.idx;
        let (expr_text, end_idx) = extract_sql_expression(&self.tokens, self.source, start_idx)?;
        self.idx = end_idx;
        self.expect_semicolon()?;

        let varno = self
            .variables_by_name
            .get(&name.to_ascii_lowercase())
            .copied()
            .unwrap_or(-1);

        Ok(PlPgSqlStmt::Assign(PlPgSqlStmtAssign {
            cmd_type: PlPgSqlStmtType::Assign,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            varno,
            fieldname,
            expr: self.make_expr(expr_text),
        }))
    }

    fn parse_if_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let if_token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::If, "expected IF")?;

        let cond = self.extract_expression_until_keywords(&[PlPgSqlKeyword::Then])?;
        self.expect_keyword(PlPgSqlKeyword::Then, "expected THEN")?;

        let then_body = self.parse_statement_list_until_keywords(&[
            PlPgSqlKeyword::Elsif,
            PlPgSqlKeyword::Else,
            PlPgSqlKeyword::End,
        ])?;

        let mut elsif_list = Vec::new();
        while self.consume_keyword(PlPgSqlKeyword::Elsif) {
            let elsif_line = self.previous_token().span.line;
            let elsif_cond = self.extract_expression_until_keywords(&[PlPgSqlKeyword::Then])?;
            self.expect_keyword(PlPgSqlKeyword::Then, "expected THEN after ELSIF condition")?;
            let elsif_stmts = self.parse_statement_list_until_keywords(&[
                PlPgSqlKeyword::Elsif,
                PlPgSqlKeyword::Else,
                PlPgSqlKeyword::End,
            ])?;
            elsif_list.push(PlPgSqlIfElsif {
                lineno: i32::try_from(elsif_line).unwrap_or(i32::MAX),
                cond: self.make_expr(elsif_cond),
                stmts: elsif_stmts,
            });
        }

        let else_body = if self.consume_keyword(PlPgSqlKeyword::Else) {
            self.parse_statement_list_until_keywords(&[PlPgSqlKeyword::End])?
        } else {
            Vec::new()
        };

        self.expect_keyword(PlPgSqlKeyword::End, "expected END to close IF")?;
        let _ = self.consume_keyword(PlPgSqlKeyword::If);
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::If(PlPgSqlStmtIf {
            cmd_type: PlPgSqlStmtType::If,
            lineno: i32::try_from(if_token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            cond: self.make_expr(cond),
            then_body,
            elsif_list,
            else_body,
        }))
    }

    fn parse_for_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        enum ForHeader {
            Integer {
                var: PlPgSqlVar,
                lower: String,
                upper: String,
                step: Option<String>,
                reverse: i32,
            },
            Query {
                var: PlPgSqlVariable,
                query: String,
            },
            Cursor {
                var: PlPgSqlVariable,
                curvar: i32,
            },
        }

        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::For, "expected FOR")?;
        let var_name = self.expect_identifier("expected loop variable after FOR")?;
        self.expect_identifier_word("in", "expected IN in FOR statement")?;
        let reverse = if self.consume_identifier_word("reverse") {
            1
        } else {
            0
        };

        let header_start_idx = self.idx;
        let mut idx = self.idx;
        let mut depth = 0usize;
        let mut loop_idx = None;
        let mut range_idx = None;
        let mut by_idx = None;

        while idx < self.tokens.len() {
            match &self.tokens[idx].kind {
                PlPgSqlTokenKind::LParen => depth += 1,
                PlPgSqlTokenKind::RParen => depth = depth.saturating_sub(1),
                PlPgSqlTokenKind::Keyword(PlPgSqlKeyword::Loop) if depth == 0 => {
                    loop_idx = Some(idx);
                    break;
                }
                PlPgSqlTokenKind::Dot if depth == 0 => {
                    if matches!(
                        self.tokens.get(idx + 1).map(|t| &t.kind),
                        Some(PlPgSqlTokenKind::Dot)
                    ) && range_idx.is_none()
                    {
                        range_idx = Some(idx);
                        idx += 1;
                    }
                }
                PlPgSqlTokenKind::Identifier(word)
                    if depth == 0 && range_idx.is_some() && by_idx.is_none() =>
                {
                    if word.eq_ignore_ascii_case("by") {
                        by_idx = Some(idx);
                    }
                }
                PlPgSqlTokenKind::Eof => break,
                _ => {}
            }
            idx += 1;
        }

        let Some(loop_idx) = loop_idx else {
            return Err(self.error_at_current("expected LOOP in FOR statement"));
        };

        let for_header = if let Some(range_idx) = range_idx {
            let lower = self.slice_token_range(header_start_idx, range_idx);
            if lower.is_empty() {
                return Err(self.error_at_current("expected lower bound expression in FOR range"));
            }

            let upper_end = by_idx.unwrap_or(loop_idx);
            let upper = self.slice_token_range(range_idx + 2, upper_end);
            if upper.is_empty() {
                return Err(self.error_at_current("expected upper bound expression in FOR range"));
            }

            let step = by_idx.map(|by_idx| self.slice_token_range(by_idx + 1, loop_idx));
            if step.as_deref().is_some_and(str::is_empty) {
                return Err(self.error_at_current("expected step expression after BY"));
            }

            let loop_var =
                self.lookup_or_create_loop_var(&var_name, token.span.line, Some("integer"))?;
            ForHeader::Integer {
                var: loop_var,
                lower,
                upper,
                step,
                reverse,
            }
        } else {
            if reverse != 0 {
                return Err(self.error_at_current("REVERSE is only valid for integer FOR loops"));
            }
            let query = self.slice_token_range(header_start_idx, loop_idx);
            if query.is_empty() {
                return Err(self.error_at_current("expected query expression in FOR statement"));
            }
            let loop_var = self.lookup_or_create_loop_var(&var_name, token.span.line, None)?;
            if let Some(curvar) = self.cursor_varno_for_header(header_start_idx, loop_idx) {
                ForHeader::Cursor {
                    var: loop_var.variable,
                    curvar,
                }
            } else {
                ForHeader::Query {
                    var: loop_var.variable,
                    query,
                }
            }
        };

        self.idx = loop_idx;
        self.expect_keyword(PlPgSqlKeyword::Loop, "expected LOOP in FOR statement")?;
        let body = self.parse_statement_list_until_keywords(&[PlPgSqlKeyword::End])?;
        self.expect_keyword(PlPgSqlKeyword::End, "expected END to close FOR loop")?;
        self.expect_keyword(PlPgSqlKeyword::Loop, "expected LOOP after END in FOR loop")?;
        self.expect_semicolon()?;

        match for_header {
            ForHeader::Integer {
                var,
                lower,
                upper,
                step,
                reverse,
            } => Ok(PlPgSqlStmt::Fori(PlPgSqlStmtFori {
                cmd_type: PlPgSqlStmtType::Fori,
                lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
                stmtid: self.alloc_stmtid(),
                label: None,
                var: Some(var),
                lower: self.make_expr(lower),
                upper: self.make_expr(upper),
                step: step.map(|s| self.make_expr(s)),
                reverse,
                body,
            })),
            ForHeader::Query { var, query } => Ok(PlPgSqlStmt::Fors(PlPgSqlStmtFors {
                cmd_type: PlPgSqlStmtType::Fors,
                lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
                stmtid: self.alloc_stmtid(),
                label: None,
                var: Some(var),
                body,
                query: self.make_expr(query),
            })),
            ForHeader::Cursor { var, curvar } => Ok(PlPgSqlStmt::Forc(PlPgSqlStmtForc {
                cmd_type: PlPgSqlStmtType::Forc,
                lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
                stmtid: self.alloc_stmtid(),
                label: None,
                var: Some(var),
                body,
                curvar,
                argquery: None,
            })),
        }
    }

    fn parse_return_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Return, "expected RETURN")?;

        let expr = if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
            None
        } else {
            let (expr_text, end_idx) = extract_sql_expression(&self.tokens, self.source, self.idx)?;
            self.idx = end_idx;
            Some(self.make_expr(expr_text))
        };
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Return(PlPgSqlStmtReturn {
            cmd_type: PlPgSqlStmtType::Return,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            expr,
            retvarno: -1,
        }))
    }

    fn parse_raise_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Raise, "expected RAISE")?;

        let mut condname = None;
        let mut elog_level = 0;

        if self.is_keyword(PlPgSqlKeyword::Exception) {
            self.advance();
            condname = Some("EXCEPTION".to_string());
        } else if let PlPgSqlTokenKind::Identifier(word) = self.current_kind().clone()
            && word.eq_ignore_ascii_case("exception")
        {
            self.advance();
            condname = Some("EXCEPTION".to_string());
        } else if self.is_keyword(PlPgSqlKeyword::Notice) {
            self.advance();
            condname = Some("NOTICE".to_string());
            elog_level = 1;
        } else if let PlPgSqlTokenKind::Identifier(word) = self.current_kind().clone()
            && word.eq_ignore_ascii_case("notice")
        {
            self.advance();
            condname = Some("NOTICE".to_string());
            elog_level = 1;
        }

        let followed_by_using_or_semicolon =
            matches!(self.peek_kind(1), Some(PlPgSqlTokenKind::Semicolon))
                || matches!(
                    self.peek_kind(1),
                    Some(PlPgSqlTokenKind::Identifier(next)) if next.eq_ignore_ascii_case("using")
                );
        if condname.is_none()
            && let PlPgSqlTokenKind::Identifier(word) = self.current_kind().clone()
            && !word.eq_ignore_ascii_case("using")
            && followed_by_using_or_semicolon
        {
            condname = Some(word);
            self.advance();
        }

        let mut parts = Vec::new();
        if !matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon)
            && !self.peek_identifier_word("using")
        {
            parts = self.extract_comma_expressions_until_semicolon_or_using()?;
        }
        let mut message = parts.first().cloned();
        let params = parts
            .iter()
            .skip(1)
            .cloned()
            .map(|query| self.make_expr(query))
            .collect::<Vec<_>>();

        let mut options = Vec::new();
        if self.consume_identifier_word("using") {
            while !matches!(
                self.current_kind(),
                PlPgSqlTokenKind::Semicolon | PlPgSqlTokenKind::Eof
            ) {
                let option_name =
                    self.expect_identifier("expected RAISE option name after USING")?;
                if !matches!(
                    self.current_kind(),
                    PlPgSqlTokenKind::Equals | PlPgSqlTokenKind::Assign
                ) {
                    return Err(self.error_at_current("expected = in RAISE USING option"));
                }
                self.advance();
                let expr = self.extract_expression_until_comma_or_semicolon()?;
                let opt_type = match option_name.to_ascii_lowercase().as_str() {
                    "message" => PlPgSqlRaiseOptionType::Message,
                    "detail" => PlPgSqlRaiseOptionType::Detail,
                    "hint" => PlPgSqlRaiseOptionType::Hint,
                    "errcode" => PlPgSqlRaiseOptionType::Errcode,
                    "column" => PlPgSqlRaiseOptionType::Column,
                    "constraint" => PlPgSqlRaiseOptionType::Constraint,
                    "datatype" => PlPgSqlRaiseOptionType::Datatype,
                    "table" => PlPgSqlRaiseOptionType::Table,
                    "schema" => PlPgSqlRaiseOptionType::Schema,
                    _ => PlPgSqlRaiseOptionType::Detail,
                };
                if matches!(opt_type, PlPgSqlRaiseOptionType::Message) {
                    message = Some(expr.clone());
                }
                options.push(PlPgSqlRaiseOption {
                    opt_type,
                    expr: self.make_expr(expr),
                });
                if matches!(self.current_kind(), PlPgSqlTokenKind::Comma) {
                    self.advance();
                }
            }
        }

        self.expect_semicolon()?;

        if options.is_empty()
            && let Some(msg) = &message
        {
            options.push(PlPgSqlRaiseOption {
                opt_type: PlPgSqlRaiseOptionType::Message,
                expr: self.make_expr(msg.clone()),
            });
        }

        Ok(PlPgSqlStmt::Raise(PlPgSqlStmtRaise {
            cmd_type: PlPgSqlStmtType::Raise,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            elog_level,
            condname,
            message,
            params,
            options,
        }))
    }

    fn parse_assert_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Assert, "expected ASSERT")?;
        let cond = self.extract_expression_until_comma_or_semicolon()?;

        let message = if matches!(self.current_kind(), PlPgSqlTokenKind::Comma) {
            self.advance();
            let (expr, end_idx) = extract_sql_expression(&self.tokens, self.source, self.idx)?;
            self.idx = end_idx;
            Some(self.make_expr(expr))
        } else {
            None
        };
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Assert(
            crate::plpgsql::types::PlPgSqlStmtAssert {
                cmd_type: PlPgSqlStmtType::Assert,
                lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
                stmtid: self.alloc_stmtid(),
                cond: self.make_expr(cond),
                message,
            },
        ))
    }

    fn parse_perform_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Perform, "expected PERFORM")?;
        let (expr_text, end_idx) = extract_sql_expression(&self.tokens, self.source, self.idx)?;
        self.idx = end_idx;
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Perform(PlPgSqlStmtPerform {
            cmd_type: PlPgSqlStmtType::Perform,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            expr: self.make_expr(expr_text),
        }))
    }

    fn parse_execute_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Execute, "expected EXECUTE")?;

        let query_start_idx = self.idx;
        let mut idx = self.idx;
        let mut depth = 0usize;
        let mut into_idx = None;
        let mut semicolon_idx = None;

        while idx < self.tokens.len() {
            match &self.tokens[idx].kind {
                PlPgSqlTokenKind::LParen => depth += 1,
                PlPgSqlTokenKind::RParen => depth = depth.saturating_sub(1),
                PlPgSqlTokenKind::Identifier(word) if depth == 0 && into_idx.is_none() => {
                    if word.eq_ignore_ascii_case("into") {
                        into_idx = Some(idx);
                    }
                }
                PlPgSqlTokenKind::Semicolon if depth == 0 => {
                    semicolon_idx = Some(idx);
                    break;
                }
                PlPgSqlTokenKind::Eof => break,
                _ => {}
            }
            idx += 1;
        }

        let Some(semicolon_idx) = semicolon_idx else {
            return Err(self.error_at_current("unterminated EXECUTE statement"));
        };

        let query_end_idx = into_idx.unwrap_or(semicolon_idx);
        let query = self.slice_token_range(query_start_idx, query_end_idx);
        if query.is_empty() {
            return Err(self.error_at_current("expected dynamic SQL expression after EXECUTE"));
        }

        let mut into = false;
        let mut strict = false;
        let mut target = None;

        if let Some(into_idx) = into_idx {
            into = true;
            let mut target_idx = into_idx + 1;

            if let Some(PlPgSqlTokenKind::Identifier(word)) =
                self.tokens.get(target_idx).map(|t| &t.kind)
                && word.eq_ignore_ascii_case("strict")
            {
                strict = true;
                target_idx += 1;
            }

            let Some(PlPgSqlToken {
                kind: PlPgSqlTokenKind::Identifier(target_name),
                ..
            }) = self.tokens.get(target_idx)
            else {
                return Err(self.error_at_current("expected target variable after INTO"));
            };

            target = self.variable_by_name(target_name);
            if target.is_none() {
                return Err(PlPgSqlCompileError {
                    message: format!("unknown variable \"{target_name}\" in EXECUTE INTO"),
                    position: self.tokens[target_idx].span.start,
                    line: self.tokens[target_idx].span.line,
                    column: self.tokens[target_idx].span.column,
                });
            }

            if target_idx + 1 != semicolon_idx {
                return Err(PlPgSqlCompileError {
                    message: "invalid EXECUTE INTO target list".to_string(),
                    position: self.tokens[target_idx + 1].span.start,
                    line: self.tokens[target_idx + 1].span.line,
                    column: self.tokens[target_idx + 1].span.column,
                });
            }
        }

        self.idx = semicolon_idx;
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::DynExecute(PlPgSqlStmtDynexecute {
            cmd_type: PlPgSqlStmtType::DynExecute,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            query: self.make_expr(query),
            into,
            strict,
            target,
            params: Vec::new(),
        }))
    }

    fn parse_open_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Open, "expected OPEN")?;
        let cursor_name = self.expect_identifier("expected cursor variable after OPEN")?;
        let curvar = self.lookup_cursor_varno(&cursor_name)?;
        let (query, dynquery) = if self.consume_keyword(PlPgSqlKeyword::For)
            || self.consume_identifier_word("for")
        {
            if self.is_keyword(PlPgSqlKeyword::Execute) {
                self.advance();
                let (dynquery_text, end_idx) =
                    extract_sql_expression(&self.tokens, self.source, self.idx)?;
                self.idx = end_idx;
                (None, Some(self.make_expr(dynquery_text)))
            } else {
                let (query_text, end_idx) =
                    extract_sql_expression(&self.tokens, self.source, self.idx)?;
                self.idx = end_idx;
                (Some(self.make_expr(query_text)), None)
            }
        } else {
            if matches!(self.current_kind(), PlPgSqlTokenKind::LParen) {
                self.advance();
                let mut depth = 1usize;
                while depth > 0 {
                    match self.current_kind() {
                        PlPgSqlTokenKind::LParen => {
                            depth += 1;
                            self.advance();
                        }
                        PlPgSqlTokenKind::RParen => {
                            depth -= 1;
                            self.advance();
                        }
                        PlPgSqlTokenKind::Eof => {
                            return Err(self.error_at_current("unterminated OPEN argument list"));
                        }
                        _ => self.advance(),
                    }
                }
            }
            (
                Some(self.make_expr("SELECT NULL WHERE FALSE".to_string())),
                None,
            )
        };
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Open(PlPgSqlStmtOpen {
            cmd_type: PlPgSqlStmtType::Open,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            curvar,
            cursor_options: 0,
            argquery: None,
            query,
            dynquery,
            params: Vec::new(),
        }))
    }

    fn parse_fetch_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Fetch, "expected FETCH")?;
        let cursor_name = self.expect_identifier("expected cursor variable after FETCH")?;
        let curvar = self.lookup_cursor_varno(&cursor_name)?;
        self.expect_identifier_word("into", "expected INTO in FETCH statement")?;
        let target_name = self.expect_identifier("expected target variable after INTO")?;
        let target = self.variable_by_name(target_name.as_str());
        let Some(target) = target else {
            return Err(PlPgSqlCompileError {
                message: format!("unknown variable \"{target_name}\" in FETCH INTO"),
                position: self.previous_token().span.start,
                line: self.previous_token().span.line,
                column: self.previous_token().span.column,
            });
        };
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Fetch(PlPgSqlStmtFetch {
            cmd_type: PlPgSqlStmtType::Fetch,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            target: Some(target),
            curvar,
            direction: None,
            how_many: 1,
            expr: None,
            is_move: false,
            returns_multiple_rows: false,
        }))
    }

    fn parse_close_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Close, "expected CLOSE")?;
        let cursor_name = self.expect_identifier("expected cursor variable after CLOSE")?;
        let curvar = self.lookup_cursor_varno(&cursor_name)?;
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Close(PlPgSqlStmtClose {
            cmd_type: PlPgSqlStmtType::Close,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            curvar,
        }))
    }

    fn parse_call_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Call, "expected CALL")?;
        let (expr_text, end_idx) = extract_sql_expression(&self.tokens, self.source, self.idx)?;
        self.idx = end_idx;
        self.expect_semicolon()?;

        Ok(PlPgSqlStmt::Call(PlPgSqlStmtCall {
            cmd_type: PlPgSqlStmtType::Call,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            expr: self.make_expr(expr_text),
            is_call: true,
            target: None,
        }))
    }

    fn parse_commit_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Commit, "expected COMMIT")?;
        self.expect_semicolon()?;
        Ok(PlPgSqlStmt::Commit(PlPgSqlStmtCommit {
            cmd_type: PlPgSqlStmtType::Commit,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            chain: false,
        }))
    }

    fn parse_rollback_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Rollback, "expected ROLLBACK")?;
        self.expect_semicolon()?;
        Ok(PlPgSqlStmt::Rollback(PlPgSqlStmtRollback {
            cmd_type: PlPgSqlStmtType::Rollback,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            chain: false,
        }))
    }

    fn parse_null_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        self.expect_keyword(PlPgSqlKeyword::Null, "expected NULL")?;
        if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
            self.advance();
        }
        // NULL is a no-op statement — just return an empty exec SQL with no effect
        let stmtid = self.alloc_stmtid();
        Ok(PlPgSqlStmt::Block(PlPgSqlStmtBlock {
            cmd_type: PlPgSqlStmtType::Block,
            lineno: 0,
            stmtid,
            label: None,
            body: Vec::new(),
            n_initvars: 0,
            initvarnos: Vec::new(),
            exceptions: None,
        }))
    }

    fn parse_getdiag_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        self.expect_keyword(PlPgSqlKeyword::Get, "expected GET")?;

        if self.is_keyword(PlPgSqlKeyword::Diagnostics) {
            self.advance();
            let mut diag_items = Vec::new();

            loop {
                let target_name = self
                    .expect_identifier("expected variable name in GET DIAGNOSTICS target list")?;
                let Some(target_dno) = self
                    .variables_by_name
                    .get(&target_name.to_ascii_lowercase())
                    .copied()
                else {
                    return Err(PlPgSqlCompileError {
                        message: format!("unknown variable \"{target_name}\" in GET DIAGNOSTICS"),
                        position: self.previous_token().span.start,
                        line: self.previous_token().span.line,
                        column: self.previous_token().span.column,
                    });
                };

                if matches!(self.current_kind(), PlPgSqlTokenKind::Assign)
                    || matches!(self.current_kind(), PlPgSqlTokenKind::Equals)
                {
                    self.advance();
                } else {
                    return Err(self.error_at_current("expected := or = in GET DIAGNOSTICS item"));
                }

                let kind = match self.current_kind() {
                    PlPgSqlTokenKind::Identifier(name)
                        if name.eq_ignore_ascii_case("row_count") =>
                    {
                        self.advance();
                        PlPgSqlGetdiagKind::RowCount
                    }
                    PlPgSqlTokenKind::Identifier(name)
                        if name.eq_ignore_ascii_case("pg_routine_oid") =>
                    {
                        self.advance();
                        PlPgSqlGetdiagKind::RoutineOid
                    }
                    _ => return Err(self.error_at_current("unsupported GET DIAGNOSTICS item")),
                };

                diag_items.push(PlPgSqlDiagItem {
                    kind,
                    target: target_dno,
                });

                if matches!(self.current_kind(), PlPgSqlTokenKind::Comma) {
                    self.advance();
                    continue;
                }
                break;
            }

            if diag_items.is_empty() {
                return Err(self.error_at_current("GET DIAGNOSTICS requires at least one item"));
            }

            self.expect_semicolon()?;
            return Ok(PlPgSqlStmt::Getdiag(PlPgSqlStmtGetdiag {
                cmd_type: PlPgSqlStmtType::GetDiag,
                lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
                stmtid: self.alloc_stmtid(),
                is_stacked: false,
                diag_items,
            }));
        }

        Err(self.error_at_current("expected DIAGNOSTICS after GET"))
    }

    fn parse_execsql_statement(&mut self) -> Result<PlPgSqlStmt, PlPgSqlCompileError> {
        let token = self.current_token().clone();
        let (sql, end_idx) = extract_sql_expression(&self.tokens, self.source, self.idx)?;
        self.idx = end_idx;
        self.expect_semicolon()?;

        let mut sqlstmt = sql;
        let mut into = false;
        let mut strict = false;
        let mut target_dnos = Vec::new();
        let mut target = None;

        if let Some(parsed_into) = self.try_parse_select_into(&sqlstmt)? {
            into = true;
            strict = parsed_into.strict;
            target_dnos = parsed_into.target_dnos;
            sqlstmt = parsed_into.rewritten_sql;
            target = self.variable_by_dno(target_dnos[0]);
        }

        Ok(PlPgSqlStmt::ExecSql(PlPgSqlStmtExecSql {
            cmd_type: PlPgSqlStmtType::ExecSql,
            lineno: i32::try_from(token.span.line).unwrap_or(i32::MAX),
            stmtid: self.alloc_stmtid(),
            sqlstmt: self.make_expr(sqlstmt),
            mod_stmt: false,
            mod_stmt_set: false,
            into,
            strict,
            target,
            target_dnos,
        }))
    }

    fn try_parse_select_into(
        &self,
        sql: &str,
    ) -> Result<Option<ParsedSelectInto>, PlPgSqlCompileError> {
        let tokens = tokenize(sql).map_err(PlPgSqlCompileError::from)?;
        if tokens.is_empty() {
            return Ok(None);
        }

        let mut first_idx = None;
        for (idx, token) in tokens.iter().enumerate() {
            if !matches!(token.kind, PlPgSqlTokenKind::Eof) {
                first_idx = Some(idx);
                break;
            }
        }
        let Some(first_idx) = first_idx else {
            return Ok(None);
        };

        let PlPgSqlTokenKind::Identifier(first_word) = &tokens[first_idx].kind else {
            return Ok(None);
        };
        if !first_word.eq_ignore_ascii_case("select") {
            return Ok(None);
        }

        let mut depth = 0usize;
        let mut into_idx = None;
        for (idx, tok) in tokens.iter().enumerate().skip(first_idx + 1) {
            match &tok.kind {
                PlPgSqlTokenKind::LParen => depth += 1,
                PlPgSqlTokenKind::RParen => depth = depth.saturating_sub(1),
                PlPgSqlTokenKind::Identifier(word)
                    if depth == 0 && word.eq_ignore_ascii_case("into") =>
                {
                    into_idx = Some(idx);
                    break;
                }
                _ => {}
            }
        }
        let Some(into_idx) = into_idx else {
            return Ok(None);
        };

        let mut strict = false;
        let mut idx = into_idx + 1;
        if let Some(PlPgSqlToken {
            kind: PlPgSqlTokenKind::Identifier(word),
            ..
        }) = tokens.get(idx)
            && word.eq_ignore_ascii_case("strict")
        {
            strict = true;
            idx += 1;
        }

        let mut target_dnos = Vec::new();
        let mut expect_target = true;
        let mut clause_start = None;

        while idx < tokens.len() {
            let token = &tokens[idx];
            match &token.kind {
                PlPgSqlTokenKind::Eof => {
                    clause_start = Some(idx);
                    break;
                }
                PlPgSqlTokenKind::Identifier(word) if expect_target => {
                    if is_select_clause_boundary(word) {
                        return Err(PlPgSqlCompileError {
                            message: "expected target variable after INTO".to_string(),
                            position: token.span.start,
                            line: token.span.line,
                            column: token.span.column,
                        });
                    }

                    let key = word.to_ascii_lowercase();
                    let Some(dno) = self.variables_by_name.get(&key).copied() else {
                        return Err(PlPgSqlCompileError {
                            message: format!("unknown variable \"{word}\" in SELECT INTO"),
                            position: token.span.start,
                            line: token.span.line,
                            column: token.span.column,
                        });
                    };
                    target_dnos.push(dno);
                    expect_target = false;
                    idx += 1;
                }
                PlPgSqlTokenKind::Comma if !expect_target => {
                    expect_target = true;
                    idx += 1;
                }
                PlPgSqlTokenKind::Identifier(word)
                    if !expect_target && is_select_clause_boundary(word) =>
                {
                    clause_start = Some(idx);
                    break;
                }
                _ => {
                    return Err(PlPgSqlCompileError {
                        message: "invalid SELECT INTO target list".to_string(),
                        position: token.span.start,
                        line: token.span.line,
                        column: token.span.column,
                    });
                }
            }
        }

        if target_dnos.is_empty() || expect_target {
            return Err(self.error_at_current("expected target variable after INTO"));
        }

        let clause_start_idx = clause_start.unwrap_or(tokens.len().saturating_sub(1));
        let into_start = tokens[into_idx].span.start;
        let into_end = match tokens[clause_start_idx].kind {
            PlPgSqlTokenKind::Eof => sql.len(),
            _ => tokens[clause_start_idx].span.start,
        };

        let prefix = sql[..into_start].trim_end();
        let suffix = sql[into_end..].trim_start();
        let rewritten_sql = if suffix.is_empty() {
            prefix.to_string()
        } else {
            format!("{prefix} {suffix}")
        };

        Ok(Some(ParsedSelectInto {
            rewritten_sql,
            strict,
            target_dnos,
        }))
    }

    fn variable_by_dno(&self, dno: i32) -> Option<PlPgSqlVariable> {
        let idx = usize::try_from(dno).ok()?;
        match self.datums.get(idx) {
            Some(PlPgSqlDatum::Var(var)) => Some(var.variable.clone()),
            _ => None,
        }
    }

    fn variable_by_name(&self, name: &str) -> Option<PlPgSqlVariable> {
        let dno = *self.variables_by_name.get(&name.to_ascii_lowercase())?;
        self.variable_by_dno(dno)
    }

    fn lookup_cursor_varno(&self, name: &str) -> Result<i32, PlPgSqlCompileError> {
        let Some(dno) = self
            .variables_by_name
            .get(&name.to_ascii_lowercase())
            .copied()
        else {
            return Err(PlPgSqlCompileError {
                message: format!("unknown cursor variable \"{name}\""),
                position: self.current_token().span.start,
                line: self.current_token().span.line,
                column: self.current_token().span.column,
            });
        };

        let idx = usize::try_from(dno).unwrap_or(usize::MAX);
        let Some(PlPgSqlDatum::Var(var)) = self.datums.get(idx) else {
            return Err(PlPgSqlCompileError {
                message: format!("cursor variable \"{name}\" is unresolved"),
                position: self.current_token().span.start,
                line: self.current_token().span.line,
                column: self.current_token().span.column,
            });
        };

        let type_name = var
            .datatype
            .as_ref()
            .map(|t| t.typname.to_ascii_lowercase())
            .unwrap_or_default();
        let is_cursor = type_name == "refcursor"
            || type_name == "cursor"
            || type_name.starts_with("cursor")
            || type_name.contains(" refcursor")
            || type_name.contains(" cursor");
        if !is_cursor {
            return Err(PlPgSqlCompileError {
                message: format!("variable \"{name}\" must be of type cursor or refcursor"),
                position: self.current_token().span.start,
                line: self.current_token().span.line,
                column: self.current_token().span.column,
            });
        }

        Ok(dno)
    }

    fn cursor_varno_for_header(&self, start_idx: usize, end_idx: usize) -> Option<i32> {
        if end_idx != start_idx + 1 {
            return None;
        }

        let PlPgSqlTokenKind::Identifier(name) = &self.tokens[start_idx].kind else {
            return None;
        };

        let dno = *self.variables_by_name.get(&name.to_ascii_lowercase())?;
        let idx = usize::try_from(dno).ok()?;
        let PlPgSqlDatum::Var(var) = self.datums.get(idx)? else {
            return None;
        };
        let type_name = var
            .datatype
            .as_ref()
            .map(|t| t.typname.to_ascii_lowercase())
            .unwrap_or_default();
        let is_cursor = type_name == "refcursor"
            || type_name == "cursor"
            || type_name.starts_with("cursor")
            || type_name.contains(" refcursor")
            || type_name.contains(" cursor");
        if is_cursor { Some(dno) } else { None }
    }

    fn lookup_or_create_loop_var(
        &mut self,
        name: &str,
        decl_line: usize,
        datatype_hint: Option<&str>,
    ) -> Result<PlPgSqlVar, PlPgSqlCompileError> {
        let key = name.to_ascii_lowercase();
        if let Some(dno) = self.variables_by_name.get(&key).copied() {
            let idx = usize::try_from(dno).unwrap_or(usize::MAX);
            match self.datums.get(idx) {
                Some(PlPgSqlDatum::Var(var)) => return Ok(var.clone()),
                Some(_) => {
                    return Err(PlPgSqlCompileError {
                        message: format!("loop variable \"{name}\" is not a scalar variable"),
                        position: self.current_token().span.start,
                        line: self.current_token().span.line,
                        column: self.current_token().span.column,
                    });
                }
                None => {
                    return Err(PlPgSqlCompileError {
                        message: format!("loop variable \"{name}\" is unresolved"),
                        position: self.current_token().span.start,
                        line: self.current_token().span.line,
                        column: self.current_token().span.column,
                    });
                }
            }
        }

        let dno = self.allocate_dno();
        let variable = PlPgSqlVariable {
            dtype: PlPgSqlDtype::Var,
            dno,
            refname: name.to_string(),
            lineno: i32::try_from(decl_line).unwrap_or(i32::MAX),
            isconst: false,
            notnull: false,
            default_val: None,
        };
        let var = PlPgSqlVar {
            variable: variable.clone(),
            datatype: datatype_hint.map(|hint| self.make_type(hint)),
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
            value: Some(PlPgSqlValue::Null),
            isnull: true,
            freeval: false,
            promise: PlPgSqlPromiseType::None,
        };

        self.variables_by_name.insert(key, dno);
        self.datums.push(PlPgSqlDatum::Var(var.clone()));
        Ok(var)
    }

    fn slice_token_range(&self, start_idx: usize, end_idx: usize) -> String {
        if end_idx <= start_idx || start_idx >= self.tokens.len() {
            return String::new();
        }

        let safe_end = end_idx.min(self.tokens.len());
        if safe_end <= start_idx {
            return String::new();
        }

        let start = self.tokens[start_idx].span.start;
        let end = self.tokens[safe_end - 1].span.end;
        self.source[start..end].trim().to_string()
    }

    fn extract_expression_until_keywords(
        &mut self,
        keywords: &[PlPgSqlKeyword],
    ) -> Result<String, PlPgSqlCompileError> {
        let start_idx = self.idx;
        let mut idx = self.idx;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;

        while idx < self.tokens.len() {
            match self.tokens[idx].kind {
                PlPgSqlTokenKind::LParen => paren_depth += 1,
                PlPgSqlTokenKind::RParen => paren_depth = paren_depth.saturating_sub(1),
                PlPgSqlTokenKind::Operator(ref op) if op == "[" => bracket_depth += 1,
                PlPgSqlTokenKind::Operator(ref op) if op == "]" => {
                    bracket_depth = bracket_depth.saturating_sub(1)
                }
                PlPgSqlTokenKind::Keyword(keyword)
                    if paren_depth == 0 && bracket_depth == 0 && keywords.contains(&keyword) =>
                {
                    if idx == start_idx {
                        return Err(PlPgSqlCompileError {
                            message: "expected expression".to_string(),
                            position: self.tokens[idx].span.start,
                            line: self.tokens[idx].span.line,
                            column: self.tokens[idx].span.column,
                        });
                    }

                    let start = self.tokens[start_idx].span.start;
                    let end = self.tokens[idx].span.start;
                    self.idx = idx;
                    return Ok(self.source[start..end].trim().to_string());
                }
                PlPgSqlTokenKind::Eof => break,
                _ => {}
            }
            idx += 1;
        }

        Err(self.error_at_current("unterminated expression"))
    }

    fn extract_comma_expressions_until_semicolon_or_using(
        &mut self,
    ) -> Result<Vec<String>, PlPgSqlCompileError> {
        let mut parts = Vec::new();
        let mut part_start = self.idx;
        let mut idx = self.idx;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;

        while idx < self.tokens.len() {
            match &self.tokens[idx].kind {
                PlPgSqlTokenKind::LParen => paren_depth += 1,
                PlPgSqlTokenKind::RParen => paren_depth = paren_depth.saturating_sub(1),
                PlPgSqlTokenKind::Operator(op) if op == "[" => bracket_depth += 1,
                PlPgSqlTokenKind::Operator(op) if op == "]" => {
                    bracket_depth = bracket_depth.saturating_sub(1)
                }
                PlPgSqlTokenKind::Comma if paren_depth == 0 && bracket_depth == 0 => {
                    let start = self.tokens[part_start].span.start;
                    let end = self.tokens[idx].span.start;
                    let part = self.source[start..end].trim().to_string();
                    if !part.is_empty() {
                        parts.push(part);
                    }
                    part_start = idx + 1;
                }
                PlPgSqlTokenKind::Identifier(word) if paren_depth == 0 && bracket_depth == 0 => {
                    if word.eq_ignore_ascii_case("using") {
                        let start = self.tokens[part_start].span.start;
                        let end = self.tokens[idx].span.start;
                        let part = self.source[start..end].trim().to_string();
                        if !part.is_empty() {
                            parts.push(part);
                        }
                        self.idx = idx;
                        return Ok(parts);
                    }
                }
                PlPgSqlTokenKind::Semicolon if paren_depth == 0 && bracket_depth == 0 => {
                    let start = self.tokens[part_start].span.start;
                    let end = self.tokens[idx].span.start;
                    let part = self.source[start..end].trim().to_string();
                    if !part.is_empty() {
                        parts.push(part);
                    }
                    self.idx = idx;
                    return Ok(parts);
                }
                PlPgSqlTokenKind::Eof => break,
                _ => {}
            }
            idx += 1;
        }

        Err(self.error_at_current("unterminated RAISE expression list"))
    }

    fn extract_expression_until_comma_or_semicolon(
        &mut self,
    ) -> Result<String, PlPgSqlCompileError> {
        let start_idx = self.idx;
        let mut idx = self.idx;
        let mut paren_depth = 0usize;
        let mut bracket_depth = 0usize;

        while idx < self.tokens.len() {
            match self.tokens[idx].kind {
                PlPgSqlTokenKind::LParen => paren_depth += 1,
                PlPgSqlTokenKind::RParen => paren_depth = paren_depth.saturating_sub(1),
                PlPgSqlTokenKind::Operator(ref op) if op == "[" => bracket_depth += 1,
                PlPgSqlTokenKind::Operator(ref op) if op == "]" => {
                    bracket_depth = bracket_depth.saturating_sub(1)
                }
                PlPgSqlTokenKind::Comma | PlPgSqlTokenKind::Semicolon
                    if paren_depth == 0 && bracket_depth == 0 =>
                {
                    if idx == start_idx {
                        return Err(self.error_at_current("expected expression"));
                    }
                    let start = self.tokens[start_idx].span.start;
                    let end = self.tokens[idx].span.start;
                    self.idx = idx;
                    return Ok(self.source[start..end].trim().to_string());
                }
                PlPgSqlTokenKind::Eof => break,
                _ => {}
            }
            idx += 1;
        }

        Err(self.error_at_current("unterminated expression"))
    }

    fn make_expr(&self, query: String) -> PlPgSqlExpr {
        PlPgSqlExpr {
            query,
            parse_mode: None,
            func_signature: None,
            ns: Vec::new(),
            target_param: -1,
            target_is_local: false,
            plan: None,
            paramnos: Vec::new(),
            expr_simple_expr: None,
            expr_simple_type: None,
            expr_simple_typmod: -1,
            expr_simple_mutable: false,
            expr_rwopt: Default::default(),
            expr_rw_param: None,
            expr_simple_plansource: None,
            expr_simple_plan: None,
            expr_simple_plan_lxid: 0,
            expr_simple_state: None,
            expr_simple_in_use: false,
            expr_simple_lxid: 0,
        }
    }

    fn make_type(&self, type_name: &str) -> PlPgSqlType {
        let trimmed = type_name.trim();
        let ttype = if trimmed.eq_ignore_ascii_case("record") {
            PlPgSqlTypeType::Rec
        } else {
            PlPgSqlTypeType::Scalar
        };
        PlPgSqlType {
            typname: trimmed.to_string(),
            typoid: 0,
            ttype,
            typlen: -1,
            typbyval: false,
            typtype: 'b',
            collation: 0,
            typisarray: trimmed.ends_with("[]"),
            atttypmod: -1,
            origtypname: Some(trimmed.to_string()),
            tcache: None,
            tupdesc_id: 0,
        }
    }

    fn allocate_dno(&mut self) -> i32 {
        let dno = self.next_dno;
        self.next_dno = self.next_dno.saturating_add(1);
        dno
    }

    fn alloc_stmtid(&mut self) -> u32 {
        self.next_stmtid = self.next_stmtid.saturating_add(1);
        self.next_stmtid
    }

    fn current_token(&self) -> &PlPgSqlToken {
        let idx = self.idx.min(self.tokens.len().saturating_sub(1));
        &self.tokens[idx]
    }

    fn previous_token(&self) -> &PlPgSqlToken {
        if self.idx == 0 {
            &self.tokens[0]
        } else {
            &self.tokens[self.idx - 1]
        }
    }

    fn current_kind(&self) -> &PlPgSqlTokenKind {
        &self.current_token().kind
    }

    fn peek_kind(&self, offset: usize) -> Option<&PlPgSqlTokenKind> {
        self.tokens.get(self.idx + offset).map(|t| &t.kind)
    }

    fn advance(&mut self) {
        if self.idx < self.tokens.len().saturating_sub(1) {
            self.idx += 1;
        }
    }

    fn consume_keyword(&mut self, keyword: PlPgSqlKeyword) -> bool {
        if self.is_keyword(keyword) {
            self.advance();
            true
        } else {
            false
        }
    }

    fn expect_keyword(
        &mut self,
        keyword: PlPgSqlKeyword,
        message: &str,
    ) -> Result<(), PlPgSqlCompileError> {
        if self.consume_keyword(keyword) {
            Ok(())
        } else {
            Err(self.error_at_current(message))
        }
    }

    fn expect_identifier(&mut self, message: &str) -> Result<String, PlPgSqlCompileError> {
        match self.current_kind().clone() {
            PlPgSqlTokenKind::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(self.error_at_current(message)),
        }
    }

    fn expect_assign(&mut self) -> Result<(), PlPgSqlCompileError> {
        if matches!(self.current_kind(), PlPgSqlTokenKind::Assign) {
            self.advance();
            Ok(())
        } else {
            Err(self.error_at_current("expected :="))
        }
    }

    fn expect_semicolon(&mut self) -> Result<(), PlPgSqlCompileError> {
        if matches!(self.current_kind(), PlPgSqlTokenKind::Semicolon) {
            self.advance();
            Ok(())
        } else {
            Err(self.error_at_current("expected ';'"))
        }
    }

    fn consume_identifier_word(&mut self, expected: &str) -> bool {
        if let PlPgSqlTokenKind::Identifier(word) = self.current_kind()
            && word.eq_ignore_ascii_case(expected)
        {
            self.advance();
            return true;
        }
        false
    }

    fn peek_identifier_word(&self, expected: &str) -> bool {
        matches!(
            self.current_kind(),
            PlPgSqlTokenKind::Identifier(word) if word.eq_ignore_ascii_case(expected)
        )
    }

    fn expect_identifier_word(
        &mut self,
        expected: &str,
        message: &str,
    ) -> Result<(), PlPgSqlCompileError> {
        if self.consume_identifier_word(expected) {
            Ok(())
        } else {
            Err(self.error_at_current(message))
        }
    }

    fn is_keyword(&self, keyword: PlPgSqlKeyword) -> bool {
        matches!(
            self.current_kind(),
            PlPgSqlTokenKind::Keyword(found) if *found == keyword
        )
    }

    fn current_keyword_in(&self, keywords: &[PlPgSqlKeyword]) -> bool {
        matches!(
            self.current_kind(),
            PlPgSqlTokenKind::Keyword(found) if keywords.contains(found)
        )
    }

    fn error_at_current(&self, message: &str) -> PlPgSqlCompileError {
        let token = self.current_token();
        PlPgSqlCompileError {
            message: message.to_string(),
            position: token.span.start,
            line: token.span.line,
            column: token.span.column,
        }
    }
}

fn is_select_clause_boundary(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "from"
            | "where"
            | "group"
            | "having"
            | "window"
            | "union"
            | "intersect"
            | "except"
            | "order"
            | "limit"
            | "offset"
            | "for"
    )
}

fn sqlstate_code_to_int(code: &str) -> Option<i32> {
    let bytes = code.as_bytes();
    if bytes.len() != 5 {
        return None;
    }

    let mut out = 0i32;
    for (idx, ch) in bytes.iter().enumerate() {
        let sixbit = i32::from((*ch).wrapping_sub(b'0') & 0x3F);
        out |= sixbit << (idx * 6);
    }
    Some(out)
}

fn is_valid_sqlstate_code(code: &str) -> bool {
    code.len() == 5
        && code
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

fn exception_condition_name_to_sqlstate(name: &str) -> Option<i32> {
    let code = match name.to_ascii_lowercase().as_str() {
        "raise_exception" => "P0001",
        "no_data_found" => "P0002",
        "too_many_rows" => "P0003",
        "assert_failure" => "P0004",
        "division_by_zero" => "22012",
        "query_canceled" => "57014",
        "undefined_table" => "42P01",
        "undefined_column" => "42703",
        "unique_violation" => "23505",
        "internal_error" => "XX000",
        "data_exception" => "22000",
        "integrity_constraint_violation" => "23000",
        "syntax_error_or_access_rule_violation" => "42000",
        _ => return None,
    };

    sqlstate_code_to_int(code)
}

fn decode_single_quoted_string(input: &str) -> Option<String> {
    if input.len() < 2 || !input.starts_with('\'') || !input.ends_with('\'') {
        return None;
    }

    let inner = &input[1..input.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let chars: Vec<char> = inner.chars().collect();
    let mut idx = 0usize;
    while idx < chars.len() {
        let ch = chars[idx];
        if ch == '\'' {
            if idx + 1 < chars.len() && chars[idx + 1] == '\'' {
                out.push('\'');
                idx += 2;
                continue;
            }
            return None;
        }
        out.push(ch);
        idx += 1;
    }
    Some(out)
}

fn type_name_to_decl_string(type_name: &TypeName) -> String {
    match type_name {
        TypeName::Bool => "boolean".to_string(),
        TypeName::Int2 => "smallint".to_string(),
        TypeName::Int4 => "integer".to_string(),
        TypeName::Int8 => "bigint".to_string(),
        TypeName::Float4 => "real".to_string(),
        TypeName::Float8 => "double precision".to_string(),
        TypeName::Text => "text".to_string(),
        TypeName::Varchar => "varchar".to_string(),
        TypeName::Char => "char".to_string(),
        TypeName::Bytea => "bytea".to_string(),
        TypeName::Uuid => "uuid".to_string(),
        TypeName::Json => "json".to_string(),
        TypeName::Jsonb => "jsonb".to_string(),
        TypeName::Date => "date".to_string(),
        TypeName::Time => "time".to_string(),
        TypeName::Timestamp => "timestamp".to_string(),
        TypeName::TimestampTz => "timestamptz".to_string(),
        TypeName::Interval => "interval".to_string(),
        TypeName::Serial => "serial".to_string(),
        TypeName::BigSerial => "bigserial".to_string(),
        TypeName::Numeric => "numeric".to_string(),
        TypeName::Vector(dim) => {
            dim.map_or_else(|| "vector".to_string(), |d| format!("vector({d})"))
        }
        TypeName::Array(inner) => format!("{}[]", type_name_to_decl_string(inner)),
        TypeName::Name => "name".to_string(),
    }
}

fn map_sql_parse_error(sql: &str, err: SqlParseError) -> PlPgSqlCompileError {
    let (line, column) = line_column_from_offset(sql, err.position);
    PlPgSqlCompileError {
        message: err.message,
        position: err.position,
        line,
        column,
    }
}

fn line_column_from_offset(source: &str, offset: usize) -> (usize, usize) {
    let mut line = 1usize;
    let mut column = 1usize;

    for (idx, ch) in source.char_indices() {
        if idx >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    (line, column)
}

fn parse_do_sql_fallback(sql: &str) -> Result<DoStatement, PlPgSqlCompileError> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let rest = strip_prefix_ci(trimmed, "do").ok_or(PlPgSqlCompileError {
        message: "expected DO statement".to_string(),
        position: 0,
        line: 1,
        column: 1,
    })?;

    let rest = rest.trim_start();
    if rest.is_empty() {
        return Err(PlPgSqlCompileError {
            message: "expected DO body".to_string(),
            position: trimmed.len(),
            line: 1,
            column: trimmed.len() + 1,
        });
    }

    let (body, language) = if let Some(after_language) = strip_prefix_ci(rest, "language") {
        let (language, consumed) = parse_language_name(after_language.trim_start())?;
        let after_lang = after_language.trim_start()[consumed..].trim_start();
        let (body, _) = parse_sql_string_literal(after_lang)?;
        (body, language)
    } else {
        let (body, consumed) = parse_sql_string_literal(rest)?;
        let after_body = rest[consumed..].trim_start();
        if let Some(after_language) = strip_prefix_ci(after_body, "language") {
            let (language, _) = parse_language_name(after_language.trim_start())?;
            (body, language)
        } else {
            (body, "plpgsql".to_string())
        }
    };

    Ok(DoStatement { body, language })
}

fn strip_prefix_ci<'a>(input: &'a str, prefix: &str) -> Option<&'a str> {
    let plen = prefix.len();
    if input.len() < plen {
        return None;
    }
    if input[..plen].eq_ignore_ascii_case(prefix) {
        Some(&input[plen..])
    } else {
        None
    }
}

fn parse_language_name(input: &str) -> Result<(String, usize), PlPgSqlCompileError> {
    if input.is_empty() {
        return Err(PlPgSqlCompileError {
            message: "expected language name after LANGUAGE".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let first = input.chars().next().expect("checked is_empty");
    if first == '\'' {
        let (name, consumed) = parse_single_quoted_string(input)?;
        return Ok((name, consumed));
    }

    let mut end = 0usize;
    for (idx, ch) in input.char_indices() {
        if idx == 0 {
            if !(ch == '_' || ch.is_ascii_alphabetic()) {
                break;
            }
            end = idx + ch.len_utf8();
            continue;
        }
        if !(ch == '_' || ch.is_ascii_alphanumeric()) {
            break;
        }
        end = idx + ch.len_utf8();
    }

    if end == 0 {
        return Err(PlPgSqlCompileError {
            message: "expected language name after LANGUAGE".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    Ok((input[..end].to_string(), end))
}

fn parse_sql_string_literal(input: &str) -> Result<(String, usize), PlPgSqlCompileError> {
    if input.is_empty() {
        return Err(PlPgSqlCompileError {
            message: "expected quoted DO body".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let first = input.chars().next().expect("checked is_empty");
    if first == '\'' {
        return parse_single_quoted_string(input);
    }
    if first == '$' {
        return parse_dollar_quoted_string(input);
    }

    Err(PlPgSqlCompileError {
        message: "expected quoted DO body".to_string(),
        position: 0,
        line: 1,
        column: 1,
    })
}

fn parse_single_quoted_string(input: &str) -> Result<(String, usize), PlPgSqlCompileError> {
    if !input.starts_with('\'') {
        return Err(PlPgSqlCompileError {
            message: "expected single-quoted string".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let mut result = String::new();
    let mut idx = 1usize;
    while idx < input.len() {
        let ch = input[idx..]
            .chars()
            .next()
            .expect("slice index checked by loop");
        idx += ch.len_utf8();
        if ch == '\'' {
            if idx < input.len() && input[idx..].starts_with('\'') {
                result.push('\'');
                idx += 1;
            } else {
                return Ok((result, idx));
            }
        } else {
            result.push(ch);
        }
    }

    Err(PlPgSqlCompileError {
        message: "unterminated single-quoted string".to_string(),
        position: input.len(),
        line: 1,
        column: input.len() + 1,
    })
}

fn parse_dollar_quoted_string(input: &str) -> Result<(String, usize), PlPgSqlCompileError> {
    if !input.starts_with('$') {
        return Err(PlPgSqlCompileError {
            message: "expected dollar-quoted string".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let mut tag_end = 1usize;
    while tag_end < input.len() {
        let ch = input[tag_end..]
            .chars()
            .next()
            .expect("slice index checked by loop");
        if ch == '$' {
            tag_end += 1;
            break;
        }
        if !(ch == '_' || ch.is_ascii_alphanumeric()) {
            return Err(PlPgSqlCompileError {
                message: "invalid dollar-quote tag".to_string(),
                position: tag_end,
                line: 1,
                column: tag_end + 1,
            });
        }
        tag_end += ch.len_utf8();
    }

    if tag_end > input.len() || !input[..tag_end].ends_with('$') {
        return Err(PlPgSqlCompileError {
            message: "unterminated dollar-quote tag".to_string(),
            position: 0,
            line: 1,
            column: 1,
        });
    }

    let delimiter = &input[..tag_end];
    if let Some(rel_end) = input[tag_end..].find(delimiter) {
        let body_start = tag_end;
        let body_end = tag_end + rel_end;
        let consumed = body_end + delimiter.len();
        return Ok((input[body_start..body_end].to_string(), consumed));
    }

    Err(PlPgSqlCompileError {
        message: "unterminated dollar-quoted string".to_string(),
        position: input.len(),
        line: 1,
        column: input.len() + 1,
    })
}

#[cfg(test)]
mod tests {
    use super::{compile_create_function_sql, compile_do_block_sql, compile_function_body};
    use crate::plpgsql::types::{PlPgSqlDatum, PlPgSqlStmt};

    #[test]
    fn compiles_declare_begin_assignment_return() {
        let src = "
DECLARE
  x integer := 1;
BEGIN
  x := x + 1;
  RETURN x;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");

        assert_eq!(compiled.datums.len(), 2);
        let action = compiled.action.expect("action should be present");
        assert_eq!(action.body.len(), 2);
        assert!(matches!(action.body[0], PlPgSqlStmt::Assign(_)));
        assert!(matches!(action.body[1], PlPgSqlStmt::Return(_)));
    }

    #[test]
    fn compiles_if_and_raise_notice() {
        let src = "
BEGIN
  IF x > 0 THEN
    RAISE NOTICE 'ok: %', x;
  ELSE
    RETURN 0;
  END IF;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        let action = compiled.action.expect("action should be present");
        assert_eq!(action.body.len(), 1);
        assert!(matches!(action.body[0], PlPgSqlStmt::If(_)));
    }

    #[test]
    fn compiles_do_sql_wrapper() {
        let sql = "DO $$ BEGIN RETURN; END; $$ LANGUAGE plpgsql";
        let compiled = compile_do_block_sql(sql).expect("compile should succeed");
        assert_eq!(compiled.fn_signature, "inline_code_block");
    }

    #[test]
    fn compiles_select_into_targets() {
        let src = "
DECLARE
  a integer;
  b integer;
BEGIN
  SELECT 1, 2 INTO a, b;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        let action = compiled.action.expect("action should be present");
        let PlPgSqlStmt::ExecSql(stmt) = &action.body[0] else {
            panic!("first statement should be execsql");
        };
        assert!(stmt.into);
        assert_eq!(stmt.target_dnos.len(), 2);
        assert_eq!(stmt.sqlstmt.query, "SELECT 1, 2");
    }

    #[test]
    fn allocates_found_variable_datum() {
        let src = "
BEGIN
  RETURN found;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        assert!(compiled.found_varno >= 0);
        let found_idx = usize::try_from(compiled.found_varno).expect("valid found_varno");
        let Some(PlPgSqlDatum::Var(found_var)) = compiled.datums.get(found_idx) else {
            panic!("found_varno should reference a scalar variable datum");
        };
        assert_eq!(found_var.variable.refname.to_ascii_lowercase(), "found");
    }

    #[test]
    fn compiles_exception_block_with_sqlstate_condition() {
        let src = "
BEGIN
  PERFORM 1 / 0;
EXCEPTION
  WHEN SQLSTATE '22012' THEN
    RETURN 42;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        assert!(compiled.has_exception_block);
        let action = compiled.action.expect("action should be present");
        let exception_block = action
            .exceptions
            .expect("top level block should have exception handlers");
        assert_eq!(exception_block.exc_list.len(), 1);
        assert_eq!(exception_block.exc_list[0].conditions.len(), 1);
    }

    #[test]
    fn compiles_open_fetch_close_cursor_statements() {
        let src = "
DECLARE
  c refcursor;
  x integer;
BEGIN
  OPEN c FOR SELECT 1;
  FETCH c INTO x;
  CLOSE c;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        let action = compiled.action.expect("action should be present");
        assert_eq!(action.body.len(), 3);
        assert!(matches!(action.body[0], PlPgSqlStmt::Open(_)));
        assert!(matches!(action.body[1], PlPgSqlStmt::Fetch(_)));
        assert!(matches!(action.body[2], PlPgSqlStmt::Close(_)));
    }

    #[test]
    fn compiles_record_field_assignment() {
        let src = "
DECLARE
  rec record;
BEGIN
  rec.a := 42;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        let action = compiled.action.expect("action should be present");
        let PlPgSqlStmt::Assign(assign) = &action.body[0] else {
            panic!("first statement should be assignment");
        };
        assert_eq!(assign.fieldname.as_deref(), Some("a"));
    }

    #[test]
    fn compiles_get_diagnostics_row_count() {
        let src = "
DECLARE
  rc integer;
BEGIN
  GET DIAGNOSTICS rc := ROW_COUNT;
END;
";
        let compiled = compile_function_body(src).expect("compile should succeed");
        let action = compiled.action.expect("action should be present");
        let PlPgSqlStmt::Getdiag(stmt) = &action.body[0] else {
            panic!("first statement should be GET DIAGNOSTICS");
        };
        assert_eq!(stmt.diag_items.len(), 1);
    }

    #[test]
    fn compiles_out_and_inout_parameters() {
        let out_sql = "
CREATE FUNCTION out_only(OUT x integer)
RETURNS integer
AS $$
BEGIN
  x := 5;
  RETURN;
END;
$$ LANGUAGE plpgsql;";
        let out_compiled = compile_create_function_sql(out_sql).expect("compile should succeed");
        assert_eq!(out_compiled.fn_argvarnos.len(), 0);
        assert!(out_compiled.out_param_varno >= 0);

        let inout_sql = "
CREATE FUNCTION bump(INOUT x integer)
RETURNS integer
AS $$
BEGIN
  x := x + 1;
  RETURN;
END;
$$ LANGUAGE plpgsql;";
        let inout_compiled =
            compile_create_function_sql(inout_sql).expect("compile should succeed");
        assert_eq!(inout_compiled.fn_argvarnos.len(), 1);
        assert_eq!(
            inout_compiled.out_param_varno,
            inout_compiled.fn_argvarnos[0]
        );
    }
}
