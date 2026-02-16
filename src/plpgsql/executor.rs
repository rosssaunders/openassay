//! PL/pgSQL runtime executor.
//!
//! This module ports the core control-flow/runtime behavior from PostgreSQL
//! `pl_exec.c` into OpenAssay's Rust executor.

use std::collections::HashMap;

use futures::executor::block_on;

use crate::executor::exec_expr::eval_cast_scalar;
use crate::executor::exec_main::execute_query_with_outer;
use crate::parser::ast::Statement;
use crate::parser::sql_parser::parse_statement;
use crate::plpgsql::scanner::{PlPgSqlTokenKind, tokenize};
use crate::plpgsql::types::{
    PlPgSqlDatum, PlPgSqlExpr, PlPgSqlFunction, PlPgSqlStmt, PlPgSqlStmtAssign, PlPgSqlStmtBlock,
    PlPgSqlStmtDynexecute, PlPgSqlStmtExecSql, PlPgSqlStmtExit, PlPgSqlStmtFori, PlPgSqlStmtFors,
    PlPgSqlStmtIf, PlPgSqlStmtLoop, PlPgSqlStmtPerform, PlPgSqlStmtRaise, PlPgSqlStmtReturn,
    PlPgSqlStmtReturnNext, PlPgSqlStmtWhile, PlPgSqlValue,
};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::{QueryResult, execute_planned_query, plan_statement};
use crate::utils::adt::misc::{parse_bool_scalar, parse_i64_scalar};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecResultCode {
    Ok,
    Return,
    Exit,
    Continue,
}

/// PL/pgSQL execution state, mirroring key runtime fields in PostgreSQL's
/// `PLpgSQL_execstate` (`pl_exec.c:493`, `plpgsql.h:1012-1086`).
#[derive(Debug, Clone)]
pub struct PLpgSQLExecState {
    pub func: PlPgSqlFunction,
    pub datums: Vec<PlPgSqlDatum>,
    pub found: bool,
    pub retval: Option<ScalarValue>,
    pub ret_is_null: bool,
    pub exitlabel: Option<String>,
    datum_name_index: HashMap<String, i32>,
    retset_values: Vec<ScalarValue>,
}

impl PLpgSQLExecState {
    fn new(func: &PlPgSqlFunction) -> Self {
        let datums = func.datums.clone();
        let mut datum_name_index = HashMap::new();

        for datum in &datums {
            if let PlPgSqlDatum::Var(var) = datum {
                datum_name_index
                    .insert(var.variable.refname.to_ascii_lowercase(), var.variable.dno);
            }
        }

        Self {
            func: func.clone(),
            datums,
            found: false,
            retval: None,
            ret_is_null: true,
            exitlabel: None,
            datum_name_index,
            retset_values: Vec::new(),
        }
    }

    fn datum_index(dno: i32) -> Result<usize, String> {
        usize::try_from(dno).map_err(|_| format!("invalid datum number {dno}"))
    }

    fn datum_value_by_name(&self, name: &str) -> Option<ScalarValue> {
        if name.eq_ignore_ascii_case("found") {
            return Some(ScalarValue::Bool(self.found));
        }

        let dno = *self.datum_name_index.get(&name.to_ascii_lowercase())?;
        self.datum_value(dno).ok()
    }

    fn datum_value(&self, dno: i32) -> Result<ScalarValue, String> {
        let idx = Self::datum_index(dno)?;
        let datum = self
            .datums
            .get(idx)
            .ok_or_else(|| format!("datum {dno} is out of range"))?;

        match datum {
            PlPgSqlDatum::Var(var) => {
                if var.isnull {
                    Ok(ScalarValue::Null)
                } else {
                    Ok(plpgsql_value_to_scalar(
                        var.value.as_ref().unwrap_or(&PlPgSqlValue::Null),
                    ))
                }
            }
            _ => Err(format!("datum {dno} is not a scalar variable")),
        }
    }
}

/// Function entry point equivalent to PostgreSQL `plpgsql_exec_function`
/// (`pl_exec.c:474-813`) for OpenAssay's simplified runtime.
pub fn plpgsql_exec_function(
    func: &PlPgSqlFunction,
    args: &[ScalarValue],
) -> Result<Option<ScalarValue>, String> {
    let mut estate = PLpgSQLExecState::new(func);
    set_found(&mut estate, false)?;
    bind_call_arguments(&mut estate, args)?;

    let Some(action) = estate.func.action.clone() else {
        return Ok(None);
    };

    let rc = exec_stmt_block(&mut estate, &action)?;
    match rc {
        ExecResultCode::Ok | ExecResultCode::Return => Ok(estate.retval.clone()),
        ExecResultCode::Exit | ExecResultCode::Continue => {
            Err("unexpected EXIT/CONTINUE outside loop".to_string())
        }
    }
}

fn bind_call_arguments(estate: &mut PLpgSQLExecState, args: &[ScalarValue]) -> Result<(), String> {
    let argvarnos = estate.func.fn_argvarnos.clone();
    if !argvarnos.is_empty() {
        if argvarnos.len() != args.len() {
            return Err(format!(
                "function expected {} arguments but got {}",
                argvarnos.len(),
                args.len()
            ));
        }

        for (dno, arg) in argvarnos.iter().zip(args.iter()) {
            exec_assign_value_dno(estate, *dno, arg.clone())?;
        }
        return Ok(());
    }

    if !args.is_empty() {
        let expected = usize::try_from(estate.func.fn_nargs.max(0)).unwrap_or(0);
        return Err(format!(
            "function expected {expected} arguments but got {}",
            args.len()
        ));
    }

    Ok(())
}

/// Statement block execution, corresponding to `exec_stmt_block`
/// (`pl_exec.c:1662-1946`) with simplified exception handling.
fn exec_stmt_block(
    estate: &mut PLpgSQLExecState,
    block: &PlPgSqlStmtBlock,
) -> Result<ExecResultCode, String> {
    for dno in &block.initvarnos {
        initialize_datum(estate, *dno)?;
    }

    let rc = exec_stmts(estate, &block.body)?;

    match rc {
        ExecResultCode::Ok | ExecResultCode::Return | ExecResultCode::Continue => Ok(rc),
        ExecResultCode::Exit => {
            let Some(exitlabel) = estate.exitlabel.clone() else {
                return Ok(ExecResultCode::Exit);
            };
            if block.label.as_deref() == Some(exitlabel.as_str()) {
                estate.exitlabel = None;
                Ok(ExecResultCode::Ok)
            } else {
                Ok(ExecResultCode::Exit)
            }
        }
    }
}

fn initialize_datum(estate: &mut PLpgSQLExecState, dno: i32) -> Result<(), String> {
    let idx = PLpgSQLExecState::datum_index(dno)?;
    let datum = estate
        .datums
        .get(idx)
        .ok_or_else(|| format!("datum {dno} is out of range"))?;

    match datum {
        PlPgSqlDatum::Var(var) => {
            let default_expr = var.variable.default_val.clone();
            if let Some(expr) = default_expr {
                let value = exec_eval_expr(estate, &expr)?;
                exec_assign_value_dno(estate, dno, value)
            } else {
                exec_assign_value_dno(estate, dno, ScalarValue::Null)
            }
        }
        _ => Err(format!(
            "block initialization for datum type {:?} is not implemented",
            datum.dtype()
        )),
    }
}

/// Statement dispatcher corresponding to `exec_stmts`/switch dispatch
/// (`pl_exec.c:1954-2145`).
fn exec_stmts(
    estate: &mut PLpgSQLExecState,
    stmts: &[PlPgSqlStmt],
) -> Result<ExecResultCode, String> {
    for stmt in stmts {
        let rc = match stmt {
            PlPgSqlStmt::Block(stmt) => exec_stmt_block(estate, stmt)?,
            PlPgSqlStmt::Assign(stmt) => exec_stmt_assign(estate, stmt)?,
            PlPgSqlStmt::If(stmt) => exec_stmt_if(estate, stmt)?,
            PlPgSqlStmt::Loop(stmt) => exec_stmt_loop(estate, stmt)?,
            PlPgSqlStmt::While(stmt) => exec_stmt_while(estate, stmt)?,
            PlPgSqlStmt::Fori(stmt) => exec_stmt_fori(estate, stmt)?,
            PlPgSqlStmt::Fors(stmt) => exec_stmt_fors(estate, stmt)?,
            PlPgSqlStmt::Exit(stmt) => exec_stmt_exit(estate, stmt)?,
            PlPgSqlStmt::Return(stmt) => exec_stmt_return(estate, stmt)?,
            PlPgSqlStmt::ReturnNext(stmt) => exec_stmt_return_next(estate, stmt)?,
            PlPgSqlStmt::Raise(stmt) => exec_stmt_raise(estate, stmt)?,
            PlPgSqlStmt::ExecSql(stmt) => exec_stmt_execsql(estate, stmt)?,
            PlPgSqlStmt::DynExecute(stmt) => exec_stmt_dynexecute(estate, stmt)?,
            PlPgSqlStmt::Perform(stmt) => exec_stmt_perform(estate, stmt)?,
            _ => {
                return Err(format!(
                    "statement type {:?} is not implemented in PL/pgSQL executor",
                    stmt.cmd_type()
                ));
            }
        };
        if rc != ExecResultCode::Ok {
            return Ok(rc);
        }
    }
    Ok(ExecResultCode::Ok)
}

/// Assignment execution equivalent to `exec_stmt_assign` (`pl_exec.c:2158-2171`).
fn exec_stmt_assign(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtAssign,
) -> Result<ExecResultCode, String> {
    if stmt.varno < 0 {
        return Err("assignment target variable is unresolved".to_string());
    }
    let value = exec_eval_expr(estate, &stmt.expr)?;
    exec_assign_value_dno(estate, stmt.varno, value)?;
    Ok(ExecResultCode::Ok)
}

/// IF execution corresponding to `exec_stmt_if` (`pl_exec.c:2519-2548`).
fn exec_stmt_if(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtIf,
) -> Result<ExecResultCode, String> {
    let (cond, is_null) = exec_eval_boolean(estate, &stmt.cond)?;
    if !is_null && cond {
        return exec_stmts(estate, &stmt.then_body);
    }

    for elsif in &stmt.elsif_list {
        let (elsif_cond, elsif_is_null) = exec_eval_boolean(estate, &elsif.cond)?;
        if !elsif_is_null && elsif_cond {
            return exec_stmts(estate, &elsif.stmts);
        }
    }

    exec_stmts(estate, &stmt.else_body)
}

/// LOOP execution corresponding to `exec_stmt_loop` (`pl_exec.c:2637-2655`).
fn exec_stmt_loop(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtLoop,
) -> Result<ExecResultCode, String> {
    loop {
        let rc = exec_stmts(estate, &stmt.body)?;
        match process_loop_rc(estate, stmt.label.as_deref(), rc) {
            LoopControl::Continue => continue,
            LoopControl::BreakWith(rc) => return Ok(rc),
            LoopControl::Propagate(rc) => return Ok(rc),
        }
    }
}

/// WHILE execution corresponding to `exec_stmt_while` (`pl_exec.c:2658-2686`).
fn exec_stmt_while(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtWhile,
) -> Result<ExecResultCode, String> {
    loop {
        let (cond, is_null) = exec_eval_boolean(estate, &stmt.cond)?;
        if is_null || !cond {
            break;
        }

        let rc = exec_stmts(estate, &stmt.body)?;
        match process_loop_rc(estate, stmt.label.as_deref(), rc) {
            LoopControl::Continue => continue,
            LoopControl::BreakWith(rc) => return Ok(rc),
            LoopControl::Propagate(rc) => return Ok(rc),
        }
    }

    Ok(ExecResultCode::Ok)
}

/// Integer FOR-loop execution corresponding to `exec_stmt_fori`
/// (`pl_exec.c:2689-2797`).
fn exec_stmt_fori(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtFori,
) -> Result<ExecResultCode, String> {
    let loop_var = stmt
        .var
        .as_ref()
        .ok_or_else(|| "FOR loop variable is missing".to_string())?;
    let loop_dno = loop_var.variable.dno;

    let (mut loop_value, lower_null) = exec_eval_integer(estate, &stmt.lower)?;
    if lower_null {
        return Err("lower bound of FOR loop cannot be null".to_string());
    }

    let (end_value, upper_null) = exec_eval_integer(estate, &stmt.upper)?;
    if upper_null {
        return Err("upper bound of FOR loop cannot be null".to_string());
    }

    let step_value = if let Some(step) = &stmt.step {
        let (step, step_null) = exec_eval_integer(estate, step)?;
        if step_null {
            return Err("BY value of FOR loop cannot be null".to_string());
        }
        if step <= 0 {
            return Err("BY value of FOR loop must be greater than zero".to_string());
        }
        step
    } else {
        1
    };

    let reverse = stmt.reverse != 0;
    let mut found = false;

    loop {
        if reverse {
            if loop_value < end_value {
                break;
            }
        } else if loop_value > end_value {
            break;
        }

        found = true;
        exec_assign_value_dno(estate, loop_dno, ScalarValue::Int(loop_value))?;

        let rc = exec_stmts(estate, &stmt.body)?;
        match process_loop_rc(estate, stmt.label.as_deref(), rc) {
            LoopControl::Continue => {}
            LoopControl::BreakWith(rc) => {
                set_found(estate, found)?;
                return Ok(rc);
            }
            LoopControl::Propagate(rc) => {
                set_found(estate, found)?;
                return Ok(rc);
            }
        }

        if reverse {
            if loop_value < (i64::MIN + step_value) {
                break;
            }
            loop_value -= step_value;
        } else {
            if loop_value > (i64::MAX - step_value) {
                break;
            }
            loop_value += step_value;
        }
    }

    set_found(estate, found)?;
    Ok(ExecResultCode::Ok)
}

/// Query FOR-loop execution corresponding to `exec_stmt_fors`
/// (`pl_exec.c:2930-3024`) for single-target scalar variables.
fn exec_stmt_fors(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtFors,
) -> Result<ExecResultCode, String> {
    let loop_var = stmt
        .var
        .as_ref()
        .ok_or_else(|| "FOR query variable is missing".to_string())?;

    let result = execute_sql_statement(estate, &stmt.query.query)?;
    let mut found = false;

    for row in &result.rows {
        if row.len() != 1 {
            return Err(format!(
                "FOR query returned {} columns, but loop variable \"{}\" expects exactly 1",
                row.len(),
                loop_var.refname
            ));
        }

        found = true;
        exec_assign_value_dno(estate, loop_var.dno, row[0].clone())?;

        let rc = exec_stmts(estate, &stmt.body)?;
        match process_loop_rc(estate, stmt.label.as_deref(), rc) {
            LoopControl::Continue => {}
            LoopControl::BreakWith(rc) => {
                set_found(estate, found)?;
                return Ok(rc);
            }
            LoopControl::Propagate(rc) => {
                set_found(estate, found)?;
                return Ok(rc);
            }
        }
    }

    set_found(estate, found)?;
    Ok(ExecResultCode::Ok)
}

/// EXIT/CONTINUE execution corresponding to `exec_stmt_exit`
/// (`pl_exec.c:3163-3186`).
fn exec_stmt_exit(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtExit,
) -> Result<ExecResultCode, String> {
    if let Some(cond) = &stmt.cond {
        let (value, is_null) = exec_eval_boolean(estate, cond)?;
        if is_null || !value {
            return Ok(ExecResultCode::Ok);
        }
    }

    estate.exitlabel = stmt.label.clone();
    if stmt.is_exit {
        Ok(ExecResultCode::Exit)
    } else {
        Ok(ExecResultCode::Continue)
    }
}

/// RETURN execution corresponding to `exec_stmt_return` (`pl_exec.c:3188-3328`).
fn exec_stmt_return(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtReturn,
) -> Result<ExecResultCode, String> {
    if stmt.retvarno >= 0 {
        let value = estate.datum_value(stmt.retvarno)?;
        estate.ret_is_null = matches!(value, ScalarValue::Null);
        estate.retval = Some(value);
        return Ok(ExecResultCode::Return);
    }

    if let Some(expr) = &stmt.expr {
        let value = exec_eval_expr(estate, expr)?;
        estate.ret_is_null = matches!(value, ScalarValue::Null);
        estate.retval = Some(value);
        return Ok(ExecResultCode::Return);
    }

    estate.retval = None;
    estate.ret_is_null = true;
    Ok(ExecResultCode::Return)
}

/// RETURN NEXT execution corresponding to `exec_stmt_return_next`
/// (`pl_exec.c:3333-3550`) for scalar/set accumulation.
fn exec_stmt_return_next(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtReturnNext,
) -> Result<ExecResultCode, String> {
    if !estate.func.fn_retset {
        return Err("cannot use RETURN NEXT in a non-SETOF function".to_string());
    }

    let value = if stmt.retvarno >= 0 {
        estate.datum_value(stmt.retvarno)?
    } else if let Some(expr) = &stmt.expr {
        exec_eval_expr(estate, expr)?
    } else {
        return Err("RETURN NEXT must have a parameter".to_string());
    };

    estate.retset_values.push(value);
    Ok(ExecResultCode::Ok)
}

/// RAISE execution corresponding to `exec_stmt_raise` (`pl_exec.c:3734-3955`).
fn exec_stmt_raise(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtRaise,
) -> Result<ExecResultCode, String> {
    let message_template = stmt
        .message
        .as_deref()
        .map(decode_sql_string_literal)
        .unwrap_or_default();

    let mut param_values = Vec::with_capacity(stmt.params.len());
    for param in &stmt.params {
        let value = exec_eval_expr(estate, param)?;
        if matches!(value, ScalarValue::Null) {
            param_values.push("<NULL>".to_string());
        } else {
            param_values.push(value.render());
        }
    }

    let formatted = format_raise_message(&message_template, &param_values)?;
    let is_notice = stmt
        .condname
        .as_deref()
        .is_some_and(|c| c.eq_ignore_ascii_case("NOTICE") || c.eq_ignore_ascii_case("WARNING"));

    if stmt.elog_level > 0 || is_notice {
        eprintln!("NOTICE: {formatted}");
        Ok(ExecResultCode::Ok)
    } else {
        Err(formatted)
    }
}

/// SQL statement execution corresponding to `exec_stmt_execsql`
/// (`pl_exec.c:4214-4478`) via OpenAssay's SQL executor.
fn exec_stmt_execsql(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtExecSql,
) -> Result<ExecResultCode, String> {
    let result = execute_sql_statement(estate, &stmt.sqlstmt.query)?;

    if stmt.into {
        exec_select_into_targets(estate, stmt, &result)?;
        set_found(estate, !result.rows.is_empty())?;
    } else {
        let found = result.rows_affected > 0 || !result.rows.is_empty();
        set_found(estate, found)?;
    }

    Ok(ExecResultCode::Ok)
}

/// Dynamic EXECUTE execution corresponding to `exec_stmt_dynexecute`
/// (`pl_exec.c:4483-4848`) with simplified INTO handling.
fn exec_stmt_dynexecute(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtDynexecute,
) -> Result<ExecResultCode, String> {
    let query_value = exec_eval_expr(estate, &stmt.query)?;
    let query = scalar_to_dynamic_sql(query_value)?;
    let result = execute_sql_statement(estate, &query)?;

    if stmt.into {
        let target = stmt
            .target
            .as_ref()
            .ok_or_else(|| "EXECUTE INTO target variable is missing".to_string())?;

        if stmt.strict && result.rows.len() != 1 {
            return Err(format!(
                "query returned {} rows but EXECUTE INTO STRICT expects exactly one row",
                result.rows.len()
            ));
        }

        let source_cols = if !result.columns.is_empty() {
            result.columns.len()
        } else if let Some(row) = result.rows.first() {
            row.len()
        } else {
            0
        };

        if source_cols != 1 {
            return Err(format!(
                "EXECUTE INTO target count (1) does not match query column count ({source_cols})"
            ));
        }

        let value = result
            .rows
            .first()
            .and_then(|row| row.first())
            .cloned()
            .unwrap_or(ScalarValue::Null);
        exec_assign_value_dno(estate, target.dno, value)?;
        set_found(estate, !result.rows.is_empty())?;
    } else {
        let found = result.rows_affected > 0 || !result.rows.is_empty();
        set_found(estate, found)?;
    }

    Ok(ExecResultCode::Ok)
}

/// PERFORM execution corresponding to `exec_stmt_perform` (`pl_exec.c:2173-2188`).
fn exec_stmt_perform(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtPerform,
) -> Result<ExecResultCode, String> {
    let expr = stmt.expr.query.trim();
    let sql = format!("SELECT {expr}");
    let result = execute_sql_statement(estate, &sql)?;
    set_found(estate, !result.rows.is_empty())?;
    Ok(ExecResultCode::Ok)
}

fn execute_sql_statement(estate: &mut PLpgSQLExecState, sql: &str) -> Result<QueryResult, String> {
    let substituted = substitute_variables(sql, estate)?;
    let statement = parse_statement(&substituted).map_err(|e| e.message)?;

    match statement {
        Statement::Query(query) => {
            block_on(execute_query_with_outer(&query, &[], None)).map_err(|e| e.message)
        }
        other => {
            let planned = plan_statement(other).map_err(|e| e.message)?;
            block_on(execute_planned_query(&planned, &[])).map_err(|e| e.message)
        }
    }
}

fn exec_select_into_targets(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtExecSql,
    result: &QueryResult,
) -> Result<(), String> {
    if !result.command_tag.eq_ignore_ascii_case("SELECT") {
        return Err("SELECT INTO requires a SELECT statement".to_string());
    }

    if stmt.target_dnos.is_empty() {
        return Err("SELECT INTO has no target variables".to_string());
    }

    if stmt.strict && result.rows.len() != 1 {
        return Err(format!(
            "query returned {} rows but SELECT INTO STRICT expects exactly one row",
            result.rows.len()
        ));
    }

    let source_cols = if !result.columns.is_empty() {
        result.columns.len()
    } else if let Some(row) = result.rows.first() {
        row.len()
    } else {
        0
    };

    if source_cols != stmt.target_dnos.len() {
        return Err(format!(
            "SELECT INTO target count ({}) does not match query column count ({source_cols})",
            stmt.target_dnos.len()
        ));
    }

    if let Some(row) = result.rows.first() {
        for (col_idx, dno) in stmt.target_dnos.iter().enumerate() {
            let value = row.get(col_idx).cloned().unwrap_or(ScalarValue::Null);
            exec_assign_value_dno(estate, *dno, value)?;
        }
    } else {
        for dno in &stmt.target_dnos {
            exec_assign_value_dno(estate, *dno, ScalarValue::Null)?;
        }
    }

    Ok(())
}

/// Expression evaluation equivalent to `exec_eval_expr` (`pl_exec.c:5671-5752`) using
/// OpenAssay query evaluation for non-trivial expressions.
fn exec_eval_expr(
    estate: &mut PLpgSQLExecState,
    expr: &PlPgSqlExpr,
) -> Result<ScalarValue, String> {
    let raw = expr.query.trim();

    if let Some(simple) = try_eval_direct_expression(estate, raw) {
        return Ok(simple);
    }

    let substituted = substitute_variables(raw, estate)?;
    if let Some(simple) = try_parse_simple_constant(&substituted) {
        return Ok(simple);
    }

    let sql = format!("SELECT {substituted}");
    let result = execute_sql_statement(estate, &sql)?;

    if result.rows.is_empty() {
        return Ok(ScalarValue::Null);
    }
    if result.rows.len() > 1 {
        return Err("query returned more than one row".to_string());
    }

    let row = &result.rows[0];
    if row.len() != 1 {
        return Err(format!("query returned {} columns", row.len()));
    }

    Ok(row[0].clone())
}

/// Integer coercion helper equivalent to `exec_eval_integer` (`pl_exec.c:5623-5646`).
fn exec_eval_integer(
    estate: &mut PLpgSQLExecState,
    expr: &PlPgSqlExpr,
) -> Result<(i64, bool), String> {
    let value = exec_eval_expr(estate, expr)?;
    if matches!(value, ScalarValue::Null) {
        return Ok((0, true));
    }

    let parsed = parse_i64_scalar(&value, "cannot cast value to integer").map_err(|e| e.message)?;
    Ok((parsed, false))
}

/// Boolean coercion helper equivalent to `exec_eval_boolean` (`pl_exec.c:5648-5669`).
fn exec_eval_boolean(
    estate: &mut PLpgSQLExecState,
    expr: &PlPgSqlExpr,
) -> Result<(bool, bool), String> {
    let value = exec_eval_expr(estate, expr)?;
    if matches!(value, ScalarValue::Null) {
        return Ok((false, true));
    }

    let parsed =
        parse_bool_scalar(&value, "cannot cast value to boolean").map_err(|e| e.message)?;
    Ok((parsed, false))
}

/// Assignment helper equivalent to `exec_assign_value` (`pl_exec.c:5066-5252`) for
/// scalar variable datums.
fn exec_assign_value_dno(
    estate: &mut PLpgSQLExecState,
    dno: i32,
    mut value: ScalarValue,
) -> Result<(), String> {
    let idx = PLpgSQLExecState::datum_index(dno)?;
    let datum = estate
        .datums
        .get_mut(idx)
        .ok_or_else(|| format!("datum {dno} is out of range"))?;

    match datum {
        PlPgSqlDatum::Var(var) => {
            if let Some(datatype) = &var.datatype {
                value = coerce_scalar_to_type(value, datatype.typname.as_str())?;
            }

            if matches!(value, ScalarValue::Null) && var.variable.notnull {
                return Err(format!(
                    "null value cannot be assigned to variable \"{}\" declared NOT NULL",
                    var.variable.refname
                ));
            }

            var.isnull = matches!(value, ScalarValue::Null);
            var.value = Some(scalar_to_plpgsql_value(&value));
            Ok(())
        }
        _ => Err(format!(
            "assignment to datum type {:?} is not implemented",
            datum.dtype()
        )),
    }
}

fn coerce_scalar_to_type(value: ScalarValue, type_name: &str) -> Result<ScalarValue, String> {
    if matches!(value, ScalarValue::Null) {
        return Ok(ScalarValue::Null);
    }

    let normalized = type_name.trim().to_ascii_lowercase();
    let cast_type = if normalized.starts_with("int") || normalized == "integer" {
        "int8"
    } else if normalized == "bool" || normalized == "boolean" {
        "boolean"
    } else if normalized == "text"
        || normalized == "varchar"
        || normalized == "char"
        || normalized == "name"
    {
        "text"
    } else if normalized == "numeric" || normalized == "decimal" {
        "numeric"
    } else if normalized == "real" || normalized == "float4" {
        "float4"
    } else if normalized == "double precision" || normalized == "float8" || normalized == "float" {
        "float8"
    } else {
        // Fall back to the declared type text for supported cast names.
        normalized.as_str()
    };

    eval_cast_scalar(value, cast_type).map_err(|e| e.message)
}

fn set_found(estate: &mut PLpgSQLExecState, state: bool) -> Result<(), String> {
    estate.found = state;
    if estate.func.found_varno >= 0 {
        exec_assign_value_dno(estate, estate.func.found_varno, ScalarValue::Bool(state))?;
    }
    Ok(())
}

enum LoopControl {
    Continue,
    BreakWith(ExecResultCode),
    Propagate(ExecResultCode),
}

fn process_loop_rc(
    estate: &mut PLpgSQLExecState,
    loop_label: Option<&str>,
    rc: ExecResultCode,
) -> LoopControl {
    match rc {
        ExecResultCode::Return => LoopControl::Propagate(ExecResultCode::Return),
        ExecResultCode::Exit => {
            if estate.exitlabel.is_none() {
                LoopControl::BreakWith(ExecResultCode::Ok)
            } else if loop_label.is_some() && estate.exitlabel.as_deref() == loop_label {
                estate.exitlabel = None;
                LoopControl::BreakWith(ExecResultCode::Ok)
            } else {
                LoopControl::Propagate(ExecResultCode::Exit)
            }
        }
        ExecResultCode::Continue => {
            if estate.exitlabel.is_none() {
                LoopControl::Continue
            } else if loop_label.is_some() && estate.exitlabel.as_deref() == loop_label {
                estate.exitlabel = None;
                LoopControl::Continue
            } else {
                LoopControl::Propagate(ExecResultCode::Continue)
            }
        }
        ExecResultCode::Ok => LoopControl::Continue,
    }
}

fn try_eval_direct_expression(estate: &PLpgSQLExecState, expr: &str) -> Option<ScalarValue> {
    if let Some(value) = try_parse_simple_constant(expr) {
        return Some(value);
    }

    if is_simple_identifier(expr) {
        return estate.datum_value_by_name(expr);
    }

    None
}

fn try_parse_simple_constant(expr: &str) -> Option<ScalarValue> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return None;
    }

    if trimmed.eq_ignore_ascii_case("null") {
        return Some(ScalarValue::Null);
    }
    if trimmed.eq_ignore_ascii_case("true") {
        return Some(ScalarValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(ScalarValue::Bool(false));
    }

    if let Some(text) = decode_single_quoted_string(trimmed) {
        return Some(ScalarValue::Text(text));
    }

    if let Ok(int_val) = trimmed.parse::<i64>() {
        return Some(ScalarValue::Int(int_val));
    }

    if let Ok(numeric) = trimmed.parse::<rust_decimal::Decimal>() {
        return Some(ScalarValue::Numeric(numeric));
    }

    if let Ok(float_val) = trimmed.parse::<f64>() {
        return Some(ScalarValue::Float(float_val));
    }

    None
}

fn decode_single_quoted_string(input: &str) -> Option<String> {
    if input.len() < 2 || !input.starts_with('\'') || !input.ends_with('\'') {
        return None;
    }

    let inner = &input[1..input.len() - 1];
    let mut out = String::with_capacity(inner.len());
    let chars: Vec<char> = inner.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        if ch == '\'' {
            if i + 1 < chars.len() && chars[i + 1] == '\'' {
                out.push('\'');
                i += 2;
                continue;
            }
            return None;
        }
        out.push(ch);
        i += 1;
    }

    Some(out)
}

fn decode_sql_string_literal(input: &str) -> String {
    decode_single_quoted_string(input).unwrap_or_else(|| input.to_string())
}

fn scalar_to_dynamic_sql(value: ScalarValue) -> Result<String, String> {
    match value {
        ScalarValue::Null => Err("query string argument of EXECUTE is null".to_string()),
        ScalarValue::Text(text) => Ok(text),
        other => Ok(other.render()),
    }
}

fn format_raise_message(template: &str, params: &[String]) -> Result<String, String> {
    let mut out = String::with_capacity(template.len() + params.len() * 8);
    let mut param_idx = 0usize;
    let chars: Vec<char> = template.chars().collect();
    let mut i = 0usize;

    while i < chars.len() {
        let ch = chars[i];
        if ch == '%' {
            if i + 1 < chars.len() && chars[i + 1] == '%' {
                out.push('%');
                i += 2;
                continue;
            }
            let value = params
                .get(param_idx)
                .ok_or_else(|| "unexpected RAISE parameter list length".to_string())?;
            out.push_str(value);
            param_idx += 1;
            i += 1;
            continue;
        }

        out.push(ch);
        i += 1;
    }

    if param_idx != params.len() {
        return Err("unexpected RAISE parameter list length".to_string());
    }

    Ok(out)
}

fn substitute_variables(sql: &str, estate: &PLpgSQLExecState) -> Result<String, String> {
    let mut output = substitute_named_variables(sql, estate)?;

    let argvarnos = &estate.func.fn_argvarnos;
    if !argvarnos.is_empty() {
        for idx in (0..argvarnos.len()).rev() {
            let placeholder = format!("${}", idx + 1);
            let value = estate.datum_value(argvarnos[idx])?;
            let replacement = scalar_to_sql_literal(&value);
            output = substitute_dollar_placeholder(&output, &placeholder, &replacement);
        }
    }

    Ok(output)
}

fn substitute_named_variables(sql: &str, estate: &PLpgSQLExecState) -> Result<String, String> {
    let tokens = tokenize(sql).map_err(|e| e.message)?;
    let mut output = String::with_capacity(sql.len());
    let mut cursor = 0usize;

    for token in &tokens {
        if matches!(token.kind, PlPgSqlTokenKind::Eof) {
            break;
        }

        let replacement = match &token.kind {
            PlPgSqlTokenKind::Identifier(name) => estate
                .datum_value_by_name(name)
                .map(|value| scalar_to_sql_literal(&value)),
            _ => None,
        };

        if let Some(replacement) = replacement {
            output.push_str(&sql[cursor..token.span.start]);
            output.push_str(&replacement);
            cursor = token.span.end;
        }
    }

    output.push_str(&sql[cursor..]);
    Ok(output)
}

fn substitute_dollar_placeholder(input: &str, needle: &str, replacement: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;

    while i < input.len() {
        if bytes[i] == b'\'' {
            // Copy single-quoted literal verbatim.
            let start = i;
            i += 1;
            while i < input.len() {
                if bytes[i] == b'\'' {
                    if i + 1 < input.len() && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        i += 1;
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            out.push_str(&input[start..i]);
            continue;
        }

        if input[i..].starts_with(needle) {
            out.push_str(replacement);
            i += needle.len();
            continue;
        }

        if let Some(ch) = input[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    out
}

fn is_simple_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn scalar_to_sql_literal(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => "NULL".to_string(),
        ScalarValue::Bool(v) => {
            if *v {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        ScalarValue::Int(v) => v.to_string(),
        ScalarValue::Float(v) => v.to_string(),
        ScalarValue::Numeric(v) => v.to_string(),
        ScalarValue::Text(v) => format!("'{}'", v.replace('\'', "''")),
        ScalarValue::Array(_) | ScalarValue::Record(_) => {
            format!("'{}'", value.render().replace('\'', "''"))
        }
    }
}

fn plpgsql_value_to_scalar(value: &PlPgSqlValue) -> ScalarValue {
    match value {
        PlPgSqlValue::Null => ScalarValue::Null,
        PlPgSqlValue::Bool(v) => ScalarValue::Bool(*v),
        PlPgSqlValue::Int(v) => ScalarValue::Int(*v),
        PlPgSqlValue::Numeric(v) => {
            if let Ok(decimal) = v.parse::<rust_decimal::Decimal>() {
                ScalarValue::Numeric(decimal)
            } else {
                ScalarValue::Text(v.clone())
            }
        }
        PlPgSqlValue::Text(v) => ScalarValue::Text(v.clone()),
    }
}

fn scalar_to_plpgsql_value(value: &ScalarValue) -> PlPgSqlValue {
    match value {
        ScalarValue::Null => PlPgSqlValue::Null,
        ScalarValue::Bool(v) => PlPgSqlValue::Bool(*v),
        ScalarValue::Int(v) => PlPgSqlValue::Int(*v),
        ScalarValue::Numeric(v) => PlPgSqlValue::Numeric(v.to_string()),
        ScalarValue::Float(v) => PlPgSqlValue::Numeric(v.to_string()),
        ScalarValue::Text(v) => PlPgSqlValue::Text(v.clone()),
        ScalarValue::Array(_) | ScalarValue::Record(_) => PlPgSqlValue::Text(value.render()),
    }
}

#[cfg(test)]
mod tests {
    use super::plpgsql_exec_function;
    use crate::plpgsql::compiler::compile_function_body;
    use crate::plpgsql::types::{
        PlPgSqlDatum, PlPgSqlDtype, PlPgSqlExpr, PlPgSqlFunction, PlPgSqlPromiseType, PlPgSqlStmt,
        PlPgSqlStmtAssign, PlPgSqlStmtBlock, PlPgSqlStmtFori, PlPgSqlStmtLoop, PlPgSqlStmtReturn,
        PlPgSqlStmtType, PlPgSqlStmtWhile, PlPgSqlType, PlPgSqlTypeType, PlPgSqlValue, PlPgSqlVar,
        PlPgSqlVariable,
    };
    use crate::storage::tuple::ScalarValue;

    fn expr(sql: &str) -> PlPgSqlExpr {
        PlPgSqlExpr {
            query: sql.to_string(),
            ..PlPgSqlExpr::default()
        }
    }

    fn scalar_type(name: &str) -> PlPgSqlType {
        PlPgSqlType {
            typname: name.to_string(),
            ttype: PlPgSqlTypeType::Scalar,
            ..PlPgSqlType::default()
        }
    }

    fn mk_var(dno: i32, name: &str, type_name: &str, default: Option<&str>) -> PlPgSqlDatum {
        let variable = PlPgSqlVariable {
            dtype: PlPgSqlDtype::Var,
            dno,
            refname: name.to_string(),
            lineno: 1,
            isconst: false,
            notnull: false,
            default_val: default.map(expr),
        };
        PlPgSqlDatum::Var(PlPgSqlVar {
            variable,
            datatype: Some(scalar_type(type_name)),
            cursor_explicit_expr: None,
            cursor_explicit_argrow: -1,
            cursor_options: 0,
            value: Some(PlPgSqlValue::Null),
            isnull: true,
            freeval: false,
            promise: PlPgSqlPromiseType::None,
        })
    }

    fn mk_function(
        datums: Vec<PlPgSqlDatum>,
        body: Vec<PlPgSqlStmt>,
        initvarnos: Vec<i32>,
    ) -> PlPgSqlFunction {
        let block = PlPgSqlStmtBlock {
            cmd_type: PlPgSqlStmtType::Block,
            lineno: 1,
            stmtid: 1,
            label: None,
            body,
            n_initvars: i32::try_from(initvarnos.len()).unwrap_or(0),
            initvarnos,
            exceptions: None,
        };

        let mut func = PlPgSqlFunction::default();
        func.fn_signature = "test_function".to_string();
        func.datums = datums;
        func.ndatums = i32::try_from(func.datums.len()).unwrap_or(0);
        func.action = Some(block);
        func
    }

    #[test]
    fn executes_assignment_and_return() {
        let src = "
DECLARE
  x int := 42;
BEGIN
  x := x + 1;
  RETURN x;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(43)));
    }

    #[test]
    fn executes_if_else_branching() {
        let src = "
BEGIN
  IF 1 = 1 THEN
    RETURN 10;
  ELSE
    RETURN 20;
  END IF;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(10)));
    }

    #[test]
    fn executes_while_loop_with_counter() {
        let datums = vec![mk_var(0, "x", "integer", Some("0"))];
        let body = vec![
            PlPgSqlStmt::While(PlPgSqlStmtWhile {
                cmd_type: PlPgSqlStmtType::While,
                lineno: 1,
                stmtid: 2,
                label: None,
                cond: expr("x < 3"),
                body: vec![PlPgSqlStmt::Assign(PlPgSqlStmtAssign {
                    cmd_type: PlPgSqlStmtType::Assign,
                    lineno: 1,
                    stmtid: 3,
                    varno: 0,
                    expr: expr("x + 1"),
                })],
            }),
            PlPgSqlStmt::Return(PlPgSqlStmtReturn {
                cmd_type: PlPgSqlStmtType::Return,
                lineno: 1,
                stmtid: 4,
                expr: Some(expr("x")),
                retvarno: -1,
            }),
        ];
        let func = mk_function(datums, body, vec![0]);

        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(3)));
    }

    #[test]
    fn executes_fori_loop_range() {
        let datums = vec![
            mk_var(0, "i", "integer", None),
            mk_var(1, "sum", "integer", Some("0")),
        ];

        let loop_var = match &datums[0] {
            PlPgSqlDatum::Var(var) => Some(var.clone()),
            _ => None,
        };

        let body = vec![
            PlPgSqlStmt::Fori(PlPgSqlStmtFori {
                cmd_type: PlPgSqlStmtType::Fori,
                lineno: 1,
                stmtid: 2,
                label: None,
                var: loop_var,
                lower: expr("1"),
                upper: expr("10"),
                step: None,
                reverse: 0,
                body: vec![PlPgSqlStmt::Assign(PlPgSqlStmtAssign {
                    cmd_type: PlPgSqlStmtType::Assign,
                    lineno: 1,
                    stmtid: 3,
                    varno: 1,
                    expr: expr("sum + i"),
                })],
            }),
            PlPgSqlStmt::Return(PlPgSqlStmtReturn {
                cmd_type: PlPgSqlStmtType::Return,
                lineno: 1,
                stmtid: 4,
                expr: Some(expr("sum")),
                retvarno: -1,
            }),
        ];

        let func = mk_function(datums, body, vec![1]);
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(55)));
    }

    #[test]
    fn executes_raise_notice_format() {
        let src = "
BEGIN
  RAISE NOTICE 'value: %', 42;
  RETURN 1;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(1)));
    }

    #[test]
    fn executes_nested_blocks() {
        let datums = vec![
            mk_var(0, "x", "integer", Some("1")),
            mk_var(1, "y", "integer", Some("2")),
        ];

        let inner = PlPgSqlStmt::Block(PlPgSqlStmtBlock {
            cmd_type: PlPgSqlStmtType::Block,
            lineno: 1,
            stmtid: 2,
            label: None,
            body: vec![PlPgSqlStmt::Assign(PlPgSqlStmtAssign {
                cmd_type: PlPgSqlStmtType::Assign,
                lineno: 1,
                stmtid: 3,
                varno: 0,
                expr: expr("x + y"),
            })],
            n_initvars: 1,
            initvarnos: vec![1],
            exceptions: None,
        });

        let body = vec![
            inner,
            PlPgSqlStmt::Return(PlPgSqlStmtReturn {
                cmd_type: PlPgSqlStmtType::Return,
                lineno: 1,
                stmtid: 4,
                expr: Some(expr("x")),
                retvarno: -1,
            }),
        ];

        let func = mk_function(datums, body, vec![0]);
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(3)));
    }

    #[test]
    fn sets_found_after_perform_sql() {
        let src = "
BEGIN
  PERFORM 1;
  RETURN found;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Bool(true)));
    }

    #[test]
    fn executes_select_into_and_sets_found() {
        let src = "
DECLARE
  a integer;
  b integer;
BEGIN
  SELECT 1, 2 INTO a, b;
  RETURN found AND a = 1 AND b = 2;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Bool(true)));
    }

    #[test]
    fn loop_statement_handles_exit_label() {
        let datums = vec![mk_var(0, "x", "integer", Some("0"))];
        let body = vec![
            PlPgSqlStmt::Loop(PlPgSqlStmtLoop {
                cmd_type: PlPgSqlStmtType::Loop,
                lineno: 1,
                stmtid: 2,
                label: Some("l1".to_string()),
                body: vec![
                    PlPgSqlStmt::Assign(PlPgSqlStmtAssign {
                        cmd_type: PlPgSqlStmtType::Assign,
                        lineno: 1,
                        stmtid: 3,
                        varno: 0,
                        expr: expr("x + 1"),
                    }),
                    PlPgSqlStmt::Exit(crate::plpgsql::types::PlPgSqlStmtExit {
                        cmd_type: PlPgSqlStmtType::Exit,
                        lineno: 1,
                        stmtid: 4,
                        is_exit: true,
                        label: Some("l1".to_string()),
                        cond: None,
                    }),
                ],
            }),
            PlPgSqlStmt::Return(PlPgSqlStmtReturn {
                cmd_type: PlPgSqlStmtType::Return,
                lineno: 1,
                stmtid: 5,
                expr: Some(expr("x")),
                retvarno: -1,
            }),
        ];
        let func = mk_function(datums, body, vec![0]);
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(1)));
    }
}
