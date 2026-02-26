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
    PLPGSQL_OTHERS, PlPgSqlCondition, PlPgSqlDatum, PlPgSqlExpr, PlPgSqlFunction,
    PlPgSqlGetdiagKind, PlPgSqlStmt, PlPgSqlStmtAssign, PlPgSqlStmtBlock, PlPgSqlStmtClose,
    PlPgSqlStmtDynexecute, PlPgSqlStmtExecSql, PlPgSqlStmtExit, PlPgSqlStmtFetch, PlPgSqlStmtForc,
    PlPgSqlStmtFori, PlPgSqlStmtFors, PlPgSqlStmtGetdiag, PlPgSqlStmtIf, PlPgSqlStmtLoop,
    PlPgSqlStmtOpen, PlPgSqlStmtPerform, PlPgSqlStmtRaise, PlPgSqlStmtReturn,
    PlPgSqlStmtReturnNext, PlPgSqlStmtWhile, PlPgSqlTypeType, PlPgSqlValue,
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

#[derive(Debug, Clone, Default)]
struct CursorState {
    columns: Vec<String>,
    rows: Vec<Vec<ScalarValue>>,
    position: usize,
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
    runtime_values: HashMap<i32, ScalarValue>,
    record_field_names: HashMap<i32, Vec<String>>,
    eval_processed: u64,
    retset_values: Vec<ScalarValue>,
    cursor_states: HashMap<i32, CursorState>,
}

#[derive(Debug, Clone)]
pub struct PLpgSQLTriggerContext {
    pub tg_op: String,
    pub tg_table_name: String,
    pub new_row: Option<(Vec<String>, Vec<ScalarValue>)>,
    pub old_row: Option<(Vec<String>, Vec<ScalarValue>)>,
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
            runtime_values: HashMap::new(),
            record_field_names: HashMap::new(),
            eval_processed: 0,
            retset_values: Vec::new(),
            cursor_states: HashMap::new(),
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

    fn record_field_value_by_name(&self, record: &str, field: &str) -> Result<ScalarValue, String> {
        let dno = *self
            .datum_name_index
            .get(&record.to_ascii_lowercase())
            .ok_or_else(|| format!("unknown variable \"{record}\""))?;
        self.record_field_value(dno, record, field)
    }

    fn record_field_value(
        &self,
        dno: i32,
        record: &str,
        field: &str,
    ) -> Result<ScalarValue, String> {
        let Some(field_names) = self.record_field_names.get(&dno) else {
            return Err(format!(
                "record \"{record}\" is not assigned yet and has no field \"{field}\""
            ));
        };

        let pos = field_names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(field))
            .ok_or_else(|| format!("record \"{record}\" has no field \"{field}\""))?;

        match self.runtime_values.get(&dno) {
            Some(ScalarValue::Record(values)) => {
                Ok(values.get(pos).cloned().unwrap_or(ScalarValue::Null))
            }
            Some(ScalarValue::Null) | None => Err(format!(
                "record \"{record}\" is null and has no field \"{field}\""
            )),
            _ => Err(format!("variable \"{record}\" is not a record")),
        }
    }

    fn datum_value(&self, dno: i32) -> Result<ScalarValue, String> {
        if let Some(value) = self.runtime_values.get(&dno) {
            return Ok(value.clone());
        }

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
            PlPgSqlDatum::Row(row) => {
                let mut values = Vec::with_capacity(row.varnos.len());
                for varno in &row.varnos {
                    values.push(self.datum_value(*varno)?);
                }
                Ok(ScalarValue::Record(values))
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
    plpgsql_exec_function_with_trigger(func, args, None)
}

pub fn plpgsql_exec_function_with_trigger(
    func: &PlPgSqlFunction,
    args: &[ScalarValue],
    trigger_context: Option<&PLpgSQLTriggerContext>,
) -> Result<Option<ScalarValue>, String> {
    let mut estate = PLpgSQLExecState::new(func);
    set_found(&mut estate, false)?;
    bind_call_arguments(&mut estate, args)?;
    if let Some(context) = trigger_context {
        bind_trigger_context(&mut estate, context)?;
    }

    let Some(action) = estate.func.action.clone() else {
        return Ok(None);
    };

    let rc = exec_stmt_block(&mut estate, &action)?;
    if estate.retval.is_none() && estate.func.out_param_varno >= 0 {
        let value = estate.datum_value(estate.func.out_param_varno)?;
        estate.ret_is_null = matches!(value, ScalarValue::Null);
        estate.retval = Some(value);
    }
    match rc {
        ExecResultCode::Ok | ExecResultCode::Return => Ok(estate.retval.clone()),
        ExecResultCode::Exit | ExecResultCode::Continue => {
            Err("unexpected EXIT/CONTINUE outside loop".to_string())
        }
    }
}

fn bind_trigger_context(
    estate: &mut PLpgSQLExecState,
    context: &PLpgSQLTriggerContext,
) -> Result<(), String> {
    assign_trigger_text_variable(estate, "tg_op", &context.tg_op)?;
    assign_trigger_text_variable(estate, "tg_table_name", &context.tg_table_name)?;
    assign_trigger_record_variable(estate, "new", context.new_row.as_ref())?;
    assign_trigger_record_variable(estate, "old", context.old_row.as_ref())?;
    Ok(())
}

fn assign_trigger_text_variable(
    estate: &mut PLpgSQLExecState,
    name: &str,
    value: &str,
) -> Result<(), String> {
    let Some(dno) = estate.datum_name_index.get(name).copied() else {
        return Ok(());
    };
    exec_assign_value_dno(estate, dno, ScalarValue::Text(value.to_string()))
}

fn assign_trigger_record_variable(
    estate: &mut PLpgSQLExecState,
    name: &str,
    row: Option<&(Vec<String>, Vec<ScalarValue>)>,
) -> Result<(), String> {
    let Some(dno) = estate.datum_name_index.get(name).copied() else {
        return Ok(());
    };
    if let Some((field_names, values)) = row {
        assign_record_row_dno(estate, dno, values, field_names)?;
    } else {
        exec_assign_value_dno(estate, dno, ScalarValue::Null)?;
    }
    Ok(())
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

    let rc = if let Some(exceptions) = &block.exceptions {
        match exec_stmts(estate, &block.body) {
            Ok(rc) => rc,
            Err(err) => {
                let (sqlstate, sqlerrm) = classify_error_sqlstate(&err);
                let sqlerrstate =
                    sqlstate_code_to_int(sqlstate.as_str()).unwrap_or_else(|| sqlstate_internal());

                let mut matched = None;
                for exception in &exceptions.exc_list {
                    if exception_matches_conditions(sqlerrstate, &exception.conditions) {
                        matched = Some(exception);
                        break;
                    }
                }

                let Some(exception) = matched else {
                    return Err(err);
                };

                if exceptions.sqlstate_varno >= 0 {
                    exec_assign_value_dno(
                        estate,
                        exceptions.sqlstate_varno,
                        ScalarValue::Text(sqlstate),
                    )?;
                }
                if exceptions.sqlerrm_varno >= 0 {
                    exec_assign_value_dno(
                        estate,
                        exceptions.sqlerrm_varno,
                        ScalarValue::Text(sqlerrm),
                    )?;
                }

                exec_stmts(estate, &exception.action)?
            }
        }
    } else {
        exec_stmts(estate, &block.body)?
    };

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
            PlPgSqlStmt::Forc(stmt) => exec_stmt_forc(estate, stmt)?,
            PlPgSqlStmt::Exit(stmt) => exec_stmt_exit(estate, stmt)?,
            PlPgSqlStmt::Return(stmt) => exec_stmt_return(estate, stmt)?,
            PlPgSqlStmt::ReturnNext(stmt) => exec_stmt_return_next(estate, stmt)?,
            PlPgSqlStmt::Raise(stmt) => exec_stmt_raise(estate, stmt)?,
            PlPgSqlStmt::ExecSql(stmt) => exec_stmt_execsql(estate, stmt)?,
            PlPgSqlStmt::DynExecute(stmt) => exec_stmt_dynexecute(estate, stmt)?,
            PlPgSqlStmt::Getdiag(stmt) => exec_stmt_getdiag(estate, stmt)?,
            PlPgSqlStmt::Open(stmt) => exec_stmt_open(estate, stmt)?,
            PlPgSqlStmt::Fetch(stmt) => exec_stmt_fetch(estate, stmt)?,
            PlPgSqlStmt::Close(stmt) => exec_stmt_close(estate, stmt)?,
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
    if let Some(fieldname) = &stmt.fieldname {
        exec_assign_record_field_dno(estate, stmt.varno, fieldname, value)?;
    } else {
        exec_assign_value_dno(estate, stmt.varno, value)?;
    }
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
        found = true;
        assign_single_row_to_variable(
            estate,
            loop_var.dno,
            &loop_var.refname,
            row,
            Some(result.columns.as_slice()),
        )?;

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

/// Cursor FOR-loop execution corresponding to `exec_stmt_forc` (`pl_exec.c`) with
/// simplified row assignment into a single scalar loop target.
fn exec_stmt_forc(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtForc,
) -> Result<ExecResultCode, String> {
    let loop_var = stmt
        .var
        .as_ref()
        .ok_or_else(|| "FOR cursor variable is missing".to_string())?;
    if !estate.cursor_states.contains_key(&stmt.curvar) {
        return Err("cursor is not open".to_string());
    }

    let mut found = false;
    loop {
        let row = {
            let cursor_state = estate
                .cursor_states
                .get_mut(&stmt.curvar)
                .ok_or_else(|| "cursor is not open".to_string())?;
            if cursor_state.position >= cursor_state.rows.len() {
                break;
            }
            let row = cursor_state.rows[cursor_state.position].clone();
            cursor_state.position += 1;
            row
        };

        found = true;
        let columns = estate
            .cursor_states
            .get(&stmt.curvar)
            .map(|state| state.columns.clone())
            .unwrap_or_default();
        assign_single_row_to_variable(
            estate,
            loop_var.dno,
            &loop_var.refname,
            &row,
            Some(columns.as_slice()),
        )?;

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

    if estate.func.out_param_varno >= 0 {
        let value = estate.datum_value(estate.func.out_param_varno)?;
        estate.ret_is_null = matches!(value, ScalarValue::Null);
        estate.retval = Some(value);
    } else {
        estate.retval = None;
        estate.ret_is_null = true;
    }
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
        Err(tag_sqlstate_error("P0001", formatted))
    }
}

/// SQL statement execution corresponding to `exec_stmt_execsql`
/// (`pl_exec.c:4214-4478`) via OpenAssay's SQL executor.
fn exec_stmt_execsql(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtExecSql,
) -> Result<ExecResultCode, String> {
    let result = execute_sql_statement(estate, &stmt.sqlstmt.query)?;
    set_eval_processed(estate, row_count_from_query_result(&result));

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
        set_eval_processed(estate, result.rows.len() as u64);
    } else {
        let found = result.rows_affected > 0 || !result.rows.is_empty();
        set_found(estate, found)?;
        set_eval_processed(estate, row_count_from_query_result(&result));
    }

    Ok(ExecResultCode::Ok)
}

fn exec_stmt_getdiag(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtGetdiag,
) -> Result<ExecResultCode, String> {
    if stmt.is_stacked {
        return Err("GET STACKED DIAGNOSTICS is not implemented".to_string());
    }

    for item in &stmt.diag_items {
        match item.kind {
            PlPgSqlGetdiagKind::RowCount => {
                let row_count = i64::try_from(estate.eval_processed).unwrap_or(i64::MAX);
                exec_assign_value_dno(estate, item.target, ScalarValue::Int(row_count))?;
            }
            PlPgSqlGetdiagKind::RoutineOid => {
                let oid = i64::from(estate.func.fn_oid);
                exec_assign_value_dno(estate, item.target, ScalarValue::Int(oid))?;
            }
            _ => {
                return Err(
                    "only GET DIAGNOSTICS ROW_COUNT and PG_ROUTINE_OID are implemented"
                        .to_string(),
                );
            }
        }
    }

    Ok(ExecResultCode::Ok)
}

/// OPEN cursor execution corresponding to `exec_stmt_open` (`pl_exec.c`) with
/// static query and dynamic EXECUTE query support.
fn exec_stmt_open(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtOpen,
) -> Result<ExecResultCode, String> {
    let result = if let Some(query) = &stmt.query {
        execute_sql_statement(estate, &query.query)?
    } else if let Some(dynquery) = &stmt.dynquery {
        let query_value = exec_eval_expr(estate, dynquery)?;
        let sql = scalar_to_dynamic_sql(query_value)?;
        execute_sql_statement(estate, &sql)?
    } else {
        return Err("OPEN requires either FOR query or FOR EXECUTE".to_string());
    };

    estate.cursor_states.insert(
        stmt.curvar,
        CursorState {
            columns: result.columns,
            rows: result.rows,
            position: 0,
        },
    );
    set_eval_processed(estate, 0);
    Ok(ExecResultCode::Ok)
}

/// FETCH cursor execution corresponding to `exec_stmt_fetch` (`pl_exec.c`) for
/// single-target assignments.
fn exec_stmt_fetch(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtFetch,
) -> Result<ExecResultCode, String> {
    let target = stmt
        .target
        .as_ref()
        .ok_or_else(|| "FETCH target variable is missing".to_string())?;
    let Some(cursor_state) = estate.cursor_states.get_mut(&stmt.curvar) else {
        return Err("cursor is not open".to_string());
    };

    if cursor_state.position >= cursor_state.rows.len() {
        exec_assign_value_dno(estate, target.dno, ScalarValue::Null)?;
        set_found(estate, false)?;
        set_eval_processed(estate, 0);
        return Ok(ExecResultCode::Ok);
    }

    let row = cursor_state.rows[cursor_state.position].clone();
    let columns = cursor_state.columns.clone();
    cursor_state.position += 1;
    assign_single_row_to_variable(
        estate,
        target.dno,
        &target.refname,
        &row,
        Some(columns.as_slice()),
    )?;
    set_found(estate, true)?;
    set_eval_processed(estate, 1);
    Ok(ExecResultCode::Ok)
}

fn assign_single_row_to_variable(
    estate: &mut PLpgSQLExecState,
    dno: i32,
    refname: &str,
    row: &[ScalarValue],
    columns: Option<&[String]>,
) -> Result<(), String> {
    if datum_is_record_variable(estate, dno)? {
        let field_names: Vec<String> = if let Some(columns) = columns {
            if columns.len() == row.len() {
                columns.iter().map(|name| name.to_string()).collect()
            } else {
                (0..row.len()).map(|idx| format!("f{}", idx + 1)).collect()
            }
        } else {
            (0..row.len()).map(|idx| format!("f{}", idx + 1)).collect()
        };
        assign_record_row_dno(estate, dno, row, &field_names)
    } else {
        if row.len() != 1 {
            return Err(format!(
                "target variable \"{refname}\" expects exactly 1 column but cursor returned {}",
                row.len()
            ));
        }
        exec_assign_value_dno(estate, dno, row[0].clone())
    }
}

/// CLOSE cursor execution corresponding to `exec_stmt_close` (`pl_exec.c`).
fn exec_stmt_close(
    estate: &mut PLpgSQLExecState,
    stmt: &PlPgSqlStmtClose,
) -> Result<ExecResultCode, String> {
    let removed = estate.cursor_states.remove(&stmt.curvar);
    if removed.is_none() {
        return Err("cursor is not open".to_string());
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
    set_eval_processed(estate, result.rows.len() as u64);
    set_found(estate, !result.rows.is_empty())?;
    Ok(ExecResultCode::Ok)
}

fn execute_sql_statement(estate: &mut PLpgSQLExecState, sql: &str) -> Result<QueryResult, String> {
    let substituted = substitute_variables(sql, estate)?;
    let statement = parse_statement(&substituted).map_err(|e| e.message)?;
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match statement {
        Statement::Query(query) => {
            block_on(execute_query_with_outer(&query, &[], None)).map_err(|e| e.message)
        }
        other => {
            let planned = plan_statement(other).map_err(|e| e.message)?;
            block_on(execute_planned_query(&planned, &[])).map_err(|e| e.message)
        }
    }));
    match result {
        Ok(outcome) => outcome,
        Err(_) => Err("internal plpgsql execution panic".to_string()),
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

fn datum_is_record_variable(estate: &PLpgSQLExecState, dno: i32) -> Result<bool, String> {
    let idx = PLpgSQLExecState::datum_index(dno)?;
    let datum = estate
        .datums
        .get(idx)
        .ok_or_else(|| format!("datum {dno} is out of range"))?;

    let is_record = match datum {
        PlPgSqlDatum::Var(var) => var.datatype.as_ref().is_some_and(|datatype| {
            datatype.ttype == PlPgSqlTypeType::Rec
                || datatype.typname.eq_ignore_ascii_case("record")
        }),
        _ => false,
    };
    Ok(is_record)
}

fn assign_record_row_dno(
    estate: &mut PLpgSQLExecState,
    dno: i32,
    row: &[ScalarValue],
    field_names: &[String],
) -> Result<(), String> {
    exec_assign_value_dno(estate, dno, ScalarValue::Record(row.to_vec()))?;
    estate.record_field_names.insert(dno, field_names.to_vec());
    Ok(())
}

fn exec_assign_record_field_dno(
    estate: &mut PLpgSQLExecState,
    dno: i32,
    fieldname: &str,
    value: ScalarValue,
) -> Result<(), String> {
    if !datum_is_record_variable(estate, dno)? {
        return Err(format!(
            "cannot assign to field \"{fieldname}\" because target is not a record"
        ));
    }

    let mut fields = estate
        .record_field_names
        .get(&dno)
        .cloned()
        .unwrap_or_default();
    let mut values = match estate.runtime_values.get(&dno) {
        Some(ScalarValue::Record(values)) => values.clone(),
        _ => Vec::new(),
    };

    if let Some(pos) = fields
        .iter()
        .position(|name| name.eq_ignore_ascii_case(fieldname))
    {
        if pos >= values.len() {
            values.resize(pos + 1, ScalarValue::Null);
        }
        values[pos] = value;
    } else {
        fields.push(fieldname.to_string());
        values.push(value);
    }

    exec_assign_value_dno(estate, dno, ScalarValue::Record(values))?;
    estate.record_field_names.insert(dno, fields);
    Ok(())
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
                let is_record = datatype.ttype == PlPgSqlTypeType::Rec
                    || datatype.typname.eq_ignore_ascii_case("record");
                if is_record {
                    if !matches!(value, ScalarValue::Null | ScalarValue::Record(_)) {
                        return Err(format!(
                            "cannot assign non-record value to record variable \"{}\"",
                            var.variable.refname
                        ));
                    }
                } else {
                    value = coerce_scalar_to_type(value, datatype.typname.as_str())?;
                }
            }

            if matches!(value, ScalarValue::Null) && var.variable.notnull {
                return Err(format!(
                    "null value cannot be assigned to variable \"{}\" declared NOT NULL",
                    var.variable.refname
                ));
            }

            var.isnull = matches!(value, ScalarValue::Null);
            var.value = Some(scalar_to_plpgsql_value(&value));
            if matches!(value, ScalarValue::Null) {
                estate.record_field_names.remove(&dno);
            }
            estate.runtime_values.insert(dno, value);
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

fn row_count_from_query_result(result: &QueryResult) -> u64 {
    if result.command_tag.eq_ignore_ascii_case("SELECT") {
        result.rows.len() as u64
    } else {
        result.rows_affected
    }
}

fn set_eval_processed(estate: &mut PLpgSQLExecState, processed: u64) {
    estate.eval_processed = processed;
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

fn exception_matches_conditions(sqlerrstate: i32, conditions: &[PlPgSqlCondition]) -> bool {
    for condition in conditions {
        if condition.sqlerrstate == PLPGSQL_OTHERS {
            if sqlerrstate != sqlstate_query_canceled() && sqlerrstate != sqlstate_assert_failure()
            {
                return true;
            }
            continue;
        }

        if condition.sqlerrstate == sqlerrstate {
            return true;
        }

        if errcode_is_category(condition.sqlerrstate)
            && errcode_to_category(sqlerrstate) == condition.sqlerrstate
        {
            return true;
        }
    }
    false
}

fn classify_error_sqlstate(err: &str) -> (String, String) {
    if let Some((sqlstate, message)) = parse_tagged_sqlstate_error(err) {
        return (sqlstate, message);
    }

    let lower = err.to_ascii_lowercase();
    let sqlstate = if lower.starts_with("parse error:") {
        "42601"
    } else if lower.contains("current transaction is aborted") {
        "25P02"
    } else if lower.contains("cannot be executed from a transaction block") {
        "25001"
    } else if lower.contains("privilege") || lower.contains("permission denied") {
        "42501"
    } else if lower.contains("duplicate value for key") {
        "23505"
    } else if lower.contains("does not allow null values") {
        "23502"
    } else if lower.contains("already exists") {
        if lower.contains("relation")
            || lower.contains("table")
            || lower.contains("view")
            || lower.contains("index")
        {
            "42P07"
        } else {
            "42710"
        }
    } else if lower.contains("unknown column") {
        "42703"
    } else if lower.contains("does not exist") {
        if lower.contains("column") {
            "42703"
        } else if lower.contains("schema")
            || lower.contains("relation")
            || lower.contains("table")
            || lower.contains("view")
            || lower.contains("sequence")
        {
            "42P01"
        } else {
            "42704"
        }
    } else if lower.contains("division by zero") {
        "22012"
    } else {
        "XX000"
    };

    (sqlstate.to_string(), err.to_string())
}

fn parse_tagged_sqlstate_error(err: &str) -> Option<(String, String)> {
    let prefix = "SQLSTATE ";
    if !err.starts_with(prefix) {
        return None;
    }

    let rest = &err[prefix.len()..];
    let (code, message) = rest.split_once(": ")?;
    if !is_valid_sqlstate_code(code) {
        return None;
    }
    Some((code.to_string(), message.to_string()))
}

fn tag_sqlstate_error(code: &str, message: String) -> String {
    format!("SQLSTATE {code}: {message}")
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

fn errcode_to_category(errcode: i32) -> i32 {
    errcode & ((1 << 12) - 1)
}

fn errcode_is_category(errcode: i32) -> bool {
    (errcode & !((1 << 12) - 1)) == 0
}

fn is_valid_sqlstate_code(code: &str) -> bool {
    code.len() == 5
        && code
            .chars()
            .all(|ch| ch.is_ascii_uppercase() || ch.is_ascii_digit())
}

fn sqlstate_internal() -> i32 {
    sqlstate_code_to_int("XX000").expect("valid internal SQLSTATE")
}

fn sqlstate_query_canceled() -> i32 {
    sqlstate_code_to_int("57014").expect("valid query canceled SQLSTATE")
}

fn sqlstate_assert_failure() -> i32 {
    sqlstate_code_to_int("P0004").expect("valid assert failure SQLSTATE")
}

fn try_eval_direct_expression(estate: &PLpgSQLExecState, expr: &str) -> Option<ScalarValue> {
    if let Some(value) = try_parse_simple_constant(expr) {
        return Some(value);
    }

    if let Some((record, field)) = parse_simple_dotted_identifier(expr) {
        return estate.record_field_value_by_name(record, field).ok();
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
    let mut idx = 0usize;

    while idx < tokens.len() {
        let token = &tokens[idx];
        if matches!(token.kind, PlPgSqlTokenKind::Eof) {
            break;
        }

        if let PlPgSqlTokenKind::Identifier(record_name) = &token.kind
            && matches!(
                tokens.get(idx + 1).map(|t| &t.kind),
                Some(PlPgSqlTokenKind::Dot)
            )
            && let Some(field_token) = tokens.get(idx + 2)
            && let PlPgSqlTokenKind::Identifier(field_name) = &field_token.kind
        {
            let value = estate.record_field_value_by_name(record_name, field_name)?;
            output.push_str(&sql[cursor..token.span.start]);
            output.push_str(&scalar_to_sql_literal(&value));
            cursor = field_token.span.end;
            idx += 3;
            continue;
        }

        if let PlPgSqlTokenKind::Identifier(name) = &token.kind
            && let Some(value) = estate.datum_value_by_name(name)
        {
            output.push_str(&sql[cursor..token.span.start]);
            output.push_str(&scalar_to_sql_literal(&value));
            cursor = token.span.end;
        }
        idx += 1;
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

fn parse_simple_dotted_identifier(text: &str) -> Option<(&str, &str)> {
    let trimmed = text.trim();
    let dot = trimmed.find('.')?;
    if trimmed[dot + 1..].contains('.') {
        return None;
    }
    let (lhs, rhs_with_dot) = trimmed.split_at(dot);
    let rhs = &rhs_with_dot[1..];
    if is_simple_identifier(lhs) && is_simple_identifier(rhs) {
        Some((lhs, rhs))
    } else {
        None
    }
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
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
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
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
            PlPgSqlValue::Text(value.render())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::plpgsql_exec_function;
    use crate::plpgsql::compiler::{
        compile_create_function_sql, compile_do_block_sql, compile_function_body,
    };
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
                    fieldname: None,
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
                    fieldname: None,
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
                fieldname: None,
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
    fn executes_open_fetch_close_cursor() {
        let src = "
DECLARE
  c refcursor;
  x integer;
BEGIN
  OPEN c FOR SELECT 7;
  FETCH c INTO x;
  CLOSE c;
  RETURN x;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(7)));
    }

    #[test]
    fn executes_for_cursor_loop_with_scalar_target() {
        let src = "
DECLARE
  c refcursor;
  rec integer;
  sum integer := 0;
BEGIN
  OPEN c FOR SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3;
  FOR rec IN c LOOP
    sum := sum + rec;
  END LOOP;
  CLOSE c;
  RETURN sum;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(6)));
    }

    #[test]
    fn executes_record_field_assignment_and_read() {
        let src = "
DECLARE
  rec record;
BEGIN
  rec.a := 41;
  rec.b := 1;
  RETURN rec.a + rec.b;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(42)));
    }

    #[test]
    fn executes_for_cursor_loop_with_record_target_and_dot_notation() {
        let src = "
DECLARE
  c refcursor;
  rec record;
  sum integer := 0;
BEGIN
  OPEN c FOR SELECT 2 AS v UNION ALL SELECT 3 AS v;
  FOR rec IN c LOOP
    sum := sum + rec.v;
  END LOOP;
  CLOSE c;
  RETURN sum;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(5)));
    }

    #[test]
    fn executes_get_diagnostics_row_count() {
        let src = "
DECLARE
  rc integer;
BEGIN
  SELECT 1 UNION ALL SELECT 2;
  GET DIAGNOSTICS rc := ROW_COUNT;
  RETURN rc;
END;
";
        let func = compile_function_body(src).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(2)));
    }

    #[test]
    fn executes_function_with_out_parameter() {
        let sql = "
CREATE FUNCTION out_only(OUT x integer)
RETURNS integer
AS $$
BEGIN
  x := 9;
  RETURN;
END;
$$ LANGUAGE plpgsql;";
        let func = compile_create_function_sql(sql).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(9)));
    }

    #[test]
    fn executes_function_with_inout_parameter() {
        let sql = "
CREATE FUNCTION bump(INOUT x integer)
RETURNS integer
AS $$
BEGIN
  x := x + 1;
  RETURN;
END;
$$ LANGUAGE plpgsql;";
        let func = compile_create_function_sql(sql).expect("compile should succeed");
        let result =
            plpgsql_exec_function(&func, &[ScalarValue::Int(4)]).expect("execution should succeed");
        assert_eq!(result, Some(ScalarValue::Int(5)));
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
                        fieldname: None,
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

    #[test]
    fn executes_do_block_with_sqlstate_exception_handler() {
        let sql = r"
DO $$
BEGIN
  BEGIN
    RAISE EXCEPTION 'boom';
  EXCEPTION
    WHEN OTHERS THEN
      IF sqlstate = 'P0001' AND sqlerrm = 'boom' THEN
        NULL;
      ELSE
        RAISE EXCEPTION 'unexpected exception context';
      END IF;
  END;
END;
$$ LANGUAGE plpgsql;
";

        let func = compile_do_block_sql(sql).expect("compile should succeed");
        let result = plpgsql_exec_function(&func, &[]).expect("execution should succeed");
        assert_eq!(result, None);
    }

    #[test]
    fn unmatched_exception_handler_rethrows_error() {
        let sql = r"
DO $$
BEGIN
  BEGIN
    RAISE EXCEPTION 'boom';
  EXCEPTION
    WHEN SQLSTATE '22012' THEN
      NULL;
  END;
END;
$$ LANGUAGE plpgsql;
";

        let func = compile_do_block_sql(sql).expect("compile should succeed");
        let err = plpgsql_exec_function(&func, &[]).expect_err("execution should fail");
        assert!(
            err.contains("boom"),
            "error should contain original exception message: {err}"
        );
    }
}
