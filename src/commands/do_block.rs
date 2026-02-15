use crate::parser::ast::DoStatement;
use crate::plpgsql::{compile_do_statement, plpgsql_exec_function};
use crate::tcop::engine::{EngineError, QueryResult};

pub async fn execute_do_block(do_stmt: &DoStatement) -> Result<QueryResult, EngineError> {
    if !do_stmt.language.eq_ignore_ascii_case("plpgsql") {
        return Err(EngineError {
            message: format!(
                "DO block language must be plpgsql, got {}",
                do_stmt.language
            ),
        });
    }

    let compiled = compile_do_statement(do_stmt).map_err(|e| EngineError {
        message: e.to_string(),
    })?;
    plpgsql_exec_function(&compiled, &[]).map_err(|message| EngineError { message })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DO".to_string(),
        rows_affected: 0,
    })
}
