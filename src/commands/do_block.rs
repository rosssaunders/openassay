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

    let compiled = match compile_do_statement(do_stmt) {
        Ok(compiled) => compiled,
        Err(_) => {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DO".to_string(),
                rows_affected: 0,
            });
        }
    };
    let _ = plpgsql_exec_function(&compiled, &[]);

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DO".to_string(),
        rows_affected: 0,
    })
}
