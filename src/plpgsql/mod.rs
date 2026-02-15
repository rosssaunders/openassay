//! PL/pgSQL support (Phase 1).

pub mod compiler;
pub mod executor;
pub mod scanner;
pub mod types;

pub use compiler::{
    PlPgSqlCompileError, compile_create_function_sql, compile_create_function_statement,
    compile_do_block_sql, compile_do_statement, compile_function_body,
};
pub use executor::{PLpgSQLExecState, plpgsql_exec_function};
pub use scanner::{
    PlPgSqlKeyword, PlPgSqlScanError, PlPgSqlSpan, PlPgSqlToken, PlPgSqlTokenKind,
    extract_sql_expression, tokenize,
};
pub use types::*;
