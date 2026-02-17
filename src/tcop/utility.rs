#[cfg(not(target_arch = "wasm32"))]
use crate::commands::subscription;
use crate::commands::{
    alter, create_table, do_block, drop, explain, extension, function, index, matview, schema,
    sequence, trigger, variable, view,
};
use crate::parser::ast::Statement;
use crate::tcop::engine::{EngineError, QueryResult};

pub async fn execute_utility_statement(
    statement: &Statement,
    params: &[Option<String>],
) -> Result<QueryResult, EngineError> {
    match statement {
        Statement::CreateTable(create) => create_table::execute_create_table(create).await,
        Statement::CreateSchema(create) => schema::execute_create_schema(create).await,
        Statement::CreateIndex(create) => index::execute_create_index(create).await,
        Statement::CreateSequence(create) => sequence::execute_create_sequence(create).await,
        Statement::CreateView(create) => {
            if create.materialized {
                matview::execute_create_materialized_view(create, params).await
            } else {
                view::execute_create_view(create, params).await
            }
        }
        Statement::RefreshMaterializedView(refresh) => {
            matview::execute_refresh_materialized_view(refresh, params).await
        }
        Statement::AlterSequence(alter) => sequence::execute_alter_sequence(alter).await,
        Statement::AlterView(alter) => {
            if alter.materialized {
                matview::execute_alter_materialized_view(alter).await
            } else {
                view::execute_alter_view(alter).await
            }
        }
        Statement::DropTable(drop_table) => drop::execute_drop_table(drop_table).await,
        Statement::DropSchema(drop_schema) => schema::execute_drop_schema(drop_schema).await,
        Statement::DropIndex(drop_index) => index::execute_drop_index(drop_index).await,
        Statement::DropSequence(drop_sequence) => {
            sequence::execute_drop_sequence(drop_sequence).await
        }
        Statement::DropView(drop_view) => {
            if drop_view.materialized {
                matview::execute_drop_materialized_view(drop_view).await
            } else {
                view::execute_drop_view(drop_view).await
            }
        }
        Statement::Truncate(truncate) => drop::execute_truncate(truncate).await,
        Statement::AlterTable(alter_table) => alter::execute_alter_table(alter_table).await,
        Statement::Explain(explain_stmt) => explain::execute_explain(explain_stmt, params).await,
        Statement::Set(set_stmt) => variable::execute_set(set_stmt).await,
        Statement::Show(show_stmt) => variable::execute_show(show_stmt).await,
        Statement::Discard(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "DISCARD".to_string(),
            rows_affected: 0,
        }),
        Statement::Do(do_stmt) => do_block::execute_do_block(do_stmt).await,
        Statement::Listen(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "LISTEN".to_string(),
            rows_affected: 0,
        }),
        Statement::Notify(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "NOTIFY".to_string(),
            rows_affected: 0,
        }),
        Statement::Unlisten(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "UNLISTEN".to_string(),
            rows_affected: 0,
        }),
        Statement::CreateExtension(create) => extension::execute_create_extension(create).await,
        Statement::DropExtension(drop_ext) => extension::execute_drop_extension(drop_ext).await,
        Statement::CreateFunction(create) => function::execute_create_function(create).await,
        Statement::CreateTrigger(create) => trigger::execute_create_trigger(create).await,
        #[cfg(not(target_arch = "wasm32"))]
        Statement::CreateSubscription(create) => {
            subscription::execute_create_subscription(create).await
        }
        #[cfg(target_arch = "wasm32")]
        Statement::CreateSubscription(_) => Err(EngineError {
            message: "subscriptions are not supported in wasm builds".to_string(),
        }),
        #[cfg(not(target_arch = "wasm32"))]
        Statement::DropSubscription(drop_subscription) => {
            subscription::execute_drop_subscription(drop_subscription).await
        }
        #[cfg(target_arch = "wasm32")]
        Statement::DropSubscription(_) => Err(EngineError {
            message: "subscriptions are not supported in wasm builds".to_string(),
        }),
        Statement::CreateType(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TYPE".to_string(),
            rows_affected: 0,
        }),
        Statement::CreateDomain(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE DOMAIN".to_string(),
            rows_affected: 0,
        }),
        Statement::DropType(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "DROP TYPE".to_string(),
            rows_affected: 0,
        }),
        Statement::DropDomain(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "DROP DOMAIN".to_string(),
            rows_affected: 0,
        }),
        _ => Err(EngineError {
            message: "statement is not a utility command".to_string(),
        }),
    }
}

pub fn is_utility_statement(statement: &Statement) -> bool {
    matches!(
        statement,
        Statement::CreateTable(_)
            | Statement::CreateSchema(_)
            | Statement::CreateIndex(_)
            | Statement::CreateSequence(_)
            | Statement::CreateView(_)
            | Statement::RefreshMaterializedView(_)
            | Statement::AlterSequence(_)
            | Statement::AlterView(_)
            | Statement::DropTable(_)
            | Statement::DropSchema(_)
            | Statement::DropIndex(_)
            | Statement::DropSequence(_)
            | Statement::DropView(_)
            | Statement::Truncate(_)
            | Statement::AlterTable(_)
            | Statement::Explain(_)
            | Statement::Set(_)
            | Statement::Show(_)
            | Statement::Discard(_)
            | Statement::Do(_)
            | Statement::Listen(_)
            | Statement::Notify(_)
            | Statement::Unlisten(_)
            | Statement::CreateExtension(_)
            | Statement::DropExtension(_)
            | Statement::CreateFunction(_)
            | Statement::CreateTrigger(_)
            | Statement::CreateSubscription(_)
            | Statement::DropSubscription(_)
            | Statement::CreateType(_)
            | Statement::CreateDomain(_)
            | Statement::DropType(_)
            | Statement::DropDomain(_)
    )
}
