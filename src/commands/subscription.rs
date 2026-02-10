use crate::parser::ast::{CreateSubscriptionStatement, DropSubscriptionStatement};
use crate::replication::subscription::{create_subscription, drop_subscription, SubscriptionConfig};
use crate::tcop::engine::{EngineError, QueryResult};

pub async fn execute_create_subscription(
    statement: &CreateSubscriptionStatement,
) -> Result<QueryResult, EngineError> {
    let config = SubscriptionConfig {
        name: statement.name.clone(),
        conninfo: statement.connection.clone(),
        publication: statement.publication.clone(),
        slot_name: statement
            .options
            .slot_name
            .clone()
            .unwrap_or_else(|| statement.name.clone()),
        copy_data: statement.options.copy_data,
    };
    create_subscription(config).map_err(|err| EngineError { message: err.message })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE SUBSCRIPTION".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_subscription(
    statement: &DropSubscriptionStatement,
) -> Result<QueryResult, EngineError> {
    match drop_subscription(&statement.name) {
        Ok(()) => {}
        Err(err) if statement.if_exists => {}
        Err(err) => return Err(EngineError { message: err.message }),
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP SUBSCRIPTION".to_string(),
        rows_affected: 0,
    })
}
