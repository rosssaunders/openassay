use crate::catalog::{SearchPath, with_catalog_read};
use crate::parser::ast::CreateTriggerStatement;
use crate::tcop::engine::{EngineError, QueryResult, UserTrigger, with_ext_write};

pub async fn execute_create_trigger(
    create: &CreateTriggerStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&create.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    let trigger = UserTrigger {
        name: create.name.to_ascii_lowercase(),
        table_name: vec![table.schema_name().to_string(), table.name().to_string()],
        timing: create.timing,
        events: create.events.clone(),
        function_name: create
            .function_name
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect(),
    };

    with_ext_write(|ext| {
        if ext.triggers.iter().any(|existing| {
            existing.name == trigger.name && existing.table_name == trigger.table_name
        }) {
            return Err(EngineError {
                message: format!(
                    "trigger \"{}\" for relation \"{}.{}\" already exists",
                    trigger.name, trigger.table_name[0], trigger.table_name[1]
                ),
            });
        }
        ext.triggers.push(trigger);
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE TRIGGER".to_string(),
        rows_affected: 0,
    })
}
