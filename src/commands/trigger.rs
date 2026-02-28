use crate::catalog::{SearchPath, with_catalog_read};
use crate::parser::ast::{CreateTriggerStatement, DropTriggerStatement};
use crate::tcop::engine::{EngineError, QueryResult, UserTrigger, with_ext_write};

pub async fn execute_create_trigger(
    create: &CreateTriggerStatement,
) -> Result<QueryResult, EngineError> {
    let resolved_table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&create.table_name, &SearchPath::default())
            .cloned()
    })
    .ok();

    let table_name = if let Some(table) = resolved_table {
        vec![table.schema_name().to_string(), table.name().to_string()]
    } else {
        create
            .table_name
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect::<Vec<_>>()
    };

    let trigger = UserTrigger {
        name: create.name.to_ascii_lowercase(),
        table_name,
        timing: create.timing,
        events: create.events.clone(),
        function_name: create
            .function_name
            .iter()
            .map(|part| part.to_ascii_lowercase())
            .collect(),
    };

    with_ext_write(|ext| {
        ext.triggers.retain(|existing| {
            !(existing.name == trigger.name && existing.table_name == trigger.table_name)
        });
        ext.triggers.push(trigger.clone());
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE TRIGGER".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_trigger(
    drop_trigger: &DropTriggerStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&drop_trigger.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    let trigger_name = drop_trigger.name.to_ascii_lowercase();
    let table_schema = table.schema_name().to_ascii_lowercase();
    let table_name = table.name().to_ascii_lowercase();

    with_ext_write(|ext| {
        let before = ext.triggers.len();
        ext.triggers.retain(|existing| {
            !(existing.name.eq_ignore_ascii_case(&trigger_name)
                && existing.table_name.len() == 2
                && existing.table_name[0].eq_ignore_ascii_case(&table_schema)
                && existing.table_name[1].eq_ignore_ascii_case(&table_name))
        });
        if ext.triggers.len() == before && !drop_trigger.if_exists {
            return Err(EngineError {
                message: format!(
                    "trigger \"{}\" for relation \"{}.{}\" does not exist",
                    drop_trigger.name,
                    table.schema_name(),
                    table.name()
                ),
            });
        }
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP TRIGGER".to_string(),
        rows_affected: 0,
    })
}
