use std::collections::HashMap;

use crate::catalog::search_path::SearchPath;
use crate::catalog::{ColumnSpec, TableKind, with_catalog_write};
use crate::commands::create_table::{column_spec_from_ast, relation_name_for_create};
use crate::foreign::{ForeignServer, ForeignTableDef, with_fdw_write};
use crate::parser::ast::{CreateForeignTableStatement, CreateServerStatement};
use crate::tcop::engine::{EngineError, QueryResult};

/// Execute `CREATE SERVER <name> FOREIGN DATA WRAPPER <fdw_name> OPTIONS (...)`
pub async fn execute_create_server(
    create: &CreateServerStatement,
) -> Result<QueryResult, EngineError> {
    let server_name = create.name.to_ascii_lowercase();

    // Check if server already exists.
    let exists = with_fdw_write(|reg| reg.get_server(&server_name).is_some());
    if exists {
        if create.if_not_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "CREATE SERVER".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("server \"{}\" already exists", server_name),
        });
    }

    let oid = with_catalog_write(|catalog| catalog.next_oid())
        .map_err(|e| EngineError { message: e.message })?;

    let options: HashMap<String, String> = create
        .options
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect();

    with_fdw_write(|reg| {
        reg.register_server(ForeignServer {
            oid,
            name: server_name,
            fdw_name: create.fdw_name.to_ascii_lowercase(),
            options,
        });
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE SERVER".to_string(),
        rows_affected: 0,
    })
}

/// Execute `CREATE FOREIGN TABLE <name> (cols) SERVER <server> OPTIONS (...)`
pub async fn execute_create_foreign_table(
    create: &CreateForeignTableStatement,
) -> Result<QueryResult, EngineError> {
    let (schema_name, table_name) = relation_name_for_create(&create.name)?;

    // Build column specs from the AST definitions.
    let column_specs: Vec<ColumnSpec> = create
        .columns
        .iter()
        .map(column_spec_from_ast)
        .collect::<Result<Vec<_>, _>>()?;

    let server_name = create.server_name.to_ascii_lowercase();

    // Handle IF NOT EXISTS.
    let table_exists = with_catalog_write(|catalog| {
        let search_path = SearchPath::new(vec![schema_name.clone()]);
        catalog.resolve_table(&create.name, &search_path).is_ok()
    });
    if table_exists {
        if create.if_not_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "CREATE FOREIGN TABLE".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("relation \"{}\" already exists", table_name),
        });
    }

    // Verify the server exists.
    let server_exists = with_fdw_write(|reg| reg.get_server(&server_name).is_some());
    if !server_exists {
        return Err(EngineError {
            message: format!(
                "server \"{}\" does not exist for CREATE FOREIGN TABLE",
                server_name
            ),
        });
    }

    // Register the table in the catalog as TableKind::Foreign.
    let table_oid = with_catalog_write(|catalog| {
        let oid = catalog.create_table(
            &schema_name,
            &table_name,
            TableKind::Foreign,
            column_specs,
            Vec::new(),
            Vec::new(),
        )?;
        // Set the server_name on the newly created table.
        catalog.set_table_server_name(oid, server_name.clone());
        Ok::<_, crate::catalog::CatalogError>(oid)
    })
    .map_err(|e| EngineError { message: e.message })?;

    // Register the FDW table definition.
    let options: HashMap<String, String> = create
        .options
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
        .collect();

    with_fdw_write(|reg| {
        reg.register_table(ForeignTableDef {
            table_oid,
            server_name,
            options,
        });
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE FOREIGN TABLE".to_string(),
        rows_affected: 0,
    })
}
