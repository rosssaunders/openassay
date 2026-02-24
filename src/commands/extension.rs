use crate::parser::ast::{CreateExtensionStatement, DropExtensionStatement};
use crate::tcop::engine::{EngineError, ExtensionRecord, QueryResult, with_ext_write};

pub async fn execute_create_extension(
    create: &CreateExtensionStatement,
) -> Result<QueryResult, EngineError> {
    let name = create.name.to_ascii_lowercase();
    with_ext_write(|ext| {
        if ext.extensions.iter().any(|e| e.name == name) {
            if create.if_not_exists {
                return Ok(());
            }
            return Err(EngineError {
                message: format!("extension \"{name}\" already exists"),
            });
        }
        let (version, description) = match name.as_str() {
            "ws" => ("1.0".to_string(), "WebSocket client extension".to_string()),
            "http" => ("1.0".to_string(), "HTTP client extension".to_string()),
            "uuid-ossp" => ("1.0".to_string(), "UUID generation functions".to_string()),
            "pgcrypto" => ("1.0".to_string(), "Cryptographic functions".to_string()),
            "vector" => (
                "1.0".to_string(),
                "Vector type and distance functions".to_string(),
            ),
            _ => {
                return Err(EngineError {
                    message: format!("extension \"{name}\" is not available"),
                });
            }
        };
        ext.extensions.push(ExtensionRecord {
            name: name.clone(),
            version,
            description,
        });
        if name == "ws" {
            ext.ws_next_id = 1;
            ext.ws_connections.clear();
        }
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE EXTENSION".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_extension(
    drop_ext: &DropExtensionStatement,
) -> Result<QueryResult, EngineError> {
    let name = drop_ext.name.to_ascii_lowercase();
    with_ext_write(|ext| {
        let before = ext.extensions.len();
        ext.extensions.retain(|e| e.name != name);
        if ext.extensions.len() == before && !drop_ext.if_exists {
            return Err(EngineError {
                message: format!("extension \"{name}\" does not exist"),
            });
        }
        if name == "ws" {
            ext.ws_connections.clear();
            ext.user_functions
                .retain(|f| f.name.first().map(|s| s.as_str()) != Some("ws"));
        }
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP EXTENSION".to_string(),
        rows_affected: 0,
    })
}
