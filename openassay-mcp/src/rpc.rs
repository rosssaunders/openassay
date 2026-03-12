use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::server::McpServer;

const JSONRPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Debug)]
pub(crate) struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcError {
    pub(crate) fn parse_error(message: String) -> Self {
        Self {
            code: -32700,
            message,
            data: None,
        }
    }

    pub(crate) fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32600,
            message: message.into(),
            data: None,
        }
    }

    pub(crate) fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }

    pub(crate) fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }

    pub(crate) fn message(&self) -> &str {
        &self.message
    }
}

pub(crate) async fn process_line(server: &mut McpServer, line: &str) -> Option<Value> {
    let parsed = match serde_json::from_str::<Value>(line) {
        Ok(value) => value,
        Err(error) => {
            return Some(error_response(
                Value::Null,
                RpcError::parse_error(error.to_string()),
            ));
        }
    };

    process_request(server, parsed).await
}

async fn process_request(server: &mut McpServer, parsed: Value) -> Option<Value> {
    let object = match parsed.as_object() {
        Some(object) => object,
        None => {
            return Some(error_response(
                Value::Null,
                RpcError::invalid_request("request must be a JSON object"),
            ));
        }
    };

    let id = object.get("id").cloned();
    let error_id = id.clone().unwrap_or(Value::Null);

    let jsonrpc = object
        .get("jsonrpc")
        .and_then(Value::as_str)
        .ok_or_else(|| RpcError::invalid_request("missing jsonrpc field"));
    if let Err(error) = jsonrpc {
        if id.is_none() {
            return Some(error_response(Value::Null, error));
        }
        return Some(error_response(error_id, error));
    }

    if jsonrpc.ok() != Some(JSONRPC_VERSION) {
        let error = RpcError::invalid_request("jsonrpc must be \"2.0\"");
        if id.is_none() {
            return Some(error_response(Value::Null, error));
        }
        return Some(error_response(error_id, error));
    }

    let Some(method) = object.get("method").and_then(Value::as_str) else {
        let error = RpcError::invalid_request("missing method field");
        if id.is_none() {
            return Some(error_response(Value::Null, error));
        }
        return Some(error_response(error_id, error));
    };

    let params = object
        .get("params")
        .cloned()
        .unwrap_or_else(|| Value::Object(Map::new()));

    match server.handle_method(method, params).await {
        Ok(result) => {
            id.map(|request_id| success_response(request_id, result.unwrap_or(Value::Null)))
        }
        Err(error) => {
            if id.is_none() {
                eprintln!(
                    "notification error for method {method}: {}",
                    error.message()
                );
                None
            } else {
                Some(error_response(error_id, error))
            }
        }
    }
}

fn success_response(id: Value, result: Value) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "result": result,
    })
}

fn error_response(id: Value, error: RpcError) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "error": {
            "code": error.code,
            "message": error.message,
            "data": error.data,
        }
    })
}

pub(crate) fn initialize_result() -> Value {
    json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {
            "tools": {
                "listChanged": false,
            },
            "resources": {
                "listChanged": false,
                "subscribe": false,
            },
        },
        "serverInfo": {
            "name": "openassay-mcp",
            "version": "0.1.0",
        },
    })
}

pub(crate) fn tools_list_result() -> Value {
    json!({
        "tools": [
            {
                "name": "query",
                "description": "Execute SQL against OpenAssay.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string" }
                    },
                    "required": ["sql"],
                    "additionalProperties": false,
                },
            },
            {
                "name": "load_url",
                "description": "Load a JSON API into a table via json_table(url, path...).",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "url": { "type": "string" },
                        "json_path": { "type": "string" },
                        "table_name": { "type": "string" }
                    },
                    "required": ["url"],
                    "additionalProperties": false,
                },
            },
            {
                "name": "load_iceberg",
                "description": "Load an Iceberg table into an in-memory OpenAssay table.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "table_name": { "type": "string" }
                    },
                    "required": ["path"],
                    "additionalProperties": false,
                },
            },
            {
                "name": "list_tables",
                "description": "List all user tables in the public schema.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                },
            },
            {
                "name": "describe_table",
                "description": "Describe columns for a table.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table_name": { "type": "string" }
                    },
                    "required": ["table_name"],
                    "additionalProperties": false,
                },
            },
            {
                "name": "similarity_search",
                "description": "Run vector similarity search using <->.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "table": { "type": "string" },
                        "column": { "type": "string" },
                        "vector": {
                            "type": "array",
                            "items": { "type": "number" }
                        },
                        "k": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["table", "column", "vector"],
                    "additionalProperties": false,
                },
            },
            {
                "name": "reset",
                "description": "Reset the OpenAssay engine state to startup baseline.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": false,
                },
            }
        ]
    })
}

pub(crate) fn resources_templates_result() -> Value {
    json!({
        "resourceTemplates": [
            {
                "uriTemplate": "schema://table/{name}",
                "name": "Table schema",
                "description": "Schema for a specific table",
                "mimeType": "application/json",
            }
        ]
    })
}

pub(crate) fn tool_success(payload: Value) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": payload.to_string(),
        }]
    })
}

pub(crate) fn tool_error(message: String) -> Value {
    let payload = json!({ "error": message });
    json!({
        "content": [{
            "type": "text",
            "text": payload.to_string(),
        }],
        "isError": true,
    })
}

pub(crate) fn parse_args<T>(value: Value) -> Result<T, RpcError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| RpcError::invalid_params(error.to_string()))
}
