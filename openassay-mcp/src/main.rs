use std::time::Instant;

use openassay::parser::sql_parser::parse_statement;
use openassay::storage::tuple::ScalarValue;
use openassay::tcop::engine::{
    EngineStateSnapshot, QueryResult, execute_planned_query, plan_statement, restore_state,
    snapshot_state,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Number, Value, json};
use tokio::io::{self, AsyncBufReadExt, AsyncWriteExt, BufReader};

const JSONRPC_VERSION: &str = "2.0";
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl RpcError {
    fn parse_error(message: String) -> Self {
        Self {
            code: -32700,
            message,
            data: None,
        }
    }

    fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            code: -32600,
            message: message.into(),
            data: None,
        }
    }

    fn method_not_found(method: &str) -> Self {
        Self {
            code: -32601,
            message: format!("method not found: {method}"),
            data: None,
        }
    }

    fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct ToolsCallParams {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct QueryArgs {
    sql: String,
}

#[derive(Debug, Deserialize)]
struct LoadUrlArgs {
    url: String,
    #[serde(default)]
    json_path: Option<String>,
    #[serde(default)]
    table_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LoadIcebergArgs {
    path: String,
    #[serde(default)]
    table_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DescribeTableArgs {
    table_name: String,
}

#[derive(Debug, Deserialize)]
struct SimilaritySearchArgs {
    table: String,
    column: String,
    vector: Vec<f64>,
    #[serde(default = "default_k")]
    k: u64,
}

#[derive(Debug, Deserialize)]
struct ResourceReadParams {
    uri: String,
}

fn default_k() -> u64 {
    10
}

struct McpServer {
    baseline_snapshot: EngineStateSnapshot,
    next_table_id: u64,
}

impl McpServer {
    fn new() -> Self {
        Self::with_baseline(snapshot_state())
    }

    fn with_baseline(baseline_snapshot: EngineStateSnapshot) -> Self {
        Self {
            baseline_snapshot,
            next_table_id: 0,
        }
    }

    async fn execute_sql(&self, sql: &str) -> Result<QueryResult, String> {
        let statement = parse_statement(sql).map_err(|error| error.to_string())?;
        let plan = plan_statement(statement).map_err(|error| error.message)?;
        execute_planned_query(&plan, &[])
            .await
            .map_err(|error| error.message)
    }

    fn next_table_name(&mut self, prefix: &str) -> String {
        self.next_table_id = self.next_table_id.saturating_add(1);
        format!("{prefix}_{}", self.next_table_id)
    }

    async fn query_tool(&self, args: QueryArgs) -> Result<Value, String> {
        let start = Instant::now();
        let result = self.execute_sql(&args.sql).await?;
        let execution_time_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok(query_result_to_json(result, execution_time_ms))
    }

    async fn load_url_tool(&mut self, args: LoadUrlArgs) -> Result<Value, String> {
        let table_name = args
            .table_name
            .unwrap_or_else(|| self.next_table_name("url_data"));
        let quoted_table_name = quote_qualified_identifier(&table_name)?;

        let mut function_args = vec![quote_literal(&args.url)];
        for path_part in split_csv_paths(args.json_path.as_deref()) {
            function_args.push(quote_literal(&path_part));
        }

        let create_sql = format!(
            "CREATE TABLE {quoted_table_name} AS SELECT * FROM json_table({})",
            function_args.join(", ")
        );
        self.execute_sql(&create_sql).await?;

        let columns = self.table_columns(&table_name).await?;
        let row_count = self.table_row_count(&table_name).await?;

        Ok(json!({
            "tableName": table_name,
            "columns": columns,
            "rowCount": row_count,
        }))
    }

    async fn load_iceberg_tool(&mut self, args: LoadIcebergArgs) -> Result<Value, String> {
        let table_name = args
            .table_name
            .unwrap_or_else(|| self.next_table_name("iceberg_data"));
        let quoted_table_name = quote_qualified_identifier(&table_name)?;

        let create_sql = format!(
            "CREATE TABLE {quoted_table_name} AS SELECT * FROM iceberg_scan({})",
            quote_literal(&args.path)
        );
        self.execute_sql(&create_sql).await?;

        let columns = self.table_columns(&table_name).await?;
        let row_count = self.table_row_count(&table_name).await?;

        Ok(json!({
            "tableName": table_name,
            "columns": columns,
            "rowCount": row_count,
        }))
    }

    async fn list_tables_data(&self) -> Result<Value, String> {
        let sql = "SELECT t.table_name, COUNT(c.column_name) AS column_count \
                   FROM information_schema.tables t \
                   LEFT JOIN information_schema.columns c \
                     ON c.table_schema = t.table_schema AND c.table_name = t.table_name \
                   WHERE t.table_schema = 'public' \
                   GROUP BY t.table_name \
                   ORDER BY t.table_name";
        let result = self.execute_sql(sql).await?;

        let mut tables = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            if row.len() != 2 {
                return Err("unexpected row shape while listing tables".to_string());
            }
            let table_name = scalar_to_string(&row[0])
                .ok_or_else(|| "invalid table name returned from information_schema".to_string())?;
            let column_count = scalar_to_u64(&row[1]).ok_or_else(|| {
                "invalid column count returned from information_schema".to_string()
            })?;
            tables.push(json!({
                "name": table_name,
                "columnCount": column_count,
            }));
        }

        Ok(Value::Array(tables))
    }

    async fn describe_table_data(&self, table_name: &str) -> Result<Value, String> {
        let (schema, table) = split_table_reference(table_name)?;
        let sql = format!(
            "SELECT column_name, data_type \
             FROM information_schema.columns \
             WHERE table_schema = {} AND table_name = {} \
             ORDER BY ordinal_position",
            quote_literal(&schema),
            quote_literal(&table)
        );

        let result = self.execute_sql(&sql).await?;
        if result.rows.is_empty() {
            return Err(format!("table not found: {table_name}"));
        }

        let mut columns = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            if row.len() != 2 {
                return Err("unexpected row shape while describing table".to_string());
            }
            let column_name = scalar_to_string(&row[0]).ok_or_else(|| {
                "invalid column name returned from information_schema".to_string()
            })?;
            let data_type = scalar_to_string(&row[1]).ok_or_else(|| {
                "invalid column type returned from information_schema".to_string()
            })?;
            columns.push(json!({
                "name": column_name,
                "type": data_type,
            }));
        }

        Ok(json!({ "columns": columns }))
    }

    async fn similarity_search_tool(&self, args: SimilaritySearchArgs) -> Result<Value, String> {
        if args.k == 0 {
            return Err("k must be greater than 0".to_string());
        }

        self.execute_sql("CREATE EXTENSION IF NOT EXISTS vector")
            .await?;

        let quoted_table = quote_qualified_identifier(&args.table)?;
        let quoted_column = quote_identifier(&args.column)?;
        let vector_literal = vector_literal(&args.vector)?;

        let sql = format!(
            "SELECT * FROM {quoted_table} ORDER BY {quoted_column} <-> {} LIMIT {}",
            quote_literal(&vector_literal),
            args.k
        );

        let result = self.execute_sql(&sql).await?;
        let rows = result
            .rows
            .iter()
            .map(|row| Value::Array(row.iter().map(scalar_to_json).collect()))
            .collect::<Vec<_>>();

        Ok(json!({
            "columns": result.columns,
            "rows": rows,
        }))
    }

    async fn reset_tool(&mut self) -> Result<Value, String> {
        restore_state(self.baseline_snapshot.clone());
        self.next_table_id = 0;
        for extension in ["vector", "http", "ws", "uuid-ossp", "pgcrypto"] {
            let sql = format!("DROP EXTENSION IF EXISTS {}", quote_identifier(extension)?);
            self.execute_sql(&sql).await?;
        }
        Ok(json!({ "status": "ok" }))
    }

    async fn table_columns(&self, table_name: &str) -> Result<Vec<String>, String> {
        let (schema, table) = split_table_reference(table_name)?;
        let sql = format!(
            "SELECT column_name \
             FROM information_schema.columns \
             WHERE table_schema = {} AND table_name = {} \
             ORDER BY ordinal_position",
            quote_literal(&schema),
            quote_literal(&table)
        );

        let result = self.execute_sql(&sql).await?;
        let mut columns = Vec::with_capacity(result.rows.len());
        for row in result.rows {
            if row.len() != 1 {
                return Err("unexpected row shape while reading columns".to_string());
            }
            let column_name = scalar_to_string(&row[0]).ok_or_else(|| {
                "invalid column name returned from information_schema".to_string()
            })?;
            columns.push(column_name);
        }
        Ok(columns)
    }

    async fn table_row_count(&self, table_name: &str) -> Result<u64, String> {
        let quoted_table = quote_qualified_identifier(table_name)?;
        let sql = format!("SELECT count(*) AS row_count FROM {quoted_table}");
        let result = self.execute_sql(&sql).await?;

        let Some(first_row) = result.rows.first() else {
            return Err("count query returned no rows".to_string());
        };
        let Some(first_value) = first_row.first() else {
            return Err("count query returned no value".to_string());
        };
        scalar_to_u64(first_value)
            .ok_or_else(|| "count query returned non-numeric value".to_string())
    }

    async fn resources_list_result(&self) -> Result<Value, String> {
        let mut resources = vec![json!({
            "uri": "schema://tables",
            "name": "Tables",
            "description": "List all user tables in the public schema",
            "mimeType": "application/json",
        })];

        let tables = self.list_tables_data().await?;
        if let Value::Array(entries) = tables {
            for entry in entries {
                let Some(name) = entry
                    .as_object()
                    .and_then(|obj| obj.get("name"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                resources.push(json!({
                    "uri": format!("schema://table/{name}"),
                    "name": format!("Table {name}"),
                    "description": format!("Column schema for table {name}"),
                    "mimeType": "application/json",
                }));
            }
        }

        Ok(json!({ "resources": resources }))
    }

    async fn resources_read_result(&self, params: Value) -> Result<Value, RpcError> {
        let args: ResourceReadParams = parse_args(params)?;

        if args.uri == "schema://tables" {
            let payload = self
                .list_tables_data()
                .await
                .map_err(RpcError::invalid_params)?;
            return Ok(json!({
                "contents": [{
                    "uri": args.uri,
                    "mimeType": "application/json",
                    "text": payload.to_string(),
                }]
            }));
        }

        if let Some(table_name) = args.uri.strip_prefix("schema://table/") {
            if table_name.trim().is_empty() {
                return Err(RpcError::invalid_params(
                    "resource uri schema://table/{name} requires a table name",
                ));
            }
            let payload = self
                .describe_table_data(table_name)
                .await
                .map_err(RpcError::invalid_params)?;
            return Ok(json!({
                "contents": [{
                    "uri": args.uri,
                    "mimeType": "application/json",
                    "text": payload.to_string(),
                }]
            }));
        }

        Err(RpcError::invalid_params(format!(
            "unknown resource uri: {}",
            args.uri
        )))
    }

    async fn tools_call_result(&mut self, params: Value) -> Result<Value, RpcError> {
        let call: ToolsCallParams = parse_args(params)?;
        let arguments = call.arguments.unwrap_or_else(|| Value::Object(Map::new()));

        let payload_result = match call.name.as_str() {
            "query" => {
                let args: QueryArgs = parse_args(arguments)?;
                self.query_tool(args).await
            }
            "load_url" => {
                let args: LoadUrlArgs = parse_args(arguments)?;
                self.load_url_tool(args).await
            }
            "load_iceberg" => {
                let args: LoadIcebergArgs = parse_args(arguments)?;
                self.load_iceberg_tool(args).await
            }
            "list_tables" => self.list_tables_data().await,
            "describe_table" => {
                let args: DescribeTableArgs = parse_args(arguments)?;
                self.describe_table_data(&args.table_name).await
            }
            "similarity_search" => {
                let args: SimilaritySearchArgs = parse_args(arguments)?;
                self.similarity_search_tool(args).await
            }
            "reset" => self.reset_tool().await,
            other => {
                return Err(RpcError::invalid_params(format!(
                    "unknown tool name: {other}"
                )));
            }
        };

        Ok(match payload_result {
            Ok(payload) => tool_success(payload),
            Err(message) => tool_error(message),
        })
    }

    async fn handle_method(
        &mut self,
        method: &str,
        params: Value,
    ) -> Result<Option<Value>, RpcError> {
        match method {
            "initialize" => Ok(Some(initialize_result())),
            "initialized" | "notifications/initialized" => Ok(None),
            "tools/list" => Ok(Some(tools_list_result())),
            "tools/call" => Ok(Some(self.tools_call_result(params).await?)),
            "resources/list" => Ok(Some(
                self.resources_list_result()
                    .await
                    .map_err(RpcError::invalid_params)?,
            )),
            "resources/templates/list" => Ok(Some(resources_templates_result())),
            "resources/read" => Ok(Some(self.resources_read_result(params).await?)),
            "ping" => Ok(Some(json!({}))),
            other => Err(RpcError::method_not_found(other)),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut server = McpServer::new();
    let mut lines = BufReader::new(io::stdin()).lines();
    let mut stdout = io::stdout();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(error) => {
                eprintln!("stdin read error: {error}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let response = process_line(&mut server, &line).await;
        if let Some(payload) = response {
            let encoded = payload.to_string();
            if let Err(error) = stdout.write_all(encoded.as_bytes()).await {
                eprintln!("stdout write error: {error}");
                break;
            }
            if let Err(error) = stdout.write_all(b"\n").await {
                eprintln!("stdout write error: {error}");
                break;
            }
            if let Err(error) = stdout.flush().await {
                eprintln!("stdout flush error: {error}");
                break;
            }
        }
    }
}

async fn process_line(server: &mut McpServer, line: &str) -> Option<Value> {
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
                eprintln!("notification error for method {method}: {}", error.message);
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

fn initialize_result() -> Value {
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

fn tools_list_result() -> Value {
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

fn resources_templates_result() -> Value {
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

fn tool_success(payload: Value) -> Value {
    json!({
        "content": [{
            "type": "text",
            "text": payload.to_string(),
        }]
    })
}

fn tool_error(message: String) -> Value {
    let payload = json!({ "error": message });
    json!({
        "content": [{
            "type": "text",
            "text": payload.to_string(),
        }],
        "isError": true,
    })
}

fn parse_args<T>(value: Value) -> Result<T, RpcError>
where
    T: DeserializeOwned,
{
    serde_json::from_value(value).map_err(|error| RpcError::invalid_params(error.to_string()))
}

fn split_table_reference(input: &str) -> Result<(String, String), String> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err("table name cannot be empty".to_string());
    }

    let parts = trimmed.split('.').map(str::trim).collect::<Vec<_>>();
    match parts.as_slice() {
        [table] if !table.is_empty() => Ok(("public".to_string(), (*table).to_string())),
        [schema, table] if !schema.is_empty() && !table.is_empty() => {
            Ok(((*schema).to_string(), (*table).to_string()))
        }
        _ => Err(format!(
            "invalid table reference '{trimmed}' (use table or schema.table)"
        )),
    }
}

fn quote_identifier(identifier: &str) -> Result<String, String> {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return Err("identifier cannot be empty".to_string());
    }
    Ok(format!("\"{}\"", trimmed.replace('"', "\"\"")))
}

fn quote_qualified_identifier(identifier: &str) -> Result<String, String> {
    let trimmed = identifier.trim();
    let parts = trimmed.split('.').map(str::trim).collect::<Vec<_>>();
    match parts.as_slice() {
        [single] if !single.is_empty() => quote_identifier(single),
        [schema, name] if !schema.is_empty() && !name.is_empty() => Ok(format!(
            "{}.{}",
            quote_identifier(schema)?,
            quote_identifier(name)?
        )),
        _ => Err(format!(
            "invalid identifier '{trimmed}' (use name or schema.name)"
        )),
    }
}

fn quote_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn split_csv_paths(path: Option<&str>) -> Vec<String> {
    let Some(path) = path else {
        return Vec::new();
    };
    path.split(',')
        .map(str::trim)
        .filter(|segment| !segment.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn vector_literal(values: &[f64]) -> Result<String, String> {
    if values.iter().any(|value| !value.is_finite()) {
        return Err("vector values must be finite numbers".to_string());
    }
    serde_json::to_string(values).map_err(|error| error.to_string())
}

fn query_result_to_json(result: QueryResult, execution_time_ms: u64) -> Value {
    let row_count = if result.columns.is_empty() {
        result.rows_affected
    } else {
        u64::try_from(result.rows.len()).unwrap_or(u64::MAX)
    };

    let rows = result
        .rows
        .iter()
        .map(|row| Value::Array(row.iter().map(scalar_to_json).collect()))
        .collect::<Vec<_>>();

    json!({
        "columns": result.columns,
        "rows": rows,
        "rowCount": row_count,
        "executionTimeMs": execution_time_ms,
    })
}

fn scalar_to_json(value: &ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Bool(boolean) => Value::Bool(*boolean),
        ScalarValue::Int(int) => Value::Number(Number::from(*int)),
        ScalarValue::Float(float) => Number::from_f64(*float)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(float.to_string())),
        ScalarValue::Numeric(decimal) => Value::String(decimal.to_string()),
        ScalarValue::Text(text) => Value::String(text.clone()),
        ScalarValue::Array(values) | ScalarValue::Record(values) => {
            Value::Array(values.iter().map(scalar_to_json).collect())
        }
        ScalarValue::Vector(values) => Value::Array(
            values
                .iter()
                .map(|value| {
                    Number::from_f64(f64::from(*value))
                        .map(Value::Number)
                        .unwrap_or_else(|| Value::String(value.to_string()))
                })
                .collect(),
        ),
    }
}

fn scalar_to_string(value: &ScalarValue) -> Option<String> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Bool(boolean) => Some(boolean.to_string()),
        ScalarValue::Int(int) => Some(int.to_string()),
        ScalarValue::Float(float) => Some(float.to_string()),
        ScalarValue::Numeric(decimal) => Some(decimal.to_string()),
        ScalarValue::Text(text) => Some(text.clone()),
        ScalarValue::Array(_) | ScalarValue::Record(_) | ScalarValue::Vector(_) => {
            Some(value.render())
        }
    }
}

fn scalar_to_u64(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::Int(int) if *int >= 0 => u64::try_from(*int).ok(),
        ScalarValue::Float(float) if *float >= 0.0 && float.is_finite() => {
            Some((*float).round() as u64)
        }
        ScalarValue::Numeric(decimal) => decimal.to_string().parse::<u64>().ok(),
        ScalarValue::Text(text) => text.parse::<u64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn test_mutex() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    async fn drop_public_tables() {
        let server = McpServer::new();
        let tables = server
            .list_tables_data()
            .await
            .unwrap_or_else(|_| Value::Array(Vec::new()));
        let Value::Array(entries) = tables else {
            return;
        };

        for entry in entries {
            let Some(table_name) = entry
                .as_object()
                .and_then(|obj| obj.get("name"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            let sql = format!(
                "DROP TABLE IF EXISTS {} CASCADE",
                quote_qualified_identifier(table_name)
                    .expect("table name from information_schema should be valid")
            );
            let _ = server.execute_sql(&sql).await;
        }
    }

    fn decode_tool_payload(result: &Value) -> Value {
        let text = result
            .get("content")
            .and_then(Value::as_array)
            .and_then(|items| items.first())
            .and_then(|item| item.get("text"))
            .and_then(Value::as_str)
            .expect("tool response should contain text payload");
        serde_json::from_str(text).expect("tool payload should be valid JSON")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn query_tool_executes_sql() {
        let _guard = test_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop_public_tables().await;

        let mut server = McpServer::with_baseline(snapshot_state());
        let result = server
            .tools_call_result(json!({
                "name": "query",
                "arguments": {
                    "sql": "SELECT 1 + 1 AS result"
                }
            }))
            .await
            .expect("query tool should return success envelope");

        let payload = decode_tool_payload(&result);
        assert_eq!(payload["columns"], json!(["result"]));
        assert_eq!(payload["rows"], json!([[2]]));
        assert_eq!(payload["rowCount"], json!(1));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_describe_similarity_and_reset_tools_work() {
        let _guard = test_mutex()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        drop_public_tables().await;

        let mut server = McpServer::with_baseline(snapshot_state());
        let _ = server
            .execute_sql("CREATE TABLE items (id int8, emb vector(2))")
            .await
            .expect("table should be created");
        let _ = server
            .execute_sql("INSERT INTO items VALUES (1, '[1,1]'), (2, '[5,5]'), (3, '[2,2]')")
            .await
            .expect("rows should be inserted");

        let list_result = server
            .tools_call_result(json!({
                "name": "list_tables",
                "arguments": {}
            }))
            .await
            .expect("list_tables should return success envelope");
        let list_payload = decode_tool_payload(&list_result);
        assert!(
            list_payload
                .as_array()
                .expect("list_tables payload should be an array")
                .iter()
                .any(|entry| entry["name"] == "items" && entry["columnCount"] == 2)
        );

        let describe_result = server
            .tools_call_result(json!({
                "name": "describe_table",
                "arguments": {
                    "table_name": "items"
                }
            }))
            .await
            .expect("describe_table should return success envelope");
        let describe_payload = decode_tool_payload(&describe_result);
        assert_eq!(
            describe_payload["columns"],
            json!([
                {"name": "id", "type": "bigint"},
                {"name": "emb", "type": "vector"}
            ])
        );

        let similarity_result = server
            .tools_call_result(json!({
                "name": "similarity_search",
                "arguments": {
                    "table": "items",
                    "column": "emb",
                    "vector": [1.0, 1.0],
                    "k": 2
                }
            }))
            .await
            .expect("similarity_search should return success envelope");
        let similarity_payload = decode_tool_payload(&similarity_result);
        assert_eq!(similarity_payload["columns"], json!(["id", "emb"]));
        assert_eq!(similarity_payload["rows"][0][0], json!(1));
        assert_eq!(similarity_payload["rows"].as_array().map(Vec::len), Some(2));

        let reset_result = server
            .tools_call_result(json!({
                "name": "reset",
                "arguments": {}
            }))
            .await
            .expect("reset should return success envelope");
        let reset_payload = decode_tool_payload(&reset_result);
        assert_eq!(reset_payload, json!({ "status": "ok" }));

        let tables_after_reset = server
            .list_tables_data()
            .await
            .expect("list tables should succeed after reset");
        assert_eq!(tables_after_reset, json!([]));
    }
}
