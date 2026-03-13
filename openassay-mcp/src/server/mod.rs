use std::time::Instant;

use openassay::parser::sql_parser::parse_statement;
use openassay::tcop::engine::{
    EngineStateSnapshot, QueryResult, execute_planned_query, plan_statement, restore_state,
    snapshot_state,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::helpers::{
    query_result_to_json, quote_identifier, quote_literal, quote_qualified_identifier,
    scalar_to_json, scalar_to_string, scalar_to_u64, split_csv_paths, split_table_reference,
    vector_literal,
};
use crate::rpc::{
    RpcError, initialize_result, parse_args, resources_templates_result, tool_error, tool_success,
    tools_list_result,
};

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

pub(crate) struct McpServer {
    baseline_snapshot: EngineStateSnapshot,
    next_table_id: u64,
}

impl McpServer {
    pub(crate) fn new() -> Self {
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
        let vector_value = vector_literal(&args.vector)?;

        let sql = format!(
            "SELECT * FROM {quoted_table} ORDER BY {quoted_column} <-> {} LIMIT {}",
            quote_literal(&vector_value),
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

    pub(crate) async fn handle_method(
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

#[cfg(test)]
mod tests;
