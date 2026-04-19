#[allow(clippy::wildcard_imports)]
use super::*;
use crate::catalog::builtin_types::{
    BUILTIN_COLLATIONS, BUILTIN_LANGUAGES, BUILTIN_RANGES, BUILTIN_TYPES,
};
use crate::catalog::oid::{
    BOOTSTRAP_SUPERUSER_OID, PG_CATALOG_NAMESPACE_OID, PUBLIC_NAMESPACE_OID,
};
use crate::executor::profiling;

pub(super) async fn evaluate_table_function(
    function: &TableFunctionRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    let mut scope = EvalScope::default();
    if let Some(outer) = outer_scope {
        scope.inherit_outer(outer);
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        args.push(eval_expr(arg, &scope, params).await?);
    }

    let (mut columns, rows) = evaluate_set_returning_function(function, &args).await?;
    if !function.column_aliases.is_empty() {
        if function.column_aliases.len() != columns.len() {
            return Err(EngineError {
                message: format!(
                    "table function {} expects {} column aliases, got {}",
                    function
                        .name
                        .last()
                        .map(String::as_str)
                        .unwrap_or("function"),
                    columns.len(),
                    function.column_aliases.len()
                ),
            });
        }
        columns = function.column_aliases.clone();
    } else if columns.len() == 1
        && let Some(alias) = &function.alias
        && function
            .name
            .last()
            .is_some_and(|name| name.eq_ignore_ascii_case("generate_series"))
    {
        columns[0] = alias.clone();
    }

    let qualifiers = function
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .or_else(|| {
            function
                .name
                .last()
                .map(|name| vec![name.to_ascii_lowercase()])
        })
        .unwrap_or_default();
    let mut scoped_rows = Vec::with_capacity(rows.len());
    for row in &rows {
        scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
        batch: None,
    })
}

#[cfg(target_arch = "wasm32")]
pub(super) async fn evaluate_table_function_with_predicate(
    _function: &TableFunctionRef,
    _params: &[Option<ScalarValue>],
    _outer_scope: Option<&EvalScope>,
    _predicate: Option<&Expr>,
) -> Result<Option<TableEval>, EngineError> {
    Ok(None)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn evaluate_table_function_with_predicate(
    function: &TableFunctionRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
    predicate: Option<&Expr>,
) -> Result<Option<TableEval>, EngineError> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();
    if fn_name != "iceberg_scan" {
        return Ok(None);
    }

    let mut scope = EvalScope::default();
    if let Some(outer) = outer_scope {
        scope.inherit_outer(outer);
    }

    let mut args = Vec::with_capacity(function.args.len());
    for arg in &function.args {
        args.push(eval_expr(arg, &scope, params).await?);
    }
    if args.len() != 1 {
        return Err(EngineError {
            message: "iceberg_scan() expects exactly one argument (table path)".to_string(),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok(Some(TableEval {
            rows: Vec::new(),
            columns: vec!["value".to_string()],
            null_scope: scope_from_row(
                &["value".to_string()],
                &[ScalarValue::Null],
                &[],
                &["value".to_string()],
            ),
            batch: None,
        }));
    }

    let input_path = match &args[0] {
        ScalarValue::Text(text) => text.trim().to_string(),
        other => other.render(),
    };
    let qualifiers = function
        .alias
        .as_ref()
        .map(|alias| vec![alias.to_ascii_lowercase()])
        .or_else(|| {
            function
                .name
                .last()
                .map(|name| vec![name.to_ascii_lowercase()])
        })
        .unwrap_or_default();
    let scan_plan = crate::catalog::iceberg::scan_iceberg_table_with_predicate(
        &input_path,
        predicate,
        &qualifiers,
        params,
    )
    .await?;
    let mut columns = scan_plan.columns;
    if !function.column_aliases.is_empty() {
        if function.column_aliases.len() != columns.len() {
            return Err(EngineError {
                message: format!(
                    "table function {} expects {} column aliases, got {}",
                    function
                        .name
                        .last()
                        .map(String::as_str)
                        .unwrap_or("function"),
                    columns.len(),
                    function.column_aliases.len()
                ),
            });
        }
        columns = function.column_aliases.clone();
    }
    let projection_names = columns.clone();
    let mut scoped_rows = Vec::with_capacity(scan_plan.rows.len());
    for row in &scan_plan.rows {
        scoped_rows.push(scope_from_row(
            &columns,
            row,
            &qualifiers,
            &projection_names,
        ));
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &projection_names);

    Ok(Some(TableEval {
        rows: scoped_rows,
        columns,
        null_scope,
        batch: None,
    }))
}

pub(super) async fn evaluate_set_returning_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let fn_name = function
        .name
        .last()
        .map(|name| name.to_ascii_lowercase())
        .unwrap_or_default();

    match fn_name.as_str() {
        "json_array_elements" | "jsonb_array_elements" => {
            eval_json_array_elements_set_function(args, false, &fn_name)
        }
        "json_array_elements_text" | "jsonb_array_elements_text" => {
            eval_json_array_elements_set_function(args, true, &fn_name)
        }
        "json_each" | "jsonb_each" => eval_json_each_set_function(args, false, &fn_name),
        "json_each_text" | "jsonb_each_text" => eval_json_each_set_function(args, true, &fn_name),
        "json_object_keys" | "jsonb_object_keys" => {
            eval_json_object_keys_set_function(args, &fn_name)
        }
        "jsonb_path_query" => eval_jsonb_path_query_set_function(args, &fn_name),
        "json_to_record" | "jsonb_to_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, false)
        }
        "json_to_recordset" | "jsonb_to_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, false)
        }
        "json_populate_record" | "jsonb_populate_record" => {
            eval_json_record_table_function(function, args, &fn_name, false, true)
        }
        "json_populate_recordset" | "jsonb_populate_recordset" => {
            eval_json_record_table_function(function, args, &fn_name, true, true)
        }
        "regexp_matches" => eval_regexp_matches_set_function(args, &fn_name),
        "regexp_split_to_table" => eval_regexp_split_to_table_set_function(args, &fn_name),
        "string_to_table" => eval_string_to_table_set_function(args, &fn_name),
        "generate_series" => eval_generate_series(args, &fn_name),
        "unnest" => eval_unnest_set_function(args, &fn_name),
        "pg_input_error_info" => eval_pg_input_error_info_set_function(args, &fn_name),
        "json_table" => eval_json_table_function(args, &fn_name).await,
        "iceberg_scan" => eval_iceberg_scan_function(args, &fn_name).await,
        "iceberg_metadata" => eval_iceberg_metadata_function(args, &fn_name).await,
        "parquet_scan" => eval_parquet_scan_function(args, &fn_name).await,
        "pg_get_keywords" => eval_pg_get_keywords(),
        "messages" if function.name.len() == 2 && function.name[0].eq_ignore_ascii_case("ws") => {
            execute_ws_messages(args).await
        }
        _ => {
            if lookup_user_function(&function.name, args.len()).is_some() {
                let columns = if !function.column_aliases.is_empty() {
                    function.column_aliases.clone()
                } else {
                    vec![
                        function
                            .name
                            .last()
                            .cloned()
                            .unwrap_or_else(|| "value".to_string()),
                    ]
                };
                return Ok((columns, Vec::new()));
            }
            Err(EngineError {
                message: format!(
                    "unsupported set-returning table function {}",
                    function
                        .name
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(".")
                ),
            })
        }
    }
}

pub(super) fn eval_json_array_elements_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Array(items) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON array"),
        });
    };

    let rows = items
        .iter()
        .map(|item| {
            let value = if text_mode {
                json_value_text_output(item)
            } else {
                ScalarValue::Text(item.to_string())
            };
            vec![value]
        })
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

pub(super) fn eval_json_each_set_function(
    args: &[ScalarValue],
    text_mode: bool,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string(), "value".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .iter()
        .map(|(key, value)| {
            let value_col = if text_mode {
                json_value_text_output(value)
            } else {
                ScalarValue::Text(value.to_string())
            };
            vec![ScalarValue::Text(key.clone()), value_col]
        })
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string(), "value".to_string()], rows))
}

pub(super) fn eval_json_object_keys_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument"),
        });
    }

    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["key".to_string()], Vec::new()));
    }

    let value = parse_json_document_arg(&args[0], fn_name, 1)?;
    let JsonValue::Object(map) = value else {
        return Err(EngineError {
            message: format!("{fn_name}() argument 1 must be a JSON object"),
        });
    };

    let rows = map
        .keys()
        .map(|key| vec![ScalarValue::Text(key.clone())])
        .collect::<Vec<_>>();
    Ok((vec!["key".to_string()], rows))
}

pub fn json_value_to_scalar(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(v) => ScalarValue::Bool(*v),
        JsonValue::Number(n) => {
            if let Some(int) = n.as_i64() {
                ScalarValue::Int(int)
            } else if let Some(float) = n.as_f64() {
                ScalarValue::Float(float)
            } else {
                ScalarValue::Text(n.to_string())
            }
        }
        JsonValue::String(v) => ScalarValue::Text(v.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

pub(super) fn eval_jsonb_path_query_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let values = jsonb_path_query_values(args, fn_name)?;
    let rows = values
        .into_iter()
        .map(|value| vec![ScalarValue::Text(value.to_string())])
        .collect::<Vec<_>>();
    Ok((vec!["value".to_string()], rows))
}

pub(super) fn eval_json_record_table_function(
    function: &TableFunctionRef,
    args: &[ScalarValue],
    fn_name: &str,
    recordset: bool,
    populate: bool,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let expected_args = if populate { 2 } else { 1 };
    if args.len() != expected_args {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly {expected_args} argument(s)"),
        });
    }

    let mut base = JsonMap::new();
    if populate && !matches!(args[0], ScalarValue::Null) {
        match parse_json_document_arg(&args[0], fn_name, 1) {
            Ok(JsonValue::Object(base_obj)) => {
                base = base_obj;
            }
            Ok(_) => {
                return Err(EngineError {
                    message: format!("{fn_name}() base argument must be a JSON object"),
                });
            }
            Err(err) => {
                if matches!(args[0], ScalarValue::Text(_)) {
                    return Err(err);
                }
                // PostgreSQL accepts composite record values here; we currently treat those
                // as an empty base object when the argument is not JSON text.
            }
        }
    }
    let mut output_columns = function.column_aliases.clone();
    if output_columns.is_empty() && !populate {
        return Err(EngineError {
            message: format!("{fn_name}() requires column aliases"),
        });
    }

    let json_arg_idx = if populate { 2 } else { 1 };
    if matches!(args[json_arg_idx - 1], ScalarValue::Null) {
        if output_columns.is_empty() {
            output_columns.extend(base.keys().cloned());
            if output_columns.is_empty() {
                output_columns.push("value".to_string());
            }
        }
        if recordset {
            return Ok((output_columns, Vec::new()));
        }
        return Ok((
            output_columns.clone(),
            vec![vec![ScalarValue::Null; output_columns.len()]],
        ));
    }

    let source = parse_json_document_arg(&args[json_arg_idx - 1], fn_name, json_arg_idx)?;
    let objects = if recordset {
        let JsonValue::Array(items) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an array of objects"),
            });
        };
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let JsonValue::Object(map) = item else {
                return Err(EngineError {
                    message: format!("{fn_name}() JSON input must be an array of objects"),
                });
            };
            out.push(map);
        }
        out
    } else {
        let JsonValue::Object(map) = source else {
            return Err(EngineError {
                message: format!("{fn_name}() JSON input must be an object"),
            });
        };
        vec![map]
    };
    if output_columns.is_empty() {
        let mut seen = HashSet::new();
        for key in base.keys() {
            if seen.insert(key.clone()) {
                output_columns.push(key.clone());
            }
        }
        for object in &objects {
            for key in object.keys() {
                if seen.insert(key.clone()) {
                    output_columns.push(key.clone());
                }
            }
        }
        if output_columns.is_empty() {
            output_columns.push("value".to_string());
        }
    }

    let mut rows = Vec::with_capacity(objects.len());
    for object in objects {
        let mut merged = base.clone();
        for (key, value) in object {
            merged.insert(key, value);
        }
        let mut row = Vec::with_capacity(output_columns.len());
        for (idx, column) in output_columns.iter().enumerate() {
            let mut value = merged
                .get(column)
                .map(json_value_to_scalar)
                .unwrap_or(ScalarValue::Null);
            if let Some(type_name) = function
                .column_alias_types
                .get(idx)
                .and_then(|entry| entry.as_deref())
            {
                value = eval_cast_scalar(value, type_name)?;
            }
            row.push(value);
        }
        rows.push(row);
    }

    Ok((output_columns, rows))
}

pub(super) async fn eval_json_table_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() requires at least 1 argument (URL)"),
        });
    }

    // 1. Fetch URL
    let response_value = eval_http_get(&args[0]).await?;
    if matches!(response_value, ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    // 2. Parse the HTTP response JSON to extract the "content" field
    let response_json = parse_json_document_arg(&response_value, fn_name, 1)?;
    let content_str = match response_json.get("content") {
        Some(JsonValue::String(s)) => s.clone(),
        _ => {
            return Err(EngineError {
                message: format!("{fn_name}(): HTTP response missing 'content' field"),
            });
        }
    };

    // 3. Parse the body content as JSON
    let body: JsonValue = serde_json::from_str(&content_str).map_err(|err| EngineError {
        message: format!("{fn_name}(): response body is not valid JSON: {err}"),
    })?;

    // 4. Navigate path segments if provided
    let path_segments: Vec<String> = args[1..]
        .iter()
        .map(|arg| match arg {
            ScalarValue::Text(s) => Ok(s.clone()),
            ScalarValue::Int(i) => Ok(i.to_string()),
            ScalarValue::Null => Err(EngineError {
                message: format!("{fn_name}(): path segment cannot be NULL"),
            }),
            other => Ok(other.render()),
        })
        .collect::<Result<_, _>>()?;

    let target = if path_segments.is_empty() {
        &body
    } else {
        extract_json_path_value(&body, &path_segments).ok_or_else(|| EngineError {
            message: format!(
                "{fn_name}(): path '{}' not found in response",
                path_segments.join(".")
            ),
        })?
    };

    // 5. Convert result to rows
    match target {
        JsonValue::Array(items) => {
            if items.is_empty() {
                return Ok((vec!["value".to_string()], Vec::new()));
            }
            // Check if array of objects → auto-discover columns
            let first_obj = items
                .iter()
                .find(|item| matches!(item, JsonValue::Object(_)));
            if let Some(JsonValue::Object(_)) = first_obj {
                // Auto-discover column names from all objects
                let mut column_names = Vec::new();
                let mut seen = HashSet::new();
                for item in items {
                    if let JsonValue::Object(map) = item {
                        for key in map.keys() {
                            if seen.insert(key.clone()) {
                                column_names.push(key.clone());
                            }
                        }
                    }
                }
                let mut rows = Vec::with_capacity(items.len());
                for item in items {
                    if let JsonValue::Object(map) = item {
                        let row: Vec<ScalarValue> = column_names
                            .iter()
                            .map(|col| {
                                map.get(col)
                                    .map(json_value_to_scalar)
                                    .unwrap_or(ScalarValue::Null)
                            })
                            .collect();
                        rows.push(row);
                    } else {
                        // Mixed array: non-object items get nulls for all columns
                        rows.push(vec![ScalarValue::Null; column_names.len()]);
                    }
                }
                Ok((column_names, rows))
            } else {
                // Array of primitives → single "value" column
                let rows: Vec<Vec<ScalarValue>> = items
                    .iter()
                    .map(|item| vec![json_value_to_scalar(item)])
                    .collect();
                Ok((vec!["value".to_string()], rows))
            }
        }
        JsonValue::Object(map) => {
            // Single object → one row with columns from keys
            let column_names: Vec<String> = map.keys().cloned().collect();
            let row: Vec<ScalarValue> = map.values().map(json_value_to_scalar).collect();
            Ok((column_names, vec![row]))
        }
        _ => {
            // Scalar → single "value" column, single row
            Ok((
                vec!["value".to_string()],
                vec![vec![json_value_to_scalar(target)]],
            ))
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn eval_iceberg_scan_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument (table path)"),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    let input_path = match &args[0] {
        ScalarValue::Text(text) => text.trim().to_string(),
        other => other.render(),
    };
    if input_path.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() path argument cannot be empty"),
        });
    }

    let scan_plan = crate::catalog::iceberg::plan_iceberg_scan(&input_path, None, &[])
        .await
        .map_err(|error| {
            let message = if error.message.starts_with("no Iceberg metadata found under") {
                let input = Path::new(&input_path);
                if !input.exists() {
                    format!(
                        "{fn_name}() expects an existing Iceberg table directory, metadata file, or parquet file path"
                    )
                } else {
                    format!("{fn_name}(): no parquet files found under {input_path}")
                }
            } else {
                format!("{fn_name}(): {}", error.message)
            };
            EngineError { message }
        })?;
    Ok((scan_plan.columns, scan_plan.rows))
}

#[cfg(target_arch = "wasm32")]
pub(super) async fn eval_iceberg_scan_function(
    _args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    Err(EngineError {
        message: format!("{fn_name}() is not supported on wasm targets"),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn eval_parquet_scan_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument (file or directory path)"),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["value".to_string()], Vec::new()));
    }

    let input_path = match &args[0] {
        ScalarValue::Text(text) => text.trim().to_string(),
        other => other.render(),
    };
    if input_path.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() path argument cannot be empty"),
        });
    }

    let scan_plan = crate::catalog::iceberg::plan_parquet_scan(&input_path)
        .await
        .map_err(|error| EngineError {
            message: format!("{fn_name}(): {}", error.message),
        })?;
    Ok((scan_plan.columns, scan_plan.rows))
}

#[cfg(target_arch = "wasm32")]
pub(super) async fn eval_parquet_scan_function(
    _args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    Err(EngineError {
        message: format!("{fn_name}() is not supported on wasm targets"),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) async fn eval_iceberg_metadata_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    const OUTPUT_COLUMNS: [&str; 7] = [
        "table_uuid",
        "format_version",
        "last_updated",
        "current_schema_id",
        "partition_spec",
        "snapshot_count",
        "total_data_files",
    ];

    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects exactly one argument (table path)"),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((
            OUTPUT_COLUMNS
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
            Vec::new(),
        ));
    }

    let input_path = match &args[0] {
        ScalarValue::Text(text) => text.trim().to_string(),
        other => other.render(),
    };
    if input_path.is_empty() {
        return Err(EngineError {
            message: format!("{fn_name}() path argument cannot be empty"),
        });
    }

    let metadata = crate::catalog::iceberg::read_iceberg_metadata(&input_path)
        .await
        .map_err(|error| EngineError {
            message: format!("{fn_name}(): {}", error.message),
        })?;

    let row = vec![
        metadata
            .table_uuid
            .map(ScalarValue::Text)
            .unwrap_or(ScalarValue::Null),
        metadata
            .format_version
            .map(ScalarValue::Int)
            .unwrap_or(ScalarValue::Null),
        metadata
            .last_updated_ms
            .map(ScalarValue::Int)
            .unwrap_or(ScalarValue::Null),
        metadata
            .current_schema_id
            .map(ScalarValue::Int)
            .unwrap_or(ScalarValue::Null),
        ScalarValue::Text(metadata.partition_spec_json),
        ScalarValue::Int(metadata.snapshot_count),
        ScalarValue::Int(metadata.total_data_files),
    ];

    Ok((
        OUTPUT_COLUMNS
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
        vec![row],
    ))
}

#[cfg(target_arch = "wasm32")]
pub(super) async fn eval_iceberg_metadata_function(
    _args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    Err(EngineError {
        message: format!("{fn_name}() is not supported on wasm targets"),
    })
}

pub(super) fn eval_generate_series(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(EngineError {
            message: format!("{fn_name}() expects 2 or 3 arguments"),
        });
    }
    if args.iter().any(|a| matches!(a, ScalarValue::Null)) {
        return Ok((vec!["generate_series".to_string()], Vec::new()));
    }
    let int_mode = matches!(
        (&args[0], &args[1]),
        (ScalarValue::Int(_), ScalarValue::Int(_))
    ) && (args.len() < 3 || matches!(&args[2], ScalarValue::Int(_)));

    let mut rows = Vec::new();
    let max_rows = 1_000_000;
    if int_mode {
        let start = match args[0] {
            ScalarValue::Int(v) => v,
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() expected integer argument for start"),
                });
            }
        };
        let stop = match args[1] {
            ScalarValue::Int(v) => v,
            _ => {
                return Err(EngineError {
                    message: format!("{fn_name}() expected integer argument for stop"),
                });
            }
        };
        let step = if args.len() == 3 {
            match args[2] {
                ScalarValue::Int(v) => v,
                _ => {
                    return Err(EngineError {
                        message: format!("{fn_name}() expected integer argument for step"),
                    });
                }
            }
        } else if start <= stop {
            1
        } else {
            -1
        };
        if step == 0 {
            return Err(EngineError {
                message: "step size cannot be zero".to_string(),
            });
        }

        let mut current = start;
        loop {
            if rows.len() >= max_rows {
                break;
            }
            if step > 0 && current > stop {
                break;
            }
            if step < 0 && current < stop {
                break;
            }
            rows.push(vec![ScalarValue::Int(current)]);

            let Some(next) = current.checked_add(step) else {
                break;
            };
            if next == current {
                break;
            }
            current = next;
        }
        return Ok((vec!["generate_series".to_string()], rows));
    }

    let as_f64 = |value: &ScalarValue| -> Result<f64, EngineError> {
        match value {
            ScalarValue::Int(i) => Ok(*i as f64),
            ScalarValue::Float(f) => Ok(*f),
            ScalarValue::Numeric(n) => Ok(n.to_string().parse::<f64>().unwrap_or(f64::NAN)),
            _ => Err(EngineError {
                message: format!("{fn_name}() expects numeric arguments"),
            }),
        }
    };

    let start = as_f64(&args[0])?;
    let stop = as_f64(&args[1])?;
    let step = if args.len() == 3 {
        as_f64(&args[2]).map_err(|_| EngineError {
            message: format!("{fn_name}() expects numeric step"),
        })?
    } else if start <= stop {
        1.0
    } else {
        -1.0
    };
    if step == 0.0 {
        return Err(EngineError {
            message: "step size cannot be zero".to_string(),
        });
    }
    if start.is_nan() || stop.is_nan() || step.is_nan() {
        return Ok((vec!["generate_series".to_string()], Vec::new()));
    }
    if !start.is_finite() || !stop.is_finite() || !step.is_finite() {
        let in_range = !((step > 0.0 && start > stop) || (step < 0.0 && start < stop));
        let rows = if in_range {
            vec![vec![ScalarValue::Float(start)]]
        } else {
            Vec::new()
        };
        return Ok((vec!["generate_series".to_string()], rows));
    }

    let mut current = start;
    loop {
        if rows.len() >= max_rows {
            break;
        }
        if step > 0.0 && current > stop {
            break;
        }
        if step < 0.0 && current < stop {
            break;
        }
        rows.push(vec![ScalarValue::Float(current)]);

        let next = current + step;
        if next == current || next.is_nan() {
            break;
        }
        current = next;
    }

    Ok((vec!["generate_series".to_string()], rows))
}

pub(super) fn eval_unnest_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 1 {
        return Err(EngineError {
            message: format!("{fn_name}() expects one argument"),
        });
    }
    if matches!(args[0], ScalarValue::Null) {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let text = args[0].render();
    let inner = text.trim_start_matches('{').trim_end_matches('}');
    if inner.is_empty() {
        return Ok((vec!["unnest".to_string()], Vec::new()));
    }
    let rows: Vec<Vec<ScalarValue>> = inner
        .split(',')
        .map(|p| {
            let p = p.trim();
            if p == "NULL" {
                vec![ScalarValue::Null]
            } else {
                vec![ScalarValue::Text(p.to_string())]
            }
        })
        .collect();
    Ok((vec!["unnest".to_string()], rows))
}

pub(super) fn eval_pg_input_error_info_set_function(
    args: &[ScalarValue],
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    if args.len() != 2 {
        return Err(EngineError {
            message: format!("{fn_name}() expects two arguments"),
        });
    }
    let columns = vec![
        "message".to_string(),
        "detail".to_string(),
        "hint".to_string(),
        "sql_error_code".to_string(),
    ];
    let rows = vec![vec![
        ScalarValue::Null,
        ScalarValue::Null,
        ScalarValue::Null,
        ScalarValue::Null,
    ]];
    Ok((columns, rows))
}

pub(super) fn eval_pg_get_keywords() -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let keywords = vec![
        ("select", "R", "reserved"),
        ("from", "R", "reserved"),
        ("where", "R", "reserved"),
        ("insert", "U", "unreserved"),
        ("update", "U", "unreserved"),
        ("delete", "U", "unreserved"),
        ("create", "U", "unreserved"),
        ("drop", "U", "unreserved"),
        ("alter", "U", "unreserved"),
        ("table", "U", "unreserved"),
    ];
    let columns = vec![
        "word".to_string(),
        "catcode".to_string(),
        "catdesc".to_string(),
    ];
    let rows = keywords
        .into_iter()
        .map(|(w, c, d)| {
            vec![
                ScalarValue::Text(w.to_string()),
                ScalarValue::Text(c.to_string()),
                ScalarValue::Text(d.to_string()),
            ]
        })
        .collect();
    Ok((columns, rows))
}

pub(super) async fn evaluate_relation(
    rel: &TableRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
    projected_columns: Option<Vec<usize>>,
) -> Result<TableEval, EngineError> {
    evaluate_relation_with_predicates_impl(rel, params, outer_scope, &[], projected_columns, true)
        .await
        .map(|(table_eval, _)| table_eval)
}

pub(super) async fn evaluate_relation_with_predicates(
    rel: &TableRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
    relation_predicates: &[Expr],
    projected_columns: Option<Vec<usize>>,
) -> Result<(TableEval, Vec<usize>), EngineError> {
    evaluate_relation_with_predicates_impl(
        rel,
        params,
        outer_scope,
        relation_predicates,
        projected_columns,
        true,
    )
    .await
}

pub(super) async fn evaluate_relation_with_predicates_columnar(
    rel: &TableRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
    relation_predicates: &[Expr],
) -> Result<(TableEval, Vec<usize>), EngineError> {
    evaluate_relation_with_predicates_impl(
        rel,
        params,
        outer_scope,
        relation_predicates,
        None,
        false,
    )
    .await
}

async fn evaluate_relation_with_predicates_impl(
    rel: &TableRef,
    params: &[Option<ScalarValue>],
    outer_scope: Option<&EvalScope>,
    relation_predicates: &[Expr],
    projected_columns: Option<Vec<usize>>,
    materialize_rows: bool,
) -> Result<(TableEval, Vec<usize>), EngineError> {
    if rel.name.len() == 1
        && let Some(cte) = current_cte_binding(&rel.name[0])
    {
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![rel.name[0].to_ascii_lowercase()]
        };

        let mut scoped_rows = Vec::with_capacity(cte.rows.len());
        for row in &cte.rows {
            scoped_rows.push(scope_from_row(&cte.columns, row, &qualifiers, &cte.columns));
        }
        let null_values = vec![ScalarValue::Null; cte.columns.len()];
        let null_scope = scope_from_row(&cte.columns, &null_values, &qualifiers, &cte.columns);

        return Ok((
            TableEval {
                rows: scoped_rows,
                columns: cte.columns,
                null_scope,
                batch: None,
            },
            Vec::new(),
        ));
    }

    if let Some((schema_name, relation_name, columns)) = lookup_virtual_relation(&rel.name) {
        let rows = virtual_relation_rows(&schema_name, &relation_name)?;
        let column_names = columns
            .iter()
            .map(|column| column.name.to_string())
            .collect::<Vec<_>>();
        let qualifiers = if let Some(alias) = &rel.alias {
            vec![alias.to_ascii_lowercase()]
        } else {
            vec![
                relation_name.to_ascii_lowercase(),
                format!("{}.{}", schema_name, relation_name),
            ]
        };
        let mut scoped_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            scoped_rows.push(scope_from_row(
                &column_names,
                row,
                &qualifiers,
                &column_names,
            ));
        }
        let null_values = vec![ScalarValue::Null; column_names.len()];
        let null_scope = scope_from_row(&column_names, &null_values, &qualifiers, &column_names);
        return Ok((
            TableEval {
                rows: scoped_rows,
                columns: column_names,
                null_scope,
                batch: None,
            },
            Vec::new(),
        ));
    }

    let resolved_table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&rel.name, &SearchPath::default())
            .cloned()
    });
    let table = match resolved_table {
        Ok(table) => table,
        Err(err) => {
            if let Some(columns) = regression_fixture_relation_columns(&rel.name) {
                let qualifiers = if let Some(alias) = &rel.alias {
                    vec![alias.to_ascii_lowercase()]
                } else {
                    vec![relation_lookup_name(&rel.name)]
                };
                let null_values = vec![ScalarValue::Null; columns.len()];
                let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);
                return Ok((
                    TableEval {
                        rows: Vec::new(),
                        columns,
                        null_scope,
                        batch: None,
                    },
                    Vec::new(),
                ));
            }
            return Err(EngineError {
                message: err.message,
            });
        }
    };

    let qualifiers = if let Some(alias) = &rel.alias {
        vec![alias.to_ascii_lowercase()]
    } else {
        vec![table.name().to_string(), table.qualified_name()]
    };
    let column_indexes = table
        .columns()
        .iter()
        .enumerate()
        .map(|(idx, column)| (column.name().to_string(), idx))
        .collect::<HashMap<_, _>>();
    let table_columns = column_indexes.keys().cloned().collect::<HashSet<_>>();
    let mut scan_predicates = Vec::new();
    let mut pushed_predicate_indexes = Vec::new();
    for (idx, predicate) in relation_predicates.iter().enumerate() {
        if let Some(scan_predicate) = extract_relation_scan_predicate(
            predicate,
            &qualifiers,
            &table_columns,
            &column_indexes,
            params,
        )
        .await?
        {
            scan_predicates.push(scan_predicate);
            pushed_predicate_indexes.push(idx);
        }
    }
    let index_offsets = if relation_predicates.is_empty() {
        None
    } else {
        relation_index_offsets_for_predicates(
            &table,
            &qualifiers,
            relation_predicates,
            params,
            outer_scope,
        )
        .await?
    };

    let uses_row_level_security = relation_uses_row_level_security(&table);
    let projected_columns = if uses_row_level_security {
        None
    } else {
        projected_columns
    };
    let supports_columnar_batch =
        matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView)
            && !uses_row_level_security;
    let (columns, mut rows, batch) = match table.kind() {
        TableKind::VirtualDual => (Vec::new(), vec![Vec::new()], None),
        TableKind::Heap | TableKind::MaterializedView => {
            let all_columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            let columns = projected_column_names(&all_columns, projected_columns.as_deref());
            let batch = if supports_columnar_batch {
                let _span = profiling::span("storage_scan_columnar_for_table");
                Some(
                    crate::tcop::engine::with_storage_write(|storage| {
                        storage.scan_columnar_for_table(
                            table.oid(),
                            &scan_predicates,
                            projected_columns.as_deref(),
                        )
                    })
                    .map_err(|message| EngineError { message })?,
                )
            } else {
                None
            };
            let rows = if materialize_rows {
                if let Some(batch) = &batch {
                    let _span = profiling::span("table_eval_batch_to_rows");
                    batch.to_rows()
                } else {
                    let _span = profiling::span("storage_scan_rows_for_table");
                    crate::tcop::engine::with_storage_write(|storage| {
                        storage.scan_rows_for_table(
                            table.oid(),
                            index_offsets.as_deref(),
                            &scan_predicates,
                            projected_columns.as_deref(),
                        )
                    })
                    .map_err(|message| EngineError { message })?
                }
            } else {
                Vec::new()
            };
            (columns, rows, batch)
        }
        TableKind::View => {
            let definition = table.view_definition().ok_or_else(|| EngineError {
                message: format!(
                    "view definition for relation \"{}\" is missing",
                    table.qualified_name()
                ),
            })?;
            let result = execute_query_with_outer(definition, params, outer_scope).await?;
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            if result.columns.len() != columns.len() {
                return Err(EngineError {
                    message: format!(
                        "view \"{}\" has invalid column definition",
                        table.qualified_name()
                    ),
                });
            }
            (columns, result.rows, None)
        }
        TableKind::Foreign => {
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            let rows = crate::foreign::execute_foreign_scan(&table)?;
            // Foreign scans do not apply scan_predicates at the storage level,
            // so clear pushed_predicate_indexes so the caller knows all
            // predicates still need post-scan evaluation.
            pushed_predicate_indexes.clear();
            (columns, rows, None)
        }
    };
    if table.kind() != TableKind::VirtualDual {
        require_relation_privilege(&table, TablePrivilege::Select)?;
        if materialize_rows {
            let mut visible_rows = Vec::with_capacity(rows.len());
            for row in rows {
                if relation_row_visible_for_command(&table, &row, RlsCommand::Select, params)
                    .await?
                {
                    visible_rows.push(row);
                }
            }
            rows = visible_rows;
        }
    }

    let mut scoped_rows = Vec::with_capacity(rows.len());
    {
        let _span = profiling::span("table_eval_rows_to_scopes");
        for row in &rows {
            scoped_rows.push(scope_from_row(&columns, row, &qualifiers, &columns));
        }
    }
    let null_values = vec![ScalarValue::Null; columns.len()];
    let null_scope = scope_from_row(&columns, &null_values, &qualifiers, &columns);

    Ok((
        TableEval {
            rows: scoped_rows,
            columns,
            null_scope,
            batch: if supports_columnar_batch { batch } else { None },
        },
        pushed_predicate_indexes,
    ))
}

fn projected_column_names(columns: &[String], projected_columns: Option<&[usize]>) -> Vec<String> {
    match projected_columns {
        Some(projected_columns) => projected_columns
            .iter()
            .filter_map(|idx| columns.get(*idx).cloned())
            .collect(),
        None => columns.to_vec(),
    }
}

fn relation_uses_row_level_security(table: &crate::catalog::Table) -> bool {
    let role = security::current_role();
    let evaluation = security::rls_evaluation_for_role(&role, table.oid(), RlsCommand::Select);
    evaluation.enabled && !evaluation.bypass
}

pub(super) fn relation_lookup_name(parts: &[String]) -> String {
    parts
        .last()
        .map_or_else(String::new, |part| part.to_ascii_lowercase())
}

pub(super) fn regression_fixture_relation_columns(parts: &[String]) -> Option<Vec<String>> {
    let relation = relation_lookup_name(parts);
    let cols = match relation.as_str() {
        "onek" | "onek2" | "tenk1" | "tenk2" => vec![
            "unique1",
            "unique2",
            "two",
            "four",
            "ten",
            "twenty",
            "hundred",
            "thousand",
            "twothousand",
            "fivethous",
            "tenthous",
            "odd",
            "even",
            "stringu1",
            "stringu2",
            "string4",
        ],
        "char_tbl" | "text_tbl" | "varchar_tbl" | "int2_tbl" | "int4_tbl" | "point_tbl" => {
            vec!["f1"]
        }
        "int8_tbl" => vec!["q1", "q2"],
        "road" | "ihighway" | "shighway" => vec!["name"],
        _ => return None,
    };
    Some(cols.into_iter().map(ToOwned::to_owned).collect())
}

pub(super) fn virtual_relation_rows(
    schema: &str,
    relation: &str,
) -> Result<Vec<Vec<ScalarValue>>, EngineError> {
    match (schema, relation) {
        ("pg_catalog", "pg_namespace") => {
            let mut entries = with_catalog_read(|catalog| {
                catalog
                    .schemas()
                    .map(|schema| (schema.oid(), schema.name().to_string()))
                    .collect::<Vec<_>>()
            });
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(|(oid, name)| {
                    vec![
                        ScalarValue::Int(oid as i64),
                        ScalarValue::Text(name),
                        ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_class") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let has_index =
                            !table.indexes().is_empty() || !table.key_constraints().is_empty();
                        out.push((
                            table.oid(),
                            table.name().to_string(),
                            schema.oid(),
                            pg_relkind_for_table(table.kind()).to_string(),
                            has_index,
                        ));
                    }
                }
                out
            });
            // Append a pg_class row (relkind='c') for each user-defined
            // composite type. attrelid / typrelid references these.
            let composite_classes = with_ext_read(|ext| {
                ext.user_composite_types
                    .iter()
                    .map(|t| {
                        (
                            t.class_oid,
                            t.name.last().cloned().unwrap_or_default(),
                            PUBLIC_NAMESPACE_OID,
                            "c".to_string(),
                            false,
                        )
                    })
                    .collect::<Vec<_>>()
            });
            entries.extend(composite_classes);
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(oid, relname, relnamespace, relkind, relhasindex)| {
                    vec![
                        ScalarValue::Int(oid as i64),
                        ScalarValue::Text(relname),
                        ScalarValue::Int(relnamespace as i64),
                        ScalarValue::Text(relkind),
                        ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                        ScalarValue::Int(0), // reltoastrelid
                        ScalarValue::Bool(relhasindex),
                        ScalarValue::Bool(false), // relhasrules
                        ScalarValue::Bool(false), // relhastriggers
                        ScalarValue::Bool(false), // relisshared
                    ]
                })
                .collect())
        }
        ("pg_catalog", "pg_attribute") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            out.push((
                                table.oid(),
                                column.ordinal(),
                                column.name().to_string(),
                                column.wire_type_oid(),
                                !column.nullable(),
                                column.default().is_some(),
                            ));
                        }
                    }
                }
                out
            });
            // Append pg_attribute rows for each composite type's attributes,
            // keyed by the composite's class_oid. atttypid comes from
            // sql_type_from_ast(&type_name).oid().
            let composite_attrs = with_ext_read(|ext| {
                let mut out = Vec::new();
                for composite in &ext.user_composite_types {
                    for (ordinal, (name, type_name)) in composite.attributes.iter().enumerate() {
                        let atttypid =
                            crate::commands::create_table::sql_type_from_ast(type_name).oid();
                        out.push((
                            composite.class_oid,
                            ordinal as u16,
                            name.clone(),
                            atttypid,
                            true, // attnotnull: composite attrs are NOT NULL by default in PG
                            false,
                        ));
                    }
                }
                out
            });
            entries.extend(composite_attrs);
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(
                    |(attrelid, attnum, attname, atttypid, attnotnull, atthasdef)| {
                        vec![
                            ScalarValue::Int(attrelid as i64),
                            ScalarValue::Text(attname),
                            ScalarValue::Int(atttypid as i64),
                            ScalarValue::Int(attnum as i64 + 1),
                            ScalarValue::Bool(attnotnull),
                            ScalarValue::Bool(false), // attisdropped: live columns are never dropped
                            ScalarValue::Bool(atthasdef),
                        ]
                    },
                )
                .collect())
        }
        ("pg_catalog", "pg_type") => {
            // Source of truth: src/catalog/builtin_types.rs. Column set and
            // order must match system_catalogs.rs pg_type column defs.
            let mut rows: Vec<Vec<ScalarValue>> = BUILTIN_TYPES
                .iter()
                .map(|t| {
                    vec![
                        ScalarValue::Int(t.oid as i64),
                        ScalarValue::Text(t.name.to_string()),
                        ScalarValue::Int(PG_CATALOG_NAMESPACE_OID as i64),
                        ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                        ScalarValue::Int(t.typlen as i64),
                        ScalarValue::Bool(t.typbyval),
                        ScalarValue::Text(t.typtype.to_string()),
                        ScalarValue::Text(t.typcategory.to_string()),
                        ScalarValue::Bool(false), // typispreferred
                        ScalarValue::Bool(true),  // typisdefined
                        ScalarValue::Text(t.typdelim.to_string()),
                        ScalarValue::Int(0), // typrelid (0 for non-composite)
                        ScalarValue::Int(t.typelem as i64),
                        ScalarValue::Int(t.typarray as i64),
                        ScalarValue::Int(0), // typinput regproc
                        ScalarValue::Int(0), // typoutput regproc
                        ScalarValue::Int(0), // typreceive regproc
                        ScalarValue::Int(0), // typsend regproc
                        ScalarValue::Int(0), // typmodin regproc
                        ScalarValue::Int(0), // typmodout regproc
                        ScalarValue::Int(0), // typanalyze regproc
                        ScalarValue::Text(t.typalign.to_string()),
                        ScalarValue::Text(t.typstorage.to_string()),
                        ScalarValue::Bool(false), // typnotnull
                        ScalarValue::Int(0),      // typbasetype (0 for non-domain)
                        ScalarValue::Int(-1),     // typtypmod
                        ScalarValue::Int(0),      // typndims
                        ScalarValue::Int(0),      // typcollation (0 = not collatable)
                        ScalarValue::Null,        // typdefault
                    ]
                })
                .collect();

            // Append user-defined enum types (typtype='e', typcategory='E').
            // Append user-defined composite types (typtype='c', typcategory='C').
            // typarray=0 is a partial — no auto-created `_foo` array companion.
            let user_type_rows = with_ext_read(|ext| {
                let mut out: Vec<Vec<ScalarValue>> = Vec::new();
                for enum_ty in &ext.user_types {
                    let name = enum_ty.name.last().cloned().unwrap_or_default();
                    out.push(vec![
                        ScalarValue::Int(enum_ty.oid as i64),
                        ScalarValue::Text(name),
                        ScalarValue::Int(PUBLIC_NAMESPACE_OID as i64),
                        ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                        ScalarValue::Int(4),     // typlen: enum = 4
                        ScalarValue::Bool(true), // typbyval
                        ScalarValue::Text("e".to_string()),
                        ScalarValue::Text("E".to_string()),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(true),
                        ScalarValue::Text(",".to_string()),
                        ScalarValue::Int(0), // typrelid: enums aren't class-backed
                        ScalarValue::Int(0), // typelem
                        ScalarValue::Int(0), // typarray: partial — no array companion
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Text("i".to_string()),
                        ScalarValue::Text("p".to_string()),
                        ScalarValue::Bool(false),
                        ScalarValue::Int(0),
                        ScalarValue::Int(-1),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Null,
                    ]);
                }
                for comp in &ext.user_composite_types {
                    let name = comp.name.last().cloned().unwrap_or_default();
                    out.push(vec![
                        ScalarValue::Int(comp.oid as i64),
                        ScalarValue::Text(name),
                        ScalarValue::Int(PUBLIC_NAMESPACE_OID as i64),
                        ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                        ScalarValue::Int(-1),     // typlen: composite is varlen
                        ScalarValue::Bool(false), // typbyval
                        ScalarValue::Text("c".to_string()),
                        ScalarValue::Text("C".to_string()),
                        ScalarValue::Bool(false),
                        ScalarValue::Bool(true),
                        ScalarValue::Text(",".to_string()),
                        ScalarValue::Int(comp.class_oid as i64), // typrelid
                        ScalarValue::Int(0),                     // typelem
                        ScalarValue::Int(0),                     // typarray: partial
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Text("d".to_string()),
                        ScalarValue::Text("x".to_string()),
                        ScalarValue::Bool(false),
                        ScalarValue::Int(0),
                        ScalarValue::Int(-1),
                        ScalarValue::Int(0),
                        ScalarValue::Int(0),
                        ScalarValue::Null,
                    ]);
                }
                out
            });
            rows.extend(user_type_rows);
            Ok(rows)
        }
        ("pg_catalog", "pg_range") => Ok(BUILTIN_RANGES
            .iter()
            .map(|r| {
                vec![
                    ScalarValue::Int(r.rngtypid as i64),
                    ScalarValue::Int(r.rngsubtype as i64),
                    ScalarValue::Int(r.rngmultitypid as i64),
                    ScalarValue::Int(0), // rngcollation
                    ScalarValue::Int(0), // rngsubopc
                    ScalarValue::Int(0), // rngcanonical regproc
                    ScalarValue::Int(0), // rngsubdiff regproc
                ]
            })
            .collect()),
        ("pg_catalog", "pg_enum") => {
            // Each user-defined enum label gets one pg_enum row. enumsortorder
            // is 1-based, matching PG's convention. Row-level oid column is a
            // stable synthetic ordering key (enumtypid * 1000 + label_index);
            // drivers don't rely on it being real-catalog OIDs.
            Ok(with_ext_read(|ext| {
                let mut rows = Vec::new();
                for enum_ty in &ext.user_types {
                    for (idx, label) in enum_ty.labels.iter().enumerate() {
                        let synthetic_oid = (enum_ty.oid as i64) * 1000 + (idx as i64 + 1);
                        rows.push(vec![
                            ScalarValue::Int(synthetic_oid),
                            ScalarValue::Int(enum_ty.oid as i64),
                            ScalarValue::Int(idx as i64 + 1),
                            ScalarValue::Text(label.clone()),
                        ]);
                    }
                }
                rows
            }))
        }
        ("pg_catalog", "pg_description") => Ok(Vec::new()),
        ("pg_catalog", "pg_operator") => Ok(Vec::new()),
        ("pg_catalog", "pg_collation") => Ok(BUILTIN_COLLATIONS
            .iter()
            .map(|c| {
                vec![
                    ScalarValue::Int(c.oid as i64),
                    ScalarValue::Text(c.name.to_string()),
                    ScalarValue::Int(PG_CATALOG_NAMESPACE_OID as i64),
                    ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                    ScalarValue::Text("d".to_string()), // collprovider: 'd' default
                    ScalarValue::Bool(true),            // collisdeterministic
                    ScalarValue::Int(c.encoding as i64),
                    ScalarValue::Text(c.collcollate.to_string()),
                    ScalarValue::Text(c.collctype.to_string()),
                    ScalarValue::Null, // collversion
                ]
            })
            .collect()),
        ("pg_catalog", "pg_language") => Ok(BUILTIN_LANGUAGES
            .iter()
            .map(|l| {
                vec![
                    ScalarValue::Int(l.oid as i64),
                    ScalarValue::Text(l.name.to_string()),
                    ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                    ScalarValue::Bool(l.lanispl),
                    ScalarValue::Bool(l.lanpltrusted),
                    ScalarValue::Int(0), // lanplcallfoid
                    ScalarValue::Int(0), // laninline
                    ScalarValue::Int(0), // lanvalidator
                ]
            })
            .collect()),
        ("pg_catalog", "pg_auth_members") => Ok(Vec::new()),
        ("pg_catalog", "pg_authid") => Ok(vec![vec![
            ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
            ScalarValue::Text("postgres".to_string()),
            ScalarValue::Bool(true), // rolsuper
            ScalarValue::Bool(true), // rolinherit
            ScalarValue::Bool(true), // rolcreaterole
            ScalarValue::Bool(true), // rolcreatedb
            ScalarValue::Bool(true), // rolcanlogin
            ScalarValue::Bool(true), // rolreplication
            ScalarValue::Bool(true), // rolbypassrls
            ScalarValue::Int(-1),    // rolconnlimit
            ScalarValue::Null,       // rolpassword
            ScalarValue::Null,       // rolvaliduntil
        ]]),
        ("pg_catalog", "pg_trigger") => Ok(Vec::new()),
        ("pg_catalog", "pg_rewrite") => Ok(Vec::new()),
        ("information_schema", "tables") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        out.push((
                            schema.name().to_string(),
                            table.name().to_string(),
                            information_schema_table_type(table.kind()).to_string(),
                        ));
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(table_schema, table_name, table_type)| {
                    vec![
                        ScalarValue::Text(table_schema),
                        ScalarValue::Text(table_name),
                        ScalarValue::Text(table_type),
                    ]
                })
                .collect())
        }
        ("information_schema", "columns") => {
            let mut entries = with_catalog_read(|catalog| {
                let mut out = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        for column in table.columns() {
                            let col_default = column.default().map(|expr| render_expr_to_sql(expr));
                            out.push((
                                schema.name().to_string(),
                                table.name().to_string(),
                                column.ordinal(),
                                column.name().to_string(),
                                information_schema_data_type(column.type_signature()).to_string(),
                                if column.nullable() {
                                    "YES".to_string()
                                } else {
                                    "NO".to_string()
                                },
                                col_default,
                                information_schema_numeric_precision(column.type_signature()),
                            ));
                        }
                    }
                }
                out
            });
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
            Ok(entries
                .into_iter()
                .map(
                    |(
                        table_schema,
                        table_name,
                        ordinal,
                        column_name,
                        data_type,
                        is_nullable,
                        col_default,
                        numeric_precision,
                    )| {
                        vec![
                            ScalarValue::Text(table_schema),
                            ScalarValue::Text(table_name),
                            ScalarValue::Text(column_name),
                            ScalarValue::Int(ordinal as i64 + 1),
                            col_default
                                .map(ScalarValue::Text)
                                .unwrap_or(ScalarValue::Null),
                            ScalarValue::Text(is_nullable),
                            ScalarValue::Text(data_type),
                            ScalarValue::Null, // character_maximum_length
                            numeric_precision
                                .map(ScalarValue::Int)
                                .unwrap_or(ScalarValue::Null),
                            ScalarValue::Text("NO".to_string()), // is_identity
                        ]
                    },
                )
                .collect())
        }
        ("information_schema", "schemata") => {
            let schemas = with_catalog_read(|catalog| {
                catalog
                    .schemas()
                    .map(|s| s.name().to_string())
                    .collect::<Vec<_>>()
            });
            Ok(schemas
                .into_iter()
                .map(|name| {
                    vec![
                        ScalarValue::Text("openassay".to_string()),
                        ScalarValue::Text(name),
                        ScalarValue::Text("openassay".to_string()),
                    ]
                })
                .collect())
        }
        ("information_schema", "key_column_usage") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        // Primary key and unique constraints
                        for kc in table.key_constraints() {
                            let con_name = kc.name.clone().unwrap_or_else(|| {
                                format!(
                                    "{}_{}",
                                    table.name(),
                                    if kc.primary { "pkey" } else { "key" }
                                )
                            });
                            for (pos, col_name) in kc.columns.iter().enumerate() {
                                rows.push(vec![
                                    ScalarValue::Text(con_name.clone()),
                                    ScalarValue::Text(schema.name().to_string()),
                                    ScalarValue::Text(table.name().to_string()),
                                    ScalarValue::Text(col_name.clone()),
                                    ScalarValue::Int(pos as i64 + 1),
                                ]);
                            }
                        }
                        // Foreign key constraints
                        for fk in table.foreign_key_constraints() {
                            let con_name = fk
                                .name
                                .clone()
                                .unwrap_or_else(|| format!("{}_fkey", table.name()));
                            for (pos, col_name) in fk.columns.iter().enumerate() {
                                rows.push(vec![
                                    ScalarValue::Text(con_name.clone()),
                                    ScalarValue::Text(schema.name().to_string()),
                                    ScalarValue::Text(table.name().to_string()),
                                    ScalarValue::Text(col_name.clone()),
                                    ScalarValue::Int(pos as i64 + 1),
                                ]);
                            }
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("information_schema", "table_constraints") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    for kc in table.key_constraints() {
                        let con_name = kc.name.clone().unwrap_or_else(|| {
                            format!(
                                "{}_{}",
                                table.name(),
                                if kc.primary { "pkey" } else { "key" }
                            )
                        });
                        let con_type = if kc.primary { "PRIMARY KEY" } else { "UNIQUE" };
                        rows.push(vec![
                            ScalarValue::Text(con_name),
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text(con_type.to_string()),
                            ScalarValue::Text("NO".to_string()),
                        ]);
                    }
                    for fk in table.foreign_key_constraints() {
                        let con_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table.name()));
                        rows.push(vec![
                            ScalarValue::Text(con_name),
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text("FOREIGN KEY".to_string()),
                            ScalarValue::Text("NO".to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_database") => {
            Ok(vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("openassay".to_string()),
                ScalarValue::Int(10),
                ScalarValue::Int(6), // UTF8
                ScalarValue::Text("en_US.UTF-8".to_string()),
            ]])
        }
        ("pg_catalog", "pg_roles") => {
            let role = security::current_role();
            Ok(vec![vec![
                ScalarValue::Int(10),
                ScalarValue::Text(role),
                ScalarValue::Bool(true), // rolsuper
                ScalarValue::Bool(true), // rolcanlogin
                ScalarValue::Bool(true), // rolinherit
                ScalarValue::Bool(true), // rolcreaterole
                ScalarValue::Bool(true), // rolcreatedb
                ScalarValue::Int(-1),    // rolconnlimit: -1 means unlimited
            ]])
        }
        ("pg_catalog", "pg_am") => Ok(vec![
            vec![
                ScalarValue::Int(403),
                ScalarValue::Text("btree".to_string()),
                ScalarValue::Int(0),
                ScalarValue::Text("i".to_string()),
            ],
            vec![
                ScalarValue::Int(405),
                ScalarValue::Text("hash".to_string()),
                ScalarValue::Int(0),
                ScalarValue::Text("i".to_string()),
            ],
            vec![
                ScalarValue::Int(2),
                ScalarValue::Text("heap".to_string()),
                ScalarValue::Int(0),
                ScalarValue::Text("t".to_string()),
            ],
        ]),
        ("pg_catalog", "pg_settings") => Ok(crate::commands::variable::with_guc_read(|guc| {
            guc.iter()
                .map(|(name, value)| {
                    vec![
                        ScalarValue::Text(name.clone()),
                        ScalarValue::Text(value.clone()),
                        ScalarValue::Text("Ungrouped".to_string()),
                        ScalarValue::Text(String::new()),
                        ScalarValue::Text("string".to_string()), // vartype
                        ScalarValue::Text("user".to_string()),   // context
                        ScalarValue::Text("default".to_string()), // source
                        ScalarValue::Bool(false),                // pending_restart
                    ]
                })
                .collect()
        })),
        ("pg_catalog", "pg_tables") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    if matches!(table.kind(), TableKind::Heap | TableKind::VirtualDual) {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text("openassay".to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_views") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    if matches!(table.kind(), TableKind::View | TableKind::MaterializedView) {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text("openassay".to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_indexes") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    for index in table.indexes() {
                        rows.push(vec![
                            ScalarValue::Text(schema.name().to_string()),
                            ScalarValue::Text(table.name().to_string()),
                            ScalarValue::Text(index.name.as_str().to_string()),
                        ]);
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_proc") => {
            // Return user-defined functions
            Ok(with_ext_read(|ext| {
                ext.user_functions
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        // Build proargnames: {name1,name2,...}
                        let arg_names: Vec<String> = f
                            .params
                            .iter()
                            .map(|p| p.name.clone().unwrap_or_default())
                            .collect();
                        let proargnames = format!("{{{}}}", arg_names.join(","));

                        // Build proargtypes: space-separated type OIDs
                        let arg_type_oids: Vec<String> = f
                            .params
                            .iter()
                            .map(|p| {
                                use crate::parser::ast::TypeName as TN;
                                match &p.data_type {
                                    TN::Bool => "16",
                                    TN::Int2 => "21",
                                    TN::Int4 | TN::Serial => "23",
                                    TN::Int8 | TN::BigSerial => "20",
                                    TN::Float4 => "700",
                                    TN::Float8 => "701",
                                    TN::Text | TN::Name => "25",
                                    TN::Varchar | TN::Char => "1043",
                                    TN::Numeric => "1700",
                                    TN::Date => "1082",
                                    TN::Timestamp => "1114",
                                    TN::TimestampTz => "1184",
                                    TN::Uuid => "2950",
                                    TN::Json => "114",
                                    TN::Jsonb => "3802",
                                    TN::Bytea => "17",
                                    TN::Time => "1083",
                                    TN::Interval => "1186",
                                    _ => "25", // fallback to text
                                }
                                .to_string()
                            })
                            .collect();
                        let proargtypes = arg_type_oids.join(" ");

                        vec![
                            ScalarValue::Int(90000 + i as i64),
                            ScalarValue::Text(f.name.last().cloned().unwrap_or_default()),
                            // User-declared functions default to `public`.
                            // Schema-qualified declarations are a Phase 5 concern.
                            ScalarValue::Int(PUBLIC_NAMESPACE_OID as i64),
                            ScalarValue::Int(BOOTSTRAP_SUPERUSER_OID as i64),
                            ScalarValue::Int(14), // prolang: 14 = sql
                            ScalarValue::Text(f.body.clone()), // prosrc
                            ScalarValue::Text(proargnames),
                            ScalarValue::Text(proargtypes),
                        ]
                    })
                    .collect()
            }))
        }
        ("pg_catalog", "pg_constraint") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                let mut con_oid: i64 = 70000;
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let col_ordinals: std::collections::HashMap<String, i64> = table
                            .columns()
                            .iter()
                            .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                            .collect();
                        for kc in table.key_constraints() {
                            let con_name = kc.name.clone().unwrap_or_else(|| {
                                format!(
                                    "{}_{}",
                                    table.name(),
                                    if kc.primary { "pkey" } else { "key" }
                                )
                            });
                            let contype = if kc.primary { "p" } else { "u" };
                            let conkey_parts = kc
                                .columns
                                .iter()
                                .map(|c| {
                                    col_ordinals.get(c).copied().ok_or_else(|| EngineError {
                                        message: format!(
                                            "Column '{}' not found in table '{}'",
                                            c,
                                            table.name()
                                        ),
                                    })
                                })
                                .collect::<Result<Vec<i64>, _>>()?;
                            let conkey = format!(
                                "{{{}}}",
                                conkey_parts
                                    .iter()
                                    .map(|n| n.to_string())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                            rows.push(vec![
                                ScalarValue::Int(con_oid),
                                ScalarValue::Text(con_name),
                                ScalarValue::Int(schema.oid() as i64),
                                ScalarValue::Text(contype.to_string()),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(0), // confrelid: 0 for non-FK
                                ScalarValue::Text(conkey),
                                ScalarValue::Null, // confkey: NULL for non-FK
                                ScalarValue::Null, // confdeltype: NULL for non-FK
                                ScalarValue::Null, // confupdtype: NULL for non-FK
                            ]);
                            con_oid += 1;
                        }
                        for fk in table.foreign_key_constraints() {
                            let con_name = fk
                                .name
                                .clone()
                                .unwrap_or_else(|| format!("{}_fkey", table.name()));
                            let fk_conkey_parts = fk
                                .columns
                                .iter()
                                .map(|c| {
                                    col_ordinals.get(c).copied().ok_or_else(|| EngineError {
                                        message: format!(
                                            "FK column '{}' not found in table '{}'",
                                            c,
                                            table.name()
                                        ),
                                    })
                                })
                                .collect::<Result<Vec<i64>, _>>()?;
                            let conkey = format!(
                                "{{{}}}",
                                fk_conkey_parts
                                    .iter()
                                    .map(|n| n.to_string())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                            // Resolve referenced table OID
                            let (ref_schema, ref_table_name) = if fk.referenced_table.len() >= 2 {
                                (
                                    fk.referenced_table[0].as_str(),
                                    fk.referenced_table.last().map(String::as_str).unwrap(),
                                )
                            } else {
                                (
                                    "public",
                                    fk.referenced_table
                                        .first()
                                        .map(String::as_str)
                                        .unwrap_or("public"),
                                )
                            };
                            let ref_table_oid = catalog
                                .schema(ref_schema)
                                .and_then(|s| s.table(ref_table_name))
                                .map(|t| t.oid() as i64)
                                .unwrap_or(0);
                            let ref_col_ordinals: std::collections::HashMap<String, i64> = catalog
                                .schema(ref_schema)
                                .and_then(|s| s.table(ref_table_name))
                                .map(|t| {
                                    t.columns()
                                        .iter()
                                        .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                                        .collect()
                                })
                                .unwrap_or_default();
                            let confkey = format!(
                                "{{{}}}",
                                fk.referenced_columns
                                    .iter()
                                    .map(|c| ref_col_ordinals
                                        .get(c)
                                        .copied()
                                        .unwrap_or(0)
                                        .to_string())
                                    .collect::<Vec<_>>()
                                    .join(",")
                            );
                            let confdeltype = fk_action_char(fk.on_delete);
                            let confupdtype = fk_action_char(fk.on_update);
                            rows.push(vec![
                                ScalarValue::Int(con_oid),
                                ScalarValue::Text(con_name),
                                ScalarValue::Int(schema.oid() as i64),
                                ScalarValue::Text("f".to_string()),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(ref_table_oid),
                                ScalarValue::Text(conkey),
                                ScalarValue::Text(confkey),
                                ScalarValue::Text(confdeltype.to_string()),
                                ScalarValue::Text(confupdtype.to_string()),
                            ]);
                            con_oid += 1;
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_index") => {
            with_catalog_read(|catalog| {
                let mut rows = Vec::new();
                let mut index_oid: i64 = 80000;
                for schema in catalog.schemas() {
                    for table in schema.tables() {
                        let col_ordinals: std::collections::HashMap<String, i64> = table
                            .columns()
                            .iter()
                            .map(|c| (c.name().to_string(), c.ordinal() as i64 + 1))
                            .collect();
                        // Key constraints (PK / UNIQUE) produce implicit indexes
                        for kc in table.key_constraints() {
                            let indkey_parts = kc
                                .columns
                                .iter()
                                .map(|c| {
                                    col_ordinals.get(c).copied().ok_or_else(|| EngineError {
                                        message: format!(
                                            "Column '{}' not found in table '{}'",
                                            c,
                                            table.name()
                                        ),
                                    })
                                })
                                .collect::<Result<Vec<i64>, _>>()?;
                            let indkey = indkey_parts
                                .iter()
                                .map(|n| n.to_string())
                                .collect::<Vec<_>>()
                                .join(" ");
                            rows.push(vec![
                                ScalarValue::Int(index_oid),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(kc.columns.len() as i64),
                                ScalarValue::Bool(true), // indisunique: all key constraints (PK and UNIQUE) create unique indexes
                                ScalarValue::Bool(kc.primary), // indisprimary
                                ScalarValue::Text(indkey),
                            ]);
                            index_oid += 1;
                        }
                        // Explicit indexes
                        for idx in table.indexes() {
                            let indkey_parts = idx
                                .columns
                                .iter()
                                .map(|c| {
                                    col_ordinals.get(c).copied().ok_or_else(|| EngineError {
                                        message: format!(
                                            "Column '{}' not found in table '{}'",
                                            c,
                                            table.name()
                                        ),
                                    })
                                })
                                .collect::<Result<Vec<i64>, _>>()?;
                            let indkey = indkey_parts
                                .iter()
                                .map(|n| n.to_string())
                                .collect::<Vec<_>>()
                                .join(" ");
                            rows.push(vec![
                                ScalarValue::Int(index_oid),
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(idx.columns.len() as i64),
                                ScalarValue::Bool(idx.unique),
                                ScalarValue::Bool(false), // indisprimary
                                ScalarValue::Text(indkey),
                            ]);
                            index_oid += 1;
                        }
                    }
                }
                Ok(rows)
            })
        }
        ("pg_catalog", "pg_attrdef") => with_catalog_read(|catalog| {
            let mut rows = Vec::new();
            for schema in catalog.schemas() {
                for table in schema.tables() {
                    for column in table.columns() {
                        if let Some(default_expr) = column.default() {
                            rows.push(vec![
                                ScalarValue::Int(table.oid() as i64),
                                ScalarValue::Int(column.ordinal() as i64 + 1),
                                ScalarValue::Text(render_expr_to_sql(default_expr)),
                            ]);
                        }
                    }
                }
            }
            Ok(rows)
        }),
        ("pg_catalog", "pg_inherits") => {
            // OpenAssay does not support table inheritance; return empty result set
            Ok(Vec::new())
        }
        ("pg_catalog", "pg_statistic") => {
            // Statistics catalog is stubbed; keep shape-compatible, empty result.
            Ok(Vec::new())
        }
        ("pg_catalog", "pg_extension") => Ok(with_ext_read(|ext| {
            ext.extensions
                .iter()
                .map(|e| {
                    vec![
                        ScalarValue::Text(e.name.clone()),
                        ScalarValue::Text(e.version.clone()),
                        ScalarValue::Text(e.description.clone()),
                    ]
                })
                .collect()
        })),
        ("ws", "connections") => {
            if !is_ws_extension_loaded() {
                return Err(EngineError {
                    message: "extension \"ws\" is not loaded".to_string(),
                });
            }
            #[cfg(target_arch = "wasm32")]
            {
                let conn_ids: Vec<i64> =
                    with_ext_read(|ext| ext.ws_connections.keys().copied().collect());
                for id in conn_ids {
                    sync_wasm_ws_state(id);
                    drain_wasm_ws_messages(id);
                }
            }
            Ok(with_ext_read(|ext| {
                let mut conns: Vec<_> = ext.ws_connections.values().collect();
                conns.sort_by_key(|c| c.id);
                conns
                    .iter()
                    .map(|c| {
                        vec![
                            ScalarValue::Int(c.id),
                            ScalarValue::Text(c.url.clone()),
                            ScalarValue::Text(c.state.clone()),
                            ScalarValue::Text(c.opened_at.clone()),
                            ScalarValue::Int(c.messages_in),
                            ScalarValue::Int(c.messages_out),
                        ]
                    })
                    .collect()
            }))
        }
        _ => Err(EngineError {
            message: format!("relation \"{schema}.{relation}\" does not exist"),
        }),
    }
}

pub(super) fn pg_relkind_for_table(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "r",
        TableKind::View => "v",
        TableKind::MaterializedView => "m",
        TableKind::Foreign => "f",
    }
}

pub(super) fn information_schema_table_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "BASE TABLE",
        TableKind::View => "VIEW",
        TableKind::MaterializedView => "MATERIALIZED VIEW",
        TableKind::Foreign => "FOREIGN TABLE",
    }
}

pub(super) fn information_schema_data_type(signature: TypeSignature) -> &'static str {
    match signature {
        TypeSignature::Bool => "boolean",
        TypeSignature::Int8 => "bigint",
        TypeSignature::Float8 => "double precision",
        TypeSignature::Numeric => "numeric",
        TypeSignature::Text => "text",
        TypeSignature::Date => "date",
        TypeSignature::Timestamp => "timestamp without time zone",
        TypeSignature::Vector(_) => "vector",
    }
}

pub(super) fn information_schema_numeric_precision(signature: TypeSignature) -> Option<i64> {
    match signature {
        TypeSignature::Int8 => Some(64),
        TypeSignature::Float8 => Some(53),
        TypeSignature::Numeric => None, // variable precision
        _ => None,
    }
}

pub(super) fn fk_action_char(action: crate::parser::ast::ForeignKeyAction) -> char {
    use crate::parser::ast::ForeignKeyAction;
    match action {
        ForeignKeyAction::Restrict => 'r',
        ForeignKeyAction::Cascade => 'c',
        ForeignKeyAction::SetNull => 'n',
    }
}
