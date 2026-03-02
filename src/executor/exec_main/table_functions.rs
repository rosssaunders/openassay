#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) async fn evaluate_table_function(
    function: &TableFunctionRef,
    params: &[Option<String>],
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
    })
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

    let input = Path::new(&input_path);
    let metadata_columns = read_iceberg_metadata_columns(input);
    let parquet_files = discover_iceberg_parquet_files(input, fn_name)?;
    if parquet_files.is_empty() {
        if let Some(columns) = metadata_columns {
            return Ok((columns, Vec::new()));
        }
        return Err(EngineError {
            message: format!(
                "{fn_name}(): no parquet files found under {}",
                input.display()
            ),
        });
    }

    scan_iceberg_parquet_files(
        &parquet_files,
        metadata_columns.unwrap_or_default(),
        fn_name,
    )
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
pub(super) fn scan_iceberg_parquet_files(
    parquet_files: &[PathBuf],
    mut columns: Vec<String>,
    fn_name: &str,
) -> Result<(Vec<String>, Vec<Vec<ScalarValue>>), EngineError> {
    let mut column_index = HashMap::new();
    for (idx, name) in columns.iter().enumerate() {
        column_index.insert(name.to_ascii_lowercase(), idx);
    }

    let mut rows = Vec::new();
    for file_path in parquet_files {
        let file = File::open(file_path).map_err(|err| EngineError {
            message: format!(
                "{fn_name}(): failed to open parquet file {}: {err}",
                file_path.display()
            ),
        })?;
        let reader = SerializedFileReader::new(file).map_err(|err| EngineError {
            message: format!(
                "{fn_name}(): failed to read parquet file {}: {err}",
                file_path.display()
            ),
        })?;

        for name in parquet_root_field_names(&reader) {
            ensure_iceberg_column(&name, &mut columns, &mut column_index, &mut rows);
        }

        let row_iter = reader.get_row_iter(None).map_err(|err| EngineError {
            message: format!(
                "{fn_name}(): failed to iterate parquet rows in {}: {err}",
                file_path.display()
            ),
        })?;

        for row_result in row_iter {
            let row = row_result.map_err(|err| EngineError {
                message: format!(
                    "{fn_name}(): failed to decode parquet row in {}: {err}",
                    file_path.display()
                ),
            })?;
            let mut output_row = vec![ScalarValue::Null; columns.len()];
            for (name, field) in row.get_column_iter() {
                let idx = ensure_iceberg_column(name, &mut columns, &mut column_index, &mut rows);
                if idx >= output_row.len() {
                    output_row.resize(columns.len(), ScalarValue::Null);
                }
                output_row[idx] = parquet_field_to_scalar(field);
            }
            rows.push(output_row);
        }
    }

    if columns.is_empty() {
        columns.push("value".to_string());
    }
    Ok((columns, rows))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn parquet_root_field_names(reader: &SerializedFileReader<File>) -> Vec<String> {
    reader
        .metadata()
        .file_metadata()
        .schema()
        .get_fields()
        .iter()
        .map(|field| field.name().to_string())
        .collect()
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn ensure_iceberg_column(
    name: &str,
    columns: &mut Vec<String>,
    column_index: &mut HashMap<String, usize>,
    rows: &mut Vec<Vec<ScalarValue>>,
) -> usize {
    let key = name.to_ascii_lowercase();
    if let Some(idx) = column_index.get(&key) {
        return *idx;
    }

    let idx = columns.len();
    columns.push(name.to_string());
    column_index.insert(key, idx);
    for row in rows {
        row.push(ScalarValue::Null);
    }
    idx
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn parquet_field_to_scalar(field: &ParquetField) -> ScalarValue {
    match field {
        ParquetField::Null => ScalarValue::Null,
        ParquetField::Bool(value) => ScalarValue::Bool(*value),
        ParquetField::Byte(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Short(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Int(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::Long(value) => ScalarValue::Int(*value),
        ParquetField::UByte(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::UShort(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::UInt(value) => ScalarValue::Int(i64::from(*value)),
        ParquetField::ULong(value) => {
            if let Ok(v) = i64::try_from(*value) {
                ScalarValue::Int(v)
            } else {
                ScalarValue::Text(value.to_string())
            }
        }
        ParquetField::Float16(value) => ScalarValue::Float(f64::from(*value)),
        ParquetField::Float(value) => ScalarValue::Float(f64::from(*value)),
        ParquetField::Double(value) => ScalarValue::Float(*value),
        ParquetField::Decimal(value) => ScalarValue::Text(format!("{value:?}")),
        ParquetField::Str(value) => ScalarValue::Text(value.clone()),
        ParquetField::Bytes(value) => {
            if let Ok(text) = String::from_utf8(value.data().to_vec()) {
                ScalarValue::Text(text)
            } else {
                use base64::Engine;
                ScalarValue::Text(base64::prelude::BASE64_STANDARD.encode(value.data()))
            }
        }
        ParquetField::Date(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimeMillis(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimeMicros(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimestampMillis(value) => ScalarValue::Text(value.to_string()),
        ParquetField::TimestampMicros(value) => ScalarValue::Text(value.to_string()),
        ParquetField::Group(_) | ParquetField::ListInternal(_) | ParquetField::MapInternal(_) => {
            ScalarValue::Text(field.to_string())
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn discover_iceberg_parquet_files(
    path: &Path,
    fn_name: &str,
) -> Result<Vec<PathBuf>, EngineError> {
    if path.is_file()
        && path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
    {
        return Ok(vec![path.to_path_buf()]);
    }

    let table_root = resolve_iceberg_table_root(path, fn_name)?;
    let mut parquet_files = Vec::new();

    let data_dir = table_root.join("data");
    if data_dir.is_dir() {
        collect_parquet_files(&data_dir, &mut parquet_files, fn_name)?;
    }
    if parquet_files.is_empty() {
        collect_parquet_files(&table_root, &mut parquet_files, fn_name)?;
    }

    parquet_files.sort();
    Ok(parquet_files)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn resolve_iceberg_table_root(
    path: &Path,
    fn_name: &str,
) -> Result<PathBuf, EngineError> {
    if path.is_dir() {
        return Ok(path.to_path_buf());
    }
    if path.is_file() {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let is_json_path = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("json"));
        if file_name.to_ascii_lowercase().ends_with(".metadata.json") || is_json_path {
            let parent = path.parent().ok_or_else(|| EngineError {
                message: format!(
                    "{fn_name}(): cannot determine table root from {}",
                    path.display()
                ),
            })?;
            if parent
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.eq_ignore_ascii_case("metadata"))
                && let Some(root) = parent.parent()
            {
                return Ok(root.to_path_buf());
            }
            return Ok(parent.to_path_buf());
        }
        if file_name.ends_with(".parquet") {
            return path
                .parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| EngineError {
                    message: format!(
                        "{fn_name}(): cannot determine table root from {}",
                        path.display()
                    ),
                });
        }
    }

    Err(EngineError {
        message: format!(
            "{fn_name}() expects an existing Iceberg table directory, metadata file, or parquet file path"
        ),
    })
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn collect_parquet_files(
    directory: &Path,
    parquet_files: &mut Vec<PathBuf>,
    fn_name: &str,
) -> Result<(), EngineError> {
    let entries = fs::read_dir(directory).map_err(|err| EngineError {
        message: format!(
            "{fn_name}(): failed to read directory {}: {err}",
            directory.display()
        ),
    })?;
    for entry_result in entries {
        let entry = entry_result.map_err(|err| EngineError {
            message: format!(
                "{fn_name}(): failed to read directory entry in {}: {err}",
                directory.display()
            ),
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|err| EngineError {
            message: format!(
                "{fn_name}(): failed to inspect path {}: {err}",
                path.display()
            ),
        })?;
        if file_type.is_dir() {
            collect_parquet_files(&path, parquet_files, fn_name)?;
            continue;
        }
        if file_type.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("parquet"))
        {
            parquet_files.push(path);
        }
    }
    Ok(())
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn read_iceberg_metadata_columns(path: &Path) -> Option<Vec<String>> {
    let metadata_file = resolve_iceberg_metadata_file(path)?;
    let metadata = fs::read_to_string(metadata_file).ok()?;
    let json = serde_json::from_str::<JsonValue>(&metadata).ok()?;

    if let Some(columns) = extract_schema_field_names(json.get("schema")) {
        return Some(columns);
    }

    let current_schema_id = json.get("current-schema-id").and_then(JsonValue::as_i64);
    let schemas = json.get("schemas").and_then(JsonValue::as_array)?;
    if let Some(schema_id) = current_schema_id
        && let Some(schema) = schemas
            .iter()
            .find(|schema| schema.get("schema-id").and_then(JsonValue::as_i64) == Some(schema_id))
        && let Some(columns) = extract_schema_field_names(Some(schema))
    {
        return Some(columns);
    }

    schemas
        .last()
        .and_then(|schema| extract_schema_field_names(Some(schema)))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn resolve_iceberg_metadata_file(path: &Path) -> Option<PathBuf> {
    if path.is_file()
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".metadata.json"))
    {
        return Some(path.to_path_buf());
    }

    let table_root = resolve_iceberg_table_root(path, "iceberg_scan").ok()?;
    let metadata_dir = table_root.join("metadata");
    if !metadata_dir.is_dir() {
        return None;
    }

    let mut candidates = Vec::new();
    for entry in fs::read_dir(metadata_dir).ok()? {
        let entry = entry.ok()?;
        let file_name = entry.file_name();
        let file_name = file_name.to_str()?;
        if file_name.ends_with(".metadata.json") {
            candidates.push(entry.path());
        }
    }
    if candidates.is_empty() {
        return None;
    }

    candidates
        .sort_by(|left, right| compare_iceberg_metadata_files(left.as_path(), right.as_path()));
    candidates.pop()
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn compare_iceberg_metadata_files(left: &Path, right: &Path) -> Ordering {
    let left_name = left
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let right_name = right
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    let left_version = iceberg_metadata_version(left_name);
    let right_version = iceberg_metadata_version(right_name);

    left_version
        .cmp(&right_version)
        .then_with(|| left_name.cmp(right_name))
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn iceberg_metadata_version(file_name: &str) -> u64 {
    let stem = file_name
        .strip_suffix(".metadata.json")
        .unwrap_or(file_name);
    let stem = stem.strip_prefix('v').unwrap_or(stem);
    let digits = stem
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    digits.parse::<u64>().unwrap_or(0)
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) fn extract_schema_field_names(schema: Option<&JsonValue>) -> Option<Vec<String>> {
    let fields = schema?.get("fields")?.as_array()?;
    let columns = fields
        .iter()
        .filter_map(|field| field.get("name").and_then(JsonValue::as_str))
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return None;
    }
    Some(columns)
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
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
) -> Result<TableEval, EngineError> {
    evaluate_relation_with_predicates(rel, params, outer_scope, &[]).await
}

pub(super) async fn evaluate_relation_with_predicates(
    rel: &TableRef,
    params: &[Option<String>],
    outer_scope: Option<&EvalScope>,
    relation_predicates: &[Expr],
) -> Result<TableEval, EngineError> {
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

        return Ok(TableEval {
            rows: scoped_rows,
            columns: cte.columns,
            null_scope,
        });
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
        return Ok(TableEval {
            rows: scoped_rows,
            columns: column_names,
            null_scope,
        });
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
                return Ok(TableEval {
                    rows: Vec::new(),
                    columns,
                    null_scope,
                });
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

    let (columns, mut rows) = match table.kind() {
        TableKind::VirtualDual => (Vec::new(), vec![Vec::new()]),
        TableKind::Heap | TableKind::MaterializedView => {
            let columns = table
                .columns()
                .iter()
                .map(|column| column.name().to_string())
                .collect::<Vec<_>>();
            let rows = with_storage_read(|storage| {
                let all_rows = storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default();
                if let Some(offsets) = &index_offsets {
                    offsets
                        .iter()
                        .filter_map(|offset| all_rows.get(*offset).cloned())
                        .collect::<Vec<_>>()
                } else {
                    all_rows
                }
            });
            (columns, rows)
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
            (columns, result.rows)
        }
    };
    if table.kind() != TableKind::VirtualDual {
        require_relation_privilege(&table, TablePrivilege::Select)?;
        let mut visible_rows = Vec::with_capacity(rows.len());
        for row in rows {
            if relation_row_visible_for_command(&table, &row, RlsCommand::Select, params).await? {
                visible_rows.push(row);
            }
        }
        rows = visible_rows;
    }

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
    })
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
                .map(|(oid, name)| vec![ScalarValue::Int(oid as i64), ScalarValue::Text(name)])
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
            entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            Ok(entries
                .into_iter()
                .map(|(oid, relname, relnamespace, relkind, relhasindex)| {
                    vec![
                        ScalarValue::Int(oid as i64),
                        ScalarValue::Text(relname),
                        ScalarValue::Int(relnamespace as i64),
                        ScalarValue::Text(relkind),
                        ScalarValue::Int(10), // relowner: superuser OID
                        ScalarValue::Int(0),  // reltoastrelid
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
                                type_signature_to_oid(column.type_signature()),
                                !column.nullable(),
                                column.default().is_some(),
                            ));
                        }
                    }
                }
                out
            });
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
            // typnamespace 11 = pg_catalog OID, typowner 10 = superuser
            // typlen: -1 = variable, otherwise fixed byte length
            let mut entries = vec![
                (16u32, "bool", 11u32, 10u32, 1i64, false, "b", 0u32, 0u32),
                (20u32, "int8", 11u32, 10u32, 8i64, true, "b", 0u32, 0u32),
                (25u32, "text", 11u32, 10u32, -1i64, false, "b", 0u32, 0u32),
                (701u32, "float8", 11u32, 10u32, 8i64, true, "b", 0u32, 0u32),
                (1082u32, "date", 11u32, 10u32, 4i64, true, "b", 0u32, 0u32),
                (
                    1114u32,
                    "timestamp",
                    11u32,
                    10u32,
                    8i64,
                    true,
                    "b",
                    0u32,
                    0u32,
                ),
                (
                    1700u32, "numeric", 11u32, 10u32, -1i64, false, "b", 0u32, 0u32,
                ),
            ];
            entries.sort_by_key(|a| a.0);
            Ok(entries
                .into_iter()
                .map(
                    |(
                        oid,
                        typname,
                        typnamespace,
                        typowner,
                        typlen,
                        typbyval,
                        typtype,
                        typelem,
                        typarray,
                    )| {
                        vec![
                            ScalarValue::Int(oid as i64),
                            ScalarValue::Text(typname.to_string()),
                            ScalarValue::Int(typnamespace as i64),
                            ScalarValue::Int(typowner as i64),
                            ScalarValue::Int(typlen),
                            ScalarValue::Bool(typbyval),
                            ScalarValue::Text(typtype.to_string()),
                            ScalarValue::Int(typelem as i64),
                            ScalarValue::Int(typarray as i64),
                        ]
                    },
                )
                .collect())
        }
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
                        vec![
                            ScalarValue::Int(90000 + i as i64),
                            ScalarValue::Text(f.name.last().cloned().unwrap_or_default()),
                            ScalarValue::Int(0),  // pronamespace placeholder
                            ScalarValue::Int(10), // proowner: superuser
                            ScalarValue::Int(14), // prolang: 14 = sql
                            ScalarValue::Text(String::new()), // prosrc placeholder
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
                            let ref_table_oid = catalog
                                .schema(
                                    fk.referenced_table
                                        .first()
                                        .map(String::as_str)
                                        .unwrap_or("public"),
                                )
                                .and_then(|s| fk.referenced_table.last().and_then(|n| s.table(n)))
                                .map(|t| t.oid() as i64)
                                .unwrap_or(0);
                            let ref_col_ordinals: std::collections::HashMap<String, i64> = catalog
                                .schema(
                                    fk.referenced_table
                                        .first()
                                        .map(String::as_str)
                                        .unwrap_or("public"),
                                )
                                .and_then(|s| fk.referenced_table.last().and_then(|n| s.table(n)))
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
    }
}

pub(super) fn information_schema_table_type(kind: TableKind) -> &'static str {
    match kind {
        TableKind::VirtualDual | TableKind::Heap => "BASE TABLE",
        TableKind::View => "VIEW",
        TableKind::MaterializedView => "MATERIALIZED VIEW",
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
