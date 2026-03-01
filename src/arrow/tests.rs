#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::storage::tuple::ScalarValue;
    use crate::tcop::engine::QueryResult;

    use super::super::record_batch::{query_result_to_arrow_ipc, query_result_to_record_batch};
    use super::super::schema::build_schema;

    fn make_result(
        columns: Vec<&str>,
        rows: Vec<Vec<ScalarValue>>,
    ) -> QueryResult {
        QueryResult {
            columns: columns.into_iter().map(String::from).collect(),
            rows,
            command_tag: "SELECT".to_string(),
            rows_affected: 0,
        }
    }

    // ── schema inference ────────────────────────────────────────────────────

    #[test]
    fn schema_infers_bool_column() {
        let result = make_result(
            vec!["flag"],
            vec![vec![ScalarValue::Bool(true)]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Boolean);
        assert!(schema.field(0).is_nullable());
    }

    #[test]
    fn schema_infers_int64_column() {
        let result = make_result(
            vec!["id"],
            vec![vec![ScalarValue::Int(42)]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn schema_infers_float64_column() {
        let result = make_result(
            vec!["val"],
            vec![vec![ScalarValue::Float(3.14)]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Float64);
    }

    #[test]
    fn schema_infers_utf8_for_text() {
        let result = make_result(
            vec!["name"],
            vec![vec![ScalarValue::Text("hello".to_string())]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn schema_falls_back_to_utf8_for_all_null_column() {
        let result = make_result(
            vec!["x"],
            vec![vec![ScalarValue::Null], vec![ScalarValue::Null]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn schema_skips_leading_nulls_to_infer_type() {
        let result = make_result(
            vec!["x"],
            vec![
                vec![ScalarValue::Null],
                vec![ScalarValue::Int(7)],
            ],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn schema_field_names_match_column_names() {
        let result = make_result(
            vec!["alpha", "beta"],
            vec![vec![ScalarValue::Int(1), ScalarValue::Text("x".to_string())]],
        );
        let schema = build_schema(&result);
        assert_eq!(schema.field(0).name(), "alpha");
        assert_eq!(schema.field(1).name(), "beta");
    }

    // ── RecordBatch conversion ───────────────────────────────────────────────

    #[test]
    fn record_batch_bool_column() {
        let result = make_result(
            vec!["flag"],
            vec![
                vec![ScalarValue::Bool(true)],
                vec![ScalarValue::Bool(false)],
                vec![ScalarValue::Null],
            ],
        );
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        assert_eq!(batch.num_rows(), 3);
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("expected BooleanArray");
        assert_eq!(col.value(0), true);
        assert_eq!(col.value(1), false);
        assert!(col.is_null(2));
    }

    #[test]
    fn record_batch_int64_column() {
        let result = make_result(
            vec!["id"],
            vec![
                vec![ScalarValue::Int(1)],
                vec![ScalarValue::Int(2)],
                vec![ScalarValue::Null],
            ],
        );
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("expected Int64Array");
        assert_eq!(col.value(0), 1);
        assert_eq!(col.value(1), 2);
        assert!(col.is_null(2));
    }

    #[test]
    fn record_batch_float64_column() {
        let result = make_result(
            vec!["val"],
            vec![
                vec![ScalarValue::Float(1.5)],
                vec![ScalarValue::Null],
            ],
        );
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("expected Float64Array");
        assert!((col.value(0) - 1.5).abs() < f64::EPSILON);
        assert!(col.is_null(1));
    }

    #[test]
    fn record_batch_text_column() {
        let result = make_result(
            vec!["name"],
            vec![
                vec![ScalarValue::Text("Ada".to_string())],
                vec![ScalarValue::Null],
            ],
        );
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("expected StringArray");
        assert_eq!(col.value(0), "Ada");
        assert!(col.is_null(1));
    }

    #[test]
    fn record_batch_empty_rows() {
        let result = make_result(vec!["id", "name"], vec![]);
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn record_batch_no_columns() {
        let result = make_result(vec![], vec![]);
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        assert_eq!(batch.num_columns(), 0);
    }

    #[test]
    fn record_batch_mixed_types() {
        let result = make_result(
            vec!["id", "name", "score"],
            vec![
                vec![
                    ScalarValue::Int(1),
                    ScalarValue::Text("Alice".to_string()),
                    ScalarValue::Float(9.5),
                ],
                vec![
                    ScalarValue::Int(2),
                    ScalarValue::Text("Bob".to_string()),
                    ScalarValue::Float(8.0),
                ],
            ],
        );
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Float64);
    }

    // ── Arrow IPC serialisation ─────────────────────────────────────────────

    #[test]
    fn arrow_ipc_bytes_are_non_empty_for_non_empty_result() {
        let result = make_result(
            vec!["id"],
            vec![vec![ScalarValue::Int(42)]],
        );
        let bytes = query_result_to_arrow_ipc(&result).expect("IPC serialisation should succeed");
        // Arrow IPC file format starts with magic bytes "ARROW1\0\0"
        assert!(bytes.starts_with(b"ARROW1\0\0"), "expected Arrow IPC magic header");
        assert!(!bytes.is_empty());
    }

    #[test]
    fn arrow_ipc_bytes_for_empty_result() {
        let result = make_result(vec!["id"], vec![]);
        let bytes = query_result_to_arrow_ipc(&result).expect("IPC serialisation should succeed");
        assert!(bytes.starts_with(b"ARROW1\0\0"));
    }

    #[test]
    fn record_batch_schema_preserved_in_expected_format() {
        let result = make_result(
            vec!["id", "name"],
            vec![vec![
                ScalarValue::Int(1),
                ScalarValue::Text("test".to_string()),
            ]],
        );
        let expected_schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]);
        let batch = query_result_to_record_batch(&result).expect("record batch should be built");
        assert_eq!(batch.schema().as_ref(), &expected_schema);
    }
}
