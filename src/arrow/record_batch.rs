use std::io::Cursor;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

use crate::tcop::engine::QueryResult;

use super::conversion::build_column_arrays;
use super::schema::build_schema;

/// Convert a [`QueryResult`] into an Arrow [`RecordBatch`].
///
/// The schema is inferred from the first non-null value in each column.
/// All fields are nullable.
///
/// When the result has no columns, an empty `RecordBatch` with 0 rows is
/// returned using [`RecordBatch::new_empty`].
pub fn query_result_to_record_batch(result: &QueryResult) -> Result<RecordBatch, ArrowError> {
    let schema = Arc::new(build_schema(result));
    if schema.fields().is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    let arrays = build_column_arrays(result, &schema)?;
    RecordBatch::try_new(schema, arrays)
}

/// Serialize an Arrow [`RecordBatch`] to Arrow IPC file format bytes.
pub fn record_batch_to_arrow_ipc(batch: &RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut buf = Cursor::new(Vec::new());
    {
        let mut writer = FileWriter::try_new(&mut buf, batch.schema_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(buf.into_inner())
}

/// Convert a [`QueryResult`] directly to Arrow IPC file format bytes.
///
/// This is the primary entry point for zero-copy transfer of query results
/// to consumers such as JavaScript Arrow libraries or Polars/pandas.
pub fn query_result_to_arrow_ipc(result: &QueryResult) -> Result<Vec<u8>, ArrowError> {
    let batch = query_result_to_record_batch(result)?;
    record_batch_to_arrow_ipc(&batch)
}
