//! Apache Arrow interop for zero-copy query results.
//!
//! Converts [`QueryResult`](crate::tcop::engine::QueryResult) to Arrow
//! [`RecordBatch`](arrow::record_batch::RecordBatch) and serialises it to the
//! Arrow IPC file format for zero-copy transfer to JavaScript visualization
//! libraries, Polars, pandas, DuckDB, and the broader modern data stack.
//!
//! # Usage
//!
//! ```rust,ignore
//! use openassay::arrow::record_batch::query_result_to_arrow_ipc;
//! let ipc_bytes = query_result_to_arrow_ipc(&query_result)?;
//! ```
//!
//! # Type mapping
//!
//! | ScalarValue variant | Arrow DataType      |
//! |---------------------|---------------------|
//! | `Bool`              | `Boolean`           |
//! | `Int`               | `Int64`             |
//! | `Float`             | `Float64`           |
//! | `Numeric`           | `Decimal128(38, 6)` |
//! | `Text`              | `Utf8`              |
//! | `Array` / `Record` / `Vector` | `Utf8` (rendered) |
//! | `Null`              | nullable in column  |

pub mod conversion;
pub mod record_batch;
pub mod schema;

#[cfg(test)]
mod tests;
