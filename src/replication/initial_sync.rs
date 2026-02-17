use futures::StreamExt;

use crate::protocol::copy::{CopyOptions, decode_text_row};
use crate::replication::ReplicationError;
use crate::replication::schema_sync::TableSchema;
use crate::replication::tuple_decoder::decode_text_value;
use crate::storage::heap::with_storage_write;
use crate::storage::tuple::ScalarValue;

pub async fn copy_tables(
    client: &tokio_postgres::Client,
    tables: &[TableSchema],
    table_oids: &[(u32, crate::catalog::oid::Oid)],
) -> Result<(), ReplicationError> {
    let mut oid_map = std::collections::HashMap::new();
    for (relation_id, oid) in table_oids {
        oid_map.insert(*relation_id, *oid);
    }

    for table in tables {
        let Some(local_oid) = oid_map.get(&table.relation_id).copied() else {
            continue;
        };
        let qualified = format!("\"{}\".\"{}\"", table.namespace, table.name);
        let sql = format!("COPY {qualified} TO STDOUT");
        let stream = client.copy_out(sql.as_str()).await?;
        tokio::pin!(stream);
        let mut buffer = Vec::new();
        let options = CopyOptions::default();
        let mut rows = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            buffer.extend_from_slice(&chunk);
            while let Some(pos) = buffer.iter().position(|b| *b == b'\n') {
                let mut line = buffer.drain(..=pos).collect::<Vec<_>>();
                if matches!(line.last(), Some(b'\n')) {
                    line.pop();
                }
                if matches!(line.last(), Some(b'\r')) {
                    line.pop();
                }
                if line == b"\\." {
                    buffer.clear();
                    break;
                }
                let line_str = String::from_utf8(line).map_err(|err| ReplicationError {
                    message: err.to_string(),
                })?;
                let values = decode_text_row(&line_str, &options);
                rows.push(decode_row(table, &values)?);
            }
        }
        if !buffer.is_empty() {
            let mut line = std::mem::take(&mut buffer);
            if matches!(line.last(), Some(b'\r')) {
                line.pop();
            }
            if line != b"\\." {
                let line_str = String::from_utf8(line).map_err(|err| ReplicationError {
                    message: err.to_string(),
                })?;
                let values = decode_text_row(&line_str, &options);
                rows.push(decode_row(table, &values)?);
            }
        }

        with_storage_write(|storage| {
            storage.rows_by_table.insert(local_oid, rows.clone());
        });
    }

    Ok(())
}

fn decode_row(
    table: &TableSchema,
    values: &[Option<String>],
) -> Result<Vec<ScalarValue>, ReplicationError> {
    let mut row = Vec::with_capacity(values.len());
    for (idx, value) in values.iter().enumerate() {
        let Some(column) = table.columns.get(idx) else {
            return Err(ReplicationError {
                message: "COPY row has more columns than expected".to_string(),
            });
        };
        let cell = match value {
            Some(text) => decode_text_value(column.type_signature, text)?,
            None => ScalarValue::Null,
        };
        row.push(cell);
    }
    Ok(row)
}
