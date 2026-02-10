use crate::catalog::search_path::SearchPath;
use crate::catalog::{ColumnSpec, TableKind, TypeSignature, with_catalog_read, with_catalog_write};
use crate::replication::pgoutput::RelationMessage;
use crate::replication::tuple_decoder::type_signature_for_oid;
use crate::replication::ReplicationError;
use crate::storage::heap::with_storage_write;

#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub name: String,
    pub type_oid: u32,
    pub type_signature: TypeSignature,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone)]
pub struct TableSchema {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    pub columns: Vec<ColumnSchema>,
}

pub async fn fetch_publication_schema(
    client: &tokio_postgres::Client,
    publication: &str,
) -> Result<Vec<TableSchema>, ReplicationError> {
    let tables = client
        .query(
            "SELECT p.relid, n.nspname, c.relname \
             FROM pg_publication_tables p \
             JOIN pg_class c ON c.oid = p.relid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE p.pubname = $1",
            &[&publication],
        )
        .await?;
    let mut out = Vec::with_capacity(tables.len());
    for row in tables {
        let relid: u32 = row.get(0);
        let namespace: String = row.get(1);
        let name: String = row.get(2);
        let columns = fetch_table_columns(client, relid).await?;
        out.push(TableSchema {
            relation_id: relid,
            namespace,
            name,
            columns,
        });
    }
    Ok(out)
}

pub fn ensure_local_table(schema: &TableSchema) -> Result<crate::catalog::oid::Oid, ReplicationError> {
    if let Ok(oid) = with_catalog_read(|catalog| {
        catalog
            .resolve_table(
                &[schema.namespace.clone(), schema.name.clone()],
                &SearchPath::default(),
            )
            .map(|table| table.oid())
    }) {
        return Ok(oid);
    }

    let mut columns = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        let mut spec = ColumnSpec::new(column.name.clone(), column.type_signature);
        if !column.nullable {
            spec = spec.not_null();
        }
        if column.primary_key {
            spec = spec.primary_key();
        }
        columns.push(spec);
    }

    let oid = with_catalog_write(|catalog| {
        catalog.create_table(
            &schema.namespace,
            &schema.name,
            TableKind::Heap,
            columns,
            Vec::new(),
            Vec::new(),
        )
    })
    .map_err(|err| ReplicationError {
        message: err.message,
    })?;

    with_storage_write(|storage| {
        storage.rows_by_table.entry(oid).or_default();
    });

    Ok(oid)
}

pub fn ensure_relation_from_message(
    message: &RelationMessage,
) -> Result<crate::catalog::oid::Oid, ReplicationError> {
    let columns = message
        .columns
        .iter()
        .map(|column| ColumnSchema {
            name: column.name.clone(),
            type_oid: column.type_oid,
            type_signature: type_signature_for_oid(column.type_oid),
            nullable: true,
            primary_key: column.flags & 0x01 != 0,
        })
        .collect::<Vec<_>>();
    let schema = TableSchema {
        relation_id: message.relation_id,
        namespace: message.namespace.clone(),
        name: message.name.clone(),
        columns,
    };
    ensure_local_table(&schema)
}

async fn fetch_table_columns(
    client: &tokio_postgres::Client,
    relid: u32,
) -> Result<Vec<ColumnSchema>, ReplicationError> {
    let rows = client
        .query(
            "SELECT a.attname, a.atttypid, a.attnotnull, a.attnum, \
                    COALESCE(i.indisprimary, false) \
             FROM pg_attribute a \
             LEFT JOIN pg_index i \
               ON i.indrelid = a.attrelid \
              AND a.attnum = ANY(i.indkey) \
             WHERE a.attrelid = $1 \
               AND a.attnum > 0 \
               AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&relid],
        )
        .await?;
    let mut columns = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.get(0);
        let type_oid: u32 = row.get(1);
        let not_null: bool = row.get(2);
        let primary_key: bool = row.get(4);
        columns.push(ColumnSchema {
            name,
            type_oid,
            type_signature: type_signature_for_oid(type_oid),
            nullable: !not_null,
            primary_key,
        });
    }
    Ok(columns)
}
