use crate::catalog::{ColumnSpec, TableKind, TypeSignature, with_catalog_write};
use crate::commands::sequence::{SequenceState, with_sequences_read, with_sequences_write};
use crate::parser::ast::{CreateTableStatement, Expr, TableConstraint, TypeName};
use crate::security;
use crate::tcop::engine::{EngineError, QueryResult, with_storage_write};

pub async fn execute_create_table(
    create: &CreateTableStatement,
) -> Result<QueryResult, EngineError> {
    // Handle CREATE TABLE AS SELECT (CTAS)
    if let Some(query) = &create.as_select {
        return execute_create_table_as_select(create, query).await;
    }

    if create.columns.is_empty() {
        return Err(EngineError {
            message: "CREATE TABLE requires at least one column".to_string(),
        });
    }

    let (schema_name, table_name) = relation_name_for_create(&create.name)?;
    let mut transformed_columns = create.columns.clone();
    let mut identity_sequence_names = Vec::new();
    // SERIAL/BIGSERIAL columns are syntactic sugar for IDENTITY — treat them the same way
    for column in &mut transformed_columns {
        if matches!(column.data_type, TypeName::Serial | TypeName::BigSerial) && !column.identity {
            column.identity = true;
        }
    }

    for column in &mut transformed_columns {
        if !column.identity {
            continue;
        }
        if column.default.is_some() {
            return Err(EngineError {
                message: format!(
                    "column \"{}\" cannot specify both IDENTITY and DEFAULT",
                    column.name
                ),
            });
        }
        let sequence_name = format!(
            "{}.{}_{}_seq",
            schema_name,
            table_name,
            column.name.to_ascii_lowercase()
        );
        column.default = Some(Expr::FunctionCall {
            name: vec!["nextval".to_string()],
            args: vec![Expr::String(sequence_name.clone())],
            distinct: false,
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter: None,
            over: None,
        });
        column.nullable = false;
        identity_sequence_names.push(sequence_name);
    }
    if !identity_sequence_names.is_empty() {
        let existing = with_sequences_read(|sequences| {
            identity_sequence_names
                .iter()
                .find(|name| sequences.contains_key(*name))
                .cloned()
        });
        if let Some(name) = existing {
            return Err(EngineError {
                message: format!("sequence \"{name}\" already exists"),
            });
        }
    }

    let column_specs = transformed_columns
        .iter()
        .map(column_spec_from_ast)
        .collect::<Result<Vec<_>, _>>()?;
    let key_specs = key_constraint_specs_from_ast(&create.constraints)?;
    let foreign_key_specs = foreign_key_constraint_specs_from_ast(&create.constraints)?;

    // For now, temporary tables work like regular heap tables in memory
    let table_kind = TableKind::Heap;

    let table_oid = with_catalog_write(|catalog| {
        catalog.create_table(
            &schema_name,
            &table_name,
            table_kind,
            column_specs,
            key_specs,
            foreign_key_specs,
        )
    });

    // Handle IF NOT EXISTS
    let table_oid = match table_oid {
        Ok(oid) => oid,
        Err(err) => {
            // If the table already exists and IF NOT EXISTS was specified, return success silently
            if create.if_not_exists && err.message.contains("already exists") {
                return Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    command_tag: "CREATE TABLE".to_string(),
                    rows_affected: 0,
                });
            }
            return Err(EngineError {
                message: err.message,
            });
        }
    };

    with_storage_write(|storage| {
        storage.rows_by_table.entry(table_oid).or_default();
    });
    security::set_relation_owner(table_oid, &security::current_role());
    if !identity_sequence_names.is_empty() {
        with_sequences_write(|sequences| {
            for name in identity_sequence_names {
                sequences.insert(
                    name,
                    SequenceState {
                        start: 1,
                        current: 1,
                        increment: 1,
                        min_value: 1,
                        max_value: i64::MAX,
                        cycle: false,
                        cache: 1,
                        called: false,
                    },
                );
            }
        });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE TABLE".to_string(),
        rows_affected: 0,
    })
}

pub(crate) fn relation_name_for_create(name: &[String]) -> Result<(String, String), EngineError> {
    match name {
        [table_name] => Ok(("public".to_string(), table_name.to_ascii_lowercase())),
        [schema_name, table_name] => Ok((
            schema_name.to_ascii_lowercase(),
            table_name.to_ascii_lowercase(),
        )),
        _ => Err(EngineError {
            message: format!("invalid relation name \"{}\"", name.join(".")),
        }),
    }
}

pub(crate) fn column_spec_from_ast(
    column: &crate::parser::ast::ColumnDefinition,
) -> Result<ColumnSpec, EngineError> {
    if column.name.trim().is_empty() {
        return Err(EngineError {
            message: "column name cannot be empty".to_string(),
        });
    }

    let references = if let Some(reference) = &column.references {
        if reference.table_name.is_empty() {
            return Err(EngineError {
                message: format!("column \"{}\" has invalid REFERENCES target", column.name),
            });
        }
        Some(crate::catalog::ForeignKeySpec {
            table_name: reference
                .table_name
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect(),
            column_name: reference
                .column_name
                .as_ref()
                .map(|name| name.to_ascii_lowercase()),
            on_delete: reference.on_delete,
            on_update: reference.on_update,
        })
    } else {
        None
    };

    Ok(ColumnSpec {
        name: column.name.to_ascii_lowercase(),
        type_signature: type_signature_from_ast(column.data_type.clone()),
        nullable: column.nullable && !column.primary_key,
        unique: column.unique || column.primary_key,
        primary_key: column.primary_key,
        references,
        check: column.check.clone(),
        default: column.default.clone(),
    })
}

pub(crate) fn key_constraint_specs_from_ast(
    constraints: &[TableConstraint],
) -> Result<Vec<crate::catalog::KeyConstraintSpec>, EngineError> {
    let mut out = Vec::new();
    for constraint in constraints {
        match constraint {
            TableConstraint::PrimaryKey { name, columns } => {
                if columns.is_empty() {
                    return Err(EngineError {
                        message: "PRIMARY KEY requires at least one column".to_string(),
                    });
                }
                out.push(crate::catalog::KeyConstraintSpec {
                    name: name.as_ref().map(|name| name.to_ascii_lowercase()),
                    columns: columns
                        .iter()
                        .map(|column| column.to_ascii_lowercase())
                        .collect(),
                    primary: true,
                });
            }
            TableConstraint::Unique { name, columns } => {
                if columns.is_empty() {
                    return Err(EngineError {
                        message: "UNIQUE requires at least one column".to_string(),
                    });
                }
                out.push(crate::catalog::KeyConstraintSpec {
                    name: name.as_ref().map(|name| name.to_ascii_lowercase()),
                    columns: columns
                        .iter()
                        .map(|column| column.to_ascii_lowercase())
                        .collect(),
                    primary: false,
                });
            }
            TableConstraint::ForeignKey { .. } => {}
        }
    }
    Ok(out)
}

pub(crate) fn foreign_key_constraint_specs_from_ast(
    constraints: &[TableConstraint],
) -> Result<Vec<crate::catalog::ForeignKeyConstraintSpec>, EngineError> {
    let mut out = Vec::new();
    for constraint in constraints {
        let TableConstraint::ForeignKey {
            name,
            columns,
            referenced_table,
            referenced_columns,
            on_delete,
            on_update,
        } = constraint
        else {
            continue;
        };
        if columns.is_empty() {
            return Err(EngineError {
                message: "FOREIGN KEY requires at least one referencing column".to_string(),
            });
        }
        if referenced_table.is_empty() {
            return Err(EngineError {
                message: "FOREIGN KEY REFERENCES target cannot be empty".to_string(),
            });
        }
        if !referenced_columns.is_empty() && columns.len() != referenced_columns.len() {
            return Err(EngineError {
                message: format!(
                    "FOREIGN KEY has {} referencing columns but {} referenced columns",
                    columns.len(),
                    referenced_columns.len()
                ),
            });
        }

        out.push(crate::catalog::ForeignKeyConstraintSpec {
            name: name.as_ref().map(|name| name.to_ascii_lowercase()),
            columns: columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect(),
            referenced_table: referenced_table
                .iter()
                .map(|part| part.to_ascii_lowercase())
                .collect(),
            referenced_columns: referenced_columns
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect(),
            on_delete: *on_delete,
            on_update: *on_update,
        });
    }
    Ok(out)
}

pub(crate) fn type_signature_from_ast(ty: TypeName) -> TypeSignature {
    match ty {
        TypeName::Bool => TypeSignature::Bool,
        TypeName::Int2
        | TypeName::Int4
        | TypeName::Int8
        | TypeName::Serial
        | TypeName::BigSerial => TypeSignature::Int8,
        TypeName::Float4 | TypeName::Float8 => TypeSignature::Float8,
        TypeName::Numeric => TypeSignature::Numeric,
        TypeName::Text
        | TypeName::Varchar
        | TypeName::Char
        | TypeName::Bytea
        | TypeName::Uuid
        | TypeName::Json
        | TypeName::Jsonb
        | TypeName::Interval
        | TypeName::Time => TypeSignature::Text,
        TypeName::Date => TypeSignature::Date,
        TypeName::Timestamp | TypeName::TimestampTz => TypeSignature::Timestamp,
        TypeName::Vector(dim) => TypeSignature::Vector(dim),
        TypeName::Array(_) | TypeName::Name => TypeSignature::Text,
    }
}

async fn execute_create_table_as_select(
    create: &CreateTableStatement,
    query: &crate::parser::ast::Query,
) -> Result<QueryResult, EngineError> {
    use crate::tcop::pquery::derive_query_output_columns;

    let (schema_name, table_name) = relation_name_for_create(&create.name)?;

    // Derive columns from the query
    let output_columns = derive_query_output_columns(query)?;

    // Convert output columns to column specs
    let column_specs: Vec<ColumnSpec> = output_columns
        .iter()
        .map(|col| {
            let type_signature = match col.type_oid {
                16 => TypeSignature::Bool,                                        // bool
                20 | 21 | 23 => TypeSignature::Int8, // bigint, smallint, int
                25 | 1043 | 1042 | 17 | 2950 | 114 | 3802 => TypeSignature::Text, // text, varchar, char, bytea, uuid, json, jsonb
                700 | 701 => TypeSignature::Float8,                               // float4, float8
                1700 => TypeSignature::Numeric,                                   // numeric
                1082 => TypeSignature::Date,                                      // date
                1114 | 1184 => TypeSignature::Timestamp, // timestamp, timestamptz
                6000 => TypeSignature::Vector(None),     // vector
                _ => TypeSignature::Text,                // default to text for unknown types
            };
            ColumnSpec {
                name: col.name.clone(),
                type_signature,
                nullable: true,
                unique: false,
                primary_key: false,
                references: None,
                check: None,
                default: None,
            }
        })
        .collect();

    // Create the table
    let table_oid = with_catalog_write(|catalog| {
        catalog.create_table(
            &schema_name,
            &table_name,
            TableKind::Heap,
            column_specs,
            Vec::new(), // no key constraints
            Vec::new(), // no foreign key constraints
        )
    });

    let table_oid = match table_oid {
        Ok(oid) => oid,
        Err(err) => {
            if create.if_not_exists && err.message.contains("already exists") {
                return Ok(QueryResult {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    command_tag: "CREATE TABLE".to_string(),
                    rows_affected: 0,
                });
            }
            return Err(EngineError {
                message: err.message,
            });
        }
    };

    with_storage_write(|storage| {
        storage.rows_by_table.entry(table_oid).or_default();
    });
    security::set_relation_owner(table_oid, &security::current_role());

    // Execute the query and insert results
    use crate::executor::exec_main::execute_query;
    let query_result = execute_query(query, &[]).await?;
    let row_count = query_result.rows.len() as u64;

    with_storage_write(|storage| {
        let rows = storage.rows_by_table.entry(table_oid).or_default();
        for row in query_result.rows {
            rows.push(row);
        }
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        // PostgreSQL returns "SELECT n" for CTAS, where n is the number of rows inserted
        command_tag: format!("SELECT {row_count}"),
        rows_affected: row_count,
    })
}
