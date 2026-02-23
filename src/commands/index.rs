use crate::catalog::dependency::index_backing_constraint_name as index_backing_constraint_name_in_catalog;
use crate::catalog::{IndexSpec, SearchPath, TableKind, with_catalog_read, with_catalog_write};
use crate::parser::ast::{CreateIndexStatement, DropBehavior, DropIndexStatement};
use crate::tcop::engine::{
    EngineError, QueryResult, validate_table_constraints, with_storage_read, with_storage_write,
};

#[derive(Debug, Clone)]
struct IndexTarget {
    schema_name: String,
    table_name: String,
    index_name: String,
}

pub async fn execute_create_index(
    create: &CreateIndexStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(&create.table_name, &SearchPath::default())
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if !matches!(table.kind(), TableKind::Heap | TableKind::MaterializedView) {
        return Err(EngineError {
            message: format!(
                "cannot CREATE INDEX on non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    crate::tcop::engine::require_relation_owner(&table)?;

    let index_name = create.name.to_ascii_lowercase();
    let index_columns = create
        .columns
        .iter()
        .map(|column| column.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let index_column_indexes = resolve_index_column_indexes(&table, &index_columns)?;

    if create.unique {
        let key_spec = crate::catalog::KeyConstraintSpec {
            name: Some(index_name.clone()),
            columns: index_columns.clone(),
            primary: false,
        };
        let preview = {
            let mut preview = table.clone();
            preview
                .key_constraints_mut()
                .push(crate::catalog::KeyConstraint {
                    name: key_spec.name.clone(),
                    columns: key_spec.columns.clone(),
                    primary: false,
                });
            preview
        };
        let rows = with_storage_read(|storage| {
            storage
                .rows_by_table
                .get(&table.oid())
                .cloned()
                .unwrap_or_default()
        });
        validate_table_constraints(&preview, &rows).await?;

        let result = with_catalog_write(|catalog| {
            catalog.add_key_constraint(table.schema_name(), table.name(), key_spec)?;
            let index = IndexSpec {
                name: index_name.clone(),
                columns: index_columns.clone(),
                unique: true,
            };
            if let Err(err) = catalog.add_index(table.schema_name(), table.name(), index) {
                let _ = catalog.drop_constraint(table.schema_name(), table.name(), &index_name);
                return Err(err);
            }
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(err) => {
                if create.if_not_exists && err.message.contains("already exists") {
                    return Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        command_tag: "CREATE INDEX".to_string(),
                        rows_affected: 0,
                    });
                }
                return Err(EngineError {
                    message: err.message,
                });
            }
        }
    } else {
        let index = IndexSpec {
            name: index_name.clone(),
            columns: index_columns.clone(),
            unique: false,
        };
        let result = with_catalog_write(|catalog| {
            catalog.add_index(table.schema_name(), table.name(), index)
        });
        match result {
            Ok(()) => {}
            Err(err) => {
                if create.if_not_exists && err.message.contains("already exists") {
                    return Ok(QueryResult {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        command_tag: "CREATE INDEX".to_string(),
                        rows_affected: 0,
                    });
                }
                return Err(EngineError {
                    message: err.message,
                });
            }
        }
    }

    let register_result = with_storage_write(|storage| {
        storage.register_index(
            table.oid(),
            index_name.clone(),
            index_columns.clone(),
            index_column_indexes.clone(),
            create.unique,
        )?;
        storage.rebuild_index(table.oid(), &index_name)?;
        Ok::<(), String>(())
    });
    if let Err(err) = register_result {
        with_storage_write(|storage| {
            storage.drop_index(table.oid(), &index_name);
        });
        let _ = with_catalog_write(|catalog| {
            catalog.drop_index(
                table.schema_name(),
                table.name(),
                &index_name,
                true,
            )
        });
        return Err(EngineError { message: err });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE INDEX".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_index(
    drop_index: &DropIndexStatement,
) -> Result<QueryResult, EngineError> {
    let resolved = resolve_index_target(&drop_index.name)?;
    let Some(target) = resolved else {
        if drop_index.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP INDEX".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("index \"{}\" does not exist", drop_index.name.join(".")),
        });
    };
    let relation = with_catalog_read(|catalog| {
        catalog
            .table(&target.schema_name, &target.table_name)
            .cloned()
    })
    .ok_or_else(|| EngineError {
        message: format!(
            "relation \"{}.{}\" does not exist",
            target.schema_name, target.table_name
        ),
    })?;
    crate::tcop::engine::require_relation_owner(&relation)?;

    let backing_constraint = with_catalog_read(|catalog| {
        index_backing_constraint_name_in_catalog(
            catalog,
            &target.schema_name,
            &target.table_name,
            &target.index_name,
        )
    });
    if let Some(constraint_name) = backing_constraint
        && matches!(drop_index.behavior, DropBehavior::Restrict)
    {
        return Err(EngineError {
            message: format!(
                "cannot drop index \"{}\" because constraint \"{}\" on relation \"{}.{}\" depends on it",
                target.index_name, constraint_name, target.schema_name, target.table_name
            ),
        });
    }

    with_catalog_write(|catalog| {
        catalog.drop_index(
            &target.schema_name,
            &target.table_name,
            &target.index_name,
            matches!(drop_index.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    with_storage_write(|storage| {
        storage.drop_index(relation.oid(), &target.index_name);
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP INDEX".to_string(),
        rows_affected: 0,
    })
}

fn resolve_index_column_indexes(
    table: &crate::catalog::Table,
    index_columns: &[String],
) -> Result<Vec<usize>, EngineError> {
    let mut out = Vec::with_capacity(index_columns.len());
    for column_name in index_columns {
        let Some((idx, _)) = table
            .columns()
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == column_name)
        else {
            return Err(EngineError {
                message: format!(
                    "column \"{column_name}\" does not exist in relation \"{}\"",
                    table.qualified_name()
                ),
            });
        };
        out.push(idx);
    }
    Ok(out)
}

fn resolve_index_target(index_name: &[String]) -> Result<Option<IndexTarget>, EngineError> {
    match index_name {
        [name] => {
            let normalized_name = name.to_ascii_lowercase();
            let mut matches = Vec::new();
            with_catalog_read(|catalog| {
                for schema_name in SearchPath::default().schemas() {
                    let Some(schema) = catalog.schema(schema_name) else {
                        continue;
                    };
                    for table in schema.tables() {
                        if table
                            .indexes()
                            .iter()
                            .any(|index| index.name == normalized_name)
                        {
                            matches.push(IndexTarget {
                                schema_name: schema.name().to_string(),
                                table_name: table.name().to_string(),
                                index_name: normalized_name.clone(),
                            });
                        }
                    }
                }
            });
            if matches.len() > 1 {
                return Err(EngineError {
                    message: format!("index reference \"{name}\" is ambiguous"),
                });
            }
            Ok(matches.pop())
        }
        [schema_name, name] => {
            let normalized_schema = schema_name.to_ascii_lowercase();
            let normalized_name = name.to_ascii_lowercase();
            let mut found = None;
            with_catalog_read(|catalog| {
                if let Some(schema) = catalog.schema(&normalized_schema) {
                    for table in schema.tables() {
                        if table
                            .indexes()
                            .iter()
                            .any(|index| index.name == normalized_name)
                        {
                            found = Some(IndexTarget {
                                schema_name: normalized_schema.clone(),
                                table_name: table.name().to_string(),
                                index_name: normalized_name.clone(),
                            });
                            break;
                        }
                    }
                }
            });
            Ok(found)
        }
        _ => Err(EngineError {
            message: format!("invalid index name \"{}\"", index_name.join(".")),
        }),
    }
}
