use crate::catalog::{ColumnSpec, TableKind, with_catalog_read, with_catalog_write};
use crate::commands::create_table::{
    column_spec_from_ast, foreign_key_constraint_specs_from_ast, key_constraint_specs_from_ast,
    type_signature_from_ast,
};
use crate::parser::ast::{AlterTableAction, AlterTableStatement, TableConstraint};
use crate::tcop::engine::{
    EngineError, QueryResult, ScalarValue, with_storage_read, with_storage_write,
};

pub async fn execute_alter_table(
    alter_table: &AlterTableStatement,
) -> Result<QueryResult, EngineError> {
    let table = with_catalog_read(|catalog| {
        catalog
            .resolve_table(
                &alter_table.table_name,
                &crate::catalog::SearchPath::default(),
            )
            .cloned()
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if table.kind() != TableKind::Heap {
        return Err(EngineError {
            message: format!(
                "cannot ALTER non-heap relation \"{}\"",
                table.qualified_name()
            ),
        });
    }
    crate::tcop::engine::require_relation_owner(&table)?;

    match &alter_table.action {
        AlterTableAction::AddColumn(column_def) => {
            let column_spec = column_spec_from_ast(column_def)?;
            let default_value = if let Some(default_expr) = &column_spec.default {
                let raw = crate::tcop::engine::eval_expr(
                    default_expr,
                    &crate::tcop::engine::EvalScope::default(),
                    &[],
                )
                .await?;
                Some(crate::tcop::engine::coerce_value_for_column_spec(
                    raw,
                    &column_spec,
                )?)
            } else {
                None
            };
            if !column_spec.nullable && default_value.is_none() {
                let has_rows = with_storage_read(|storage| {
                    storage
                        .rows_by_table
                        .get(&table.oid())
                        .is_some_and(|rows| !rows.is_empty())
                });
                if has_rows {
                    return Err(EngineError {
                        message:
                            "ALTER TABLE ADD COLUMN with NOT NULL requires empty table or DEFAULT"
                                .to_string(),
                    });
                }
            }

            with_catalog_write(|catalog| {
                catalog.add_column(table.schema_name(), table.name(), column_spec)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;

            with_storage_write(|storage| {
                let rows = storage.rows_by_table.entry(table.oid()).or_default();
                for row in rows.iter_mut() {
                    row.push(default_value.clone().unwrap_or(ScalarValue::Null));
                }
            });
        }
        AlterTableAction::AddConstraint(constraint) => {
            let current_rows = with_storage_read(|storage| {
                storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default()
            });
            let preview =
                crate::tcop::engine::preview_table_with_added_constraint(&table, constraint)?;
            crate::tcop::engine::validate_table_constraints(&preview, &current_rows).await?;

            match constraint {
                TableConstraint::PrimaryKey { .. } | TableConstraint::Unique { .. } => {
                    let mut specs =
                        key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
                    let spec = specs.pop().expect("one key spec for add constraint");
                    with_catalog_write(|catalog| {
                        catalog.add_key_constraint(table.schema_name(), table.name(), spec)
                    })
                    .map_err(|err| EngineError {
                        message: err.message,
                    })?;
                }
                TableConstraint::ForeignKey { .. } => {
                    let mut specs =
                        foreign_key_constraint_specs_from_ast(std::slice::from_ref(constraint))?;
                    let spec = specs.pop().expect("one fk spec for add constraint");
                    with_catalog_write(|catalog| {
                        catalog.add_foreign_key_constraint(table.schema_name(), table.name(), spec)
                    })
                    .map_err(|err| EngineError {
                        message: err.message,
                    })?;
                }
            }
        }
        AlterTableAction::DropColumn { name } => {
            let dropped_index = with_catalog_write(|catalog| {
                catalog.drop_column(table.schema_name(), table.name(), name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;

            with_storage_write(|storage| {
                let rows = storage.rows_by_table.entry(table.oid()).or_default();
                for row in rows.iter_mut() {
                    if dropped_index < row.len() {
                        row.remove(dropped_index);
                    }
                }
            });
        }
        AlterTableAction::DropConstraint { name } => {
            with_catalog_write(|catalog| {
                catalog.drop_constraint(table.schema_name(), table.name(), name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::RenameColumn { old_name, new_name } => {
            with_catalog_write(|catalog| {
                catalog.rename_column(table.schema_name(), table.name(), old_name, new_name)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::SetColumnType {
            name,
            data_type,
            using,
        } => {
            let target_signature = type_signature_from_ast(data_type.clone());
            let column_index = crate::tcop::engine::find_column_index(&table, name)?;
            let Some(target_column) = table.columns().get(column_index) else {
                return Err(EngineError {
                    message: format!(
                        "column \"{}\" of relation \"{}\" does not exist",
                        name,
                        table.qualified_name()
                    ),
                });
            };

            let mut target_spec =
                ColumnSpec::new(target_column.name().to_string(), target_signature);
            target_spec.nullable = target_column.nullable();

            let current_rows = with_storage_read(|storage| {
                storage
                    .rows_by_table
                    .get(&table.oid())
                    .cloned()
                    .unwrap_or_default()
            });

            let mut rewritten_rows = Vec::with_capacity(current_rows.len());
            for mut row in current_rows {
                let source_value = if let Some(using_expr) = using {
                    let mut scope = crate::tcop::engine::EvalScope::default();
                    for (column, value) in table.columns().iter().zip(row.iter()) {
                        scope.insert_unqualified(column.name(), value.clone());
                        scope.insert_qualified(
                            &format!("{}.{}", table.name(), column.name()),
                            value.clone(),
                        );
                        scope.insert_qualified(
                            &format!("{}.{}.{}", table.schema_name(), table.name(), column.name()),
                            value.clone(),
                        );
                    }
                    crate::tcop::engine::eval_expr(using_expr, &scope, &[]).await?
                } else {
                    row.get(column_index).cloned().unwrap_or(ScalarValue::Null)
                };
                let coerced =
                    crate::tcop::engine::coerce_value_for_column_spec(source_value, &target_spec)?;
                if column_index < row.len() {
                    row[column_index] = coerced;
                }
                rewritten_rows.push(row);
            }

            with_storage_write(|storage| {
                storage.rows_by_table.insert(table.oid(), rewritten_rows);
            });
            with_catalog_write(|catalog| {
                catalog.set_column_type(table.schema_name(), table.name(), name, target_signature)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::SetColumnNullable { name, nullable } => {
            if !*nullable {
                let index = crate::tcop::engine::find_column_index(&table, name)?;
                let has_nulls = with_storage_read(|storage| {
                    storage.rows_by_table.get(&table.oid()).is_some_and(|rows| {
                        rows.iter()
                            .any(|row| matches!(row.get(index), Some(ScalarValue::Null)))
                    })
                });
                if has_nulls {
                    return Err(EngineError {
                        message: format!(
                            "column \"{}\" of relation \"{}\" contains null values",
                            name,
                            table.qualified_name()
                        ),
                    });
                }
            }

            with_catalog_write(|catalog| {
                catalog.set_column_nullable(table.schema_name(), table.name(), name, *nullable)
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
        AlterTableAction::SetColumnDefault { name, default } => {
            with_catalog_write(|catalog| {
                catalog.set_column_default(table.schema_name(), table.name(), name, default.clone())
            })
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        }
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "ALTER TABLE".to_string(),
        rows_affected: 0,
    })
}
