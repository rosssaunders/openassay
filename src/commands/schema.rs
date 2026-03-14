use crate::catalog::dependency::{
    describe_table, expand_and_order_relation_drop,
    plan_sequence_drop as plan_sequence_drop_in_catalog,
};
use crate::catalog::{with_catalog_read, with_catalog_write};
use crate::commands::sequence::{with_sequences_read, with_sequences_write};
use crate::parser::ast::{CreateSchemaStatement, DropBehavior, DropSchemaStatement};
use crate::tcop::engine::{EngineError, QueryResult};

pub async fn execute_create_schema(
    create: &CreateSchemaStatement,
) -> Result<QueryResult, EngineError> {
    let created = with_catalog_write(|catalog| catalog.create_schema(&create.name));
    match created {
        Ok(_) => Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE SCHEMA".to_string(),
            rows_affected: 0,
        }),
        Err(err) if create.if_not_exists && err.message.contains("already exists") => {
            Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "CREATE SCHEMA".to_string(),
                rows_affected: 0,
            })
        }
        Err(err) => Err(EngineError {
            message: err.message,
        }),
    }
}

pub async fn execute_drop_schema(
    drop_schema: &DropSchemaStatement,
) -> Result<QueryResult, EngineError> {
    let schema_name = drop_schema.name.clone();
    let schema_tables = with_catalog_read(|catalog| {
        catalog
            .schema(&schema_name)
            .map(|schema| schema.tables().map(|table| table.oid()).collect::<Vec<_>>())
    });
    let Some(initial_tables) = schema_tables else {
        if drop_schema.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP SCHEMA".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("schema \"{}\" does not exist", drop_schema.name),
        });
    };

    let schema_sequences = with_sequences_read(|sequences| {
        let prefix = format!("{schema_name}.");
        sequences
            .keys()
            .filter(|name| name.starts_with(&prefix))
            .cloned()
            .collect::<Vec<_>>()
    });

    if matches!(drop_schema.behavior, DropBehavior::Restrict)
        && (!initial_tables.is_empty() || !schema_sequences.is_empty())
    {
        return Err(EngineError {
            message: format!("schema \"{}\" is not empty", drop_schema.name),
        });
    }

    let mut relation_oids = initial_tables;
    if matches!(drop_schema.behavior, DropBehavior::Cascade) && !schema_sequences.is_empty() {
        let sequence_plans = with_catalog_read(|catalog| {
            schema_sequences
                .iter()
                .map(|sequence| plan_sequence_drop_in_catalog(catalog, sequence, true))
                .collect::<Result<Vec<_>, _>>()
        })
        .map_err(|err| EngineError {
            message: err.message,
        })?;
        for plan in sequence_plans {
            for oid in plan.relation_drop_order {
                if !relation_oids.contains(&oid) {
                    relation_oids.push(oid);
                }
            }
        }
    }

    let drop_order = with_catalog_read(|catalog| {
        expand_and_order_relation_drop(
            catalog,
            &relation_oids,
            matches!(drop_schema.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;
    for relation_oid in &drop_order {
        let relation = with_catalog_read(|catalog| describe_table(catalog, *relation_oid))
            .map_err(|err| EngineError {
                message: err.message,
            })?;
        let relation_table = with_catalog_read(|catalog| {
            catalog
                .table(&relation.schema_name, &relation.table_name)
                .cloned()
        })
        .ok_or_else(|| EngineError {
            message: format!(
                "relation \"{}.{}\" does not exist",
                relation.schema_name, relation.table_name
            ),
        })?;
        crate::tcop::engine::require_relation_owner(&relation_table)?;
    }
    crate::commands::drop::drop_relations_by_oid_order(&drop_order)?;

    if !schema_sequences.is_empty() {
        for sequence_name in &schema_sequences {
            with_catalog_write(|catalog| {
                catalog.clear_sequence_defaults(sequence_name);
            });
        }
        with_sequences_write(|sequences| {
            for sequence_name in &schema_sequences {
                sequences.remove(sequence_name);
            }
        });
    }

    with_catalog_write(|catalog| catalog.drop_schema(&schema_name)).map_err(|err| EngineError {
        message: err.message,
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP SCHEMA".to_string(),
        rows_affected: (drop_order.len() + schema_sequences.len()) as u64,
    })
}
