use crate::catalog::with_catalog_write;
use crate::parser::ast::{
    CreateCastStatement, CreateDomainStatement, CreateTypeStatement, DropDomainStatement,
    DropTypeStatement,
};
use crate::tcop::engine::{
    EngineError, ExtensionState, QueryResult, UserCompositeType, UserDomain, UserEnumType,
    UserRangeType, with_ext_write,
};

fn name_exists(ext: &ExtensionState, name: &[String]) -> bool {
    ext.user_types.iter().any(|t| t.name == name)
        || ext.user_composite_types.iter().any(|t| t.name == name)
        || ext.user_range_types.iter().any(|t| t.name == name)
}

pub async fn execute_create_type(create: &CreateTypeStatement) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = create.name.clone();

    if !create.as_composite.is_empty() {
        // Reject duplicates before allocating OIDs so failed CREATEs don't
        // burn OID space.
        with_ext_write(|ext| {
            if name_exists(ext, &normalized_name) {
                Err(EngineError {
                    message: format!("type \"{}\" already exists", create.name.join(".")),
                })
            } else {
                Ok(())
            }
        })?;

        let (type_oid, class_oid) = with_catalog_write(|catalog| {
            let type_oid = catalog
                .next_oid()
                .map_err(|e| EngineError { message: e.message })?;
            let class_oid = catalog
                .next_oid()
                .map_err(|e| EngineError { message: e.message })?;
            Ok::<_, EngineError>((type_oid, class_oid))
        })?;

        let attributes = create
            .as_composite
            .iter()
            .map(|attr| (attr.name.clone(), attr.data_type.clone()))
            .collect();

        with_ext_write(|ext| {
            if name_exists(ext, &normalized_name) {
                return Err(EngineError {
                    message: format!("type \"{}\" already exists", create.name.join(".")),
                });
            }
            ext.user_composite_types.push(UserCompositeType {
                oid: type_oid,
                class_oid,
                name: normalized_name,
                attributes,
            });
            Ok(())
        })?;

        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TYPE".to_string(),
            rows_affected: 0,
        });
    }

    if let Some(subtype) = &create.as_range_subtype {
        with_ext_write(|ext| {
            if name_exists(ext, &normalized_name) {
                Err(EngineError {
                    message: format!("type \"{}\" already exists", create.name.join(".")),
                })
            } else {
                Ok(())
            }
        })?;

        let subtype_oid = crate::commands::create_table::sql_type_from_ast(subtype).oid();

        let type_oid = with_catalog_write(|catalog| {
            catalog
                .next_oid()
                .map_err(|e| EngineError { message: e.message })
        })?;

        with_ext_write(|ext| {
            if name_exists(ext, &normalized_name) {
                return Err(EngineError {
                    message: format!("type \"{}\" already exists", create.name.join(".")),
                });
            }
            ext.user_range_types.push(UserRangeType {
                oid: type_oid,
                subtype_oid,
                name: normalized_name,
            });
            Ok(())
        })?;

        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TYPE".to_string(),
            rows_affected: 0,
        });
    }

    if create.as_enum.is_empty() {
        // Non-enum, non-composite, non-range CREATE TYPE is accepted but not
        // stored (shell type).
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TYPE".to_string(),
            rows_affected: 0,
        });
    }

    with_ext_write(|ext| {
        if name_exists(ext, &normalized_name) {
            Err(EngineError {
                message: format!("type \"{}\" already exists", create.name.join(".")),
            })
        } else {
            Ok(())
        }
    })?;

    let type_oid = with_catalog_write(|catalog| {
        catalog
            .next_oid()
            .map_err(|e| EngineError { message: e.message })
    })?;

    with_ext_write(|ext| {
        if name_exists(ext, &normalized_name) {
            return Err(EngineError {
                message: format!("type \"{}\" already exists", create.name.join(".")),
            });
        }
        ext.user_types.push(UserEnumType {
            oid: type_oid,
            name: normalized_name,
            labels: create.as_enum.clone(),
        });
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE TYPE".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_create_cast(
    _create: &CreateCastStatement,
) -> Result<QueryResult, EngineError> {
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE CAST".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_type(drop: &DropTypeStatement) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = drop.name.clone();

    with_ext_write(|ext| {
        ext.user_types.retain(|t| t.name != normalized_name);
        ext.user_composite_types
            .retain(|t| t.name != normalized_name);
        ext.user_range_types.retain(|t| t.name != normalized_name);
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP TYPE".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_create_domain(
    create: &CreateDomainStatement,
) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = create.name.clone();
    let base_type = format!("{:?}", create.base_type).to_ascii_lowercase();

    with_ext_write(|ext| {
        if ext.user_domains.iter().any(|d| d.name == normalized_name) {
            return Err(EngineError {
                message: format!("type \"{}\" already exists", create.name.join(".")),
            });
        }
        ext.user_domains.push(UserDomain {
            name: normalized_name,
            base_type,
        });
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE DOMAIN".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_domain(drop: &DropDomainStatement) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = drop.name.clone();

    let removed = with_ext_write(|ext| {
        let before = ext.user_domains.len();
        ext.user_domains.retain(|d| d.name != normalized_name);
        before - ext.user_domains.len()
    });

    if removed == 0 && !drop.if_exists {
        return Err(EngineError {
            message: format!("type \"{}\" does not exist", drop.name.join(".")),
        });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP DOMAIN".to_string(),
        rows_affected: 0,
    })
}
