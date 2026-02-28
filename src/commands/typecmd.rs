use crate::parser::ast::{
    CreateCastStatement, CreateDomainStatement, CreateTypeStatement, DropDomainStatement,
    DropTypeStatement,
};
use crate::tcop::engine::{EngineError, QueryResult, UserDomain, UserEnumType, with_ext_write};

pub async fn execute_create_type(create: &CreateTypeStatement) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = create.name.iter().map(|s| s.to_ascii_lowercase()).collect();

    if create.as_enum.is_empty() {
        // Non-enum CREATE TYPE is accepted but not stored (shell type or composite)
        return Ok(QueryResult {
            columns: Vec::new(),
            rows: Vec::new(),
            command_tag: "CREATE TYPE".to_string(),
            rows_affected: 0,
        });
    }

    with_ext_write(|ext| {
        if ext.user_types.iter().any(|t| t.name == normalized_name) {
            return Err(EngineError {
                message: format!("type \"{}\" already exists", create.name.join(".")),
            });
        }
        ext.user_types.push(UserEnumType {
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
    let normalized_name: Vec<String> = drop.name.iter().map(|s| s.to_ascii_lowercase()).collect();

    with_ext_write(|ext| {
        ext.user_types.retain(|t| t.name != normalized_name);
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
    let normalized_name: Vec<String> = create.name.iter().map(|s| s.to_ascii_lowercase()).collect();
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
    let normalized_name: Vec<String> = drop.name.iter().map(|s| s.to_ascii_lowercase()).collect();

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
