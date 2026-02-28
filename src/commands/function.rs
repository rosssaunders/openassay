use crate::parser::ast::{CreateFunctionStatement, DropFunctionStatement};
use crate::tcop::engine::{
    EngineError, QueryResult, UserFunction, same_function_identity, with_ext_write,
};

pub async fn execute_create_function(
    create: &CreateFunctionStatement,
) -> Result<QueryResult, EngineError> {
    let uf = UserFunction {
        name: create.name.iter().map(|s| s.to_ascii_lowercase()).collect(),
        params: create.params.clone(),
        return_type: create.return_type.clone(),
        is_trigger: create.is_trigger,
        body: create.body.trim().to_string(),
        language: create.language.clone(),
    };
    with_ext_write(|ext| {
        let has_existing = ext
            .user_functions
            .iter()
            .any(|existing| same_function_identity(existing, &uf));
        if create.or_replace || has_existing {
            ext.user_functions
                .retain(|existing| !same_function_identity(existing, &uf));
        }
        ext.user_functions.push(uf);
        Ok(())
    })?;
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE FUNCTION".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_function(
    drop: &DropFunctionStatement,
) -> Result<QueryResult, EngineError> {
    let normalized_name: Vec<String> = drop.name.iter().map(|s| s.to_ascii_lowercase()).collect();
    let removed = with_ext_write(|ext| {
        let matches_name = |existing: &UserFunction| {
            if existing.name == normalized_name {
                return true;
            }
            normalized_name.len() == 1 && existing.name.last() == normalized_name.last()
        };
        if let Some(position) = ext.user_functions.iter().position(matches_name) {
            ext.user_functions.remove(position);
            1usize
        } else {
            0usize
        }
    });
    if removed == 0 && !drop.if_exists {
        return Err(EngineError {
            message: format!("function {} does not exist", drop.name.join(".")),
        });
    }
    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP FUNCTION".to_string(),
        rows_affected: 0,
    })
}
