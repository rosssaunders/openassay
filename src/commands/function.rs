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
        if create.or_replace {
            ext.user_functions
                .retain(|existing| !same_function_identity(existing, &uf));
        } else if ext
            .user_functions
            .iter()
            .any(|existing| same_function_identity(existing, &uf))
        {
            return Err(EngineError {
                message: format!("function \"{}\" already exists", uf.name.join(".")),
            });
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
        let before = ext.user_functions.len();
        ext.user_functions
            .retain(|existing| existing.name != normalized_name);
        before - ext.user_functions.len()
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
