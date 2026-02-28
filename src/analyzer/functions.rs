//! Function and operator resolution.
//!
//! Validates function calls in expressions: checks that function names are
//! recognized and argument counts are plausible. This is a best-effort check;
//! the executor performs full resolution with runtime type information.

use crate::parser::ast::{Expr, SelectStatement};
use crate::tcop::engine::EngineError;

/// Known built-in function signatures: (name, min_args, max_args).
/// This is not exhaustive — the executor handles the full set. We only
/// validate the most common functions to catch obvious mistakes early.
const KNOWN_FUNCTIONS: &[(&str, usize, usize)] = &[
    // Aggregate functions
    ("count", 0, 1), // count(*) = 0 args after expansion, count(x) = 1
    ("sum", 1, 1),
    ("avg", 1, 1),
    ("min", 1, 1),
    ("max", 1, 1),
    ("array_agg", 1, 1),
    ("string_agg", 2, 2),
    ("bool_and", 1, 1),
    ("bool_or", 1, 1),
    ("every", 1, 1),
    ("bit_and", 1, 1),
    ("bit_or", 1, 1),
    // String functions
    ("length", 1, 1),
    ("char_length", 1, 1),
    ("character_length", 1, 1),
    ("octet_length", 1, 1),
    ("lower", 1, 1),
    ("upper", 1, 1),
    ("trim", 1, 3),
    ("ltrim", 1, 2),
    ("rtrim", 1, 2),
    ("btrim", 1, 2),
    ("lpad", 2, 3),
    ("rpad", 2, 3),
    ("substr", 2, 3),
    ("substring", 1, 3),
    ("replace", 3, 3),
    ("translate", 3, 3),
    ("concat", 1, 255),
    ("concat_ws", 2, 255),
    ("left", 2, 2),
    ("right", 2, 2),
    ("repeat", 2, 2),
    ("reverse", 1, 1),
    ("split_part", 3, 3),
    ("position", 2, 2),
    ("strpos", 2, 2),
    ("starts_with", 2, 2),
    ("encode", 2, 2),
    ("decode", 2, 2),
    ("md5", 1, 1),
    ("ascii", 1, 1),
    ("chr", 1, 1),
    ("initcap", 1, 1),
    ("regexp_replace", 3, 6),
    ("regexp_matches", 2, 3),
    ("regexp_count", 2, 4),
    ("regexp_instr", 2, 7),
    ("regexp_substr", 2, 6),
    ("regexp_like", 2, 3),
    ("regexp_match", 2, 3),
    ("regexp_split_to_array", 2, 3),
    ("string_to_table", 2, 3),
    ("to_hex", 1, 1),
    ("to_oct", 1, 1),
    ("to_bin", 1, 1),
    ("unistr", 1, 1),
    ("bit_length", 1, 1),
    ("set_byte", 3, 3),
    ("get_byte", 2, 2),
    ("format", 1, 255),
    ("to_char", 2, 2),
    ("to_number", 2, 2),
    ("to_date", 2, 2),
    ("to_timestamp", 1, 2),
    ("quote_literal", 1, 1),
    ("quote_ident", 1, 1),
    ("quote_nullable", 1, 1),
    // Math functions
    ("abs", 1, 1),
    ("ceil", 1, 1),
    ("ceiling", 1, 1),
    ("floor", 1, 1),
    ("round", 1, 2),
    ("trunc", 1, 2),
    ("mod", 2, 2),
    ("power", 2, 2),
    ("sqrt", 1, 1),
    ("cbrt", 1, 1),
    ("sign", 1, 1),
    ("log", 1, 2),
    ("log10", 1, 1),
    ("ln", 1, 1),
    ("exp", 1, 1),
    ("pi", 0, 0),
    ("random", 0, 0),
    ("greatest", 1, 255),
    ("least", 1, 255),
    ("div", 2, 2),
    ("bs_price", 6, 6),
    ("bs_greeks", 6, 6),
    ("barrier_price", 9, 9),
    ("american_price", 6, 7),
    ("heston_price", 10, 10),
    // Type/cast functions
    ("coalesce", 1, 255),
    ("nullif", 2, 2),
    // Date/time functions
    ("now", 0, 0),
    ("current_date", 0, 0),
    ("current_timestamp", 0, 0),
    ("date_trunc", 2, 2),
    ("date_part", 2, 2),
    ("extract", 2, 2),
    ("age", 1, 2),
    ("make_date", 3, 3),
    ("make_time", 3, 3),
    ("make_timestamp", 6, 6),
    ("make_interval", 0, 7),
    // Array functions
    ("array_length", 2, 2),
    ("array_upper", 2, 2),
    ("array_lower", 2, 2),
    ("unnest", 1, 1),
    ("array_position", 2, 3),
    ("array_remove", 2, 2),
    ("array_cat", 2, 2),
    ("array_append", 2, 2),
    ("array_prepend", 2, 2),
    ("cardinality", 1, 1),
    ("int4range", 2, 3),
    ("float8range", 2, 3),
    ("textrange", 2, 3),
    // JSON functions
    ("json_build_object", 0, 255),
    ("jsonb_build_object", 0, 255),
    ("json_build_array", 0, 255),
    ("jsonb_build_array", 0, 255),
    ("json_object", 1, 2),
    ("jsonb_object", 1, 2),
    ("json_agg", 1, 1),
    ("jsonb_agg", 1, 1),
    ("json_object_agg", 2, 2),
    ("jsonb_object_agg", 2, 2),
    ("json_array_length", 1, 1),
    ("jsonb_array_length", 1, 1),
    ("json_each", 1, 1),
    ("jsonb_each", 1, 1),
    ("json_each_text", 1, 1),
    ("jsonb_each_text", 1, 1),
    ("json_array_elements", 1, 1),
    ("jsonb_array_elements", 1, 1),
    ("json_array_elements_text", 1, 1),
    ("jsonb_array_elements_text", 1, 1),
    ("json_object_keys", 1, 1),
    ("jsonb_object_keys", 1, 1),
    ("json_extract_path", 2, 255),
    ("jsonb_extract_path", 2, 255),
    ("json_extract_path_text", 2, 255),
    ("jsonb_extract_path_text", 2, 255),
    ("json_typeof", 1, 1),
    ("jsonb_typeof", 1, 1),
    ("json_strip_nulls", 1, 2),
    ("jsonb_strip_nulls", 1, 2),
    ("row_to_json", 1, 2),
    ("array_to_json", 1, 2),
    ("to_json", 1, 1),
    ("to_jsonb", 1, 1),
    ("json_pretty", 1, 1),
    ("jsonb_pretty", 1, 1),
    ("jsonb_set", 3, 4),
    ("jsonb_concat", 2, 2),
    ("jsonb_contains", 2, 2),
    ("jsonb_contained", 2, 2),
    ("jsonb_delete", 2, 2),
    ("jsonb_delete_path", 2, 2),
    ("jsonb_insert", 3, 4),
    ("jsonb_set_lax", 3, 5),
    ("jsonb_path_exists", 2, 4),
    ("jsonb_path_match", 2, 4),
    ("jsonb_path_query", 2, 4),
    ("jsonb_path_query_array", 2, 4),
    ("jsonb_path_query_first", 2, 4),
    ("jsonb_exists", 2, 2),
    ("jsonb_exists_any", 2, 2),
    ("jsonb_exists_all", 2, 2),
    // HTTP extension functions
    ("http_get", 1, 2),
    ("http_post", 2, 3),
    ("http_put", 3, 3),
    ("http_patch", 3, 3),
    ("http_delete", 1, 1),
    ("http_head", 1, 1),
    ("urlencode", 1, 1),
    // Misc
    ("pg_typeof", 1, 1),
    ("pg_input_is_valid", 2, 2),
    ("gen_random_uuid", 0, 0),
    ("generate_series", 2, 3),
    ("generate_subscripts", 2, 3),
    ("row_number", 0, 0),
    ("rank", 0, 255),
    ("dense_rank", 0, 255),
    ("ntile", 1, 1),
    ("lag", 1, 3),
    ("lead", 1, 3),
    ("first_value", 1, 1),
    ("last_value", 1, 1),
    ("nth_value", 2, 2),
    ("cume_dist", 0, 255),
    ("percent_rank", 0, 255),
    ("exists", 1, 1),
    ("current_schema", 0, 0),
    ("current_schemas", 1, 1),
    ("current_user", 0, 0),
    ("current_database", 0, 0),
    ("version", 0, 0),
    ("pg_backend_pid", 0, 0),
    ("pg_get_constraintdef", 1, 2),
    ("pg_get_indexdef", 1, 3),
    ("pg_get_viewdef", 1, 2),
    ("pg_relation_size", 1, 2),
    ("pg_total_relation_size", 1, 1),
    ("obj_description", 1, 2),
    ("col_description", 2, 2),
    ("txid_current", 0, 0),
    ("set_config", 3, 3),
    ("current_setting", 1, 2),
    ("has_table_privilege", 2, 3),
    ("has_schema_privilege", 2, 3),
];

/// Check function calls within a SELECT statement for obvious errors.
pub fn check_select_functions(select: &SelectStatement) -> Result<(), EngineError> {
    for item in &select.targets {
        check_expr_functions(&item.expr)?;
    }
    if let Some(ref where_clause) = select.where_clause {
        check_expr_functions(where_clause)?;
    }
    if let Some(ref having) = select.having {
        check_expr_functions(having)?;
    }
    Ok(())
}

/// Recursively check function calls in an expression.
fn check_expr_functions(expr: &Expr) -> Result<(), EngineError> {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            let func_name = name.last().map(|s| s.as_str()).unwrap_or("");
            let lower_name = func_name.to_lowercase();

            // Check argument count against known signatures
            if let Some((_name, min_args, max_args)) =
                KNOWN_FUNCTIONS.iter().find(|(n, _, _)| *n == lower_name)
            {
                let arg_count = if args.len() == 1 && matches!(args[0], Expr::Wildcard) {
                    // count(*) — treat as 0 args for validation purposes
                    0
                } else {
                    args.len()
                };

                if arg_count < *min_args || arg_count > *max_args {
                    if min_args == max_args {
                        return Err(EngineError {
                            message: format!(
                                "function {}() requires {} argument{}, got {}",
                                lower_name,
                                min_args,
                                if *min_args == 1 { "" } else { "s" },
                                arg_count
                            ),
                        });
                    }
                    return Err(EngineError {
                        message: format!(
                            "function {lower_name}() requires between {min_args} and {max_args} arguments, got {arg_count}"
                        ),
                    });
                }
            }
            // Unknown functions are allowed — they might be user-defined

            // Recurse into arguments
            for arg in args {
                check_expr_functions(arg)?;
            }
        }
        Expr::Binary { left, right, .. } => {
            check_expr_functions(left)?;
            check_expr_functions(right)?;
        }
        Expr::Unary { expr, .. } => {
            check_expr_functions(expr)?;
        }
        Expr::Cast { expr, .. } => {
            check_expr_functions(expr)?;
        }
        Expr::IsNull { expr, .. } => {
            check_expr_functions(expr)?;
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            check_expr_functions(expr)?;
            check_expr_functions(low)?;
            check_expr_functions(high)?;
        }
        Expr::Like { expr, pattern, .. } => {
            check_expr_functions(expr)?;
            check_expr_functions(pattern)?;
        }
        Expr::InList { expr, list, .. } => {
            check_expr_functions(expr)?;
            for item in list {
                check_expr_functions(item)?;
            }
        }
        Expr::CaseSearched {
            when_then,
            else_expr,
        } => {
            for (when, then) in when_then {
                check_expr_functions(when)?;
                check_expr_functions(then)?;
            }
            if let Some(else_expr) = else_expr {
                check_expr_functions(else_expr)?;
            }
        }
        Expr::CaseSimple {
            operand,
            when_then,
            else_expr,
        } => {
            check_expr_functions(operand)?;
            for (when, then) in when_then {
                check_expr_functions(when)?;
                check_expr_functions(then)?;
            }
            if let Some(else_expr) = else_expr {
                check_expr_functions(else_expr)?;
            }
        }
        Expr::AnyAll { left, right, .. } => {
            check_expr_functions(left)?;
            check_expr_functions(right)?;
        }
        Expr::IsDistinctFrom { left, right, .. } => {
            check_expr_functions(left)?;
            check_expr_functions(right)?;
        }
        Expr::ArrayConstructor(exprs) => {
            for e in exprs {
                check_expr_functions(e)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Look up a function signature by name. Returns (min_args, max_args) if known.
pub fn lookup_function(name: &str) -> Option<(usize, usize)> {
    let lower = name.to_lowercase();
    KNOWN_FUNCTIONS
        .iter()
        .find(|(n, _, _)| *n == lower)
        .map(|(_, min, max)| (*min, *max))
}

#[cfg(test)]
mod function_tests {
    use super::*;

    #[test]
    fn test_lookup_known_function() {
        let (min, max) = lookup_function("substr").unwrap();
        assert_eq!(min, 2);
        assert_eq!(max, 3);
    }

    #[test]
    fn test_lookup_unknown_function() {
        assert!(lookup_function("not_a_real_function").is_none());
    }

    #[test]
    fn test_check_function_wrong_arg_count() {
        let expr = Expr::FunctionCall {
            name: vec!["length".to_string()],
            args: vec![],
            distinct: false,
            order_by: vec![],
            within_group: vec![],
            filter: None,
            over: None,
        };
        let result = check_expr_functions(&expr);
        assert!(result.is_err());
        assert!(result.unwrap_err().message.contains("requires 1 argument"));
    }

    #[test]
    fn test_check_function_valid_arg_count() {
        let expr = Expr::FunctionCall {
            name: vec!["length".to_string()],
            args: vec![Expr::String("hello".to_string())],
            distinct: false,
            order_by: vec![],
            within_group: vec![],
            filter: None,
            over: None,
        };
        assert!(check_expr_functions(&expr).is_ok());
    }

    #[test]
    fn test_check_function_variadic() {
        let expr = Expr::FunctionCall {
            name: vec!["concat".to_string()],
            args: vec![
                Expr::String("a".to_string()),
                Expr::String("b".to_string()),
                Expr::String("c".to_string()),
            ],
            distinct: false,
            order_by: vec![],
            within_group: vec![],
            filter: None,
            over: None,
        };
        assert!(check_expr_functions(&expr).is_ok());
    }

    #[test]
    fn test_check_count_star() {
        let expr = Expr::FunctionCall {
            name: vec!["count".to_string()],
            args: vec![Expr::Wildcard],
            distinct: false,
            order_by: vec![],
            within_group: vec![],
            filter: None,
            over: None,
        };
        assert!(check_expr_functions(&expr).is_ok());
    }

    #[test]
    fn test_unknown_function_allowed() {
        let expr = Expr::FunctionCall {
            name: vec!["my_custom_func".to_string()],
            args: vec![Expr::Integer(1), Expr::Integer(2), Expr::Integer(3)],
            distinct: false,
            order_by: vec![],
            within_group: vec![],
            filter: None,
            over: None,
        };
        assert!(check_expr_functions(&expr).is_ok());
    }
}
