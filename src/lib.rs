#![allow(
    clippy::unreadable_literal,
    clippy::redundant_clone,
    clippy::needless_pass_by_ref_mut,
    clippy::suspicious_op_assign_impl,
    clippy::single_char_pattern,
    clippy::needless_pass_by_value,
    clippy::many_single_char_names,
    clippy::large_enum_variant,
    clippy::items_after_statements,
    clippy::unnecessary_to_owned,
    clippy::needless_continue,
    clippy::manual_map,
    clippy::match_like_matches_macro,
    clippy::useless_format,
    clippy::redundant_closure,
    clippy::inefficient_to_string,
    clippy::format_collect,
    // clippy::unused_format_args — renamed to unused_format_specs in newer clippy
    clippy::assigning_clones,
    clippy::manual_let_else,
    clippy::option_as_ref_deref,
    clippy::implicit_hasher,
    clippy::used_underscore_binding,
    clippy::if_then_some_else_none,
    clippy::trivially_copy_pass_by_ref,
    clippy::or_fun_call,
    clippy::unused_self,
    clippy::assign_op_pattern,
    clippy::if_not_else,
    clippy::match_same_arms,
    clippy::float_cmp,
    clippy::format_push_string,
    clippy::manual_assert,
    clippy::comparison_chain,
    clippy::needless_bool,
    clippy::collapsible_else_if,
    clippy::blocks_in_conditions,
    clippy::match_wildcard_for_single_variants,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_lossless,
    clippy::module_name_repetitions,
    clippy::struct_excessive_bools,
    clippy::similar_names,
    clippy::fn_params_excessive_bools,
    clippy::default_trait_access,
    clippy::if_same_then_else,
    clippy::suspicious_arithmetic_impl,
    clippy::needless_late_init,
    clippy::branches_sharing_code,
    clippy::useless_let_if_seq,
    clippy::suspicious_operation_groupings,
    clippy::bool_to_int_with_if,
    clippy::collapsible_str_replace
)]

pub mod access;
pub mod analyzer;
pub mod browser;
pub mod catalog;
pub mod commands;
pub mod extensions;
pub mod executor;
pub mod parser;
pub mod planner;
pub mod plpgsql;
pub mod protocol;
#[cfg(not(target_arch = "wasm32"))]
pub mod replication;
pub mod security;
pub mod storage;
pub mod tcop;
pub mod txn;
pub mod utils;
