use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use crate::catalog::dependency::plan_sequence_drop as plan_sequence_drop_in_catalog;
use crate::catalog::{with_catalog_read, with_catalog_write};
use crate::parser::ast::DropBehavior;
use crate::parser::ast::{
    AlterSequenceAction, AlterSequenceStatement, CreateSequenceStatement, DropSequenceStatement,
};
use crate::tcop::engine::{EngineError, QueryResult};

#[derive(Debug, Clone)]
pub struct SequenceState {
    pub start: i64,
    pub current: i64,
    pub increment: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub cycle: bool,
    pub cache: i64,
    pub called: bool,
}

static GLOBAL_SEQUENCES: OnceLock<RwLock<HashMap<String, SequenceState>>> = OnceLock::new();

fn global_sequences() -> &'static RwLock<HashMap<String, SequenceState>> {
    GLOBAL_SEQUENCES.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn with_sequences_read<T>(f: impl FnOnce(&HashMap<String, SequenceState>) -> T) -> T {
    let sequences = global_sequences()
        .read()
        .expect("global sequences lock poisoned for read");
    f(&sequences)
}

pub fn with_sequences_write<T>(f: impl FnOnce(&mut HashMap<String, SequenceState>) -> T) -> T {
    let mut sequences = global_sequences()
        .write()
        .expect("global sequences lock poisoned for write");
    f(&mut sequences)
}

pub async fn execute_create_sequence(
    create: &CreateSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&create.name)?;
    let increment = create.increment.unwrap_or(1);
    if increment == 0 {
        return Err(EngineError {
            message: "INCREMENT must not be zero".to_string(),
        });
    }
    let mut min_value = create
        .min_value
        .unwrap_or(Some(default_sequence_min_value(increment)))
        .unwrap_or_else(|| default_sequence_min_value(increment));
    let mut max_value = create
        .max_value
        .unwrap_or(Some(default_sequence_max_value(increment)))
        .unwrap_or_else(|| default_sequence_max_value(increment));
    if min_value > max_value {
        return Err(EngineError {
            message: "MINVALUE must be less than or equal to MAXVALUE".to_string(),
        });
    }
    let start = create
        .start
        .unwrap_or_else(|| default_sequence_start(increment, min_value, max_value));
    if start < min_value || start > max_value {
        return Err(EngineError {
            message: "START value is out of sequence bounds".to_string(),
        });
    }
    let cycle = create.cycle.unwrap_or(false);
    let cache = create.cache.unwrap_or(1);
    if cache <= 0 {
        return Err(EngineError {
            message: "CACHE must be greater than zero".to_string(),
        });
    }
    if create.min_value == Some(None) {
        min_value = default_sequence_min_value(increment);
    }
    if create.max_value == Some(None) {
        max_value = default_sequence_max_value(increment);
    }

    let inserted = with_sequences_write(|sequences| {
        if sequences.contains_key(&key) {
            return false;
        }
        sequences.insert(
            key,
            SequenceState {
                start,
                current: start,
                increment,
                min_value,
                max_value,
                cycle,
                cache,
                called: false,
            },
        );
        true
    });
    if !inserted {
        if create.if_not_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "CREATE SEQUENCE".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("sequence \"{}\" already exists", create.name.join(".")),
        });
    }

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "CREATE SEQUENCE".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_alter_sequence(
    alter: &AlterSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&alter.name)?;
    with_sequences_write(|sequences| {
        let Some(state) = sequences.get_mut(&key) else {
            if alter.if_exists {
                return Ok(());
            }
            return Err(EngineError {
                message: format!("sequence \"{key}\" does not exist"),
            });
        };
        for action in &alter.actions {
            match action {
                AlterSequenceAction::NoOp => {}
                AlterSequenceAction::Restart { with } => {
                    state.current = with.unwrap_or(state.start);
                    state.called = false;
                }
                AlterSequenceAction::SetStart { start } => {
                    state.start = *start;
                    if !state.called {
                        state.current = *start;
                    }
                }
                AlterSequenceAction::SetIncrement { increment } => {
                    if *increment == 0 {
                        return Err(EngineError {
                            message: "INCREMENT must not be zero".to_string(),
                        });
                    }
                    state.increment = *increment;
                }
                AlterSequenceAction::SetMinValue { min } => {
                    state.min_value =
                        min.unwrap_or_else(|| default_sequence_min_value(state.increment));
                }
                AlterSequenceAction::SetMaxValue { max } => {
                    state.max_value =
                        max.unwrap_or_else(|| default_sequence_max_value(state.increment));
                }
                AlterSequenceAction::SetCycle { cycle } => {
                    state.cycle = *cycle;
                }
                AlterSequenceAction::SetCache { cache } => {
                    if *cache <= 0 {
                        return Err(EngineError {
                            message: "CACHE must be greater than zero".to_string(),
                        });
                    }
                    state.cache = *cache;
                }
            }
        }

        if state.min_value > state.max_value {
            return Err(EngineError {
                message: "MINVALUE must be less than or equal to MAXVALUE".to_string(),
            });
        }
        if state.start < state.min_value || state.start > state.max_value {
            return Err(EngineError {
                message: "START value is out of sequence bounds".to_string(),
            });
        }
        if !state.called {
            if state.current < state.min_value || state.current > state.max_value {
                return Err(EngineError {
                    message: "RESTART value is out of sequence bounds".to_string(),
                });
            }
        } else if state.current < state.min_value || state.current > state.max_value {
            return Err(EngineError {
                message: "sequence current value is out of bounds after ALTER SEQUENCE".to_string(),
            });
        }
        Ok(())
    })?;

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "ALTER SEQUENCE".to_string(),
        rows_affected: 0,
    })
}

pub async fn execute_drop_sequence(
    drop_sequence: &DropSequenceStatement,
) -> Result<QueryResult, EngineError> {
    let key = normalize_sequence_name(&drop_sequence.name)?;
    let exists = with_sequences_read(|sequences| sequences.contains_key(&key));
    if !exists {
        if drop_sequence.if_exists {
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                command_tag: "DROP SEQUENCE".to_string(),
                rows_affected: 0,
            });
        }
        return Err(EngineError {
            message: format!("sequence \"{key}\" does not exist"),
        });
    }

    let dependency_plan = with_catalog_read(|catalog| {
        plan_sequence_drop_in_catalog(
            catalog,
            &key,
            matches!(drop_sequence.behavior, DropBehavior::Cascade),
        )
    })
    .map_err(|err| EngineError {
        message: err.message,
    })?;

    if !dependency_plan.default_dependents.is_empty() {
        with_catalog_write(|catalog| {
            catalog.clear_sequence_defaults(&key);
        });
    }
    if !dependency_plan.relation_drop_order.is_empty() {
        crate::commands::drop::drop_relations_by_oid_order(&dependency_plan.relation_drop_order)?;
    }

    with_sequences_write(|sequences| {
        sequences.remove(&key);
    });

    Ok(QueryResult {
        columns: Vec::new(),
        rows: Vec::new(),
        command_tag: "DROP SEQUENCE".to_string(),
        rows_affected: 0,
    })
}

pub fn normalize_sequence_name(name: &[String]) -> Result<String, EngineError> {
    match name {
        [seq_name] => Ok(format!("public.{}", seq_name.to_ascii_lowercase())),
        [schema_name, seq_name] => Ok(format!(
            "{}.{}",
            schema_name.to_ascii_lowercase(),
            seq_name.to_ascii_lowercase()
        )),
        _ => Err(EngineError {
            message: format!("invalid sequence name \"{}\"", name.join(".")),
        }),
    }
}

pub fn normalize_sequence_name_from_text(raw: &str) -> Result<String, EngineError> {
    let parts = raw
        .split('.')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .map(|part| part.to_ascii_lowercase())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [seq_name] => Ok(format!("public.{seq_name}")),
        [schema_name, seq_name] => Ok(format!("{schema_name}.{seq_name}")),
        _ => Err(EngineError {
            message: format!("invalid sequence name \"{raw}\""),
        }),
    }
}

pub fn default_sequence_min_value(increment: i64) -> i64 {
    if increment > 0 { 1 } else { i64::MIN }
}

pub fn default_sequence_max_value(increment: i64) -> i64 {
    if increment > 0 { i64::MAX } else { -1 }
}

pub fn default_sequence_start(increment: i64, min_value: i64, max_value: i64) -> i64 {
    if increment > 0 { min_value } else { max_value }
}

pub fn sequence_next_value(
    state: &mut SequenceState,
    sequence_name: &str,
) -> Result<i64, EngineError> {
    let mut next = state.current.saturating_add(state.increment);
    if state.called {
        if next < state.min_value || next > state.max_value {
            if !state.cycle {
                return Err(EngineError {
                    message: format!("sequence \"{sequence_name}\" is out of bounds"),
                });
            }
            next = if state.increment > 0 {
                state.min_value
            } else {
                state.max_value
            };
        }
        state.current = next;
    } else {
        state.called = true;
        next = state.current;
    }

    if next < state.min_value || next > state.max_value {
        return Err(EngineError {
            message: format!("sequence \"{sequence_name}\" overflowed"),
        });
    }
    if state.increment > 0 && next > state.max_value
        || state.increment < 0 && next < state.min_value
    {
        return Err(EngineError {
            message: format!("nextval: sequence \"{sequence_name}\" has reached its limit"),
        });
    }
    Ok(next)
}

pub fn set_sequence_value(
    state: &mut SequenceState,
    sequence_name: &str,
    value: i64,
    is_called: bool,
) -> Result<(), EngineError> {
    if value < state.min_value || value > state.max_value {
        return Err(EngineError {
            message: format!(
                "setval: value {value} is out of bounds for sequence \"{sequence_name}\""
            ),
        });
    }
    state.current = value;
    state.called = is_called;
    Ok(())
}
