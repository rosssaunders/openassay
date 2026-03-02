#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) fn scope_from_row(
    columns: &[String],
    row: &[ScalarValue],
    qualifiers: &[String],
    visible_columns: &[String],
) -> EvalScope {
    let mut scope = EvalScope::default();
    for (col, value) in columns.iter().zip(row.iter()) {
        scope.insert_unqualified(col, value.clone());
        for qualifier in qualifiers {
            scope.insert_qualified(&format!("{qualifier}.{col}"), value.clone());
        }
    }
    if !row.is_empty() {
        let column_names = columns
            .iter()
            .map(|col| col.to_ascii_lowercase())
            .collect::<HashSet<_>>();
        let record_value = ScalarValue::Record(row.to_vec());
        for qualifier in qualifiers {
            let lower = qualifier.to_ascii_lowercase();
            if !column_names.contains(&lower) {
                scope.insert_unqualified(qualifier, record_value.clone());
            }
        }
    }

    // Ensure all visible columns exist even if row data is empty (e.g. relation with no rows).
    for col in visible_columns {
        if scope.lookup_join_column(col).is_none() {
            scope.insert_unqualified(col, ScalarValue::Null);
            for qualifier in qualifiers {
                scope.insert_qualified(&format!("{qualifier}.{col}"), ScalarValue::Null);
            }
        }
    }
    scope
}

pub fn scope_for_table_row(table: &crate::catalog::Table, row: &[ScalarValue]) -> EvalScope {
    let qualifiers = vec![table.name().to_string(), table.qualified_name()];
    scope_for_table_row_with_qualifiers(table, row, &qualifiers)
}

pub fn scope_for_table_row_with_qualifiers(
    table: &crate::catalog::Table,
    row: &[ScalarValue],
    qualifiers: &[String],
) -> EvalScope {
    let columns = table
        .columns()
        .iter()
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    scope_from_row(&columns, row, qualifiers, &columns)
}

pub fn combine_scopes(
    left: &EvalScope,
    right: &EvalScope,
    using_columns: &HashSet<String>,
) -> EvalScope {
    let mut out = left.clone();
    out.merge(right);

    for col in using_columns {
        if let Some(value) = left
            .lookup_join_column(col)
            .or_else(|| right.lookup_join_column(col))
        {
            out.force_unqualified(col, value);
        }
    }

    out
}
