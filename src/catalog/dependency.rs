use std::collections::HashSet;

use crate::catalog::oid::Oid;
use crate::catalog::{Catalog, CatalogError, SearchPath, TableKind};
use crate::parser::ast::{Expr, GroupByExpr, JoinCondition, Query, QueryExpr, TableExpression};

#[derive(Debug, Clone)]
pub struct TableRefByOid {
    pub oid: Oid,
    pub schema_name: String,
    pub table_name: String,
    pub kind: TableKind,
}

pub fn describe_table(catalog: &Catalog, oid: Oid) -> Result<TableRefByOid, CatalogError> {
    for schema in catalog.schemas() {
        for table in schema.tables() {
            if table.oid() == oid {
                return Ok(TableRefByOid {
                    oid,
                    schema_name: schema.name().to_string(),
                    table_name: table.name().to_string(),
                    kind: table.kind(),
                });
            }
        }
    }
    Err(CatalogError {
        message: format!("relation oid {} does not exist", oid),
    })
}

pub fn table_dependency_edges(catalog: &Catalog) -> Vec<(Oid, Oid)> {
    let mut edges = Vec::new();
    for schema in catalog.schemas() {
        for child in schema.tables() {
            if child.kind() != TableKind::Heap {
                continue;
            }
            for constraint in child.foreign_key_constraints() {
                let Ok(parent) =
                    catalog.resolve_table(&constraint.referenced_table, &SearchPath::default())
                else {
                    continue;
                };
                if parent.kind() != TableKind::Heap {
                    continue;
                }
                edges.push((parent.oid(), child.oid()));
            }
        }
    }
    edges
}

fn relation_dependency_edges(catalog: &Catalog) -> Vec<(Oid, Oid)> {
    let mut edges = table_dependency_edges(catalog);
    let mut seen = edges.iter().cloned().collect::<HashSet<_>>();

    for schema in catalog.schemas() {
        for relation in schema.tables() {
            if !matches!(
                relation.kind(),
                TableKind::View | TableKind::MaterializedView
            ) {
                continue;
            }
            let Some(definition) = relation.view_definition() else {
                continue;
            };
            let mut refs = Vec::new();
            collect_query_relation_refs(definition, &HashSet::new(), &mut refs);
            for referenced in refs {
                let Ok(parent) = catalog.resolve_table(&referenced, &SearchPath::default()) else {
                    continue;
                };
                if parent.oid() == relation.oid() {
                    continue;
                }
                let edge = (parent.oid(), relation.oid());
                if seen.insert(edge) {
                    edges.push(edge);
                }
            }
        }
    }

    edges
}

pub fn sequence_relation_dependency_oids(catalog: &Catalog, sequence_names: &[String]) -> Vec<Oid> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for sequence_name in sequence_names {
        for (schema_name, relation_name) in catalog.sequence_relation_dependents(sequence_name) {
            let Some(relation) = catalog.table(&schema_name, &relation_name) else {
                continue;
            };
            if seen.insert(relation.oid()) {
                out.push(relation.oid());
            }
        }
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SequenceDropPlan {
    pub default_dependents: Vec<(String, String, String)>,
    pub relation_drop_order: Vec<Oid>,
}

pub fn expand_and_order_relation_drop(
    catalog: &Catalog,
    initial_relation_oids: &[Oid],
    cascade: bool,
) -> Result<Vec<Oid>, CatalogError> {
    if initial_relation_oids.is_empty() {
        return Ok(Vec::new());
    }
    let targets = expand_relation_dependency_set(catalog, initial_relation_oids, cascade)?;
    order_relations_for_drop(catalog, &targets)
}

pub fn plan_sequence_drop(
    catalog: &Catalog,
    sequence_name: &str,
    cascade: bool,
) -> Result<SequenceDropPlan, CatalogError> {
    let mut default_dependents = catalog.sequence_default_dependents(sequence_name);
    default_dependents.sort();

    let mut relation_oids =
        sequence_relation_dependency_oids(catalog, &[sequence_name.to_string()]);
    relation_oids.sort_unstable();
    relation_oids.dedup();

    if !cascade && (!default_dependents.is_empty() || !relation_oids.is_empty()) {
        if let Some((schema_name, table_name, column_name)) = default_dependents.first() {
            return Err(CatalogError {
                message: format!(
                    "cannot drop sequence \"{}\" because column \"{}.{}.{}\" depends on it",
                    sequence_name, schema_name, table_name, column_name
                ),
            });
        }
        if let Some(first_oid) = relation_oids.first() {
            let dependent = describe_table(catalog, *first_oid)?;
            return Err(CatalogError {
                message: format!(
                    "cannot drop sequence \"{}\" because relation \"{}.{}\" depends on it",
                    sequence_name, dependent.schema_name, dependent.table_name
                ),
            });
        }
    }

    let relation_drop_order = if cascade && !relation_oids.is_empty() {
        expand_and_order_relation_drop(catalog, &relation_oids, true)?
    } else {
        Vec::new()
    };

    Ok(SequenceDropPlan {
        default_dependents,
        relation_drop_order,
    })
}

pub fn index_backing_constraint_name(
    catalog: &Catalog,
    schema_name: &str,
    table_name: &str,
    index_name: &str,
) -> Option<String> {
    let table = catalog.table(schema_name, table_name)?;
    table.key_constraints().iter().find_map(|constraint| {
        let name = constraint.name.as_ref()?;
        if name.eq_ignore_ascii_case(index_name) {
            Some(name.clone())
        } else {
            None
        }
    })
}

pub fn expand_relation_dependency_set(
    catalog: &Catalog,
    initial_relation_oids: &[Oid],
    cascade: bool,
) -> Result<HashSet<Oid>, CatalogError> {
    let mut set = HashSet::new();
    for relation_oid in initial_relation_oids {
        let ident = describe_table(catalog, *relation_oid)?;
        match ident.kind {
            TableKind::Heap | TableKind::View | TableKind::MaterializedView => {
                set.insert(*relation_oid);
            }
            TableKind::VirtualDual => {
                return Err(CatalogError {
                    message: format!(
                        "cannot drop system relation \"{}.{}\"",
                        ident.schema_name, ident.table_name
                    ),
                });
            }
        }
    }

    let edges = relation_dependency_edges(catalog);
    let mut changed = true;
    while changed {
        changed = false;
        for (parent_oid, child_oid) in &edges {
            if !set.contains(parent_oid) || set.contains(child_oid) {
                continue;
            }
            if cascade {
                set.insert(*child_oid);
                changed = true;
            } else {
                let parent = describe_table(catalog, *parent_oid)?;
                let child = describe_table(catalog, *child_oid)?;
                return Err(CatalogError {
                    message: format!(
                        "cannot drop relation \"{}.{}\" because relation \"{}.{}\" depends on it",
                        parent.schema_name, parent.table_name, child.schema_name, child.table_name
                    ),
                });
            }
        }
    }

    Ok(set)
}

pub fn order_relations_for_drop(
    catalog: &Catalog,
    targets: &HashSet<Oid>,
) -> Result<Vec<Oid>, CatalogError> {
    let edges = relation_dependency_edges(catalog);
    let mut remaining = targets.clone();
    let mut ordered = Vec::with_capacity(targets.len());
    while !remaining.is_empty() {
        let mut leaves = remaining
            .iter()
            .filter(|oid| {
                !edges
                    .iter()
                    .any(|(parent, child)| parent == *oid && remaining.contains(child))
            })
            .cloned()
            .collect::<Vec<_>>();
        if leaves.is_empty() {
            return Err(CatalogError {
                message: "cyclic dependency detected in drop graph".to_string(),
            });
        }
        leaves.sort_unstable();
        for oid in leaves {
            remaining.remove(&oid);
            ordered.push(oid);
        }
    }
    Ok(ordered)
}

fn collect_query_relation_refs(
    query: &Query,
    outer_ctes: &HashSet<String>,
    out: &mut Vec<Vec<String>>,
) {
    let mut scope = outer_ctes.clone();
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            let mut cte_scope = scope.clone();
            if with.recursive {
                cte_scope.insert(cte.name.to_ascii_lowercase());
            }
            collect_query_relation_refs(&cte.query, &cte_scope, out);
            scope.insert(cte.name.to_ascii_lowercase());
        }
    }
    collect_query_expr_relation_refs(&query.body, &scope, out);
    for order in &query.order_by {
        collect_expr_relation_refs(&order.expr, &scope, out);
    }
    if let Some(limit) = &query.limit {
        collect_expr_relation_refs(limit, &scope, out);
    }
    if let Some(offset) = &query.offset {
        collect_expr_relation_refs(offset, &scope, out);
    }
}

fn collect_query_expr_relation_refs(
    expr: &QueryExpr,
    ctes: &HashSet<String>,
    out: &mut Vec<Vec<String>>,
) {
    match expr {
        QueryExpr::Select(select) => {
            for table in &select.from {
                collect_table_expression_refs(table, ctes, out);
            }
            for target in &select.targets {
                collect_expr_relation_refs(&target.expr, ctes, out);
            }
            if let Some(where_clause) = &select.where_clause {
                collect_expr_relation_refs(where_clause, ctes, out);
            }
            for group_expr in &select.group_by {
                match group_expr {
                    GroupByExpr::Expr(expr) => collect_expr_relation_refs(expr, ctes, out),
                    GroupByExpr::GroupingSets(sets) => {
                        for set in sets {
                            for expr in set {
                                collect_expr_relation_refs(expr, ctes, out);
                            }
                        }
                    }
                    GroupByExpr::Rollup(exprs) | GroupByExpr::Cube(exprs) => {
                        for expr in exprs {
                            collect_expr_relation_refs(expr, ctes, out);
                        }
                    }
                }
            }
            if let Some(having) = &select.having {
                collect_expr_relation_refs(having, ctes, out);
            }
        }
        QueryExpr::SetOperation { left, right, .. } => {
            collect_query_expr_relation_refs(left, ctes, out);
            collect_query_expr_relation_refs(right, ctes, out);
        }
        QueryExpr::Nested(query) => collect_query_relation_refs(query, ctes, out),
        QueryExpr::Values(rows) => {
            // Collect relation references from value expressions
            for row in rows {
                for expr in row {
                    collect_expr_relation_refs(expr, ctes, out);
                }
            }
        }
        QueryExpr::Insert(_) | QueryExpr::Update(_) | QueryExpr::Delete(_) => {
            // DML statements in CTEs not yet fully supported
            // No dependencies to collect for now
        }
    }
}

fn collect_table_expression_refs(
    table: &TableExpression,
    ctes: &HashSet<String>,
    out: &mut Vec<Vec<String>>,
) {
    match table {
        TableExpression::Relation(rel) => {
            if rel.name.len() == 1 && ctes.contains(&rel.name[0].to_ascii_lowercase()) {
                return;
            }
            out.push(rel.name.clone());
        }
        TableExpression::Function(function) => {
            for arg in &function.args {
                collect_expr_relation_refs(arg, ctes, out);
            }
        }
        TableExpression::Subquery(sub) => collect_query_relation_refs(&sub.query, ctes, out),
        TableExpression::Join(join) => {
            collect_table_expression_refs(&join.left, ctes, out);
            collect_table_expression_refs(&join.right, ctes, out);
            if let Some(condition) = &join.condition {
                match condition {
                    JoinCondition::On(expr) => collect_expr_relation_refs(expr, ctes, out),
                    JoinCondition::Using(_) => {}
                }
            }
        }
    }
}

fn collect_expr_relation_refs(expr: &Expr, ctes: &HashSet<String>, out: &mut Vec<Vec<String>>) {
    match expr {
        Expr::FunctionCall {
            args,
            order_by,
            within_group,
            filter,
            ..
        } => {
            for arg in args {
                collect_expr_relation_refs(arg, ctes, out);
            }
            for entry in order_by {
                collect_expr_relation_refs(&entry.expr, ctes, out);
            }
            for entry in within_group {
                collect_expr_relation_refs(&entry.expr, ctes, out);
            }
            if let Some(expr) = filter {
                collect_expr_relation_refs(expr, ctes, out);
            }
        }
        Expr::Unary { expr, .. } => collect_expr_relation_refs(expr, ctes, out),
        Expr::Binary { left, right, .. } => {
            collect_expr_relation_refs(left, ctes, out);
            collect_expr_relation_refs(right, ctes, out);
        }
        Expr::Exists(query) | Expr::ScalarSubquery(query) => {
            collect_query_relation_refs(query, ctes, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_relation_refs(expr, ctes, out);
            for item in list {
                collect_expr_relation_refs(item, ctes, out);
            }
        }
        Expr::InSubquery { expr, subquery, .. } => {
            collect_expr_relation_refs(expr, ctes, out);
            collect_query_relation_refs(subquery, ctes, out);
        }
        _ => {}
    }
}

pub fn expand_table_dependency_set(
    catalog: &Catalog,
    initial_table_oids: &[Oid],
    cascade: bool,
) -> Result<HashSet<Oid>, CatalogError> {
    let mut set = HashSet::new();
    for table_oid in initial_table_oids {
        let ident = describe_table(catalog, *table_oid)?;
        let table = catalog
            .table(&ident.schema_name, &ident.table_name)
            .ok_or_else(|| CatalogError {
                message: format!(
                    "relation \"{}.{}\" does not exist",
                    ident.schema_name, ident.table_name
                ),
            })?;
        if table.kind() != TableKind::Heap {
            return Err(CatalogError {
                message: format!("cannot drop system relation \"{}\"", table.qualified_name()),
            });
        }
        set.insert(*table_oid);
    }

    let edges = table_dependency_edges(catalog);
    let mut changed = true;
    while changed {
        changed = false;
        for (parent_oid, child_oid) in &edges {
            if !set.contains(parent_oid) || set.contains(child_oid) {
                continue;
            }
            if cascade {
                set.insert(*child_oid);
                changed = true;
            } else {
                let parent = describe_table(catalog, *parent_oid)?;
                let child = describe_table(catalog, *child_oid)?;
                return Err(CatalogError {
                    message: format!(
                        "cannot drop relation \"{}.{}\" because relation \"{}.{}\" depends on it",
                        parent.schema_name, parent.table_name, child.schema_name, child.table_name
                    ),
                });
            }
        }
    }

    Ok(set)
}

pub fn order_tables_for_drop(
    catalog: &Catalog,
    targets: &HashSet<Oid>,
) -> Result<Vec<Oid>, CatalogError> {
    order_relations_for_drop(catalog, targets)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnSpec, ForeignKeyConstraintSpec, KeyConstraintSpec, TypeSignature};

    fn make_parent_child_catalog() -> Catalog {
        let mut catalog = Catalog::default();
        catalog
            .create_table(
                "public",
                "parents",
                TableKind::Heap,
                vec![ColumnSpec::new("id", TypeSignature::Int8).not_null()],
                vec![KeyConstraintSpec {
                    name: Some("parents_pkey".to_string()),
                    columns: vec!["id".to_string()],
                    primary: true,
                }],
                Vec::new(),
            )
            .expect("parent table should create");
        catalog
            .create_table(
                "public",
                "children",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("id", TypeSignature::Int8).not_null(),
                    ColumnSpec::new("parent_id", TypeSignature::Int8),
                ],
                vec![KeyConstraintSpec {
                    name: Some("children_pkey".to_string()),
                    columns: vec!["id".to_string()],
                    primary: true,
                }],
                vec![ForeignKeyConstraintSpec {
                    name: Some("children_parent_fkey".to_string()),
                    columns: vec!["parent_id".to_string()],
                    referenced_table: vec!["parents".to_string()],
                    referenced_columns: vec!["id".to_string()],
                    on_delete: crate::parser::ast::ForeignKeyAction::Restrict,
                    on_update: crate::parser::ast::ForeignKeyAction::Restrict,
                }],
            )
            .expect("child table should create");
        catalog
    }

    #[test]
    fn restrict_dependency_expansion_rejects_parent_drop() {
        let catalog = make_parent_child_catalog();
        let parent_oid = catalog
            .table("public", "parents")
            .expect("parent exists")
            .oid();
        let err =
            expand_table_dependency_set(&catalog, &[parent_oid], false).expect_err("should fail");
        assert!(err.message.contains("depends on it"));
    }

    #[test]
    fn cascade_orders_children_before_parents() {
        let catalog = make_parent_child_catalog();
        let parent_oid = catalog
            .table("public", "parents")
            .expect("parent exists")
            .oid();
        let child_oid = catalog
            .table("public", "children")
            .expect("child exists")
            .oid();
        let targets =
            expand_table_dependency_set(&catalog, &[parent_oid], true).expect("should expand");
        let order = order_tables_for_drop(&catalog, &targets).expect("order should build");
        assert_eq!(order, vec![child_oid, parent_oid]);
    }
}
