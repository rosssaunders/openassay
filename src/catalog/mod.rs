use crate::parser::ast::{Expr, Query, QueryExpr, TableExpression};
use std::collections::{HashMap, HashSet};
use std::fmt;
#[cfg(test)]
use std::sync::Mutex;
use std::sync::{OnceLock, RwLock};

pub mod dependency;
pub mod oid;
pub mod schema;
pub mod search_path;
pub mod table;

use oid::{Oid, OidGenerator};
use schema::Schema;
pub use search_path::SearchPath;
pub use table::{
    Column, ColumnSpec, ForeignKeyConstraint, ForeignKeyConstraintSpec, ForeignKeySpec, IndexSpec,
    KeyConstraint, KeyConstraintSpec, Table, TableKind, TypeSignature,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogError {
    pub message: String,
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CatalogError {}

#[derive(Debug, Clone)]
pub struct Catalog {
    oid_gen: OidGenerator,
    schemas: HashMap<String, Schema>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new_bootstrap()
    }
}

impl Catalog {
    pub fn new_bootstrap() -> Self {
        let mut catalog = Self {
            oid_gen: OidGenerator::default(),
            schemas: HashMap::new(),
        };
        catalog
            .create_schema("pg_catalog")
            .expect("bootstrap should create pg_catalog");
        catalog
            .create_schema("public")
            .expect("bootstrap should create public schema");
        catalog
            .create_table(
                "pg_catalog",
                "dual",
                TableKind::VirtualDual,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("bootstrap should create pg_catalog.dual");
        catalog
    }

    pub fn schemas(&self) -> impl Iterator<Item = &Schema> {
        self.schemas.values()
    }

    pub fn schema(&self, name: &str) -> Option<&Schema> {
        let key = normalize_name(name).ok()?;
        self.schemas.get(&key)
    }

    pub fn create_schema(&mut self, name: &str) -> Result<Oid, CatalogError> {
        let normalized_name = normalize_name(name)?;
        if self.schemas.contains_key(&normalized_name) {
            return Err(CatalogError {
                message: format!("schema \"{name}\" already exists"),
            });
        }

        let schema_oid = self.oid_gen.next_oid();
        self.schemas.insert(
            normalized_name.clone(),
            Schema::new(schema_oid, normalized_name),
        );
        Ok(schema_oid)
    }

    pub fn drop_schema(&mut self, name: &str) -> Result<(), CatalogError> {
        let normalized_name = normalize_name(name)?;
        let Some(schema) = self.schemas.get(&normalized_name) else {
            return Err(CatalogError {
                message: format!("schema \"{name}\" does not exist"),
            });
        };
        if schema.tables().next().is_some() {
            return Err(CatalogError {
                message: format!("schema \"{name}\" is not empty"),
            });
        }

        self.schemas.remove(&normalized_name);
        Ok(())
    }

    pub fn create_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        kind: TableKind,
        columns: Vec<ColumnSpec>,
        key_constraints: Vec<KeyConstraintSpec>,
        foreign_key_constraints: Vec<ForeignKeyConstraintSpec>,
    ) -> Result<Oid, CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        if schema.table(&table_name).is_some() {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" already exists", schema_name, table_name),
            });
        }

        let table_oid = self.oid_gen.next_oid();
        let mut normalized_columns = Vec::with_capacity(columns.len());
        for (idx, column) in columns.into_iter().enumerate() {
            let column_name = normalize_name(&column.name)?;
            if normalized_columns
                .iter()
                .any(|existing: &Column| existing.name() == column_name)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" specified more than once for relation \"{}.{}\"",
                        column_name, schema_name, table_name
                    ),
                });
            }
            normalized_columns.push(Column::new(
                self.oid_gen.next_oid(),
                column_name,
                column.type_signature,
                idx as u16,
                column.nullable,
                column.unique,
                column.primary_key,
                column.references,
                column.check,
                column.default,
            ));
        }

        let mut all_key_constraints: Vec<KeyConstraint> = Vec::new();
        let mut all_foreign_key_constraints: Vec<ForeignKeyConstraint> = Vec::new();
        for column in &normalized_columns {
            if column.primary_key() {
                all_key_constraints.push(KeyConstraint {
                    name: None,
                    columns: vec![column.name().to_string()],
                    primary: true,
                });
            } else if column.unique() {
                all_key_constraints.push(KeyConstraint {
                    name: None,
                    columns: vec![column.name().to_string()],
                    primary: false,
                });
            }
            if let Some(reference) = column.references().cloned() {
                all_foreign_key_constraints.push(ForeignKeyConstraint {
                    name: None,
                    columns: vec![column.name().to_string()],
                    referenced_table: reference.table_name,
                    referenced_columns: reference.column_name.into_iter().collect(),
                    on_delete: reference.on_delete,
                    on_update: reference.on_update,
                });
            }
        }
        for mut constraint in key_constraints {
            if let Some(name) = constraint.name.as_mut() {
                *name = normalize_name(name)?;
            }
            let mut normalized_cols = Vec::with_capacity(constraint.columns.len());
            for col in constraint.columns {
                let col = normalize_name(&col)?;
                if !normalized_columns
                    .iter()
                    .any(|existing| existing.name() == col)
                {
                    return Err(CatalogError {
                        message: format!(
                            "column \"{}\" does not exist in relation \"{}.{}\"",
                            col, schema_name, table_name
                        ),
                    });
                }
                if !normalized_cols.iter().any(|existing| existing == &col) {
                    normalized_cols.push(col);
                }
            }
            if normalized_cols.is_empty() {
                return Err(CatalogError {
                    message: format!(
                        "empty key constraint is invalid for relation \"{}.{}\"",
                        schema_name, table_name
                    ),
                });
            }
            all_key_constraints.push(KeyConstraint {
                name: constraint.name,
                columns: normalized_cols,
                primary: constraint.primary,
            });
        }
        for mut constraint in foreign_key_constraints {
            if let Some(name) = constraint.name.as_mut() {
                *name = normalize_name(name)?;
            }
            let mut normalized_cols = Vec::with_capacity(constraint.columns.len());
            for col in constraint.columns {
                let col = normalize_name(&col)?;
                if !normalized_columns
                    .iter()
                    .any(|existing| existing.name() == col)
                {
                    return Err(CatalogError {
                        message: format!(
                            "column \"{}\" does not exist in relation \"{}.{}\"",
                            col, schema_name, table_name
                        ),
                    });
                }
                if !normalized_cols.iter().any(|existing| existing == &col) {
                    normalized_cols.push(col);
                }
            }
            if normalized_cols.is_empty() {
                return Err(CatalogError {
                    message: format!(
                        "empty foreign key constraint is invalid for relation \"{}.{}\"",
                        schema_name, table_name
                    ),
                });
            }

            let mut normalized_referenced_table =
                Vec::with_capacity(constraint.referenced_table.len());
            for part in constraint.referenced_table {
                normalized_referenced_table.push(normalize_name(&part)?);
            }
            if normalized_referenced_table.is_empty() {
                return Err(CatalogError {
                    message: format!(
                        "foreign key constraint has invalid REFERENCES target for relation \"{}.{}\"",
                        schema_name, table_name
                    ),
                });
            }

            let mut normalized_referenced_columns =
                Vec::with_capacity(constraint.referenced_columns.len());
            for col in constraint.referenced_columns {
                let col = normalize_name(&col)?;
                if !normalized_referenced_columns
                    .iter()
                    .any(|existing| existing == &col)
                {
                    normalized_referenced_columns.push(col);
                }
            }
            if !normalized_referenced_columns.is_empty()
                && normalized_referenced_columns.len() != normalized_cols.len()
            {
                return Err(CatalogError {
                    message: format!(
                        "foreign key constraint has {} referencing columns but {} referenced columns in relation \"{}.{}\"",
                        normalized_cols.len(),
                        normalized_referenced_columns.len(),
                        schema_name,
                        table_name
                    ),
                });
            }

            all_foreign_key_constraints.push(ForeignKeyConstraint {
                name: constraint.name,
                columns: normalized_cols,
                referenced_table: normalized_referenced_table,
                referenced_columns: normalized_referenced_columns,
                on_delete: constraint.on_delete,
                on_update: constraint.on_update,
            });
        }

        let mut seen_constraint_names = HashSet::new();
        for constraint in &all_key_constraints {
            let Some(name) = &constraint.name else {
                continue;
            };
            if !seen_constraint_names.insert(name.clone()) {
                return Err(CatalogError {
                    message: format!(
                        "constraint \"{}\" specified more than once for relation \"{}.{}\"",
                        name, schema_name, table_name
                    ),
                });
            }
        }
        for constraint in &all_foreign_key_constraints {
            let Some(name) = &constraint.name else {
                continue;
            };
            if !seen_constraint_names.insert(name.clone()) {
                return Err(CatalogError {
                    message: format!(
                        "constraint \"{}\" specified more than once for relation \"{}.{}\"",
                        name, schema_name, table_name
                    ),
                });
            }
        }

        let primary_key_count = all_key_constraints
            .iter()
            .filter(|constraint| constraint.primary)
            .count();
        if primary_key_count > 1 {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" has more than one primary key",
                    schema_name, table_name
                ),
            });
        }
        for constraint in &all_key_constraints {
            if constraint.primary {
                for key_col in &constraint.columns {
                    if let Some(column) = normalized_columns
                        .iter_mut()
                        .find(|column| column.name() == key_col)
                    {
                        column.set_nullable(false);
                    }
                }
            }
        }

        let table = Table::new(
            table_oid,
            schema.oid(),
            schema.name().to_string(),
            table_name,
            kind,
            normalized_columns,
            all_key_constraints,
            all_foreign_key_constraints,
            Vec::new(),
            None,
        );
        schema.insert_table(table);
        Ok(table_oid)
    }

    pub fn create_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        materialized: bool,
        columns: Vec<String>,
        definition: Query,
    ) -> Result<Oid, CatalogError> {
        let kind = if materialized {
            TableKind::MaterializedView
        } else {
            TableKind::View
        };
        let specs = columns
            .into_iter()
            .map(|name| ColumnSpec::new(name, TypeSignature::Text))
            .collect::<Vec<_>>();
        let oid = self.create_table(schema_name, view_name, kind, specs, Vec::new(), Vec::new())?;
        let schema_name = normalize_name(schema_name)?;
        let view_name = normalize_name(view_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&view_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, view_name),
            });
        };
        table.set_view_definition(Some(definition));
        Ok(oid)
    }

    pub fn replace_view(
        &mut self,
        schema_name: &str,
        view_name: &str,
        materialized: bool,
        columns: Vec<String>,
        definition: Query,
    ) -> Result<Oid, CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let view_name = normalize_name(view_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&view_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, view_name),
            });
        };
        let expected_kind = if materialized {
            TableKind::MaterializedView
        } else {
            TableKind::View
        };
        if table.kind() != expected_kind {
            return Err(CatalogError {
                message: if materialized {
                    format!(
                        "\"{}.{}\" is not a materialized view",
                        schema_name, view_name
                    )
                } else {
                    format!("\"{}.{}\" is not a view", schema_name, view_name)
                },
            });
        }

        let mut normalized_columns = Vec::with_capacity(columns.len());
        for (idx, column_name) in columns.into_iter().enumerate() {
            let column_name = normalize_name(&column_name)?;
            if normalized_columns
                .iter()
                .any(|existing: &Column| existing.name() == column_name)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" specified more than once for relation \"{}.{}\"",
                        column_name, schema_name, view_name
                    ),
                });
            }
            normalized_columns.push(Column::new(
                self.oid_gen.next_oid(),
                column_name,
                TypeSignature::Text,
                idx as u16,
                true,
                false,
                false,
                None,
                None,
                None,
            ));
        }

        table.columns_mut().clear();
        table.columns_mut().extend(normalized_columns);
        table.key_constraints_mut().clear();
        table.foreign_key_constraints_mut().clear();
        table.indexes_mut().clear();
        table.set_view_definition(Some(definition));
        Ok(table.oid())
    }

    pub fn rename_relation(
        &mut self,
        schema_name: &str,
        relation_name: &str,
        new_name: &str,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let relation_name = normalize_name(relation_name)?;
        let new_name = normalize_name(new_name)?;

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        if schema.table(&new_name).is_some() {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" already exists", schema_name, new_name),
            });
        }
        let Some(mut table) = schema.remove_table(&relation_name) else {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" does not exist",
                    schema_name, relation_name
                ),
            });
        };
        table.set_name(new_name);
        schema.insert_table(table);
        Ok(())
    }

    pub fn move_relation_to_schema(
        &mut self,
        source_schema_name: &str,
        relation_name: &str,
        target_schema_name: &str,
    ) -> Result<(), CatalogError> {
        let source_schema_name = normalize_name(source_schema_name)?;
        let relation_name = normalize_name(relation_name)?;
        let target_schema_name = normalize_name(target_schema_name)?;
        if source_schema_name == target_schema_name {
            return Ok(());
        }

        let Some(target_schema) = self.schemas.get(&target_schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{target_schema_name}\" does not exist"),
            });
        };
        if target_schema.table(&relation_name).is_some() {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" already exists",
                    target_schema_name, relation_name
                ),
            });
        }
        let target_schema_oid = target_schema.oid();
        let target_schema_name_copy = target_schema.name().to_string();

        let Some(source_schema) = self.schemas.get_mut(&source_schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{source_schema_name}\" does not exist"),
            });
        };
        let Some(mut table) = source_schema.remove_table(&relation_name) else {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" does not exist",
                    source_schema_name, relation_name
                ),
            });
        };
        table.set_schema(target_schema_oid, target_schema_name_copy);

        let Some(target_schema) = self.schemas.get_mut(&target_schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{target_schema_name}\" does not exist"),
            });
        };
        target_schema.insert_table(table);
        Ok(())
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let Some(target_table) = self.table(&schema_name, &table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };
        let target_oid = target_table.oid();

        for schema in self.schemas.values() {
            for child_table in schema.tables() {
                if child_table.oid() == target_oid {
                    continue;
                }
                for constraint in child_table.foreign_key_constraints() {
                    let Ok(referenced) =
                        self.resolve_table(&constraint.referenced_table, &SearchPath::default())
                    else {
                        continue;
                    };
                    if referenced.oid() == target_oid {
                        return Err(CatalogError {
                            message: format!(
                                "cannot drop relation \"{}.{}\" because relation \"{}\" depends on it",
                                schema_name,
                                table_name,
                                child_table.qualified_name()
                            ),
                        });
                    }
                }
            }
        }

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };

        let Some(_) = schema.remove_table(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        Ok(())
    }

    pub fn add_column(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column: ColumnSpec,
    ) -> Result<Oid, CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        let column_name = normalize_name(&column.name)?;
        if table
            .columns()
            .iter()
            .any(|existing| existing.name() == column_name)
        {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" already exists in relation \"{}.{}\"",
                    column_name, schema_name, table_name
                ),
            });
        }
        if column.primary_key
            && table
                .key_constraints()
                .iter()
                .any(|constraint| constraint.primary)
        {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" already has a primary key",
                    schema_name, table_name
                ),
            });
        }

        let ColumnSpec {
            type_signature,
            nullable,
            unique,
            primary_key,
            references,
            check,
            default,
            ..
        } = column;

        let column_oid = self.oid_gen.next_oid();
        let ordinal = table.columns().len() as u16;
        table.add_column(Column::new(
            column_oid,
            column_name.clone(),
            type_signature,
            ordinal,
            nullable,
            unique,
            primary_key,
            references.clone(),
            check,
            default,
        ));
        if primary_key {
            table.key_constraints_mut().push(KeyConstraint {
                name: None,
                columns: vec![column_name.clone()],
                primary: true,
            });
        } else if unique {
            table.key_constraints_mut().push(KeyConstraint {
                name: None,
                columns: vec![column_name.clone()],
                primary: false,
            });
        }
        if let Some(reference) = references {
            table
                .foreign_key_constraints_mut()
                .push(ForeignKeyConstraint {
                    name: None,
                    columns: vec![column_name],
                    referenced_table: reference.table_name,
                    referenced_columns: reference.column_name.into_iter().collect(),
                    on_delete: reference.on_delete,
                    on_update: reference.on_update,
                });
        }
        Ok(column_oid)
    }

    pub fn add_key_constraint(
        &mut self,
        schema_name: &str,
        table_name: &str,
        mut constraint: KeyConstraintSpec,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        if let Some(name) = constraint.name.as_mut() {
            *name = normalize_name(name)?;
        }

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        if let Some(name) = &constraint.name {
            if constraint_name_exists(table, name) {
                return Err(CatalogError {
                    message: format!(
                        "constraint \"{}\" already exists for relation \"{}.{}\"",
                        name, schema_name, table_name
                    ),
                });
            }
        }
        if constraint.primary
            && table
                .key_constraints()
                .iter()
                .any(|existing| existing.primary)
        {
            return Err(CatalogError {
                message: format!(
                    "relation \"{}.{}\" already has a primary key",
                    schema_name, table_name
                ),
            });
        }

        let mut normalized_cols = Vec::with_capacity(constraint.columns.len());
        for col in constraint.columns {
            let col = normalize_name(&col)?;
            if !table
                .columns()
                .iter()
                .any(|existing| existing.name() == col)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" does not exist in relation \"{}.{}\"",
                        col, schema_name, table_name
                    ),
                });
            }
            if !normalized_cols.iter().any(|existing| existing == &col) {
                normalized_cols.push(col);
            }
        }
        if normalized_cols.is_empty() {
            return Err(CatalogError {
                message: format!(
                    "empty key constraint is invalid for relation \"{}.{}\"",
                    schema_name, table_name
                ),
            });
        }

        table.key_constraints_mut().push(KeyConstraint {
            name: constraint.name,
            columns: normalized_cols.clone(),
            primary: constraint.primary,
        });
        if constraint.primary {
            for key_col in normalized_cols {
                if let Some(column) = table
                    .columns_mut()
                    .iter_mut()
                    .find(|column| column.name() == key_col)
                {
                    column.set_nullable(false);
                }
            }
        }

        Ok(())
    }

    pub fn add_foreign_key_constraint(
        &mut self,
        schema_name: &str,
        table_name: &str,
        mut constraint: ForeignKeyConstraintSpec,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        if let Some(name) = constraint.name.as_mut() {
            *name = normalize_name(name)?;
        }

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        if let Some(name) = &constraint.name {
            if constraint_name_exists(table, name) {
                return Err(CatalogError {
                    message: format!(
                        "constraint \"{}\" already exists for relation \"{}.{}\"",
                        name, schema_name, table_name
                    ),
                });
            }
        }

        let mut normalized_cols = Vec::with_capacity(constraint.columns.len());
        for col in constraint.columns {
            let col = normalize_name(&col)?;
            if !table
                .columns()
                .iter()
                .any(|existing| existing.name() == col)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" does not exist in relation \"{}.{}\"",
                        col, schema_name, table_name
                    ),
                });
            }
            if !normalized_cols.iter().any(|existing| existing == &col) {
                normalized_cols.push(col);
            }
        }
        if normalized_cols.is_empty() {
            return Err(CatalogError {
                message: format!(
                    "empty foreign key constraint is invalid for relation \"{}.{}\"",
                    schema_name, table_name
                ),
            });
        }

        let mut normalized_referenced_table = Vec::with_capacity(constraint.referenced_table.len());
        for part in constraint.referenced_table {
            normalized_referenced_table.push(normalize_name(&part)?);
        }
        if normalized_referenced_table.is_empty() {
            return Err(CatalogError {
                message: format!(
                    "foreign key constraint has invalid REFERENCES target for relation \"{}.{}\"",
                    schema_name, table_name
                ),
            });
        }

        let mut normalized_referenced_columns =
            Vec::with_capacity(constraint.referenced_columns.len());
        for col in constraint.referenced_columns {
            let col = normalize_name(&col)?;
            if !normalized_referenced_columns
                .iter()
                .any(|existing| existing == &col)
            {
                normalized_referenced_columns.push(col);
            }
        }
        if !normalized_referenced_columns.is_empty()
            && normalized_referenced_columns.len() != normalized_cols.len()
        {
            return Err(CatalogError {
                message: format!(
                    "foreign key constraint has {} referencing columns but {} referenced columns in relation \"{}.{}\"",
                    normalized_cols.len(),
                    normalized_referenced_columns.len(),
                    schema_name,
                    table_name
                ),
            });
        }

        table
            .foreign_key_constraints_mut()
            .push(ForeignKeyConstraint {
                name: constraint.name,
                columns: normalized_cols,
                referenced_table: normalized_referenced_table,
                referenced_columns: normalized_referenced_columns,
                on_delete: constraint.on_delete,
                on_update: constraint.on_update,
            });
        Ok(())
    }

    pub fn add_index(
        &mut self,
        schema_name: &str,
        table_name: &str,
        mut index: IndexSpec,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        index.name = normalize_name(&index.name)?;

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        if table
            .indexes()
            .iter()
            .any(|existing| existing.name == index.name)
        {
            return Err(CatalogError {
                message: format!(
                    "index \"{}\" already exists on relation \"{}.{}\"",
                    index.name, schema_name, table_name
                ),
            });
        }
        let mut normalized_cols = Vec::new();
        for col in index.columns {
            let col = normalize_name(&col)?;
            if !table
                .columns()
                .iter()
                .any(|existing| existing.name() == col)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" does not exist in relation \"{}.{}\"",
                        col, schema_name, table_name
                    ),
                });
            }
            if !normalized_cols.iter().any(|existing| existing == &col) {
                normalized_cols.push(col);
            }
        }
        if normalized_cols.is_empty() {
            return Err(CatalogError {
                message: format!(
                    "index \"{}\" on relation \"{}.{}\" must reference at least one column",
                    index.name, schema_name, table_name
                ),
            });
        }

        table.indexes_mut().push(IndexSpec {
            name: index.name,
            columns: normalized_cols,
            unique: index.unique,
        });
        Ok(())
    }

    pub fn drop_index(
        &mut self,
        schema_name: &str,
        table_name: &str,
        index_name: &str,
        cascade: bool,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let index_name = normalize_name(index_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        let Some(index_pos) = table
            .indexes()
            .iter()
            .position(|index| index.name == index_name)
        else {
            return Err(CatalogError {
                message: format!(
                    "index \"{}\" does not exist on relation \"{}.{}\"",
                    index_name, schema_name, table_name
                ),
            });
        };

        let constraint_pos = table.key_constraints().iter().position(|constraint| {
            constraint
                .name
                .as_deref()
                .is_some_and(|name| name == index_name)
        });
        if constraint_pos.is_some() && !cascade {
            return Err(CatalogError {
                message: format!(
                    "cannot drop index \"{}\" because constraint \"{}\" on relation \"{}.{}\" depends on it",
                    index_name, index_name, schema_name, table_name
                ),
            });
        }

        table.indexes_mut().remove(index_pos);
        if let Some(pos) = constraint_pos {
            table.key_constraints_mut().remove(pos);
        }
        Ok(())
    }

    pub fn drop_column(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Result<usize, CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let column_name = normalize_name(column_name)?;
        let Some(table) = self.table(&schema_name, &table_name).cloned() else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };
        let target_table_oid = table.oid();

        let Some(index) = table
            .columns()
            .iter()
            .position(|column| column.name() == column_name)
        else {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" does not exist",
                    column_name, schema_name, table_name
                ),
            });
        };
        if table.key_constraints().iter().any(|constraint| {
            constraint
                .columns
                .iter()
                .any(|column| column == &column_name)
        }) {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" is referenced by a key constraint",
                    column_name, schema_name, table_name
                ),
            });
        }
        for constraint in table.foreign_key_constraints() {
            if constraint
                .columns
                .iter()
                .any(|column| column == &column_name)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" of relation \"{}.{}\" is referenced by a foreign key constraint",
                        column_name, schema_name, table_name
                    ),
                });
            }
            let references_this_table = matches!(constraint.referenced_table.as_slice(), [table] if table == &table_name)
                || matches!(
                    constraint.referenced_table.as_slice(),
                    [schema, table] if schema == &schema_name && table == &table_name
                );
            if !references_this_table {
                continue;
            }
            let referenced_columns = referenced_columns_for_constraint(&table, constraint, &table);
            if referenced_columns
                .iter()
                .any(|column| column == &column_name)
            {
                return Err(CatalogError {
                    message: format!(
                        "column \"{}\" of relation \"{}.{}\" is referenced by a foreign key constraint",
                        column_name, schema_name, table_name
                    ),
                });
            }
        }
        if table
            .indexes()
            .iter()
            .any(|index| index.columns.iter().any(|column| column == &column_name))
        {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" is referenced by an index",
                    column_name, schema_name, table_name
                ),
            });
        }

        for schema in self.schemas.values() {
            for child_table in schema.tables() {
                if child_table.oid() == target_table_oid {
                    continue;
                }
                for constraint in child_table.foreign_key_constraints() {
                    let Ok(referenced) =
                        self.resolve_table(&constraint.referenced_table, &SearchPath::default())
                    else {
                        continue;
                    };
                    if referenced.oid() != target_table_oid {
                        continue;
                    }
                    let referenced_columns =
                        referenced_columns_for_constraint(child_table, constraint, referenced);
                    if referenced_columns
                        .iter()
                        .any(|column| column == &column_name)
                    {
                        return Err(CatalogError {
                            message: format!(
                                "column \"{}\" of relation \"{}.{}\" is referenced by foreign key on relation \"{}\"",
                                column_name,
                                schema_name,
                                table_name,
                                child_table.qualified_name()
                            ),
                        });
                    }
                }
            }
        }

        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        table.columns_mut().remove(index);
        for (ordinal, column) in table.columns_mut().iter_mut().enumerate() {
            column.set_ordinal(ordinal as u16);
        }
        Ok(index)
    }

    pub fn drop_constraint(
        &mut self,
        schema_name: &str,
        table_name: &str,
        constraint_name: &str,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let constraint_name = normalize_name(constraint_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        if let Some(index) = table.key_constraints().iter().position(|constraint| {
            constraint
                .name
                .as_deref()
                .is_some_and(|name| name == constraint_name)
        }) {
            table.key_constraints_mut().remove(index);
            return Ok(());
        }
        if let Some(index) = table
            .foreign_key_constraints()
            .iter()
            .position(|constraint| {
                constraint
                    .name
                    .as_deref()
                    .is_some_and(|name| name == constraint_name)
            })
        {
            table.foreign_key_constraints_mut().remove(index);
            return Ok(());
        }

        Err(CatalogError {
            message: format!(
                "constraint \"{}\" of relation \"{}.{}\" does not exist",
                constraint_name, schema_name, table_name
            ),
        })
    }

    pub fn rename_column(
        &mut self,
        schema_name: &str,
        table_name: &str,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let old_name = normalize_name(old_name)?;
        let new_name = normalize_name(new_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };

        if table
            .columns()
            .iter()
            .any(|column| column.name() == new_name)
        {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" already exists in relation \"{}.{}\"",
                    new_name, schema_name, table_name
                ),
            });
        }

        let Some(column) = table
            .columns_mut()
            .iter_mut()
            .find(|column| column.name() == old_name)
        else {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" does not exist",
                    old_name, schema_name, table_name
                ),
            });
        };

        column.set_name(new_name.clone());
        for constraint in table.key_constraints_mut() {
            for key_col in &mut constraint.columns {
                if key_col == &old_name {
                    *key_col = new_name.clone();
                }
            }
        }
        for constraint in table.foreign_key_constraints_mut() {
            for fk_col in &mut constraint.columns {
                if fk_col == &old_name {
                    *fk_col = new_name.clone();
                }
            }
            let references_this_table = match constraint.referenced_table.as_slice() {
                [table_only] => table_only == &table_name,
                [schema_part, table_part] => {
                    schema_part == &schema_name && table_part == &table_name
                }
                _ => false,
            };
            if references_this_table {
                for parent_col in &mut constraint.referenced_columns {
                    if parent_col == &old_name {
                        *parent_col = new_name.clone();
                    }
                }
            }
        }
        Ok(())
    }

    pub fn set_column_nullable(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        nullable: bool,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let column_name = normalize_name(column_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };
        let part_of_primary_key = table.key_constraints().iter().any(|constraint| {
            constraint.primary
                && constraint
                    .columns
                    .iter()
                    .any(|column| column == &column_name)
        });
        let Some(column) = table
            .columns_mut()
            .iter_mut()
            .find(|column| column.name() == column_name)
        else {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" does not exist",
                    column_name, schema_name, table_name
                ),
            });
        };
        if nullable && part_of_primary_key {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" is part of a primary key",
                    column_name, schema_name, table_name
                ),
            });
        }

        column.set_nullable(nullable);
        Ok(())
    }

    pub fn set_column_default(
        &mut self,
        schema_name: &str,
        table_name: &str,
        column_name: &str,
        default: Option<Expr>,
    ) -> Result<(), CatalogError> {
        let schema_name = normalize_name(schema_name)?;
        let table_name = normalize_name(table_name)?;
        let column_name = normalize_name(column_name)?;
        let Some(schema) = self.schemas.get_mut(&schema_name) else {
            return Err(CatalogError {
                message: format!("schema \"{schema_name}\" does not exist"),
            });
        };
        let Some(table) = schema.table_mut(&table_name) else {
            return Err(CatalogError {
                message: format!("relation \"{}.{}\" does not exist", schema_name, table_name),
            });
        };
        let Some(column) = table
            .columns_mut()
            .iter_mut()
            .find(|column| column.name() == column_name)
        else {
            return Err(CatalogError {
                message: format!(
                    "column \"{}\" of relation \"{}.{}\" does not exist",
                    column_name, schema_name, table_name
                ),
            });
        };

        column.set_default(default);
        Ok(())
    }

    pub fn sequence_default_dependents(
        &self,
        sequence_name: &str,
    ) -> Vec<(String, String, String)> {
        let Some(normalized_sequence) = normalize_sequence_ref(sequence_name) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for schema in self.schemas.values() {
            for table in schema.tables() {
                for column in table.columns() {
                    let Some(default) = column.default() else {
                        continue;
                    };
                    if default_references_sequence(default, &normalized_sequence) {
                        out.push((
                            schema.name().to_string(),
                            table.name().to_string(),
                            column.name().to_string(),
                        ));
                    }
                }
            }
        }
        out
    }

    pub fn sequence_relation_dependents(&self, sequence_name: &str) -> Vec<(String, String)> {
        let Some(normalized_sequence) = normalize_sequence_ref(sequence_name) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for schema in self.schemas.values() {
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
                if query_references_sequence(definition, &normalized_sequence) {
                    out.push((schema.name().to_string(), relation.name().to_string()));
                }
            }
        }
        out
    }

    pub fn clear_sequence_defaults(&mut self, sequence_name: &str) -> usize {
        let Some(normalized_sequence) = normalize_sequence_ref(sequence_name) else {
            return 0;
        };
        let mut cleared = 0usize;
        for schema in self.schemas.values_mut() {
            for table in schema.tables_mut() {
                for column in table.columns_mut() {
                    let Some(default) = column.default() else {
                        continue;
                    };
                    if default_references_sequence(default, &normalized_sequence) {
                        column.set_default(None);
                        cleared += 1;
                    }
                }
            }
        }
        cleared
    }

    pub fn table(&self, schema_name: &str, table_name: &str) -> Option<&Table> {
        let schema_name = normalize_name(schema_name).ok()?;
        let table_name = normalize_name(table_name).ok()?;
        self.schemas.get(&schema_name)?.table(&table_name)
    }

    pub fn resolve_table(
        &self,
        relation_name: &[String],
        search_path: &SearchPath,
    ) -> Result<&Table, CatalogError> {
        match relation_name {
            [] => Err(CatalogError {
                message: "empty relation name".to_string(),
            }),
            [table_name] => {
                let normalized_table_name = normalize_name(table_name)?;
                for schema_name in search_path.schemas() {
                    if let Some(table) = self.table(schema_name, &normalized_table_name) {
                        return Ok(table);
                    }
                }
                Err(CatalogError {
                    message: format!("relation \"{}\" does not exist", table_name),
                })
            }
            [schema_name, table_name] => {
                let schema_name = normalize_name(schema_name)?;
                let table_name = normalize_name(table_name)?;
                let Some(table) = self.table(&schema_name, &table_name) else {
                    return Err(CatalogError {
                        message: format!(
                            "relation \"{}.{}\" does not exist",
                            schema_name, table_name
                        ),
                    });
                };
                Ok(table)
            }
            _ => Err(CatalogError {
                message: format!("invalid relation name \"{}\"", relation_name.join(".")),
            }),
        }
    }
}

fn normalize_name(name: &str) -> Result<String, CatalogError> {
    let normalized = name.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CatalogError {
            message: "name cannot be empty".to_string(),
        });
    }
    Ok(normalized)
}

fn normalize_sequence_ref(raw: &str) -> Option<String> {
    let parts = raw
        .split('.')
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    match parts.as_slice() {
        [name] => Some(format!("public.{}", name)),
        [schema, name] => Some(format!("{}.{}", schema, name)),
        _ => None,
    }
}

fn default_references_sequence(default: &Expr, sequence_name: &str) -> bool {
    let Expr::FunctionCall { name, args, .. } = default else {
        return false;
    };
    if !name
        .last()
        .is_some_and(|part| part.eq_ignore_ascii_case("nextval"))
    {
        return false;
    }
    let Some(Expr::String(raw)) = args.first() else {
        return false;
    };
    normalize_sequence_ref(raw).is_some_and(|normalized| normalized == sequence_name)
}

fn query_references_sequence(query: &Query, sequence_name: &str) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            if query_references_sequence(&cte.query, sequence_name) {
                return true;
            }
        }
    }
    query_expr_references_sequence(&query.body, sequence_name)
        || query
            .order_by
            .iter()
            .any(|order| expr_references_sequence(&order.expr, sequence_name))
        || query
            .limit
            .as_ref()
            .is_some_and(|expr| expr_references_sequence(expr, sequence_name))
        || query
            .offset
            .as_ref()
            .is_some_and(|expr| expr_references_sequence(expr, sequence_name))
}

fn query_expr_references_sequence(expr: &QueryExpr, sequence_name: &str) -> bool {
    match expr {
        QueryExpr::Select(select) => {
            select
                .targets
                .iter()
                .any(|target| expr_references_sequence(&target.expr, sequence_name))
                || select
                    .from
                    .iter()
                    .any(|from| table_expression_references_sequence(from, sequence_name))
                || select
                    .where_clause
                    .as_ref()
                    .is_some_and(|expr| expr_references_sequence(expr, sequence_name))
                || select
                    .group_by
                    .iter()
                    .any(|expr| expr_references_sequence(expr, sequence_name))
                || select
                    .having
                    .as_ref()
                    .is_some_and(|expr| expr_references_sequence(expr, sequence_name))
        }
        QueryExpr::SetOperation { left, right, .. } => {
            query_expr_references_sequence(left, sequence_name)
                || query_expr_references_sequence(right, sequence_name)
        }
        QueryExpr::Nested(query) => query_references_sequence(query, sequence_name),
    }
}

fn table_expression_references_sequence(table: &TableExpression, sequence_name: &str) -> bool {
    match table {
        TableExpression::Relation(_) => false,
        TableExpression::Function(function) => function
            .args
            .iter()
            .any(|arg| expr_references_sequence(arg, sequence_name)),
        TableExpression::Subquery(sub) => query_references_sequence(&sub.query, sequence_name),
        TableExpression::Join(join) => {
            table_expression_references_sequence(&join.left, sequence_name)
                || table_expression_references_sequence(&join.right, sequence_name)
                || join
                    .condition
                    .as_ref()
                    .is_some_and(|condition| match condition {
                        crate::parser::ast::JoinCondition::On(expr) => {
                            expr_references_sequence(expr, sequence_name)
                        }
                        crate::parser::ast::JoinCondition::Using(_) => false,
                    })
        }
    }
}

fn expr_references_sequence(expr: &Expr, sequence_name: &str) -> bool {
    match expr {
        Expr::FunctionCall {
            name,
            args,
            order_by,
            filter,
            ..
        } => {
            if name
                .last()
                .is_some_and(|part| part.eq_ignore_ascii_case("nextval"))
                && args
                    .first()
                    .is_some_and(|arg| matches!(arg, Expr::String(_)))
                && {
                    let Expr::String(raw) = &args[0] else {
                        return false;
                    };
                    normalize_sequence_ref(raw)
                        .is_some_and(|normalized| normalized == sequence_name)
                }
            {
                return true;
            }
            args.iter()
                .any(|arg| expr_references_sequence(arg, sequence_name))
                || order_by
                    .iter()
                    .any(|entry| expr_references_sequence(&entry.expr, sequence_name))
                || filter
                    .as_ref()
                    .is_some_and(|expr| expr_references_sequence(expr, sequence_name))
        }
        Expr::Unary { expr, .. } => expr_references_sequence(expr, sequence_name),
        Expr::Binary { left, right, .. } => {
            expr_references_sequence(left, sequence_name)
                || expr_references_sequence(right, sequence_name)
        }
        Expr::Exists(query) | Expr::ScalarSubquery(query) => {
            query_references_sequence(query, sequence_name)
        }
        Expr::InList { expr, list, .. } => {
            expr_references_sequence(expr, sequence_name)
                || list
                    .iter()
                    .any(|item| expr_references_sequence(item, sequence_name))
        }
        Expr::InSubquery { expr, subquery, .. } => {
            expr_references_sequence(expr, sequence_name)
                || query_references_sequence(subquery, sequence_name)
        }
        _ => false,
    }
}

fn constraint_name_exists(table: &Table, name: &str) -> bool {
    table
        .key_constraints()
        .iter()
        .any(|constraint| constraint.name.as_deref() == Some(name))
        || table
            .foreign_key_constraints()
            .iter()
            .any(|constraint| constraint.name.as_deref() == Some(name))
}

fn referenced_columns_for_constraint(
    _child_table: &Table,
    constraint: &ForeignKeyConstraint,
    referenced_table: &Table,
) -> Vec<String> {
    if !constraint.referenced_columns.is_empty() {
        return constraint.referenced_columns.clone();
    }
    if constraint.columns.len() == 1
        && referenced_table
            .columns()
            .iter()
            .any(|column| column.name() == constraint.columns[0])
    {
        return vec![constraint.columns[0].clone()];
    }
    if let Some(primary_key) = referenced_table
        .key_constraints()
        .iter()
        .find(|constraint| constraint.primary)
    {
        return primary_key.columns.clone();
    }
    constraint.columns.clone()
}

static GLOBAL_CATALOG: OnceLock<RwLock<Catalog>> = OnceLock::new();

fn global_catalog() -> &'static RwLock<Catalog> {
    GLOBAL_CATALOG.get_or_init(|| RwLock::new(Catalog::default()))
}

pub fn with_catalog_read<T>(f: impl FnOnce(&Catalog) -> T) -> T {
    let catalog = global_catalog()
        .read()
        .expect("global catalog lock poisoned for read");
    f(&catalog)
}

pub fn with_catalog_write<T>(f: impl FnOnce(&mut Catalog) -> T) -> T {
    let mut catalog = global_catalog()
        .write()
        .expect("global catalog lock poisoned for write");
    f(&mut catalog)
}

#[cfg(test)]
pub fn reset_global_catalog_for_tests() {
    with_catalog_write(|catalog| {
        *catalog = Catalog::default();
    });
}

#[cfg(test)]
pub fn with_global_state_lock<T>(f: impl FnOnce() -> T) -> T {
    static TEST_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    let mutex = TEST_MUTEX.get_or_init(|| Mutex::new(()));
    let _guard = mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creates_looks_up_and_drops_tables() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("id", TypeSignature::Int8).not_null(),
                    ColumnSpec::new("email", TypeSignature::Text).not_null(),
                ],
                Vec::new(),
                Vec::new(),
            )
            .expect("table should be creatable");

        let table = catalog.table("app", "users").expect("table should exist");
        assert_eq!(table.qualified_name(), "app.users");
        assert_eq!(table.columns().len(), 2);
        assert_eq!(table.columns()[0].name(), "id");

        catalog
            .drop_table("app", "users")
            .expect("table should be droppable");
        assert!(catalog.table("app", "users").is_none());
    }

    #[test]
    fn resolves_unqualified_names_using_search_path() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("tenant")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "public",
                "events",
                TableKind::Heap,
                vec![ColumnSpec::new("id", TypeSignature::Int8)],
                Vec::new(),
                Vec::new(),
            )
            .expect("public table should be creatable");
        catalog
            .create_table(
                "tenant",
                "events",
                TableKind::Heap,
                vec![ColumnSpec::new("id", TypeSignature::Int8)],
                Vec::new(),
                Vec::new(),
            )
            .expect("tenant table should be creatable");

        let custom_path = SearchPath::new(["tenant", "public"]);
        let table = catalog
            .resolve_table(&["events".to_string()], &custom_path)
            .expect("search path should resolve unqualified relation");
        assert_eq!(table.qualified_name(), "tenant.events");
    }

    #[test]
    fn rejects_duplicates_and_missing_schemas() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        let duplicate_schema = catalog.create_schema("app");
        assert!(duplicate_schema.is_err());

        let missing_schema_table = catalog.create_table(
            "missing",
            "users",
            TableKind::Heap,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        assert!(missing_schema_table.is_err());

        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                Vec::new(),
                Vec::new(),
                Vec::new(),
            )
            .expect("table should be creatable");
        let duplicate_table = catalog.create_table(
            "app",
            "users",
            TableKind::Heap,
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        assert!(duplicate_table.is_err());
    }

    #[test]
    fn adds_column_to_existing_table() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                vec![ColumnSpec::new("id", TypeSignature::Int8).not_null()],
                Vec::new(),
                Vec::new(),
            )
            .expect("table should be creatable");

        catalog
            .add_column("app", "users", ColumnSpec::new("name", TypeSignature::Text))
            .expect("column should be added");
        let table = catalog.table("app", "users").expect("table should exist");
        assert_eq!(table.columns().len(), 2);
        assert_eq!(table.columns()[1].name(), "name");
    }

    #[test]
    fn mutates_columns_with_drop_rename_and_nullability() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("id", TypeSignature::Int8).not_null(),
                    ColumnSpec::new("name", TypeSignature::Text),
                ],
                Vec::new(),
                Vec::new(),
            )
            .expect("table should be creatable");

        catalog
            .rename_column("app", "users", "name", "full_name")
            .expect("column should be renamed");
        catalog
            .set_column_nullable("app", "users", "full_name", false)
            .expect("column nullability should update");
        let dropped_index = catalog
            .drop_column("app", "users", "id")
            .expect("column should drop");
        assert_eq!(dropped_index, 0);

        let table = catalog.table("app", "users").expect("table should exist");
        assert_eq!(table.columns().len(), 1);
        assert_eq!(table.columns()[0].name(), "full_name");
        assert!(!table.columns()[0].nullable());
        assert_eq!(table.columns()[0].ordinal(), 0);
    }

    #[test]
    fn sets_and_drops_column_default() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("id", TypeSignature::Int8).not_null(),
                    ColumnSpec::new("note", TypeSignature::Text),
                ],
                Vec::new(),
                Vec::new(),
            )
            .expect("table should be creatable");

        catalog
            .set_column_default("app", "users", "note", Some(Expr::String("x".to_string())))
            .expect("default should set");
        let table = catalog.table("app", "users").expect("table should exist");
        assert_eq!(
            table.columns()[1].default(),
            Some(&Expr::String("x".to_string()))
        );

        catalog
            .set_column_default("app", "users", "note", None)
            .expect("default should drop");
        let table = catalog.table("app", "users").expect("table should exist");
        assert!(table.columns()[1].default().is_none());
    }

    #[test]
    fn supports_table_level_key_constraints() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "membership",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("user_id", TypeSignature::Int8),
                    ColumnSpec::new("org_id", TypeSignature::Int8),
                    ColumnSpec::new("email", TypeSignature::Text),
                ],
                vec![
                    KeyConstraintSpec {
                        name: None,
                        columns: vec!["user_id".to_string(), "org_id".to_string()],
                        primary: true,
                    },
                    KeyConstraintSpec {
                        name: None,
                        columns: vec!["email".to_string()],
                        primary: false,
                    },
                ],
                Vec::new(),
            )
            .expect("table should be creatable");

        let table = catalog
            .table("app", "membership")
            .expect("table should exist");
        assert_eq!(table.key_constraints().len(), 2);
        assert!(!table.columns()[0].nullable());
        assert!(!table.columns()[1].nullable());
    }

    #[test]
    fn drops_named_constraint() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users",
                TableKind::Heap,
                vec![
                    ColumnSpec::new("id", TypeSignature::Int8),
                    ColumnSpec::new("email", TypeSignature::Text),
                ],
                vec![KeyConstraintSpec {
                    name: Some("uq_email".to_string()),
                    columns: vec!["email".to_string()],
                    primary: false,
                }],
                Vec::new(),
            )
            .expect("table should be creatable");

        catalog
            .drop_constraint("app", "users", "uq_email")
            .expect("constraint should drop");

        let table = catalog.table("app", "users").expect("table should exist");
        assert!(table.key_constraints().is_empty());
    }

    #[test]
    fn renames_relation() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "users_v",
                TableKind::View,
                vec![ColumnSpec::new("id", TypeSignature::Text)],
                Vec::new(),
                Vec::new(),
            )
            .expect("relation should be creatable");

        catalog
            .rename_relation("app", "users_v", "users_view")
            .expect("relation should be renamed");
        assert!(catalog.table("app", "users_v").is_none());
        assert!(catalog.table("app", "users_view").is_some());
    }

    #[test]
    fn moves_relation_between_schemas() {
        let mut catalog = Catalog::default();
        catalog
            .create_schema("app")
            .expect("schema should be creatable");
        catalog
            .create_schema("archive")
            .expect("schema should be creatable");
        catalog
            .create_table(
                "app",
                "mv_users",
                TableKind::MaterializedView,
                vec![ColumnSpec::new("id", TypeSignature::Text)],
                Vec::new(),
                Vec::new(),
            )
            .expect("relation should be creatable");

        catalog
            .move_relation_to_schema("app", "mv_users", "archive")
            .expect("relation should move");
        assert!(catalog.table("app", "mv_users").is_none());
        let moved = catalog
            .table("archive", "mv_users")
            .expect("moved relation should exist");
        assert_eq!(moved.schema_name(), "archive");
    }
}
