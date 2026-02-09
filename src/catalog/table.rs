use super::oid::Oid;
use crate::parser::ast::{Expr, ForeignKeyAction, Query};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    /// Bootstrap system relation used for single-row `SELECT ... FROM dual` queries.
    VirtualDual,
    Heap,
    View,
    MaterializedView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeSignature {
    Bool,
    Int8,
    Float8,
    Text,
    Date,
    Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeySpec {
    pub table_name: Vec<String>,
    pub column_name: Option<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: Vec<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyConstraintSpec {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub referenced_table: Vec<String>,
    pub referenced_columns: Vec<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyConstraintSpec {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    oid: Oid,
    name: String,
    type_signature: TypeSignature,
    ordinal: u16,
    nullable: bool,
    unique: bool,
    primary_key: bool,
    references: Option<ForeignKeySpec>,
    check: Option<Expr>,
    default: Option<Expr>,
}

impl Column {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        oid: Oid,
        name: String,
        type_signature: TypeSignature,
        ordinal: u16,
        nullable: bool,
        unique: bool,
        primary_key: bool,
        references: Option<ForeignKeySpec>,
        check: Option<Expr>,
        default: Option<Expr>,
    ) -> Self {
        Self {
            oid,
            name,
            type_signature,
            ordinal,
            nullable,
            unique,
            primary_key,
            references,
            check,
            default,
        }
    }

    pub fn oid(&self) -> Oid {
        self.oid
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_signature(&self) -> TypeSignature {
        self.type_signature
    }

    pub fn ordinal(&self) -> u16 {
        self.ordinal
    }

    pub fn nullable(&self) -> bool {
        self.nullable
    }

    pub fn unique(&self) -> bool {
        self.unique
    }

    pub fn primary_key(&self) -> bool {
        self.primary_key
    }

    pub fn references(&self) -> Option<&ForeignKeySpec> {
        self.references.as_ref()
    }

    pub fn check(&self) -> Option<&Expr> {
        self.check.as_ref()
    }

    pub fn default(&self) -> Option<&Expr> {
        self.default.as_ref()
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_nullable(&mut self, nullable: bool) {
        self.nullable = nullable;
    }

    pub fn set_ordinal(&mut self, ordinal: u16) {
        self.ordinal = ordinal;
    }

    pub fn set_default(&mut self, default: Option<Expr>) {
        self.default = default;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSpec {
    pub name: String,
    pub type_signature: TypeSignature,
    pub nullable: bool,
    pub unique: bool,
    pub primary_key: bool,
    pub references: Option<ForeignKeySpec>,
    pub check: Option<Expr>,
    pub default: Option<Expr>,
}

impl ColumnSpec {
    pub fn new(name: impl Into<String>, type_signature: TypeSignature) -> Self {
        Self {
            name: name.into(),
            type_signature,
            nullable: true,
            unique: false,
            primary_key: false,
            references: None,
            check: None,
            default: None,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.unique = true;
        self.nullable = false;
        self
    }

    pub fn references(mut self, table_name: Vec<String>, column_name: Option<String>) -> Self {
        self.references = Some(ForeignKeySpec {
            table_name,
            column_name,
            on_delete: ForeignKeyAction::Restrict,
            on_update: ForeignKeyAction::Restrict,
        });
        self
    }

    pub fn check(mut self, expr: Expr) -> Self {
        self.check = Some(expr);
        self
    }

    pub fn default(mut self, expr: Expr) -> Self {
        self.default = Some(expr);
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    oid: Oid,
    schema_oid: Oid,
    schema_name: String,
    name: String,
    kind: TableKind,
    columns: Vec<Column>,
    key_constraints: Vec<KeyConstraint>,
    foreign_key_constraints: Vec<ForeignKeyConstraint>,
    indexes: Vec<IndexSpec>,
    view_definition: Option<Query>,
}

impl Table {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        oid: Oid,
        schema_oid: Oid,
        schema_name: String,
        name: String,
        kind: TableKind,
        columns: Vec<Column>,
        key_constraints: Vec<KeyConstraint>,
        foreign_key_constraints: Vec<ForeignKeyConstraint>,
        indexes: Vec<IndexSpec>,
        view_definition: Option<Query>,
    ) -> Self {
        Self {
            oid,
            schema_oid,
            schema_name,
            name,
            kind,
            columns,
            key_constraints,
            foreign_key_constraints,
            indexes,
            view_definition,
        }
    }

    pub fn oid(&self) -> Oid {
        self.oid
    }

    pub fn schema_oid(&self) -> Oid {
        self.schema_oid
    }

    pub fn schema_name(&self) -> &str {
        &self.schema_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn kind(&self) -> TableKind {
        self.kind
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    pub fn add_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn key_constraints(&self) -> &[KeyConstraint] {
        &self.key_constraints
    }

    pub fn key_constraints_mut(&mut self) -> &mut Vec<KeyConstraint> {
        &mut self.key_constraints
    }

    pub fn foreign_key_constraints(&self) -> &[ForeignKeyConstraint] {
        &self.foreign_key_constraints
    }

    pub fn foreign_key_constraints_mut(&mut self) -> &mut Vec<ForeignKeyConstraint> {
        &mut self.foreign_key_constraints
    }

    pub fn indexes(&self) -> &[IndexSpec] {
        &self.indexes
    }

    pub fn indexes_mut(&mut self) -> &mut Vec<IndexSpec> {
        &mut self.indexes
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema_name, self.name)
    }

    pub fn view_definition(&self) -> Option<&Query> {
        self.view_definition.as_ref()
    }

    pub fn set_view_definition(&mut self, definition: Option<Query>) {
        self.view_definition = definition;
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_schema(&mut self, schema_oid: Oid, schema_name: String) {
        self.schema_oid = schema_oid;
        self.schema_name = schema_name;
    }
}
