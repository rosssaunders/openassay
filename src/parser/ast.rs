#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Query(Query),
    CreateTable(CreateTableStatement),
    CreateSchema(CreateSchemaStatement),
    CreateIndex(CreateIndexStatement),
    CreateSequence(CreateSequenceStatement),
    CreateView(CreateViewStatement),
    RefreshMaterializedView(RefreshMaterializedViewStatement),
    AlterSequence(AlterSequenceStatement),
    AlterView(AlterViewStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Merge(MergeStatement),
    DropTable(DropTableStatement),
    DropSchema(DropSchemaStatement),
    DropIndex(DropIndexStatement),
    DropSequence(DropSequenceStatement),
    DropView(DropViewStatement),
    DropSubscription(DropSubscriptionStatement),
    Truncate(TruncateStatement),
    AlterTable(AlterTableStatement),
    Explain(ExplainStatement),
    Set(SetStatement),
    Show(ShowStatement),
    Discard(DiscardStatement),
    Do(DoStatement),
    Listen(ListenStatement),
    Notify(NotifyStatement),
    Unlisten(UnlistenStatement),
    CreateExtension(CreateExtensionStatement),
    DropExtension(DropExtensionStatement),
    CreateFunction(CreateFunctionStatement),
    CreateSubscription(CreateSubscriptionStatement),
    CreateRole(CreateRoleStatement),
    AlterRole(AlterRoleStatement),
    DropRole(DropRoleStatement),
    Grant(GrantStatement),
    Revoke(RevokeStatement),
    Copy(CopyStatement),
    Transaction(TransactionStatement),
    CreateType(CreateTypeStatement),
    CreateDomain(CreateDomainStatement),
    DropType(DropTypeStatement),
    DropDomain(DropDomainStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStatement {
    pub analyze: bool,
    pub verbose: bool,
    pub statement: Box<Statement>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatement {
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetStatement {
    pub name: String,
    pub value: String,
    pub is_local: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShowStatement {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscardStatement {
    pub target: String, // ALL, PLANS, SEQUENCES, TEMP
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DoStatement {
    pub body: String,
    pub language: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenStatement {
    pub channel: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotifyStatement {
    pub channel: String,
    pub payload: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnlistenStatement {
    pub channel: Option<String>, // None means UNLISTEN *
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateRoleStatement {
    pub name: String,
    pub options: Vec<RoleOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterRoleStatement {
    pub name: String,
    pub options: Vec<RoleOption>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropRoleStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleOption {
    Superuser(bool),
    Login(bool),
    Password(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TablePrivilegeKind {
    Select,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl TablePrivilegeKind {
    pub fn from_keyword(keyword: &str) -> Option<Self> {
        match keyword {
            "SELECT" => Some(Self::Select),
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            "TRUNCATE" => Some(Self::Truncate),
            _ => None,
        }
    }

    pub fn all() -> &'static [Self] {
        &[
            Self::Select,
            Self::Insert,
            Self::Update,
            Self::Delete,
            Self::Truncate,
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantRoleStatement {
    pub role_name: String,
    pub member: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrantTablePrivilegesStatement {
    pub privileges: Vec<TablePrivilegeKind>,
    pub table_name: Vec<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GrantStatement {
    Role(GrantRoleStatement),
    TablePrivileges(GrantTablePrivilegesStatement),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeRoleStatement {
    pub role_name: String,
    pub member: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevokeTablePrivilegesStatement {
    pub privileges: Vec<TablePrivilegeKind>,
    pub table_name: Vec<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RevokeStatement {
    Role(RevokeRoleStatement),
    TablePrivileges(RevokeTablePrivilegesStatement),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyDirection {
    To,
    From,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyOptions {
    pub format: Option<CopyFormat>,
    pub delimiter: Option<String>,
    pub null_marker: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyStatement {
    pub table_name: Vec<String>,
    pub direction: CopyDirection,
    pub options: CopyOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeName {
    Bool,
    Int2,   // SMALLINT
    Int4,   // INTEGER
    Int8,   // BIGINT
    Float4, // REAL
    Float8, // DOUBLE PRECISION
    Text,
    Varchar, // VARCHAR(n) - length stored separately in ColumnDefinition
    Char,    // CHAR(n)
    Bytea,
    Uuid,
    Json,
    Jsonb,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    Serial,    // auto-incrementing INT4
    BigSerial, // auto-incrementing INT8
    Numeric,   // DECIMAL/NUMERIC
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: TypeName,
    pub nullable: bool,
    pub identity: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub references: Option<ForeignKeyReference>,
    pub check: Option<Expr>,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub name: Vec<String>,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub if_not_exists: bool,
    pub temporary: bool,
    pub unlogged: bool,
    pub as_select: Option<Box<Query>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyReference {
    pub table_name: Vec<String>,
    pub column_name: Option<String>,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        referenced_table: Vec<String>,
        referenced_columns: Vec<String>,
        on_delete: ForeignKeyAction,
        on_update: ForeignKeyAction,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table_name: Vec<String>,
    pub table_alias: Option<String>,
    pub columns: Vec<String>,
    pub source: InsertSource,
    pub on_conflict: Option<OnConflictClause>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum InsertSource {
    Values(Vec<Vec<Expr>>),
    Query(Query),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictTarget {
    Columns(Vec<String>),
    Constraint(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictClause {
    DoNothing {
        conflict_target: Option<ConflictTarget>,
    },
    DoUpdate {
        conflict_target: Option<ConflictTarget>,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table_name: Vec<String>,
    pub assignments: Vec<Assignment>,
    pub from: Vec<TableExpression>,
    pub where_clause: Option<Expr>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table_name: Vec<String>,
    pub using: Vec<TableExpression>,
    pub where_clause: Option<Expr>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropBehavior {
    Restrict,
    Cascade,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTableStatement {
    pub name: Vec<String>,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSchemaStatement {
    pub name: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTypeStatement {
    pub name: Vec<String>,
    pub as_enum: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDomainStatement {
    pub name: Vec<String>,
    pub base_type: TypeName,
    pub check_constraint: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropTypeStatement {
    pub name: Vec<String>,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropDomainStatement {
    pub name: Vec<String>,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSchemaStatement {
    pub name: String,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropIndexStatement {
    pub name: Vec<String>,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSequenceStatement {
    pub name: Vec<String>,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateStatement {
    pub table_names: Vec<Vec<String>>,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateViewStatement {
    pub name: Vec<String>,
    pub or_replace: bool,
    pub materialized: bool,
    pub with_data: bool,
    pub query: Query,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshMaterializedViewStatement {
    pub name: Vec<String>,
    pub concurrently: bool,
    pub with_data: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropViewStatement {
    pub names: Vec<Vec<String>>,
    pub materialized: bool,
    pub if_exists: bool,
    pub behavior: DropBehavior,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MergeStatement {
    pub target_table: Vec<String>,
    pub target_alias: Option<String>,
    pub source: TableExpression,
    pub on: Expr,
    pub when_clauses: Vec<MergeWhenClause>,
    pub returning: Vec<SelectItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenClause {
    MatchedUpdate {
        condition: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    MatchedDelete {
        condition: Option<Expr>,
    },
    MatchedDoNothing {
        condition: Option<Expr>,
    },
    NotMatchedBySourceUpdate {
        condition: Option<Expr>,
        assignments: Vec<Assignment>,
    },
    NotMatchedBySourceDelete {
        condition: Option<Expr>,
    },
    NotMatchedBySourceDoNothing {
        condition: Option<Expr>,
    },
    NotMatchedInsert {
        condition: Option<Expr>,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
    NotMatchedDoNothing {
        condition: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table_name: Vec<String>,
    pub columns: Vec<String>,
    pub unique: bool,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSequenceStatement {
    pub name: Vec<String>,
    pub start: Option<i64>,
    pub increment: Option<i64>,
    pub min_value: Option<Option<i64>>,
    pub max_value: Option<Option<i64>>,
    pub cycle: Option<bool>,
    pub cache: Option<i64>,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterSequenceAction {
    Restart { with: Option<i64> },
    SetStart { start: i64 },
    SetIncrement { increment: i64 },
    SetMinValue { min: Option<i64> },
    SetMaxValue { max: Option<i64> },
    SetCycle { cycle: bool },
    SetCache { cache: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterSequenceStatement {
    pub name: Vec<String>,
    pub actions: Vec<AlterSequenceAction>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlterViewAction {
    RenameTo { new_name: String },
    RenameColumn { old_name: String, new_name: String },
    SetSchema { schema_name: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterViewStatement {
    pub name: Vec<String>,
    pub materialized: bool,
    pub action: AlterViewAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableAction {
    AddColumn(ColumnDefinition),
    AddConstraint(TableConstraint),
    DropColumn { name: String },
    DropConstraint { name: String },
    RenameColumn { old_name: String, new_name: String },
    SetColumnNullable { name: String, nullable: bool },
    SetColumnDefault { name: String, default: Option<Expr> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table_name: Vec<String>,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub with: Option<WithClause>,
    pub body: QueryExpr,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WithClause {
    pub recursive: bool,
    pub ctes: Vec<CommonTableExpr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommonTableExpr {
    pub name: String,
    pub column_names: Vec<String>, // Optional column list: WITH cte(a, b) AS (...)
    pub materialized: Option<bool>, // None = default, Some(true) = MATERIALIZED, Some(false) = NOT MATERIALIZED
    pub query: Query,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum QueryExpr {
    Select(SelectStatement),
    SetOperation {
        left: Box<QueryExpr>,
        op: SetOperator,
        quantifier: SetQuantifier,
        right: Box<QueryExpr>,
    },
    Nested(Box<Query>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetQuantifier {
    All,
    Distinct,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SelectQuantifier {
    All,
    Distinct,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub quantifier: Option<SelectQuantifier>,
    pub distinct_on: Vec<Expr>,
    pub targets: Vec<SelectItem>,
    pub from: Vec<TableExpression>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<GroupByExpr>,
    pub having: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GroupByExpr {
    Expr(Expr),
    GroupingSets(Vec<Vec<Expr>>),
    Rollup(Vec<Expr>),
    Cube(Vec<Expr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: Vec<String>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableFunctionRef {
    pub name: Vec<String>,
    pub args: Vec<Expr>,
    pub alias: Option<String>,
    pub column_aliases: Vec<String>,
    pub column_alias_types: Vec<Option<String>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SubqueryRef {
    pub query: Query,
    pub alias: Option<String>,
    pub lateral: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct JoinExpr {
    pub left: Box<TableExpression>,
    pub kind: JoinType,
    pub right: Box<TableExpression>,
    pub condition: Option<JoinCondition>,
    pub natural: bool,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum TableExpression {
    Relation(TableRef),
    Function(TableFunctionRef),
    Subquery(SubqueryRef),
    Join(JoinExpr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On(Expr),
    Using(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub ascending: Option<bool>,
    pub using_operator: Option<String>, // PostgreSQL USING operator (e.g., "<", ">")
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowSpec {
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    OffsetPreceding(Expr),
    CurrentRow,
    OffsetFollowing(Expr),
    UnboundedFollowing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameExclusion {
    CurrentRow,
    Group,
    Ties,
    NoOthers,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
    pub exclusion: Option<WindowFrameExclusion>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Identifier(Vec<String>),
    String(String),
    Integer(i64),
    Float(String),
    Boolean(bool),
    Null,
    Parameter(i32),
    FunctionCall {
        name: Vec<String>,
        args: Vec<Expr>,
        distinct: bool,
        order_by: Vec<OrderByExpr>,
        within_group: Vec<OrderByExpr>,
        filter: Option<Box<Expr>>,
        over: Option<Box<WindowSpec>>,
    },
    Cast {
        expr: Box<Expr>,
        type_name: String,
    },
    Wildcard,
    QualifiedWildcard(Vec<String>), // e.g., table.* or schema.table.*
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    AnyAll {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
        quantifier: ComparisonQuantifier,
    },
    Exists(Box<Query>),
    ScalarSubquery(Box<Query>),
    ArrayConstructor(Vec<Expr>),
    ArraySubquery(Box<Query>),
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        subquery: Box<Query>,
        negated: bool,
    },
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        case_insensitive: bool,
        negated: bool,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },
    CaseSimple {
        operand: Box<Expr>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    CaseSearched {
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
    },
    ArraySubscript {
        expr: Box<Expr>,
        index: Box<Expr>,
    },
    ArraySlice {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    TypedLiteral {
        type_name: String,
        value: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComparisonQuantifier {
    Any,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnaryOp {
    Plus,
    Minus,
    Not,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BinaryOp {
    Or,
    And,
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    JsonGet,
    JsonGetText,
    JsonPath,
    JsonPathText,
    JsonConcat,
    JsonContains,
    JsonContainedBy,
    JsonPathExists,
    JsonPathMatch,
    JsonHasKey,
    JsonHasAny,
    JsonHasAll,
    JsonDeletePath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExtensionStatement {
    pub name: String,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropExtensionStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionOptions {
    pub copy_data: bool,
    pub slot_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSubscriptionStatement {
    pub name: String,
    pub connection: String,
    pub publication: String,
    pub options: SubscriptionOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DropSubscriptionStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FunctionParam {
    pub name: Option<String>,
    pub data_type: TypeName,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionReturnType {
    Type(TypeName),
    Table(Vec<ColumnDefinition>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateFunctionStatement {
    pub name: Vec<String>,
    pub params: Vec<FunctionParam>,
    pub return_type: Option<FunctionReturnType>,
    pub body: String,
    pub language: String,
    pub or_replace: bool,
}
