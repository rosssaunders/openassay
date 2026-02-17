//! PL/pgSQL AST and runtime model types.
//!
//! This module mirrors PostgreSQL's PL/pgSQL definitions from
//! `postgres/src/pl/plpgsql/src/plpgsql.h`.

/// Corresponds to PostgreSQL `Oid`-typed fields used in `plpgsql.h`.
pub type Oid = u32;

/// Corresponds to PostgreSQL `PLPGSQL_OTHERS` in `plpgsql.h:511`.
pub const PLPGSQL_OTHERS: i32 = -1;

/// Corresponds to `PLpgSQL_nsitem_type` in `plpgsql.h:42-50`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlNsitemType {
    Label,
    Var,
    Rec,
}

/// Corresponds to `PLpgSQL_label_type` in `plpgsql.h:52-59`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlLabelType {
    Block,
    Loop,
    Other,
}

/// Corresponds to `PLpgSQL_datum_type` in `plpgsql.h:62-71`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlDtype {
    Var,
    Row,
    Rec,
    Recfield,
    Promise,
}

/// Alias matching PostgreSQL naming in `plpgsql.h` (`PLpgSQL_datum_type`).
pub type PlPgSqlDatumType = PlPgSqlDtype;

/// Corresponds to `PLpgSQL_promise_type` in `plpgsql.h:74-89`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlPgSqlPromiseType {
    #[default]
    None,
    TgName,
    TgWhen,
    TgLevel,
    TgOp,
    TgRelid,
    TgTableName,
    TgTableSchema,
    TgNargs,
    TgArgv,
    TgEvent,
    TgTag,
}

/// Corresponds to `PLpgSQL_type_type` in `plpgsql.h:93-100`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlPgSqlTypeType {
    #[default]
    Scalar,
    Rec,
    Pseudo,
}

/// Corresponds to `PLpgSQL_stmt_type` in `plpgsql.h:103-132`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlStmtType {
    Block,
    Assign,
    If,
    Case,
    Loop,
    While,
    Fori,
    Fors,
    Forc,
    ForeachA,
    Exit,
    Return,
    ReturnNext,
    ReturnQuery,
    Raise,
    Assert,
    ExecSql,
    DynExecute,
    DynFors,
    GetDiag,
    Open,
    Fetch,
    Close,
    Perform,
    Call,
    Commit,
    Rollback,
}

/// Corresponds to `PLpgSQL_getdiag_kind` in `plpgsql.h:148-165`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlGetdiagKind {
    RowCount,
    RoutineOid,
    Context,
    ErrorContext,
    ErrorDetail,
    ErrorHint,
    ReturnedSqlstate,
    ColumnName,
    ConstraintName,
    DatatypeName,
    MessageText,
    TableName,
    SchemaName,
}

/// Corresponds to `PLpgSQL_raise_option_type` in `plpgsql.h:168-181`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlRaiseOptionType {
    Errcode,
    Message,
    Detail,
    Hint,
    Column,
    Constraint,
    Datatype,
    Table,
    Schema,
}

/// Corresponds to `PLpgSQL_resolve_option` in `plpgsql.h:184-191`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlPgSqlResolveOption {
    #[default]
    Error,
    Variable,
    Column,
}

/// Corresponds to `PLpgSQL_rwopt` in `plpgsql.h:194-206`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlPgSqlRwopt {
    #[default]
    Unknown,
    Nope,
    Transfer,
    Inplace,
}

/// Corresponds to `PLpgSQL_type` in `plpgsql.h:210-227`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlPgSqlType {
    pub typname: String,
    pub typoid: Oid,
    pub ttype: PlPgSqlTypeType,
    pub typlen: i16,
    pub typbyval: bool,
    pub typtype: char,
    pub collation: Oid,
    pub typisarray: bool,
    pub atttypmod: i32,
    pub origtypname: Option<String>,
    pub tcache: Option<String>,
    pub tupdesc_id: u64,
}

/// Corresponds to `PLpgSQL_nsitem` in `plpgsql.h:460-472`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlNsitem {
    pub itemtype: PlPgSqlNsitemType,
    pub itemno: i32,
    pub name: String,
}

/// Corresponds to `PLpgSQL_expr` in `plpgsql.h:230-292`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlPgSqlExpr {
    pub query: String,
    pub parse_mode: Option<String>,
    pub func_signature: Option<String>,
    pub ns: Vec<PlPgSqlNsitem>,
    pub target_param: i32,
    pub target_is_local: bool,
    pub plan: Option<String>,
    pub paramnos: Vec<i32>,
    pub expr_simple_expr: Option<String>,
    pub expr_simple_type: Option<Oid>,
    pub expr_simple_typmod: i32,
    pub expr_simple_mutable: bool,
    pub expr_rwopt: PlPgSqlRwopt,
    pub expr_rw_param: Option<String>,
    pub expr_simple_plansource: Option<String>,
    pub expr_simple_plan: Option<String>,
    pub expr_simple_plan_lxid: u32,
    pub expr_simple_state: Option<String>,
    pub expr_simple_in_use: bool,
    pub expr_simple_lxid: u32,
}

/// Corresponds to base `PLpgSQL_datum` in `plpgsql.h:298-304`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlDatumHeader {
    pub dtype: PlPgSqlDtype,
    pub dno: i32,
}

/// Corresponds to base `PLpgSQL_variable` in `plpgsql.h:310-319`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlVariable {
    pub dtype: PlPgSqlDtype,
    pub dno: i32,
    pub refname: String,
    pub lineno: i32,
    pub isconst: bool,
    pub notnull: bool,
    pub default_val: Option<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_var` in `plpgsql.h:332-366`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlVar {
    pub variable: PlPgSqlVariable,
    pub datatype: Option<PlPgSqlType>,
    pub cursor_explicit_expr: Option<PlPgSqlExpr>,
    pub cursor_explicit_argrow: i32,
    pub cursor_options: i32,
    pub value: Option<PlPgSqlValue>,
    pub isnull: bool,
    pub freeval: bool,
    pub promise: PlPgSqlPromiseType,
}

/// Corresponds to `PLpgSQL_row` in `plpgsql.h:386-409`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlRow {
    pub variable: PlPgSqlVariable,
    pub rowtupdesc: Option<String>,
    pub nfields: i32,
    pub fieldnames: Vec<String>,
    pub varnos: Vec<i32>,
}

/// Corresponds to `PLpgSQL_rec` in `plpgsql.h:412-440`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlRec {
    pub variable: PlPgSqlVariable,
    pub datatype: Option<PlPgSqlType>,
    pub rectypeid: Oid,
    pub firstfield: i32,
    pub erh: Option<String>,
}

/// Corresponds to `PLpgSQL_recfield` in `plpgsql.h:443-455`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlRecfield {
    pub datum: PlPgSqlDatumHeader,
    pub fieldname: String,
    pub recparentno: i32,
    pub nextfield: i32,
    pub rectupledescid: u64,
    pub finfo: Option<String>,
}

/// Corresponds to typed datum variants in `plpgsql.h:295-455`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlPgSqlDatum {
    Var(PlPgSqlVar),
    Row(PlPgSqlRow),
    Rec(PlPgSqlRec),
    Recfield(PlPgSqlRecfield),
}

/// Corresponds to `PLpgSQL_stmt` in `plpgsql.h:476-488`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtHeader {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
}

/// Corresponds to `PLpgSQL_condition` in `plpgsql.h:492-500`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlCondition {
    pub sqlerrstate: i32,
    pub condname: String,
}

/// Corresponds to `PLpgSQL_exception` in `plpgsql.h:515-521`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlException {
    pub lineno: i32,
    pub conditions: Vec<PlPgSqlCondition>,
    pub action: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_exception_block` in `plpgsql.h:505-512`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlExceptionBlock {
    pub sqlstate_varno: i32,
    pub sqlerrm_varno: i32,
    pub exc_list: Vec<PlPgSqlException>,
}

/// Corresponds to `PLpgSQL_stmt_block` in `plpgsql.h:525-535`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtBlock {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub body: Vec<PlPgSqlStmt>,
    pub n_initvars: i32,
    pub initvarnos: Vec<i32>,
    pub exceptions: Option<PlPgSqlExceptionBlock>,
}

/// Corresponds to `PLpgSQL_stmt_assign` in `plpgsql.h:540-547`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtAssign {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub varno: i32,
    pub fieldname: Option<String>,
    pub expr: PlPgSqlExpr,
}

/// Corresponds to `PLpgSQL_stmt_perform` in `plpgsql.h:552-558`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtPerform {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub expr: PlPgSqlExpr,
}

/// Corresponds to `PLpgSQL_stmt_call` in `plpgsql.h:563-571`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtCall {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub expr: PlPgSqlExpr,
    pub is_call: bool,
    pub target: Option<PlPgSqlVariable>,
}

/// Corresponds to `PLpgSQL_stmt_commit` in `plpgsql.h:576-582`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtCommit {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub chain: bool,
}

/// Corresponds to `PLpgSQL_stmt_rollback` in `plpgsql.h:587-593`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtRollback {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub chain: bool,
}

/// Corresponds to `PLpgSQL_diag_item` in `plpgsql.h:598-604`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlDiagItem {
    pub kind: PlPgSqlGetdiagKind,
    pub target: i32,
}

/// Corresponds to `PLpgSQL_stmt_getdiag` in `plpgsql.h:607-614`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtGetdiag {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub is_stacked: bool,
    pub diag_items: Vec<PlPgSqlDiagItem>,
}

/// Corresponds to `PLpgSQL_if_elsif` in `plpgsql.h:633-640`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlIfElsif {
    pub lineno: i32,
    pub cond: PlPgSqlExpr,
    pub stmts: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_if` in `plpgsql.h:619-628`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtIf {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub cond: PlPgSqlExpr,
    pub then_body: Vec<PlPgSqlStmt>,
    pub elsif_list: Vec<PlPgSqlIfElsif>,
    pub else_body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_case_when` in `plpgsql.h:658-665`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlCaseWhen {
    pub lineno: i32,
    pub expr: PlPgSqlExpr,
    pub stmts: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_case` in `plpgsql.h:643-653`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtCase {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub t_expr: Option<PlPgSqlExpr>,
    pub t_varno: i32,
    pub case_when_list: Vec<PlPgSqlCaseWhen>,
    pub have_else: bool,
    pub else_stmts: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_loop` in `plpgsql.h:668-675`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtLoop {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_while` in `plpgsql.h:680-688`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtWhile {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub cond: PlPgSqlExpr,
    pub body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_fori` in `plpgsql.h:693-705`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtFori {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<PlPgSqlVar>,
    pub lower: PlPgSqlExpr,
    pub upper: PlPgSqlExpr,
    pub step: Option<PlPgSqlExpr>,
    pub reverse: i32,
    pub body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_forq` in `plpgsql.h:712-720`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtForq {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<PlPgSqlVariable>,
    pub body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_fors` in `plpgsql.h:725-735`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtFors {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<PlPgSqlVariable>,
    pub body: Vec<PlPgSqlStmt>,
    pub query: PlPgSqlExpr,
}

/// Corresponds to `PLpgSQL_stmt_forc` in `plpgsql.h:740-751`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtForc {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<PlPgSqlVariable>,
    pub body: Vec<PlPgSqlStmt>,
    pub curvar: i32,
    pub argquery: Option<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_stmt_dynfors` in `plpgsql.h:756-767`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtDynfors {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub var: Option<PlPgSqlVariable>,
    pub body: Vec<PlPgSqlStmt>,
    pub query: PlPgSqlExpr,
    pub params: Vec<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_stmt_foreach_a` in `plpgsql.h:772-782`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtForeachA {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub label: Option<String>,
    pub varno: i32,
    pub slice: i32,
    pub expr: PlPgSqlExpr,
    pub body: Vec<PlPgSqlStmt>,
}

/// Corresponds to `PLpgSQL_stmt_open` in `plpgsql.h:787-798`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtOpen {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub curvar: i32,
    pub cursor_options: i32,
    pub argquery: Option<PlPgSqlExpr>,
    pub query: Option<PlPgSqlExpr>,
    pub dynquery: Option<PlPgSqlExpr>,
    pub params: Vec<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_stmt_fetch` in `plpgsql.h:803-815`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtFetch {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub target: Option<PlPgSqlVariable>,
    pub curvar: i32,
    pub direction: Option<String>,
    pub how_many: i64,
    pub expr: Option<PlPgSqlExpr>,
    pub is_move: bool,
    pub returns_multiple_rows: bool,
}

/// Corresponds to `PLpgSQL_stmt_close` in `plpgsql.h:820-826`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtClose {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub curvar: i32,
}

/// Corresponds to `PLpgSQL_stmt_exit` in `plpgsql.h:831-839`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtExit {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub is_exit: bool,
    pub label: Option<String>,
    pub cond: Option<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_stmt_return` in `plpgsql.h:844-851`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtReturn {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub expr: Option<PlPgSqlExpr>,
    pub retvarno: i32,
}

/// Corresponds to `PLpgSQL_stmt_return_next` in `plpgsql.h:856-863`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtReturnNext {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub expr: Option<PlPgSqlExpr>,
    pub retvarno: i32,
}

/// Corresponds to `PLpgSQL_stmt_return_query` in `plpgsql.h:868-876`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtReturnQuery {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub query: Option<PlPgSqlExpr>,
    pub dynquery: Option<PlPgSqlExpr>,
    pub params: Vec<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_raise_option` in `plpgsql.h:896-902`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlRaiseOption {
    pub opt_type: PlPgSqlRaiseOptionType,
    pub expr: PlPgSqlExpr,
}

/// Corresponds to `PLpgSQL_stmt_raise` in `plpgsql.h:881-891`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtRaise {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub elog_level: i32,
    pub condname: Option<String>,
    pub message: Option<String>,
    pub params: Vec<PlPgSqlExpr>,
    pub options: Vec<PlPgSqlRaiseOption>,
}

/// Corresponds to `PLpgSQL_stmt_assert` in `plpgsql.h:905-912`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtAssert {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub cond: PlPgSqlExpr,
    pub message: Option<PlPgSqlExpr>,
}

/// Corresponds to `PLpgSQL_stmt_execsql` in `plpgsql.h:917-928`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtExecSql {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub sqlstmt: PlPgSqlExpr,
    pub mod_stmt: bool,
    pub mod_stmt_set: bool,
    pub into: bool,
    pub strict: bool,
    pub target: Option<PlPgSqlVariable>,
    pub target_dnos: Vec<i32>,
}

/// Corresponds to `PLpgSQL_stmt_dynexecute` in `plpgsql.h:933-943`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlStmtDynexecute {
    pub cmd_type: PlPgSqlStmtType,
    pub lineno: i32,
    pub stmtid: u32,
    pub query: PlPgSqlExpr,
    pub into: bool,
    pub strict: bool,
    pub target: Option<PlPgSqlVariable>,
    pub params: Vec<PlPgSqlExpr>,
}

/// Corresponds to all executable statement node structs in `plpgsql.h:525-943`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlPgSqlStmt {
    Block(PlPgSqlStmtBlock),
    Assign(PlPgSqlStmtAssign),
    If(PlPgSqlStmtIf),
    Case(PlPgSqlStmtCase),
    Loop(PlPgSqlStmtLoop),
    While(PlPgSqlStmtWhile),
    Fori(PlPgSqlStmtFori),
    Fors(PlPgSqlStmtFors),
    Forc(PlPgSqlStmtForc),
    ForeachA(PlPgSqlStmtForeachA),
    Exit(PlPgSqlStmtExit),
    Return(PlPgSqlStmtReturn),
    ReturnNext(PlPgSqlStmtReturnNext),
    ReturnQuery(PlPgSqlStmtReturnQuery),
    Raise(PlPgSqlStmtRaise),
    Assert(PlPgSqlStmtAssert),
    ExecSql(PlPgSqlStmtExecSql),
    DynExecute(PlPgSqlStmtDynexecute),
    DynFors(PlPgSqlStmtDynfors),
    Getdiag(PlPgSqlStmtGetdiag),
    Open(PlPgSqlStmtOpen),
    Fetch(PlPgSqlStmtFetch),
    Close(PlPgSqlStmtClose),
    Perform(PlPgSqlStmtPerform),
    Call(PlPgSqlStmtCall),
    Commit(PlPgSqlStmtCommit),
    Rollback(PlPgSqlStmtRollback),
}

/// Corresponds to `PLpgSQL_trigtype` in `plpgsql.h:948-956`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlPgSqlTrigtype {
    DmlTrigger,
    EventTrigger,
    NotTrigger,
}

/// Corresponds to generic runtime values assigned into `PLpgSQL_var.value`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlPgSqlValue {
    Null,
    Bool(bool),
    Int(i64),
    Numeric(String),
    Text(String),
}

/// Corresponds to `PLpgSQL_function` in `plpgsql.h:958-1007`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlPgSqlFunction {
    pub cached_function: Option<String>,
    pub fn_signature: String,
    pub fn_oid: Oid,
    pub fn_is_trigger: PlPgSqlTrigtype,
    pub fn_input_collation: Oid,
    pub fn_cxt: Option<String>,
    pub fn_rettype: Oid,
    pub fn_rettyplen: i16,
    pub fn_retbyval: bool,
    pub fn_retistuple: bool,
    pub fn_retisdomain: bool,
    pub fn_retset: bool,
    pub fn_readonly: bool,
    pub fn_prokind: char,
    pub fn_nargs: i32,
    pub fn_argvarnos: Vec<i32>,
    pub out_param_varno: i32,
    pub found_varno: i32,
    pub new_varno: i32,
    pub old_varno: i32,
    pub resolve_option: PlPgSqlResolveOption,
    pub print_strict_params: bool,
    pub extra_warnings: i32,
    pub extra_errors: i32,
    pub ndatums: i32,
    pub datums: Vec<PlPgSqlDatum>,
    pub copiable_size: usize,
    pub action: Option<PlPgSqlStmtBlock>,
    pub nstatements: u32,
    pub requires_procedure_resowner: bool,
    pub has_exception_block: bool,
    pub cur_estate: Option<String>,
}

impl Default for PlPgSqlFunction {
    fn default() -> Self {
        Self {
            cached_function: None,
            fn_signature: String::new(),
            fn_oid: 0,
            fn_is_trigger: PlPgSqlTrigtype::NotTrigger,
            fn_input_collation: 0,
            fn_cxt: None,
            fn_rettype: 0,
            fn_rettyplen: 0,
            fn_retbyval: false,
            fn_retistuple: false,
            fn_retisdomain: false,
            fn_retset: false,
            fn_readonly: false,
            fn_prokind: 'f',
            fn_nargs: 0,
            fn_argvarnos: Vec::new(),
            out_param_varno: -1,
            found_varno: -1,
            new_varno: -1,
            old_varno: -1,
            resolve_option: PlPgSqlResolveOption::Error,
            print_strict_params: false,
            extra_warnings: 0,
            extra_errors: 0,
            ndatums: 0,
            datums: Vec::new(),
            copiable_size: 0,
            action: None,
            nstatements: 0,
            requires_procedure_resowner: false,
            has_exception_block: false,
            cur_estate: None,
        }
    }
}

/// Corresponds to `PLpgSQL_execstate` in `plpgsql.h:1012-1086`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PlPgSqlExecstate {
    pub func_signature: Option<String>,
    pub trigdata: Option<String>,
    pub evtrigdata: Option<String>,
    pub retval: Option<PlPgSqlValue>,
    pub retisnull: bool,
    pub rettype: Oid,
    pub fn_rettype: Oid,
    pub retistuple: bool,
    pub retisset: bool,
    pub readonly_func: bool,
    pub atomic: bool,
    pub exitlabel: Option<String>,
    pub cur_error: Option<String>,
    pub tuple_store: Option<String>,
    pub tuple_store_desc: Option<String>,
    pub tuple_store_cxt: Option<String>,
    pub tuple_store_owner: Option<String>,
    pub rsi: Option<String>,
    pub found_varno: i32,
    pub ndatums: i32,
    pub datums: Vec<PlPgSqlDatum>,
    pub datum_context: Option<String>,
    pub param_li: Option<String>,
    pub simple_eval_estate: Option<String>,
    pub simple_eval_resowner: Option<String>,
    pub procedure_resowner: Option<String>,
    pub cast_hash: Option<String>,
    pub stmt_mcontext: Option<String>,
    pub stmt_mcontext_parent: Option<String>,
    pub eval_tuptable: Option<String>,
    pub eval_processed: u64,
    pub eval_econtext: Option<String>,
    pub err_stmt: Option<PlPgSqlStmt>,
    pub err_var: Option<PlPgSqlVariable>,
    pub err_text: Option<String>,
    pub plugin_info: Option<String>,
}

impl PlPgSqlStmt {
    /// Corresponds to statement kind dispatch in `pl_funcs.c:214-276`.
    pub fn cmd_type(&self) -> PlPgSqlStmtType {
        match self {
            Self::Block(_) => PlPgSqlStmtType::Block,
            Self::Assign(_) => PlPgSqlStmtType::Assign,
            Self::If(_) => PlPgSqlStmtType::If,
            Self::Case(_) => PlPgSqlStmtType::Case,
            Self::Loop(_) => PlPgSqlStmtType::Loop,
            Self::While(_) => PlPgSqlStmtType::While,
            Self::Fori(_) => PlPgSqlStmtType::Fori,
            Self::Fors(_) => PlPgSqlStmtType::Fors,
            Self::Forc(_) => PlPgSqlStmtType::Forc,
            Self::ForeachA(_) => PlPgSqlStmtType::ForeachA,
            Self::Exit(_) => PlPgSqlStmtType::Exit,
            Self::Return(_) => PlPgSqlStmtType::Return,
            Self::ReturnNext(_) => PlPgSqlStmtType::ReturnNext,
            Self::ReturnQuery(_) => PlPgSqlStmtType::ReturnQuery,
            Self::Raise(_) => PlPgSqlStmtType::Raise,
            Self::Assert(_) => PlPgSqlStmtType::Assert,
            Self::ExecSql(_) => PlPgSqlStmtType::ExecSql,
            Self::DynExecute(_) => PlPgSqlStmtType::DynExecute,
            Self::DynFors(_) => PlPgSqlStmtType::DynFors,
            Self::Getdiag(_) => PlPgSqlStmtType::GetDiag,
            Self::Open(_) => PlPgSqlStmtType::Open,
            Self::Fetch(_) => PlPgSqlStmtType::Fetch,
            Self::Close(_) => PlPgSqlStmtType::Close,
            Self::Perform(_) => PlPgSqlStmtType::Perform,
            Self::Call(_) => PlPgSqlStmtType::Call,
            Self::Commit(_) => PlPgSqlStmtType::Commit,
            Self::Rollback(_) => PlPgSqlStmtType::Rollback,
        }
    }

    /// Corresponds to `plpgsql_stmt_typename` in `pl_funcs.c:214-276`.
    pub fn typename(&self) -> &'static str {
        match self {
            Self::Block(_) => "statement block",
            Self::Assign(_) => "assignment",
            Self::If(_) => "IF",
            Self::Case(_) => "CASE",
            Self::Loop(_) => "LOOP",
            Self::While(_) => "WHILE",
            Self::Fori(_) => "FOR with integer loop variable",
            Self::Fors(_) => "FOR over SELECT rows",
            Self::Forc(_) => "FOR over cursor",
            Self::ForeachA(_) => "FOREACH over array",
            Self::Exit(stmt) => {
                if stmt.is_exit {
                    "EXIT"
                } else {
                    "CONTINUE"
                }
            }
            Self::Return(_) => "RETURN",
            Self::ReturnNext(_) => "RETURN NEXT",
            Self::ReturnQuery(_) => "RETURN QUERY",
            Self::Raise(_) => "RAISE",
            Self::Assert(_) => "ASSERT",
            Self::ExecSql(_) => "SQL statement",
            Self::DynExecute(_) => "EXECUTE",
            Self::DynFors(_) => "FOR over EXECUTE statement",
            Self::Getdiag(stmt) => {
                if stmt.is_stacked {
                    "GET STACKED DIAGNOSTICS"
                } else {
                    "GET DIAGNOSTICS"
                }
            }
            Self::Open(_) => "OPEN",
            Self::Fetch(stmt) => {
                if stmt.is_move {
                    "MOVE"
                } else {
                    "FETCH"
                }
            }
            Self::Close(_) => "CLOSE",
            Self::Perform(_) => "PERFORM",
            Self::Call(stmt) => {
                if stmt.is_call {
                    "CALL"
                } else {
                    "DO"
                }
            }
            Self::Commit(_) => "COMMIT",
            Self::Rollback(_) => "ROLLBACK",
        }
    }
}

/// Corresponds to `plpgsql_getdiag_kindname` in `pl_funcs.c:284-316`.
pub fn getdiag_kind_name(kind: PlPgSqlGetdiagKind) -> &'static str {
    match kind {
        PlPgSqlGetdiagKind::RowCount => "ROW_COUNT",
        PlPgSqlGetdiagKind::RoutineOid => "PG_ROUTINE_OID",
        PlPgSqlGetdiagKind::Context => "PG_CONTEXT",
        PlPgSqlGetdiagKind::ErrorContext => "PG_EXCEPTION_CONTEXT",
        PlPgSqlGetdiagKind::ErrorDetail => "PG_EXCEPTION_DETAIL",
        PlPgSqlGetdiagKind::ErrorHint => "PG_EXCEPTION_HINT",
        PlPgSqlGetdiagKind::ReturnedSqlstate => "RETURNED_SQLSTATE",
        PlPgSqlGetdiagKind::ColumnName => "COLUMN_NAME",
        PlPgSqlGetdiagKind::ConstraintName => "CONSTRAINT_NAME",
        PlPgSqlGetdiagKind::DatatypeName => "PG_DATATYPE_NAME",
        PlPgSqlGetdiagKind::MessageText => "MESSAGE_TEXT",
        PlPgSqlGetdiagKind::TableName => "TABLE_NAME",
        PlPgSqlGetdiagKind::SchemaName => "SCHEMA_NAME",
    }
}

impl PlPgSqlDatum {
    /// Returns the datum number (`dno`) from each datum variant.
    pub fn dno(&self) -> i32 {
        match self {
            Self::Var(v) => v.variable.dno,
            Self::Row(r) => r.variable.dno,
            Self::Rec(r) => r.variable.dno,
            Self::Recfield(f) => f.datum.dno,
        }
    }

    /// Returns the datum type tag (`dtype`) from each datum variant.
    pub fn dtype(&self) -> PlPgSqlDtype {
        match self {
            Self::Var(v) => v.variable.dtype,
            Self::Row(r) => r.variable.dtype,
            Self::Rec(r) => r.variable.dtype,
            Self::Recfield(f) => f.datum.dtype,
        }
    }
}
