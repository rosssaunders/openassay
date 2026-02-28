use std::io::{self, Stdout};

use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, TableState},
};

use openassay::tcop::postgres::{BackendMessage, FrontendMessage, PostgresSession};

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

struct App {
    input_lines: Vec<String>,
    cursor_row: usize,
    cursor_col: usize,
    result_columns: Vec<String>,
    result_rows: Vec<Vec<String>>,
    result_scroll: TableState,
    status_message: String,
    status_is_error: bool,
    history: Vec<String>,
    history_index: Option<usize>,
    draft_input: String,
    autocomplete_active: bool,
    autocomplete_items: Vec<String>,
    autocomplete_index: usize,
    autocomplete_all: Vec<String>,
    session: PostgresSession,
    should_quit: bool,
}

impl App {
    fn new() -> Self {
        Self {
            input_lines: vec![String::new()],
            cursor_row: 0,
            cursor_col: 0,
            result_columns: Vec::new(),
            result_rows: Vec::new(),
            result_scroll: TableState::default(),
            status_message: "Ready — Ctrl+Enter to execute, Ctrl+C to quit".into(),
            status_is_error: false,
            history: Vec::new(),
            history_index: None,
            draft_input: String::new(),
            autocomplete_active: false,
            autocomplete_items: Vec::new(),
            autocomplete_index: 0,
            autocomplete_all: build_base_dictionary(),
            session: PostgresSession::new(),
            should_quit: false,
        }
    }

    fn input_text(&self) -> String {
        self.input_lines.join("\n")
    }

    fn set_input_text(&mut self, text: &str) {
        self.input_lines = text.lines().map(String::from).collect();
        if self.input_lines.is_empty() {
            self.input_lines.push(String::new());
        }
        self.cursor_row = self.input_lines.len() - 1;
        self.cursor_col = self.input_lines[self.cursor_row].len();
    }

    fn clear_input(&mut self) {
        self.input_lines = vec![String::new()];
        self.cursor_row = 0;
        self.cursor_col = 0;
    }

    fn clamp_cursor(&mut self) {
        self.cursor_row = self.cursor_row.min(self.input_lines.len() - 1);
        self.cursor_col = self.cursor_col.min(self.input_lines[self.cursor_row].len());
    }

    // Extract the word fragment immediately before the cursor for autocomplete.
    fn word_before_cursor(&self) -> String {
        let line = &self.input_lines[self.cursor_row];
        let before = &line[..self.cursor_col];
        before
            .chars()
            .rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .chars()
            .rev()
            .collect()
    }

    fn open_autocomplete(&mut self) {
        let prefix = self.word_before_cursor().to_lowercase();
        if prefix.is_empty() {
            self.autocomplete_active = false;
            return;
        }
        self.autocomplete_items = self
            .autocomplete_all
            .iter()
            .filter(|item| {
                item.to_lowercase().starts_with(&prefix) && item.to_lowercase() != prefix
            })
            .take(10)
            .cloned()
            .collect();
        if self.autocomplete_items.is_empty() {
            self.autocomplete_active = false;
        } else {
            self.autocomplete_active = true;
            self.autocomplete_index = 0;
        }
    }

    fn accept_autocomplete(&mut self) {
        if !self.autocomplete_active || self.autocomplete_items.is_empty() {
            return;
        }
        let completion = self.autocomplete_items[self.autocomplete_index].clone();
        let prefix_len = self.word_before_cursor().len();
        // Remove the prefix already typed
        let line = &mut self.input_lines[self.cursor_row];
        let start = self.cursor_col - prefix_len;
        line.replace_range(start..self.cursor_col, &completion);
        self.cursor_col = start + completion.len();
        self.autocomplete_active = false;
    }

    fn rebuild_dictionary_from_results(&mut self) {
        // Re-add column names from latest results as potential completions.
        let mut dict = build_base_dictionary();
        for col in &self.result_columns {
            let lower = col.to_lowercase();
            if !dict.iter().any(|d| d.to_lowercase() == lower) {
                dict.push(col.clone());
            }
        }
        self.autocomplete_all = dict;
    }
}

// ---------------------------------------------------------------------------
// SQL keywords + function names + catalog names for autocomplete
// ---------------------------------------------------------------------------

fn build_base_dictionary() -> Vec<String> {
    let mut dict: Vec<String> = Vec::with_capacity(500);

    // SQL keywords
    for kw in SQL_KEYWORDS {
        dict.push((*kw).to_string());
    }
    // Built-in functions
    for f in BUILTIN_FUNCTIONS {
        dict.push((*f).to_string());
    }
    // Catalog table/column names
    for name in CATALOG_NAMES {
        dict.push((*name).to_string());
    }

    dict.sort_unstable();
    dict.dedup();
    dict
}

const SQL_KEYWORDS: &[&str] = &[
    "ABORT",
    "ABSOLUTE",
    "ACCESS",
    "ACTION",
    "ADD",
    "ADMIN",
    "AFTER",
    "AGGREGATE",
    "ALL",
    "ALSO",
    "ALTER",
    "ALWAYS",
    "ANALYSE",
    "ANALYZE",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASSERTION",
    "ASSIGNMENT",
    "ASYMMETRIC",
    "AT",
    "ATTACH",
    "ATTRIBUTE",
    "AUTHORIZATION",
    "BACKWARD",
    "BEFORE",
    "BEGIN",
    "BETWEEN",
    "BIGINT",
    "BINARY",
    "BIT",
    "BOOLEAN",
    "BOTH",
    "BY",
    "CACHE",
    "CALL",
    "CALLED",
    "CASCADE",
    "CASCADED",
    "CASE",
    "CAST",
    "CATALOG",
    "CHAIN",
    "CHAR",
    "CHARACTER",
    "CHARACTERISTICS",
    "CHECK",
    "CHECKPOINT",
    "CLASS",
    "CLOSE",
    "CLUSTER",
    "COALESCE",
    "COLLATE",
    "COLLATION",
    "COLUMN",
    "COLUMNS",
    "COMMENT",
    "COMMENTS",
    "COMMIT",
    "COMMITTED",
    "CONCURRENTLY",
    "CONFIGURATION",
    "CONFLICT",
    "CONNECTION",
    "CONSTRAINT",
    "CONSTRAINTS",
    "CONTENT",
    "CONTINUE",
    "CONVERSION",
    "COPY",
    "COST",
    "CREATE",
    "CROSS",
    "CSV",
    "CUBE",
    "CURRENT",
    "CURRENT_CATALOG",
    "CURRENT_DATE",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_USER",
    "CURSOR",
    "CYCLE",
    "DATA",
    "DATABASE",
    "DATABASES",
    "DATE",
    "DAY",
    "DEALLOCATE",
    "DEC",
    "DECIMAL",
    "DECLARE",
    "DEFAULT",
    "DEFAULTS",
    "DEFERRABLE",
    "DEFERRED",
    "DEFINER",
    "DELETE",
    "DELIMITER",
    "DELIMITERS",
    "DEPENDS",
    "DESC",
    "DETACH",
    "DICTIONARY",
    "DISABLE",
    "DISCARD",
    "DISTINCT",
    "DO",
    "DOCUMENT",
    "DOMAIN",
    "DOUBLE",
    "DROP",
    "EACH",
    "ELSE",
    "ENABLE",
    "ENCODING",
    "ENCRYPTED",
    "END",
    "ENUM",
    "ESCAPE",
    "EVENT",
    "EXCEPT",
    "EXCLUDE",
    "EXCLUDING",
    "EXCLUSIVE",
    "EXECUTE",
    "EXISTS",
    "EXPLAIN",
    "EXPRESSION",
    "EXTENSION",
    "EXTERNAL",
    "EXTRACT",
    "FALSE",
    "FAMILY",
    "FETCH",
    "FILTER",
    "FIRST",
    "FLOAT",
    "FOLLOWING",
    "FOR",
    "FORCE",
    "FOREIGN",
    "FORWARD",
    "FREEZE",
    "FROM",
    "FULL",
    "FUNCTION",
    "FUNCTIONS",
    "GENERATED",
    "GLOBAL",
    "GRANT",
    "GRANTED",
    "GREATEST",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HANDLER",
    "HAVING",
    "HEADER",
    "HOLD",
    "HOUR",
    "IDENTITY",
    "IF",
    "ILIKE",
    "IMMEDIATE",
    "IMMUTABLE",
    "IMPLICIT",
    "IMPORT",
    "IN",
    "INCLUDE",
    "INCLUDING",
    "INCREMENT",
    "INDEX",
    "INDEXES",
    "INHERIT",
    "INHERITS",
    "INITIALLY",
    "INLINE",
    "INNER",
    "INOUT",
    "INPUT",
    "INSENSITIVE",
    "INSERT",
    "INSTEAD",
    "INT",
    "INTEGER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "INVOKER",
    "IS",
    "ISNULL",
    "ISOLATION",
    "JOIN",
    "KEY",
    "LABEL",
    "LANGUAGE",
    "LARGE",
    "LAST",
    "LATERAL",
    "LEADING",
    "LEAKPROOF",
    "LEAST",
    "LEFT",
    "LEVEL",
    "LIKE",
    "LIMIT",
    "LISTEN",
    "LOAD",
    "LOCAL",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "LOCATION",
    "LOCK",
    "LOCKED",
    "LOGGED",
    "MAPPING",
    "MATCH",
    "MATERIALIZED",
    "MAXVALUE",
    "METHOD",
    "MINUTE",
    "MINVALUE",
    "MODE",
    "MONTH",
    "MOVE",
    "NAME",
    "NAMES",
    "NATIONAL",
    "NATURAL",
    "NCHAR",
    "NEW",
    "NEXT",
    "NFC",
    "NFD",
    "NFKC",
    "NFKD",
    "NO",
    "NONE",
    "NORMALIZE",
    "NORMALIZED",
    "NOT",
    "NOTHING",
    "NOTIFY",
    "NOTNULL",
    "NOWAIT",
    "NULL",
    "NULLIF",
    "NULLS",
    "NUMERIC",
    "OBJECT",
    "OF",
    "OFF",
    "OFFSET",
    "OIDS",
    "OLD",
    "ON",
    "ONLY",
    "OPERATOR",
    "OPTION",
    "OPTIONS",
    "OR",
    "ORDER",
    "ORDINALITY",
    "OTHERS",
    "OUT",
    "OUTER",
    "OVER",
    "OVERLAPS",
    "OVERLAY",
    "OVERRIDING",
    "OWNED",
    "OWNER",
    "PARALLEL",
    "PARSER",
    "PARTIAL",
    "PARTITION",
    "PASSING",
    "PASSWORD",
    "PLACING",
    "PLANS",
    "POLICY",
    "POSITION",
    "PRECEDING",
    "PRECISION",
    "PREPARE",
    "PREPARED",
    "PRESERVE",
    "PRIMARY",
    "PRIOR",
    "PRIVILEGES",
    "PROCEDURAL",
    "PROCEDURE",
    "PROCEDURES",
    "PROGRAM",
    "PUBLICATION",
    "QUOTE",
    "RANGE",
    "READ",
    "REAL",
    "REASSIGN",
    "RECHECK",
    "RECURSIVE",
    "REF",
    "REFERENCES",
    "REFERENCING",
    "REFRESH",
    "REINDEX",
    "RELATIVE",
    "RELEASE",
    "RENAME",
    "REPEATABLE",
    "REPLACE",
    "REPLICA",
    "RESET",
    "RESTART",
    "RESTRICT",
    "RETURNING",
    "RETURNS",
    "REVOKE",
    "RIGHT",
    "ROLE",
    "ROLLBACK",
    "ROLLUP",
    "ROUTINE",
    "ROUTINES",
    "ROW",
    "ROWS",
    "RULE",
    "SAVEPOINT",
    "SCHEMA",
    "SCHEMAS",
    "SCROLL",
    "SEARCH",
    "SECOND",
    "SECURITY",
    "SELECT",
    "SEQUENCE",
    "SEQUENCES",
    "SERIALIZABLE",
    "SERVER",
    "SESSION",
    "SESSION_USER",
    "SET",
    "SETOF",
    "SETS",
    "SHARE",
    "SHOW",
    "SIMILAR",
    "SIMPLE",
    "SKIP",
    "SMALLINT",
    "SNAPSHOT",
    "SOME",
    "SQL",
    "STABLE",
    "STANDALONE",
    "START",
    "STATEMENT",
    "STATISTICS",
    "STDIN",
    "STDOUT",
    "STORAGE",
    "STORED",
    "STRICT",
    "STRIP",
    "SUBSCRIPTION",
    "SUBSTRING",
    "SUPPORT",
    "SYMMETRIC",
    "SYSID",
    "SYSTEM",
    "TABLE",
    "TABLES",
    "TABLESAMPLE",
    "TABLESPACE",
    "TEMP",
    "TEMPLATE",
    "TEMPORARY",
    "TEXT",
    "THEN",
    "TIES",
    "TIME",
    "TIMESTAMP",
    "TRAILING",
    "TRANSACTION",
    "TRANSFORM",
    "TREAT",
    "TRIGGER",
    "TRIM",
    "TRUE",
    "TRUNCATE",
    "TRUSTED",
    "TYPE",
    "TYPES",
    "UESCAPE",
    "UNBOUNDED",
    "UNCOMMITTED",
    "UNENCRYPTED",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "UNLISTEN",
    "UNLOGGED",
    "UNTIL",
    "UPDATE",
    "UPPER",
    "USAGE",
    "USER",
    "USING",
    "VACUUM",
    "VALID",
    "VALIDATE",
    "VALIDATOR",
    "VALUE",
    "VALUES",
    "VARCHAR",
    "VARIADIC",
    "VARYING",
    "VERBOSE",
    "VERSION",
    "VIEW",
    "VIEWS",
    "VOLATILE",
    "WHEN",
    "WHERE",
    "WHITESPACE",
    "WINDOW",
    "WITH",
    "WITHIN",
    "WITHOUT",
    "WORK",
    "WRAPPER",
    "WRITE",
    "XML",
    "XMLATTRIBUTES",
    "XMLCONCAT",
    "XMLELEMENT",
    "XMLEXISTS",
    "XMLFOREST",
    "XMLNAMESPACES",
    "XMLPARSE",
    "XMLPI",
    "XMLROOT",
    "XMLSERIALIZE",
    "XMLTABLE",
    "YEAR",
    "YES",
    "ZONE",
];

const BUILTIN_FUNCTIONS: &[&str] = &[
    // Aggregate
    "count",
    "sum",
    "avg",
    "min",
    "max",
    "array_agg",
    "string_agg",
    "bool_and",
    "bool_or",
    "every",
    "bit_and",
    "bit_or",
    // String
    "length",
    "char_length",
    "character_length",
    "octet_length",
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim",
    "btrim",
    "lpad",
    "rpad",
    "substr",
    "substring",
    "replace",
    "translate",
    "concat",
    "concat_ws",
    "left",
    "right",
    "repeat",
    "reverse",
    "split_part",
    "position",
    "strpos",
    "starts_with",
    "encode",
    "decode",
    "md5",
    "ascii",
    "chr",
    "initcap",
    "regexp_replace",
    "regexp_matches",
    "regexp_count",
    "regexp_instr",
    "regexp_substr",
    "regexp_like",
    "regexp_match",
    "regexp_split_to_array",
    "string_to_table",
    "to_hex",
    "to_oct",
    "to_bin",
    "unistr",
    "bit_length",
    "set_byte",
    "get_byte",
    "format",
    "to_char",
    "to_number",
    "to_date",
    "to_timestamp",
    "quote_literal",
    "quote_ident",
    "quote_nullable",
    // Math
    "abs",
    "ceil",
    "ceiling",
    "floor",
    "round",
    "trunc",
    "mod",
    "power",
    "sqrt",
    "cbrt",
    "sign",
    "log",
    "log10",
    "ln",
    "exp",
    "pi",
    "random",
    "greatest",
    "least",
    "div",
    "bs_price",
    "bs_greeks",
    "barrier_price",
    "american_price",
    "heston_price",
    // Type/cast
    "coalesce",
    "nullif",
    // Date/time
    "now",
    "current_date",
    "current_timestamp",
    "date_trunc",
    "date_part",
    "extract",
    "age",
    "make_date",
    "make_time",
    "make_timestamp",
    "make_interval",
    // Array
    "array_length",
    "array_upper",
    "array_lower",
    "unnest",
    "array_position",
    "array_remove",
    "array_cat",
    "array_append",
    "array_prepend",
    "cardinality",
    "int4range",
    "float8range",
    "textrange",
    // JSON
    "json_build_object",
    "jsonb_build_object",
    "json_build_array",
    "jsonb_build_array",
    "json_object",
    "jsonb_object",
    "json_agg",
    "jsonb_agg",
    "json_object_agg",
    "jsonb_object_agg",
    "json_array_length",
    "jsonb_array_length",
    "json_each",
    "jsonb_each",
    "json_each_text",
    "jsonb_each_text",
    "json_array_elements",
    "jsonb_array_elements",
    "json_array_elements_text",
    "jsonb_array_elements_text",
    "json_object_keys",
    "jsonb_object_keys",
    "json_extract_path",
    "jsonb_extract_path",
    "json_extract_path_text",
    "jsonb_extract_path_text",
    "json_typeof",
    "jsonb_typeof",
    "json_strip_nulls",
    "jsonb_strip_nulls",
    "row_to_json",
    "array_to_json",
    "to_json",
    "to_jsonb",
    "json_pretty",
    "jsonb_pretty",
    "jsonb_set",
    "jsonb_concat",
    "jsonb_contains",
    "jsonb_contained",
    "jsonb_delete",
    "jsonb_delete_path",
    "jsonb_insert",
    "jsonb_set_lax",
    "jsonb_path_exists",
    "jsonb_path_match",
    "jsonb_path_query",
    "jsonb_path_query_array",
    "jsonb_path_query_first",
    "jsonb_exists",
    "jsonb_exists_any",
    "jsonb_exists_all",
    // Table functions
    "json_table",
    "http_get",
    "http_post",
    "http_put",
    "http_patch",
    "http_delete",
    "http_head",
    "urlencode",
    // Misc
    "pg_typeof",
    "pg_input_is_valid",
    "gen_random_uuid",
    "generate_series",
    "generate_subscripts",
    "row_number",
    "rank",
    "dense_rank",
    "ntile",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "nth_value",
    "cume_dist",
    "percent_rank",
    "exists",
    "current_schema",
    "current_schemas",
    "current_user",
    "current_database",
    "version",
    "pg_backend_pid",
    "pg_get_constraintdef",
    "pg_get_indexdef",
    "pg_get_viewdef",
    "pg_relation_size",
    "pg_total_relation_size",
    "obj_description",
    "col_description",
    "txid_current",
    "set_config",
    "current_setting",
    "has_table_privilege",
    "has_schema_privilege",
];

const CATALOG_NAMES: &[&str] = &[
    "pg_catalog",
    "pg_namespace",
    "pg_class",
    "pg_attribute",
    "pg_type",
    "pg_database",
    "pg_roles",
    "pg_settings",
    "pg_tables",
    "pg_views",
    "pg_indexes",
    "pg_proc",
    "pg_constraint",
    "pg_extension",
    "pg_index",
    "pg_attrdef",
    "pg_inherits",
    "pg_statistic",
    "information_schema",
    "schemata",
    "key_column_usage",
    "table_constraints",
];

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

async fn execute_query(session: &mut PostgresSession, sql: &str) -> QueryOutcome {
    let messages = session
        .run([FrontendMessage::Query {
            sql: sql.to_string(),
        }])
        .await;

    let mut columns: Vec<String> = Vec::new();
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut tag = String::new();
    let mut row_count: u64 = 0;
    let mut error: Option<String> = None;

    for message in messages {
        match message {
            BackendMessage::RowDescription { fields } => {
                columns = fields.into_iter().map(|f| f.name).collect();
                rows.clear();
            }
            BackendMessage::DataRow { values } => {
                rows.push(values);
            }
            BackendMessage::DataRowBinary { values } => {
                rows.push(
                    values
                        .into_iter()
                        .map(|v| match v {
                            None => "NULL".to_string(),
                            Some(bytes) => String::from_utf8(bytes).unwrap_or_else(|e| {
                                format!("<binary {} bytes>", e.into_bytes().len())
                            }),
                        })
                        .collect(),
                );
            }
            BackendMessage::CommandComplete { tag: t, rows: r } => {
                tag = t;
                row_count = r;
            }
            BackendMessage::ErrorResponse { message: msg, .. } => {
                error = Some(msg);
            }
            _ => {}
        }
    }

    if let Some(err) = error {
        QueryOutcome::Error(err)
    } else {
        QueryOutcome::Ok {
            columns,
            rows,
            tag,
            row_count,
        }
    }
}

enum QueryOutcome {
    Ok {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        tag: String,
        row_count: u64,
    },
    Error(String),
}

// ---------------------------------------------------------------------------
// Terminal setup / teardown
// ---------------------------------------------------------------------------

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) {
    let _ = disable_raw_mode();
    let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen);
}

// ---------------------------------------------------------------------------
// UI rendering
// ---------------------------------------------------------------------------

fn ui(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    // Layout: results (flexible) | status (1 line) | input (5 lines)
    let input_height = 5_u16.min(area.height.saturating_sub(3));
    let chunks = Layout::vertical([
        Constraint::Min(3),                   // results
        Constraint::Length(1),                // status bar
        Constraint::Length(input_height + 2), // input + border
    ])
    .split(area);

    render_results(frame, app, chunks[0]);
    render_status(frame, app, chunks[1]);
    render_input(frame, app, chunks[2]);

    // Autocomplete popup (rendered last so it floats on top)
    if app.autocomplete_active && !app.autocomplete_items.is_empty() {
        render_autocomplete(frame, app, chunks[2]);
    }
}

fn render_results(frame: &mut Frame, app: &mut App, area: Rect) {
    if app.result_columns.is_empty() && app.result_rows.is_empty() {
        let block = Block::default()
            .title(" Results ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::DarkGray));
        let placeholder = Paragraph::new("No results yet. Type SQL and press Ctrl+Enter.")
            .style(Style::default().fg(Color::DarkGray))
            .block(block);
        frame.render_widget(placeholder, area);
        return;
    }

    // Calculate column widths
    let col_widths: Vec<u16> = app
        .result_columns
        .iter()
        .enumerate()
        .map(|(i, header)| {
            let max_data = app
                .result_rows
                .iter()
                .map(|row| row.get(i).map_or(0, String::len))
                .max()
                .unwrap_or(0);
            let w = header.len().max(max_data);
            (w as u16).clamp(3, 40)
        })
        .collect();

    let header_cells: Vec<Cell> = app
        .result_columns
        .iter()
        .map(|h| Cell::from(h.as_str()).style(Style::default().add_modifier(Modifier::BOLD)))
        .collect();
    let header = Row::new(header_cells)
        .style(Style::default().fg(Color::Yellow))
        .bottom_margin(0);

    let data_rows: Vec<Row> = app
        .result_rows
        .iter()
        .map(|row| {
            let cells: Vec<Cell> = row.iter().map(|v| Cell::from(v.as_str())).collect();
            Row::new(cells)
        })
        .collect();

    let widths: Vec<Constraint> = col_widths.iter().map(|w| Constraint::Length(*w)).collect();

    let table = Table::new(data_rows, &widths)
        .header(header)
        .block(Block::default().title(" Results ").borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::DarkGray));

    frame.render_stateful_widget(table, area, &mut app.result_scroll);
}

fn render_status(frame: &mut Frame, app: &App, area: Rect) {
    let color = if app.status_is_error {
        Color::Red
    } else {
        Color::Green
    };
    let status = Paragraph::new(Line::from(vec![
        Span::styled(" ", Style::default()),
        Span::styled(&app.status_message, Style::default().fg(color)),
    ]))
    .style(Style::default().bg(Color::Black));
    frame.render_widget(status, area);
}

fn render_input(frame: &mut Frame, app: &App, area: Rect) {
    let display_text = app.input_lines.join("\n");
    let input = Paragraph::new(display_text)
        .block(
            Block::default()
                .title(" SQL (Ctrl+Enter to execute) ")
                .borders(Borders::ALL),
        )
        .style(Style::default().fg(Color::White));
    frame.render_widget(input, area);

    // Position cursor inside the input block (offset by 1 for the border)
    frame.set_cursor_position((
        area.x + 1 + app.cursor_col as u16,
        area.y + 1 + app.cursor_row as u16,
    ));
}

fn render_autocomplete(frame: &mut Frame, app: &App, input_area: Rect) {
    let popup_width = 30_u16.min(input_area.width.saturating_sub(2));
    let popup_height = (app.autocomplete_items.len() as u16 + 2).min(12);

    // Position popup above the input area
    let popup_x = (input_area.x + 1 + app.cursor_col as u16)
        .min(frame.area().width.saturating_sub(popup_width));
    let popup_y = input_area.y.saturating_sub(popup_height);

    let popup_area = Rect::new(popup_x, popup_y, popup_width, popup_height);

    let items: Vec<ListItem> = app
        .autocomplete_items
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let style = if i == app.autocomplete_index {
                Style::default().bg(Color::Blue).fg(Color::White)
            } else {
                Style::default().fg(Color::Gray)
            };
            ListItem::new(item.as_str()).style(style)
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .borders(Borders::ALL)
            .style(Style::default().bg(Color::Black)),
    );

    frame.render_widget(Clear, popup_area);
    frame.render_widget(list, popup_area);
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

enum Action {
    None,
    Execute,
    Quit,
}

fn handle_key(app: &mut App, key: KeyEvent) -> Action {
    // Ctrl+C / Ctrl+D — quit
    if key.modifiers.contains(KeyModifiers::CONTROL)
        && (key.code == KeyCode::Char('c') || key.code == KeyCode::Char('d'))
    {
        return Action::Quit;
    }

    // Ctrl+Enter — execute
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Enter {
        return Action::Execute;
    }

    // Autocomplete handling
    if app.autocomplete_active {
        match key.code {
            KeyCode::Tab => {
                app.autocomplete_index =
                    (app.autocomplete_index + 1) % app.autocomplete_items.len();
                return Action::None;
            }
            KeyCode::Enter => {
                app.accept_autocomplete();
                return Action::None;
            }
            KeyCode::Esc => {
                app.autocomplete_active = false;
                return Action::None;
            }
            _ => {
                app.autocomplete_active = false;
                // Fall through to normal key handling
            }
        }
    }

    match key.code {
        KeyCode::Tab => {
            app.open_autocomplete();
        }
        KeyCode::Enter => {
            // Insert newline
            let line = &app.input_lines[app.cursor_row];
            let rest = line[app.cursor_col..].to_string();
            app.input_lines[app.cursor_row].truncate(app.cursor_col);
            app.cursor_row += 1;
            app.input_lines.insert(app.cursor_row, rest);
            app.cursor_col = 0;
        }
        KeyCode::Backspace => {
            if app.cursor_col > 0 {
                app.input_lines[app.cursor_row].remove(app.cursor_col - 1);
                app.cursor_col -= 1;
            } else if app.cursor_row > 0 {
                // Merge with previous line
                let current = app.input_lines.remove(app.cursor_row);
                app.cursor_row -= 1;
                app.cursor_col = app.input_lines[app.cursor_row].len();
                app.input_lines[app.cursor_row].push_str(&current);
            }
        }
        KeyCode::Delete => {
            let line_len = app.input_lines[app.cursor_row].len();
            if app.cursor_col < line_len {
                app.input_lines[app.cursor_row].remove(app.cursor_col);
            } else if app.cursor_row + 1 < app.input_lines.len() {
                let next = app.input_lines.remove(app.cursor_row + 1);
                app.input_lines[app.cursor_row].push_str(&next);
            }
        }
        KeyCode::Left => {
            if app.cursor_col > 0 {
                app.cursor_col -= 1;
            } else if app.cursor_row > 0 {
                app.cursor_row -= 1;
                app.cursor_col = app.input_lines[app.cursor_row].len();
            }
        }
        KeyCode::Right => {
            let line_len = app.input_lines[app.cursor_row].len();
            if app.cursor_col < line_len {
                app.cursor_col += 1;
            } else if app.cursor_row + 1 < app.input_lines.len() {
                app.cursor_row += 1;
                app.cursor_col = 0;
            }
        }
        KeyCode::Up => {
            if app.cursor_row > 0 {
                app.cursor_row -= 1;
                app.clamp_cursor();
            } else if !app.history.is_empty() {
                // Navigate history
                let idx = match app.history_index {
                    None => {
                        app.draft_input = app.input_text();
                        app.history.len() - 1
                    }
                    Some(i) if i > 0 => i - 1,
                    Some(i) => i,
                };
                app.history_index = Some(idx);
                app.set_input_text(&app.history[idx].clone());
            }
        }
        KeyCode::Down => {
            if app.cursor_row + 1 < app.input_lines.len() {
                app.cursor_row += 1;
                app.clamp_cursor();
            } else if let Some(idx) = app.history_index {
                // Navigate history forward
                if idx + 1 < app.history.len() {
                    let new_idx = idx + 1;
                    app.history_index = Some(new_idx);
                    app.set_input_text(&app.history[new_idx].clone());
                } else {
                    app.history_index = None;
                    let draft = app.draft_input.clone();
                    app.set_input_text(&draft);
                }
            }
        }
        KeyCode::Home => {
            app.cursor_col = 0;
        }
        KeyCode::End => {
            app.cursor_col = app.input_lines[app.cursor_row].len();
        }
        KeyCode::PageUp => {
            // Scroll results up
            let current = app.result_scroll.selected().unwrap_or(0);
            app.result_scroll.select(Some(current.saturating_sub(10)));
        }
        KeyCode::PageDown => {
            // Scroll results down
            let current = app.result_scroll.selected().unwrap_or(0);
            let max = app.result_rows.len().saturating_sub(1);
            app.result_scroll.select(Some((current + 10).min(max)));
        }
        KeyCode::Esc => {
            // No-op when autocomplete is already closed
        }
        KeyCode::Char(c) => {
            app.input_lines[app.cursor_row].insert(app.cursor_col, c);
            app.cursor_col += 1;
        }
        _ => {}
    }

    Action::None
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    // Install panic hook to restore terminal
    let original_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let _ = disable_raw_mode();
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        original_hook(info);
    }));

    let mut terminal = setup_terminal()?;
    let mut app = App::new();

    loop {
        terminal.draw(|frame| ui(frame, &mut app))?;

        if let Event::Key(key) = event::read()? {
            match handle_key(&mut app, key) {
                Action::Quit => break,
                Action::Execute => {
                    let sql = app.input_text();
                    let trimmed = sql.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    // Save to history
                    if app.history.last().is_none_or(|last| last != trimmed) {
                        app.history.push(trimmed.to_string());
                    }
                    app.history_index = None;
                    app.draft_input.clear();

                    // Execute
                    match execute_query(&mut app.session, trimmed).await {
                        QueryOutcome::Ok {
                            columns,
                            rows,
                            tag,
                            row_count,
                        } => {
                            let msg = if columns.is_empty() {
                                format!("{tag} {row_count}")
                            } else {
                                let n = rows.len();
                                format!("{tag} — {n} row{}", if n == 1 { "" } else { "s" })
                            };
                            app.result_columns = columns;
                            app.result_rows = rows;
                            app.result_scroll = TableState::default();
                            if !app.result_rows.is_empty() {
                                app.result_scroll.select(Some(0));
                            }
                            app.status_message = msg;
                            app.status_is_error = false;
                            app.rebuild_dictionary_from_results();
                        }
                        QueryOutcome::Error(err) => {
                            app.status_message = format!("ERROR: {err}");
                            app.status_is_error = true;
                        }
                    }

                    app.clear_input();
                }
                Action::None => {}
            }

            if app.should_quit {
                break;
            }
        }
    }

    restore_terminal(&mut terminal);
    Ok(())
}
