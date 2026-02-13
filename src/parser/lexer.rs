use std::fmt;

use crate::parser::scansup::{downcase_truncate_identifier, scanner_isspace, truncate_identifier};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Keyword {
    Case,
    Add,
    All,
    Any,
    Alter,
    And,
    As,
    Asc,
    By,
    Between,
    Begin,
    Breadth,
    Check,
    Conflict,
    Cascade,
    Cache,
    Cast,
    Column,
    Concurrently,
    Cycle,
    Create,
    Cross,
    Constraint,
    Data,
    Delete,
    Depth,
    Default,
    Desc,
    Distinct,
    Do,
    Drop,
    Else,
    End,
    Escape,
    Except,
    Exclude,
    Exists,
    False,
    Filter,
    First,
    Foreign,
    Full,
    From,
    Generated,
    Group,
    Groups,
    Grouping,
    Having,
    If,
    In,
    Inner,
    Insert,
    Intersect,
    Into,
    Is,
    Join,
    Key,
    Last,
    Left,
    Limit,
    Like,
    Current,
    Commit,
    Following,
    For,
    MaxValue,
    Materialized,
    Matched,
    Merge,
    MinValue,
    Natural,
    No,
    Not,
    Nothing,
    Null,
    Offset,
    On,
    Outer,
    Over,
    Or,
    Order,
    Partition,
    Placing,
    Preceding,
    Primary,
    Range,
    Returning,
    Recursive,
    Replace,
    Restrict,
    Rename,
    Refresh,
    References,
    Right,
    Schema,
    Sequence,
    Select,
    Sets,
    Set,
    Source,
    Target,
    Then,
    Array,
    ILike,
    Index,
    Identity,
    Increment,
    Lateral,
    Row,
    Rows,
    Table,
    View,
    Unique,
    To,
    Truncate,
    True,
    Unbounded,
    Union,
    Update,
    Using,
    Rollup,
    Cube,
    Values,
    When,
    Where,
    Within,
    Always,
    Restart,
    Start,
    Transaction,
    With,
    Explain,
    Analyze,
    Verbose,
    Show,
    Discard,
    Release,
    Rollback,
    Savepoint,
    Search,
    Listen,
    Notify,
    Unlisten,
    Local,
    Reset,
    Extension,
    Function,
    Returns,
    Language,
    Temporary,
    Temp,
    Type,
    Enum,
    Domain,
    Date,
    Time,
    Timestamp,
    Interval,
    Window,
}

impl Keyword {
    fn from_ident(ident: &str) -> Option<Self> {
        match ident {
            "case" => Some(Self::Case),
            "add" => Some(Self::Add),
            "all" => Some(Self::All),
            "any" => Some(Self::Any),
            "alter" => Some(Self::Alter),
            "and" => Some(Self::And),
            "as" => Some(Self::As),
            "asc" => Some(Self::Asc),
            "by" => Some(Self::By),
            "between" => Some(Self::Between),
            "begin" => Some(Self::Begin),
            "breadth" => Some(Self::Breadth),
            "check" => Some(Self::Check),
            "conflict" => Some(Self::Conflict),
            "cascade" => Some(Self::Cascade),
            "cache" => Some(Self::Cache),
            "cast" => Some(Self::Cast),
            "column" => Some(Self::Column),
            "concurrently" => Some(Self::Concurrently),
            "cycle" => Some(Self::Cycle),
            "create" => Some(Self::Create),
            "cross" => Some(Self::Cross),
            "constraint" => Some(Self::Constraint),
            "data" => Some(Self::Data),
            "delete" => Some(Self::Delete),
            "depth" => Some(Self::Depth),
            "default" => Some(Self::Default),
            "desc" => Some(Self::Desc),
            "distinct" => Some(Self::Distinct),
            "do" => Some(Self::Do),
            "drop" => Some(Self::Drop),
            "else" => Some(Self::Else),
            "end" => Some(Self::End),
            "escape" => Some(Self::Escape),
            "except" => Some(Self::Except),
            "exclude" => Some(Self::Exclude),
            "exists" => Some(Self::Exists),
            "false" => Some(Self::False),
            "filter" => Some(Self::Filter),
            "first" => Some(Self::First),
            "foreign" => Some(Self::Foreign),
            "full" => Some(Self::Full),
            "from" => Some(Self::From),
            "generated" => Some(Self::Generated),
            "group" => Some(Self::Group),
            "groups" => Some(Self::Groups),
            "grouping" => Some(Self::Grouping),
            "having" => Some(Self::Having),
            "if" => Some(Self::If),
            "in" => Some(Self::In),
            "inner" => Some(Self::Inner),
            "insert" => Some(Self::Insert),
            "intersect" => Some(Self::Intersect),
            "into" => Some(Self::Into),
            "is" => Some(Self::Is),
            "join" => Some(Self::Join),
            "key" => Some(Self::Key),
            "last" => Some(Self::Last),
            "left" => Some(Self::Left),
            "limit" => Some(Self::Limit),
            "like" => Some(Self::Like),
            "current" => Some(Self::Current),
            "commit" => Some(Self::Commit),
            "following" => Some(Self::Following),
            "for" => Some(Self::For),
            "maxvalue" => Some(Self::MaxValue),
            "materialized" => Some(Self::Materialized),
            "matched" => Some(Self::Matched),
            "merge" => Some(Self::Merge),
            "minvalue" => Some(Self::MinValue),
            "natural" => Some(Self::Natural),
            "no" => Some(Self::No),
            "not" => Some(Self::Not),
            "nothing" => Some(Self::Nothing),
            "null" => Some(Self::Null),
            "offset" => Some(Self::Offset),
            "on" => Some(Self::On),
            "outer" => Some(Self::Outer),
            "over" => Some(Self::Over),
            "or" => Some(Self::Or),
            "order" => Some(Self::Order),
            "partition" => Some(Self::Partition),
            "placing" => Some(Self::Placing),
            "preceding" => Some(Self::Preceding),
            "primary" => Some(Self::Primary),
            "range" => Some(Self::Range),
            "returning" => Some(Self::Returning),
            "recursive" => Some(Self::Recursive),
            "replace" => Some(Self::Replace),
            "restrict" => Some(Self::Restrict),
            "rename" => Some(Self::Rename),
            "refresh" => Some(Self::Refresh),
            "references" => Some(Self::References),
            "right" => Some(Self::Right),
            "schema" => Some(Self::Schema),
            "sequence" => Some(Self::Sequence),
            "select" => Some(Self::Select),
            "sets" => Some(Self::Sets),
            "set" => Some(Self::Set),
            "source" => Some(Self::Source),
            "target" => Some(Self::Target),
            "then" => Some(Self::Then),
            "array" => Some(Self::Array),
            "ilike" => Some(Self::ILike),
            "index" => Some(Self::Index),
            "identity" => Some(Self::Identity),
            "increment" => Some(Self::Increment),
            "lateral" => Some(Self::Lateral),
            "row" => Some(Self::Row),
            "rows" => Some(Self::Rows),
            "table" => Some(Self::Table),
            "view" => Some(Self::View),
            "unique" => Some(Self::Unique),
            "to" => Some(Self::To),
            "truncate" => Some(Self::Truncate),
            "true" => Some(Self::True),
            "unbounded" => Some(Self::Unbounded),
            "union" => Some(Self::Union),
            "update" => Some(Self::Update),
            "using" => Some(Self::Using),
            "rollup" => Some(Self::Rollup),
            "cube" => Some(Self::Cube),
            "values" => Some(Self::Values),
            "when" => Some(Self::When),
            "where" => Some(Self::Where),
            "within" => Some(Self::Within),
            "always" => Some(Self::Always),
            "restart" => Some(Self::Restart),
            "start" => Some(Self::Start),
            "transaction" => Some(Self::Transaction),
            "with" => Some(Self::With),
            "explain" => Some(Self::Explain),
            "analyze" => Some(Self::Analyze),
            "verbose" => Some(Self::Verbose),
            "show" => Some(Self::Show),
            "discard" => Some(Self::Discard),
            "release" => Some(Self::Release),
            "rollback" => Some(Self::Rollback),
            "savepoint" => Some(Self::Savepoint),
            "search" => Some(Self::Search),
            "listen" => Some(Self::Listen),
            "notify" => Some(Self::Notify),
            "unlisten" => Some(Self::Unlisten),
            "local" => Some(Self::Local),
            "reset" => Some(Self::Reset),
            "extension" => Some(Self::Extension),
            "function" => Some(Self::Function),
            "returns" => Some(Self::Returns),
            "language" => Some(Self::Language),
            "temporary" => Some(Self::Temporary),
            "temp" => Some(Self::Temp),
            "type" => Some(Self::Type),
            "enum" => Some(Self::Enum),
            "domain" => Some(Self::Domain),
            "date" => Some(Self::Date),
            "time" => Some(Self::Time),
            "timestamp" => Some(Self::Timestamp),
            "interval" => Some(Self::Interval),
            "window" => Some(Self::Window),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TokenKind {
    Eof,
    Keyword(Keyword),
    Identifier(String),
    String(String),
    Integer(i64),
    Float(String),
    Parameter(i32),
    Comma,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Dot,
    Semicolon,
    Colon,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Caret,
    Less,
    Greater,
    Equal,
    Typecast,
    DotDot,
    ColonEquals,
    EqualsGreater,
    LessEquals,
    GreaterEquals,
    NotEquals,
    Operator(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Token {
    pub kind: TokenKind,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LexError {
    pub message: String,
    pub position: usize,
}

impl fmt::Display for LexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at byte {}", self.message, self.position)
    }
}

impl std::error::Error for LexError {}

pub fn lex_sql(input: &str) -> Result<Vec<Token>, LexError> {
    Lexer::new(input).lex_all()
}

struct Lexer<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn lex_all(mut self) -> Result<Vec<Token>, LexError> {
        let mut out = Vec::new();
        loop {
            self.skip_whitespace_and_comments()?;
            if self.pos >= self.input.len() {
                out.push(Token {
                    kind: TokenKind::Eof,
                    start: self.pos,
                    end: self.pos,
                });
                break;
            }
            out.push(self.next_token()?);
        }
        Ok(out)
    }

    fn next_token(&mut self) -> Result<Token, LexError> {
        let start = self.pos;
        let Some(ch) = self.peek_char() else {
            return Ok(Token {
                kind: TokenKind::Eof,
                start,
                end: start,
            });
        };

        if (ch == 'e' || ch == 'E') && self.peek_nth_char(1) == Some('\'') {
            self.advance_char();
            return self.lex_single_quoted_string(start, true);
        }

        if (ch == 'n' || ch == 'N') && self.peek_nth_char(1) == Some('\'') {
            self.advance_char();
            return self.lex_single_quoted_string(start, false);
        }

        if self.starts_with("::") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Typecast,
                start,
                end: self.pos,
            });
        }
        if self.starts_with("..") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::DotDot,
                start,
                end: self.pos,
            });
        }
        if self.starts_with(":=") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::ColonEquals,
                start,
                end: self.pos,
            });
        }
        if self.starts_with("=>") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::EqualsGreater,
                start,
                end: self.pos,
            });
        }
        if self.starts_with("<=") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::LessEquals,
                start,
                end: self.pos,
            });
        }
        if self.starts_with(">=") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::GreaterEquals,
                start,
                end: self.pos,
            });
        }
        if self.starts_with("<@") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Operator("<@".to_string()),
                start,
                end: self.pos,
            });
        }
        if self.starts_with("<>") || self.starts_with("!=") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::NotEquals,
                start,
                end: self.pos,
            });
        }
        if self.starts_with("->>") {
            self.pos += 3;
            return Ok(Token {
                kind: TokenKind::Operator("->>".to_string()),
                start,
                end: self.pos,
            });
        }
        if self.starts_with("@?") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Operator("@?".to_string()),
                start,
                end: self.pos,
            });
        }
        if self.starts_with("@@") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Operator("@@".to_string()),
                start,
                end: self.pos,
            });
        }
        if self.starts_with("->") {
            self.pos += 2;
            return Ok(Token {
                kind: TokenKind::Operator("->".to_string()),
                start,
                end: self.pos,
            });
        }

        match ch {
            '\'' => self.lex_single_quoted_string(start, false),
            '"' => self.lex_quoted_identifier(start),
            '$' => self.lex_parameter_or_operator(start),
            '.' if self.peek_nth_char(1).is_some_and(|c| c.is_ascii_digit()) => {
                self.lex_number(start, true)
            }
            c if c.is_ascii_digit() => self.lex_number(start, false),
            c if is_ident_start(c) => self.lex_identifier_or_keyword(start),
            ',' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Comma))
            }
            '(' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::LParen))
            }
            ')' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::RParen))
            }
            '[' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::LBracket))
            }
            ']' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::RBracket))
            }
            ';' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Semicolon))
            }
            ':' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Colon))
            }
            '.' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Dot))
            }
            '+' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Plus))
            }
            '-' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Minus))
            }
            '*' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Star))
            }
            '/' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Slash))
            }
            '%' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Percent))
            }
            '^' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Caret))
            }
            '<' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Less))
            }
            '>' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Greater))
            }
            '=' => {
                self.pos += 1;
                Ok(self.mk(start, TokenKind::Equal))
            }
            c if is_operator_char(c) => self.lex_operator(start),
            _ => Err(LexError {
                message: format!("unexpected character '{}'", ch),
                position: start,
            }),
        }
    }

    fn mk(&self, start: usize, kind: TokenKind) -> Token {
        Token {
            kind,
            start,
            end: self.pos,
        }
    }

    fn lex_single_quoted_string(
        &mut self,
        start: usize,
        allow_backslash_escapes: bool,
    ) -> Result<Token, LexError> {
        let quote = self.advance_char();
        debug_assert_eq!(quote, Some('\''));

        let mut out = String::new();
        loop {
            let Some(c) = self.advance_char() else {
                return Err(LexError {
                    message: "unterminated quoted string".to_string(),
                    position: start,
                });
            };

            if c == '\'' {
                if self.peek_char() == Some('\'') {
                    self.advance_char();
                    out.push('\'');
                    continue;
                }
                break;
            }

            if allow_backslash_escapes && c == '\\' {
                let Some(next) = self.advance_char() else {
                    return Err(LexError {
                        message: "unterminated escape sequence".to_string(),
                        position: self.pos,
                    });
                };
                let translated = match next {
                    'b' => '\u{0008}',
                    'f' => '\u{000c}',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '\\' => '\\',
                    '\'' => '\'',
                    other => other,
                };
                out.push(translated);
                continue;
            }

            out.push(c);
        }

        Ok(self.mk(start, TokenKind::String(out)))
    }

    fn lex_quoted_identifier(&mut self, start: usize) -> Result<Token, LexError> {
        let quote = self.advance_char();
        debug_assert_eq!(quote, Some('"'));

        let mut out = String::new();
        loop {
            let Some(c) = self.advance_char() else {
                return Err(LexError {
                    message: "unterminated quoted identifier".to_string(),
                    position: start,
                });
            };

            if c == '"' {
                if self.peek_char() == Some('"') {
                    self.advance_char();
                    out.push('"');
                    continue;
                }
                break;
            }

            out.push(c);
        }

        if out.is_empty() {
            return Err(LexError {
                message: "zero-length delimited identifier".to_string(),
                position: start,
            });
        }

        let len = out.len();
        if len >= crate::parser::scansup::NAMEDATALEN {
            truncate_identifier(&mut out, len, false);
        }

        Ok(self.mk(start, TokenKind::Identifier(out)))
    }

    fn lex_parameter_or_operator(&mut self, start: usize) -> Result<Token, LexError> {
        self.advance_char();
        if self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
            let digits_start = self.pos;
            while self.peek_char().is_some_and(|c| c.is_ascii_digit()) {
                self.advance_char();
            }
            let raw = &self.input[digits_start..self.pos];
            let value = raw.parse::<i32>().map_err(|_| LexError {
                message: "parameter number too large".to_string(),
                position: start,
            })?;
            return Ok(self.mk(start, TokenKind::Parameter(value)));
        }

        // Dollar-quoted string: $$ ... $$ or $tag$ ... $tag$
        if self.peek_char() == Some('$')
            || self
                .peek_char()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        {
            // Build the delimiter tag
            let tag_start = self.pos;
            // Collect tag chars (if any) before closing $
            while self
                .peek_char()
                .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                self.advance_char();
            }
            if self.peek_char() == Some('$') {
                self.advance_char(); // consume closing $ of opening delimiter
                let tag = &self.input[tag_start..self.pos - 1]; // tag between the two $
                let delimiter = format!("${}$", tag);
                let body_start = self.pos;
                // Find matching closing delimiter
                loop {
                    if self.pos >= self.input.len() {
                        return Err(LexError {
                            message: format!(
                                "unterminated dollar-quoted string (expected {})",
                                delimiter
                            ),
                            position: start,
                        });
                    }
                    if self.input[self.pos..].starts_with(&delimiter) {
                        let body = self.input[body_start..self.pos].to_string();
                        // Advance past the closing delimiter
                        for _ in 0..delimiter.len() {
                            self.advance_char();
                        }
                        return Ok(self.mk(start, TokenKind::String(body)));
                    }
                    self.advance_char();
                }
            }
            // Not a dollar-quoted string; reset position to after first $
            self.pos = tag_start;
        }

        Ok(self.mk(start, TokenKind::Operator("$".to_string())))
    }

    fn lex_number(&mut self, start: usize, leading_dot: bool) -> Result<Token, LexError> {
        if leading_dot {
            self.advance_char();
            let digit_start = self.pos;
            while self.peek_char().is_some_and(is_dec_digit_or_underscore) {
                self.advance_char();
            }
            let body = &self.input[digit_start..self.pos];
            let mut text = format!(".{}", body);
            self.consume_exponent(&mut text)?;
            return Ok(self.mk(start, TokenKind::Float(text)));
        }

        if self.starts_with("0x") || self.starts_with("0X") {
            return self.lex_radix_integer(start, 16, 2, "invalid hexadecimal integer");
        }
        if self.starts_with("0o") || self.starts_with("0O") {
            return self.lex_radix_integer(start, 8, 2, "invalid octal integer");
        }
        if self.starts_with("0b") || self.starts_with("0B") {
            return self.lex_radix_integer(start, 2, 2, "invalid binary integer");
        }

        let int_start = self.pos;
        while self.peek_char().is_some_and(is_dec_digit_or_underscore) {
            self.advance_char();
        }
        let mut text = self.input[int_start..self.pos].to_string();
        let mut is_float = false;

        if self.peek_char() == Some('.') && !self.starts_with("..") {
            is_float = true;
            self.advance_char();
            text.push('.');
            let frac_start = self.pos;
            while self.peek_char().is_some_and(is_dec_digit_or_underscore) {
                self.advance_char();
            }
            text.push_str(&self.input[frac_start..self.pos]);
        }

        if self.peek_char().is_some_and(|c| c == 'e' || c == 'E') {
            is_float = true;
            self.consume_exponent(&mut text)?;
        }

        if self.peek_char().is_some_and(is_ident_start) {
            return Err(LexError {
                message: "trailing junk after numeric literal".to_string(),
                position: self.pos,
            });
        }

        if is_float {
            return Ok(self.mk(start, TokenKind::Float(text)));
        }

        let sanitized = text.replace('_', "");
        let value = sanitized.parse::<i64>().map_err(|_| LexError {
            message: "integer literal out of range".to_string(),
            position: start,
        })?;
        Ok(self.mk(start, TokenKind::Integer(value)))
    }

    fn lex_radix_integer(
        &mut self,
        start: usize,
        radix: u32,
        prefix_len: usize,
        invalid_message: &'static str,
    ) -> Result<Token, LexError> {
        self.pos += prefix_len;
        let digits_start = self.pos;
        while self
            .peek_char()
            .is_some_and(|c| c == '_' || c.is_digit(radix))
        {
            self.advance_char();
        }

        let raw_digits = &self.input[digits_start..self.pos];
        if raw_digits.is_empty() || raw_digits == "_" {
            return Err(LexError {
                message: invalid_message.to_string(),
                position: start,
            });
        }

        if self.peek_char().is_some_and(is_ident_start) {
            return Err(LexError {
                message: "trailing junk after numeric literal".to_string(),
                position: self.pos,
            });
        }

        let sanitized = raw_digits.replace('_', "");
        let value = i64::from_str_radix(&sanitized, radix).map_err(|_| LexError {
            message: "integer literal out of range".to_string(),
            position: start,
        })?;
        Ok(self.mk(start, TokenKind::Integer(value)))
    }

    fn consume_exponent(&mut self, text: &mut String) -> Result<(), LexError> {
        let Some(c) = self.peek_char() else {
            return Ok(());
        };
        if c != 'e' && c != 'E' {
            return Ok(());
        }

        self.advance_char();
        text.push(c);

        if self
            .peek_char()
            .is_some_and(|sign| sign == '+' || sign == '-')
        {
            let sign = self.advance_char().unwrap_or('+');
            text.push(sign);
        }

        let digit_start = self.pos;
        while self.peek_char().is_some_and(is_dec_digit_or_underscore) {
            self.advance_char();
        }

        if digit_start == self.pos {
            return Err(LexError {
                message: "trailing junk after numeric literal".to_string(),
                position: self.pos,
            });
        }
        text.push_str(&self.input[digit_start..self.pos]);
        Ok(())
    }

    fn lex_identifier_or_keyword(&mut self, start: usize) -> Result<Token, LexError> {
        self.advance_char();
        while self.peek_char().is_some_and(is_ident_cont) {
            self.advance_char();
        }
        let raw = &self.input[start..self.pos];
        let normalized = downcase_truncate_identifier(raw, false).value;

        if let Some(kw) = Keyword::from_ident(&normalized) {
            return Ok(self.mk(start, TokenKind::Keyword(kw)));
        }
        Ok(self.mk(start, TokenKind::Identifier(normalized)))
    }

    fn lex_operator(&mut self, start: usize) -> Result<Token, LexError> {
        let op_start = self.pos;
        while let Some(c) = self.peek_char() {
            if !is_operator_char(c) {
                break;
            }
            if self.starts_with("/*") || self.starts_with("--") {
                break;
            }
            self.advance_char();
        }

        if self.pos == op_start {
            return Err(LexError {
                message: "unexpected character while parsing operator".to_string(),
                position: start,
            });
        }

        let mut op = self.input[op_start..self.pos].to_string();
        if op.len() > 1 && (op.ends_with('+') || op.ends_with('-')) {
            let has_non_sql_op_chars = op[..op.len() - 1]
                .chars()
                .any(|c| matches!(c, '~' | '!' | '@' | '#' | '^' | '&' | '|' | '`' | '?' | '%'));
            if !has_non_sql_op_chars {
                while op.len() > 1 && (op.ends_with('+') || op.ends_with('-')) {
                    op.pop();
                    self.pos -= 1;
                }
            }
        }

        let remapped = match op.as_str() {
            "=" => Some(TokenKind::Equal),
            "<" => Some(TokenKind::Less),
            ">" => Some(TokenKind::Greater),
            "+" => Some(TokenKind::Plus),
            "-" => Some(TokenKind::Minus),
            "*" => Some(TokenKind::Star),
            "/" => Some(TokenKind::Slash),
            "%" => Some(TokenKind::Percent),
            "^" => Some(TokenKind::Caret),
            "<=" => Some(TokenKind::LessEquals),
            ">=" => Some(TokenKind::GreaterEquals),
            "<>" | "!=" => Some(TokenKind::NotEquals),
            "=>" => Some(TokenKind::EqualsGreater),
            _ => None,
        };

        if let Some(kind) = remapped {
            return Ok(self.mk(start, kind));
        }
        Ok(self.mk(start, TokenKind::Operator(op)))
    }

    fn skip_whitespace_and_comments(&mut self) -> Result<(), LexError> {
        loop {
            let mut progressed = false;
            while self.peek_char().is_some_and(scanner_isspace) {
                progressed = true;
                self.advance_char();
            }

            if self.starts_with("--") {
                progressed = true;
                self.pos += 2;
                while let Some(c) = self.peek_char() {
                    self.advance_char();
                    if c == '\n' || c == '\r' {
                        break;
                    }
                }
            } else if self.starts_with("/*") {
                progressed = true;
                let comment_start = self.pos;
                self.skip_block_comment(comment_start)?;
            }

            if !progressed {
                break;
            }
        }
        Ok(())
    }

    fn skip_block_comment(&mut self, comment_start: usize) -> Result<(), LexError> {
        self.pos += 2;
        let mut depth = 1usize;
        while self.pos < self.input.len() {
            if self.starts_with("/*") {
                depth += 1;
                self.pos += 2;
                continue;
            }
            if self.starts_with("*/") {
                depth -= 1;
                self.pos += 2;
                if depth == 0 {
                    return Ok(());
                }
                continue;
            }
            self.advance_char();
        }
        Err(LexError {
            message: "unterminated /* comment".to_string(),
            position: comment_start,
        })
    }

    fn starts_with(&self, s: &str) -> bool {
        self.input[self.pos..].starts_with(s)
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_nth_char(&self, n: usize) -> Option<char> {
        self.input[self.pos..].chars().nth(n)
    }

    fn advance_char(&mut self) -> Option<char> {
        let c = self.peek_char()?;
        self.pos += c.len_utf8();
        Some(c)
    }
}

fn is_ident_start(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic() || (!c.is_ascii() && c.is_alphabetic())
}

fn is_ident_cont(c: char) -> bool {
    is_ident_start(c) || c.is_ascii_digit() || c == '$'
}

fn is_dec_digit_or_underscore(c: char) -> bool {
    c.is_ascii_digit() || c == '_'
}

fn is_operator_char(c: char) -> bool {
    matches!(
        c,
        '~' | '!'
            | '@'
            | '#'
            | '^'
            | '&'
            | '|'
            | '`'
            | '?'
            | '+'
            | '-'
            | '*'
            | '/'
            | '%'
            | '<'
            | '>'
            | '='
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexes_keywords_identifiers_and_operators() {
        let sql = "SELECT Foo, \"Bar\" AS baz FROM tbl WHERE x >= 10 AND y <> 'a''b';";
        let tokens = lex_sql(sql).expect("lexing should succeed");

        assert!(matches!(
            tokens[0].kind,
            TokenKind::Keyword(Keyword::Select)
        ));
        assert_eq!(tokens[1].kind, TokenKind::Identifier("foo".to_string()));
        assert_eq!(tokens[3].kind, TokenKind::Identifier("Bar".to_string()));
        assert!(matches!(tokens[4].kind, TokenKind::Keyword(Keyword::As)));
        assert_eq!(tokens[5].kind, TokenKind::Identifier("baz".to_string()));
        assert!(tokens.iter().any(|t| t.kind == TokenKind::GreaterEquals));
        assert!(tokens.iter().any(|t| t.kind == TokenKind::NotEquals));
        assert!(
            tokens
                .iter()
                .any(|t| t.kind == TokenKind::String("a'b".to_string()))
        );
    }

    #[test]
    fn lexes_nested_comments() {
        let sql = "SELECT /* a /* b */ c */ 1";
        let tokens = lex_sql(sql).expect("lexing should succeed");
        assert!(matches!(
            tokens[0].kind,
            TokenKind::Keyword(Keyword::Select)
        ));
        assert_eq!(tokens[1].kind, TokenKind::Integer(1));
    }

    #[test]
    fn lexes_numeric_dotdot_like_scan_l() {
        let sql = "1..10";
        let tokens = lex_sql(sql).expect("lexing should succeed");
        assert_eq!(tokens[0].kind, TokenKind::Integer(1));
        assert_eq!(tokens[1].kind, TokenKind::DotDot);
        assert_eq!(tokens[2].kind, TokenKind::Integer(10));
    }

    #[test]
    fn errors_on_unterminated_string() {
        let err = lex_sql("SELECT 'abc").expect_err("lexing should fail");
        assert!(err.message.contains("unterminated quoted string"));
    }

    #[test]
    fn lexes_json_operators() {
        let tokens = lex_sql("SELECT doc->'a', doc->>'a', doc#>'{a,b}', doc#>>'{a,b}', doc || '{\"z\":1}', doc @> '{\"a\":1}', doc <@ '{\"a\":1,\"b\":2}', doc @? '$.a', doc @@ '$.a', doc ? 'a', doc ?| '{a,b}', doc ?& '{a,b}', doc #- '{a,b}'").expect("lexing should succeed");
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("->".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("->>".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("#>".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("#>>".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("||".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("@>".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("<@".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("@?".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("@@".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("?".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("?|".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("?&".to_string()))
        );
        assert!(
            tokens
                .iter()
                .any(|token| token.kind == TokenKind::Operator("#-".to_string()))
        );
    }
}
