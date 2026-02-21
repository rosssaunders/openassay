#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Numeric(rust_decimal::Decimal),
    Text(String),
    Array(Vec<Self>),
    /// Row/Record type for ROW(a, b, c) or (a, b, c) expressions
    Record(Vec<Self>),
    /// Fixed-dimension float vector used by the pgvector extension
    Vector(Vec<f32>),
}

impl ScalarValue {
    pub fn render(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(v) => if *v { "t" } else { "f" }.to_string(),
            Self::Int(v) => v.to_string(),
            Self::Float(v) => render_float8(*v),
            Self::Numeric(v) => v.to_string(),
            Self::Text(v) => v.clone(),
            Self::Array(values) => render_array_literal(values),
            Self::Record(values) => {
                let parts: Vec<String> = values
                    .iter()
                    .map(|v| {
                        if matches!(v, Self::Null) {
                            String::new()
                        } else {
                            v.render()
                        }
                    })
                    .collect();
                format!("({})", parts.join(","))
            }
            Self::Vector(values) => render_vector_literal(values),
        }
    }
}

/// Render a float8 (f64) value matching PostgreSQL's output format.
///
/// PostgreSQL uses shortest-representation output (extra_float_digits = 1 by default in modern PG).
/// Special values: NaN, Infinity, -Infinity.
/// Integer-valued floats are rendered without decimal point (e.g. 0, 1, -5).
pub fn render_float8(v: f64) -> String {
    if v.is_nan() {
        return "NaN".to_string();
    }
    if v.is_infinite() {
        return if v.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    // Use Rust's default Display which gives shortest representation

    v.to_string()
}

/// Render a float4 (f32) value matching PostgreSQL's output format.
/// Float4 has less precision than float8, so we cast to f32 first.
pub fn render_float4(v: f64) -> String {
    let v32 = v as f32;
    if v32.is_nan() {
        return "NaN".to_string();
    }
    if v32.is_infinite() {
        return if v32.is_sign_positive() {
            "Infinity".to_string()
        } else {
            "-Infinity".to_string()
        };
    }
    let text = format!("{v32}");
    text
}

fn render_array_literal(values: &[ScalarValue]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|value| match value {
            ScalarValue::Null => "NULL".to_string(),
            _ => value.render(),
        })
        .collect();
    format!("{{{}}}", parts.join(","))
}

fn render_vector_literal(values: &[f32]) -> String {
    let parts: Vec<String> = values
        .iter()
        .map(|v| render_float4(*v as f64))
        .collect();
    format!("[{}]", parts.join(","))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyBinaryColumn {
    pub name: String,
    pub type_oid: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CopyBinarySnapshot {
    pub qualified_name: String,
    pub columns: Vec<CopyBinaryColumn>,
    pub rows: Vec<Vec<ScalarValue>>,
}
