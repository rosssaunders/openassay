#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Array(Vec<ScalarValue>),
}

impl ScalarValue {
    pub fn render(&self) -> String {
        match self {
            Self::Null => "NULL".to_string(),
            Self::Bool(v) => v.to_string(),
            Self::Int(v) => v.to_string(),
            Self::Float(v) => {
                let mut text = v.to_string();
                if !text.contains('.') && !text.contains('e') && !text.contains('E') {
                    text.push_str(".0");
                }
                text
            }
            Self::Text(v) => v.clone(),
            Self::Array(values) => render_array_literal(values),
        }
    }
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
