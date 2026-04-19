use crate::executor::exec_expr::{
    EvalScope, eval_between_predicate, eval_binary, eval_cast_scalar, eval_is_distinct_from,
    eval_like_predicate, eval_unary, parse_param,
};
use crate::parser::ast::{BooleanTestType, Expr};
use crate::storage::tuple::ScalarValue;
use crate::tcop::engine::EngineError;

const FLAG_WIDE_OPERAND: u8 = 0x01;
const FLAG_NEGATED: u8 = 0x02;
const FLAG_CASE_INSENSITIVE: u8 = 0x04;
const FLAG_HAS_ESCAPE: u8 = 0x08;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub(crate) struct Instruction {
    opcode: u8,
    flags: u8,
    operand: u16,
}

#[derive(Debug, Clone)]
enum Constant {
    Scalar(ScalarValue),
    Identifier(Vec<String>),
    TypeName(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Opcode {
    Halt = 0x00,
    LoadConst = 0x10,
    LoadIdentifier = 0x11,
    LoadParameter = 0x12,
    MakeArray = 0x13,
    MakeRecord = 0x14,
    ApplyUnary = 0x20,
    ApplyBinary = 0x21,
    Between = 0x30,
    Like = 0x31,
    IsNull = 0x32,
    BooleanTest = 0x33,
    IsDistinctFrom = 0x34,
    Cast = 0x35,
}

impl TryFrom<u8> for Opcode {
    type Error = EngineError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Halt),
            0x10 => Ok(Self::LoadConst),
            0x11 => Ok(Self::LoadIdentifier),
            0x12 => Ok(Self::LoadParameter),
            0x13 => Ok(Self::MakeArray),
            0x14 => Ok(Self::MakeRecord),
            0x20 => Ok(Self::ApplyUnary),
            0x21 => Ok(Self::ApplyBinary),
            0x30 => Ok(Self::Between),
            0x31 => Ok(Self::Like),
            0x32 => Ok(Self::IsNull),
            0x33 => Ok(Self::BooleanTest),
            0x34 => Ok(Self::IsDistinctFrom),
            0x35 => Ok(Self::Cast),
            _ => Err(EngineError {
                message: format!("unsupported bytecode opcode {value}"),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompiledExpr {
    instructions: Vec<Instruction>,
    constants: Vec<Constant>,
    wide_operands: Vec<u32>,
}

impl CompiledExpr {
    pub(crate) fn evaluate(
        &self,
        scope: &EvalScope,
        params: &[Option<ScalarValue>],
    ) -> Result<ScalarValue, EngineError> {
        let mut ip = 0usize;
        let mut stack = Vec::with_capacity(self.instructions.len().max(1));

        while let Some(instruction) = self.instructions.get(ip).copied() {
            match Opcode::try_from(instruction.opcode)? {
                Opcode::Halt => {
                    return match stack.len() {
                        1 => stack.pop().ok_or_else(|| EngineError {
                            message: "bytecode stack underflow".to_string(),
                        }),
                        _ => Err(EngineError {
                            message: "bytecode program ended with invalid stack state".to_string(),
                        }),
                    };
                }
                Opcode::LoadConst => {
                    let index = self.operand_as_index(instruction)?;
                    let value = match self.constant(index)? {
                        Constant::Scalar(value) => value.clone(),
                        _ => {
                            return Err(EngineError {
                                message: "bytecode constant type mismatch".to_string(),
                            });
                        }
                    };
                    stack.push(value);
                }
                Opcode::LoadIdentifier => {
                    let index = self.operand_as_index(instruction)?;
                    let parts = match self.constant(index)? {
                        Constant::Identifier(parts) => parts,
                        _ => {
                            return Err(EngineError {
                                message: "bytecode constant type mismatch".to_string(),
                            });
                        }
                    };
                    stack.push(scope.lookup_identifier(parts)?);
                }
                Opcode::LoadParameter => {
                    let param_index =
                        i32::try_from(self.resolve_operand(instruction)?).map_err(|_| {
                            EngineError {
                                message: "bytecode parameter index overflow".to_string(),
                            }
                        })?;
                    stack.push(parse_param(param_index, params)?);
                }
                Opcode::MakeArray => {
                    let len = self.operand_as_index(instruction)?;
                    let values = pop_many(&mut stack, len)?;
                    stack.push(ScalarValue::Array(values));
                }
                Opcode::MakeRecord => {
                    let len = self.operand_as_index(instruction)?;
                    let values = pop_many(&mut stack, len)?;
                    stack.push(ScalarValue::Record(values));
                }
                Opcode::ApplyUnary => {
                    let value = pop_value(&mut stack)?;
                    let opcode = self.operand_as_index(instruction)?;
                    let value = eval_unary(decode_unary(opcode)?, value)?;
                    stack.push(value);
                }
                Opcode::ApplyBinary => {
                    let right = pop_value(&mut stack)?;
                    let left = pop_value(&mut stack)?;
                    let opcode = self.operand_as_index(instruction)?;
                    stack.push(eval_binary(decode_binary(opcode)?, left, right)?);
                }
                Opcode::Between => {
                    let high = pop_value(&mut stack)?;
                    let low = pop_value(&mut stack)?;
                    let value = pop_value(&mut stack)?;
                    stack.push(eval_between_predicate(
                        value,
                        low,
                        high,
                        instruction.flags & FLAG_NEGATED != 0,
                    )?);
                }
                Opcode::Like => {
                    let escape = if instruction.flags & FLAG_HAS_ESCAPE != 0 {
                        Some(pop_value(&mut stack)?)
                    } else {
                        None
                    };
                    let pattern = pop_value(&mut stack)?;
                    let value = pop_value(&mut stack)?;
                    stack.push(eval_like_predicate(
                        value,
                        pattern,
                        instruction.flags & FLAG_CASE_INSENSITIVE != 0,
                        instruction.flags & FLAG_NEGATED != 0,
                        escape,
                    )?);
                }
                Opcode::IsNull => {
                    let value = pop_value(&mut stack)?;
                    let is_null = matches!(value, ScalarValue::Null);
                    stack.push(ScalarValue::Bool(
                        if instruction.flags & FLAG_NEGATED != 0 {
                            !is_null
                        } else {
                            is_null
                        },
                    ));
                }
                Opcode::BooleanTest => {
                    let value = pop_value(&mut stack)?;
                    let test_type = decode_boolean_test(self.operand_as_index(instruction)?)?;
                    let result = match (test_type, &value) {
                        (BooleanTestType::True, ScalarValue::Bool(true)) => true,
                        (BooleanTestType::True, _) => false,
                        (BooleanTestType::False, ScalarValue::Bool(false)) => true,
                        (BooleanTestType::False, _) => false,
                        (BooleanTestType::Unknown, ScalarValue::Null) => true,
                        (BooleanTestType::Unknown, _) => false,
                    };
                    stack.push(ScalarValue::Bool(
                        if instruction.flags & FLAG_NEGATED != 0 {
                            !result
                        } else {
                            result
                        },
                    ));
                }
                Opcode::IsDistinctFrom => {
                    let right = pop_value(&mut stack)?;
                    let left = pop_value(&mut stack)?;
                    stack.push(eval_is_distinct_from(
                        left,
                        right,
                        instruction.flags & FLAG_NEGATED != 0,
                    )?);
                }
                Opcode::Cast => {
                    let value = pop_value(&mut stack)?;
                    let index = self.operand_as_index(instruction)?;
                    let type_name = match self.constant(index)? {
                        Constant::TypeName(type_name) => type_name,
                        _ => {
                            return Err(EngineError {
                                message: "bytecode constant type mismatch".to_string(),
                            });
                        }
                    };
                    stack.push(eval_cast_scalar(value, type_name)?);
                }
            }
            ip += 1;
        }

        Err(EngineError {
            message: "bytecode program terminated without halt".to_string(),
        })
    }

    fn resolve_operand(&self, instruction: Instruction) -> Result<u32, EngineError> {
        if instruction.flags & FLAG_WIDE_OPERAND == 0 {
            return Ok(u32::from(instruction.operand));
        }
        let index = usize::from(instruction.operand);
        self.wide_operands
            .get(index)
            .copied()
            .ok_or_else(|| EngineError {
                message: "bytecode wide operand out of bounds".to_string(),
            })
    }

    fn operand_as_index(&self, instruction: Instruction) -> Result<usize, EngineError> {
        usize::try_from(self.resolve_operand(instruction)?).map_err(|_| EngineError {
            message: "bytecode operand does not fit in usize".to_string(),
        })
    }

    fn constant(&self, index: usize) -> Result<&Constant, EngineError> {
        self.constants.get(index).ok_or_else(|| EngineError {
            message: "bytecode constant out of bounds".to_string(),
        })
    }
}

pub(crate) fn try_compile_expr(expr: &Expr) -> Result<Option<CompiledExpr>, EngineError> {
    let mut compiler = Compiler::default();
    if !compiler.compile_expr(expr)? {
        return Ok(None);
    }
    compiler.emit(Opcode::Halt, 0, 0);
    Ok(Some(CompiledExpr {
        instructions: compiler.instructions,
        constants: compiler.constants,
        wide_operands: compiler.wide_operands,
    }))
}

#[derive(Default)]
struct Compiler {
    instructions: Vec<Instruction>,
    constants: Vec<Constant>,
    wide_operands: Vec<u32>,
}

impl Compiler {
    fn compile_expr(&mut self, expr: &Expr) -> Result<bool, EngineError> {
        match expr {
            Expr::Null => {
                let index = self.push_constant(Constant::Scalar(ScalarValue::Null))?;
                self.emit(Opcode::LoadConst, 0, index);
                Ok(true)
            }
            Expr::Boolean(value) => {
                let index = self.push_constant(Constant::Scalar(ScalarValue::Bool(*value)))?;
                self.emit(Opcode::LoadConst, 0, index);
                Ok(true)
            }
            Expr::Integer(value) => {
                let index = self.push_constant(Constant::Scalar(ScalarValue::Int(*value)))?;
                self.emit(Opcode::LoadConst, 0, index);
                Ok(true)
            }
            Expr::Float(value) => {
                let scalar = compile_float_literal(value)?;
                let index = self.push_constant(Constant::Scalar(scalar))?;
                self.emit(Opcode::LoadConst, 0, index);
                Ok(true)
            }
            Expr::String(value) => {
                let index =
                    self.push_constant(Constant::Scalar(ScalarValue::Text(value.clone())))?;
                self.emit(Opcode::LoadConst, 0, index);
                Ok(true)
            }
            Expr::TypedLiteral { type_name, value } => {
                let index =
                    self.push_constant(Constant::Scalar(ScalarValue::Text(value.clone())))?;
                self.emit(Opcode::LoadConst, 0, index);
                let type_index = self.push_constant(Constant::TypeName(type_name.clone()))?;
                self.emit(Opcode::Cast, 0, type_index);
                Ok(true)
            }
            Expr::Parameter(index) => {
                self.emit(
                    Opcode::LoadParameter,
                    0,
                    u32::try_from(*index).map_err(|_| EngineError {
                        message: format!("invalid parameter reference ${index}"),
                    })?,
                );
                Ok(true)
            }
            Expr::Identifier(parts) => {
                let index = self.push_constant(Constant::Identifier(parts.clone()))?;
                self.emit(Opcode::LoadIdentifier, 0, index);
                Ok(true)
            }
            Expr::Unary { op, expr } => {
                if !self.compile_expr(expr)? {
                    return Ok(false);
                }
                self.emit(Opcode::ApplyUnary, 0, encode_unary(op)?);
                Ok(true)
            }
            Expr::Binary { left, op, right } => {
                if !self.compile_expr(left)? || !self.compile_expr(right)? {
                    return Ok(false);
                }
                self.emit(Opcode::ApplyBinary, 0, encode_binary(op)?);
                Ok(true)
            }
            Expr::ArrayConstructor(items) => {
                for item in items {
                    if !self.compile_expr(item)? {
                        return Ok(false);
                    }
                }
                self.emit(
                    Opcode::MakeArray,
                    0,
                    u32::try_from(items.len()).map_err(|_| EngineError {
                        message: "array constructor too large for bytecode evaluator".to_string(),
                    })?,
                );
                Ok(true)
            }
            Expr::RowConstructor(fields) => {
                for field in fields {
                    if !self.compile_expr(field)? {
                        return Ok(false);
                    }
                }
                self.emit(
                    Opcode::MakeRecord,
                    0,
                    u32::try_from(fields.len()).map_err(|_| EngineError {
                        message: "row constructor too large for bytecode evaluator".to_string(),
                    })?,
                );
                Ok(true)
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if !self.compile_expr(expr)?
                    || !self.compile_expr(low)?
                    || !self.compile_expr(high)?
                {
                    return Ok(false);
                }
                self.emit(Opcode::Between, flag_if(*negated, FLAG_NEGATED), 0);
                Ok(true)
            }
            Expr::Like {
                expr,
                pattern,
                case_insensitive,
                negated,
                escape,
            } => {
                if !self.compile_expr(expr)? || !self.compile_expr(pattern)? {
                    return Ok(false);
                }
                let mut flags = 0;
                if *case_insensitive {
                    flags |= FLAG_CASE_INSENSITIVE;
                }
                if *negated {
                    flags |= FLAG_NEGATED;
                }
                if let Some(escape) = escape {
                    if !self.compile_expr(escape)? {
                        return Ok(false);
                    }
                    flags |= FLAG_HAS_ESCAPE;
                }
                self.emit(Opcode::Like, flags, 0);
                Ok(true)
            }
            Expr::IsNull { expr, negated } => {
                if !self.compile_expr(expr)? {
                    return Ok(false);
                }
                self.emit(Opcode::IsNull, flag_if(*negated, FLAG_NEGATED), 0);
                Ok(true)
            }
            Expr::BooleanTest {
                expr,
                test_type,
                negated,
            } => {
                if !self.compile_expr(expr)? {
                    return Ok(false);
                }
                self.emit(
                    Opcode::BooleanTest,
                    flag_if(*negated, FLAG_NEGATED),
                    encode_boolean_test(test_type),
                );
                Ok(true)
            }
            Expr::IsDistinctFrom {
                left,
                right,
                negated,
            } => {
                if !self.compile_expr(left)? || !self.compile_expr(right)? {
                    return Ok(false);
                }
                self.emit(Opcode::IsDistinctFrom, flag_if(*negated, FLAG_NEGATED), 0);
                Ok(true)
            }
            Expr::Cast { expr, type_name } => {
                if !self.compile_expr(expr)? {
                    return Ok(false);
                }
                let index = self.push_constant(Constant::TypeName(type_name.clone()))?;
                self.emit(Opcode::Cast, 0, index);
                Ok(true)
            }
            Expr::Default
            | Expr::MultiColumnSubqueryRef { .. }
            | Expr::FunctionCall { .. }
            | Expr::Wildcard
            | Expr::QualifiedWildcard(_)
            | Expr::AnyAll { .. }
            | Expr::Exists(_)
            | Expr::ScalarSubquery(_)
            | Expr::ArraySubquery(_)
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::CaseSimple { .. }
            | Expr::CaseSearched { .. }
            | Expr::ArraySubscript { .. }
            | Expr::ArraySlice { .. } => Ok(false),
        }
    }

    fn push_constant(&mut self, constant: Constant) -> Result<u32, EngineError> {
        let index = u32::try_from(self.constants.len()).map_err(|_| EngineError {
            message: "bytecode constant pool overflow".to_string(),
        })?;
        self.constants.push(constant);
        Ok(index)
    }

    fn emit(&mut self, opcode: Opcode, mut flags: u8, operand: u32) {
        let encoded = if let Ok(value) = u16::try_from(operand) {
            value
        } else {
            flags |= FLAG_WIDE_OPERAND;
            let index = self.wide_operands.len();
            self.wide_operands.push(operand);
            u16::try_from(index).expect("wide operand index should fit into u16")
        };
        self.instructions.push(Instruction {
            opcode: opcode as u8,
            flags,
            operand: encoded,
        });
    }
}

fn flag_if(enabled: bool, flag: u8) -> u8 {
    if enabled { flag } else { 0 }
}

fn compile_float_literal(value: &str) -> Result<ScalarValue, EngineError> {
    if let Ok(decimal) = value.parse::<rust_decimal::Decimal>() {
        return Ok(ScalarValue::Numeric(decimal));
    }
    let parsed = value.parse::<f64>().map_err(|_| EngineError {
        message: format!("invalid float literal \"{value}\""),
    })?;
    Ok(ScalarValue::Float(parsed))
}

fn encode_unary(op: &crate::parser::ast::UnaryOp) -> Result<u32, EngineError> {
    match op {
        crate::parser::ast::UnaryOp::Plus => Ok(0),
        crate::parser::ast::UnaryOp::Minus => Ok(1),
        crate::parser::ast::UnaryOp::Not => Ok(2),
    }
}

fn decode_unary(code: usize) -> Result<crate::parser::ast::UnaryOp, EngineError> {
    match code {
        0 => Ok(crate::parser::ast::UnaryOp::Plus),
        1 => Ok(crate::parser::ast::UnaryOp::Minus),
        2 => Ok(crate::parser::ast::UnaryOp::Not),
        _ => Err(EngineError {
            message: format!("unsupported bytecode unary opcode {code}"),
        }),
    }
}

fn encode_binary(op: &crate::parser::ast::BinaryOp) -> Result<u32, EngineError> {
    use crate::parser::ast::BinaryOp;

    match op {
        BinaryOp::Or => Ok(0),
        BinaryOp::And => Ok(1),
        BinaryOp::Eq => Ok(2),
        BinaryOp::NotEq => Ok(3),
        BinaryOp::Lt => Ok(4),
        BinaryOp::Lte => Ok(5),
        BinaryOp::Gt => Ok(6),
        BinaryOp::Gte => Ok(7),
        BinaryOp::Add => Ok(8),
        BinaryOp::Sub => Ok(9),
        BinaryOp::Mul => Ok(10),
        BinaryOp::Div => Ok(11),
        BinaryOp::Mod => Ok(12),
        BinaryOp::Pow => Ok(13),
        BinaryOp::ShiftLeft => Ok(14),
        BinaryOp::ShiftRight => Ok(15),
        BinaryOp::JsonGet => Ok(16),
        BinaryOp::JsonGetText => Ok(17),
        BinaryOp::JsonPath => Ok(18),
        BinaryOp::JsonPathText => Ok(19),
        BinaryOp::JsonConcat => Ok(20),
        BinaryOp::JsonContains => Ok(21),
        BinaryOp::JsonContainedBy => Ok(22),
        BinaryOp::JsonPathExists => Ok(23),
        BinaryOp::JsonPathMatch => Ok(24),
        BinaryOp::JsonHasKey => Ok(25),
        BinaryOp::JsonHasAny => Ok(26),
        BinaryOp::JsonHasAll => Ok(27),
        BinaryOp::JsonDelete => Ok(28),
        BinaryOp::JsonDeletePath => Ok(29),
        BinaryOp::ArrayContains => Ok(30),
        BinaryOp::ArrayContainedBy => Ok(31),
        BinaryOp::ArrayOverlap => Ok(32),
        BinaryOp::ArrayConcat => Ok(33),
        BinaryOp::VectorL2Distance => Ok(34),
        BinaryOp::VectorInnerProduct => Ok(35),
        BinaryOp::VectorCosineDistance => Ok(36),
    }
}

fn decode_binary(code: usize) -> Result<crate::parser::ast::BinaryOp, EngineError> {
    use crate::parser::ast::BinaryOp;

    match code {
        0 => Ok(BinaryOp::Or),
        1 => Ok(BinaryOp::And),
        2 => Ok(BinaryOp::Eq),
        3 => Ok(BinaryOp::NotEq),
        4 => Ok(BinaryOp::Lt),
        5 => Ok(BinaryOp::Lte),
        6 => Ok(BinaryOp::Gt),
        7 => Ok(BinaryOp::Gte),
        8 => Ok(BinaryOp::Add),
        9 => Ok(BinaryOp::Sub),
        10 => Ok(BinaryOp::Mul),
        11 => Ok(BinaryOp::Div),
        12 => Ok(BinaryOp::Mod),
        13 => Ok(BinaryOp::Pow),
        14 => Ok(BinaryOp::ShiftLeft),
        15 => Ok(BinaryOp::ShiftRight),
        16 => Ok(BinaryOp::JsonGet),
        17 => Ok(BinaryOp::JsonGetText),
        18 => Ok(BinaryOp::JsonPath),
        19 => Ok(BinaryOp::JsonPathText),
        20 => Ok(BinaryOp::JsonConcat),
        21 => Ok(BinaryOp::JsonContains),
        22 => Ok(BinaryOp::JsonContainedBy),
        23 => Ok(BinaryOp::JsonPathExists),
        24 => Ok(BinaryOp::JsonPathMatch),
        25 => Ok(BinaryOp::JsonHasKey),
        26 => Ok(BinaryOp::JsonHasAny),
        27 => Ok(BinaryOp::JsonHasAll),
        28 => Ok(BinaryOp::JsonDelete),
        29 => Ok(BinaryOp::JsonDeletePath),
        30 => Ok(BinaryOp::ArrayContains),
        31 => Ok(BinaryOp::ArrayContainedBy),
        32 => Ok(BinaryOp::ArrayOverlap),
        33 => Ok(BinaryOp::ArrayConcat),
        34 => Ok(BinaryOp::VectorL2Distance),
        35 => Ok(BinaryOp::VectorInnerProduct),
        36 => Ok(BinaryOp::VectorCosineDistance),
        _ => Err(EngineError {
            message: format!("unsupported bytecode binary opcode {code}"),
        }),
    }
}

fn encode_boolean_test(test_type: &BooleanTestType) -> u32 {
    match test_type {
        BooleanTestType::True => 0,
        BooleanTestType::False => 1,
        BooleanTestType::Unknown => 2,
    }
}

fn decode_boolean_test(code: usize) -> Result<BooleanTestType, EngineError> {
    match code {
        0 => Ok(BooleanTestType::True),
        1 => Ok(BooleanTestType::False),
        2 => Ok(BooleanTestType::Unknown),
        _ => Err(EngineError {
            message: format!("unsupported bytecode boolean test {code}"),
        }),
    }
}

fn pop_value(stack: &mut Vec<ScalarValue>) -> Result<ScalarValue, EngineError> {
    stack.pop().ok_or_else(|| EngineError {
        message: "bytecode stack underflow".to_string(),
    })
}

fn pop_many(stack: &mut Vec<ScalarValue>, len: usize) -> Result<Vec<ScalarValue>, EngineError> {
    if stack.len() < len {
        return Err(EngineError {
            message: "bytecode stack underflow".to_string(),
        });
    }
    let start = stack.len() - len;
    Ok(stack.drain(start..).collect())
}

#[cfg(test)]
mod tests {
    use super::{Instruction, try_compile_expr};
    use crate::executor::exec_expr::{EvalScope, eval_expr};
    use crate::parser::ast::{BinaryOp, Expr};
    use crate::storage::tuple::ScalarValue;

    #[test]
    fn instruction_layout_is_four_bytes() {
        assert_eq!(std::mem::size_of::<Instruction>(), 4);
    }

    #[tokio::test]
    async fn compiled_expr_matches_tree_walk_for_filter_shape() {
        let expr = Expr::Binary {
            left: Box::new(Expr::Like {
                expr: Box::new(Expr::Identifier(vec!["name".to_string()])),
                pattern: Box::new(Expr::String("a%".to_string())),
                case_insensitive: true,
                negated: false,
                escape: None,
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Cast {
                    expr: Box::new(Expr::Identifier(vec!["qty".to_string()])),
                    type_name: "int4".to_string(),
                }),
                op: BinaryOp::Gt,
                right: Box::new(Expr::Integer(2)),
            }),
        };
        let compiled = try_compile_expr(&expr)
            .expect("bytecode compilation should succeed")
            .expect("expression should be supported");
        let mut scope = EvalScope::default();
        scope.insert_unqualified("name", ScalarValue::Text("Alice".to_string()));
        scope.insert_unqualified("qty", ScalarValue::Text("3".to_string()));

        let compiled_value = compiled
            .evaluate(&scope, &[])
            .expect("compiled evaluation should succeed");
        let tree_value = eval_expr(&expr, &scope, &[])
            .await
            .expect("tree evaluation should succeed");

        assert_eq!(compiled_value, tree_value);
    }

    #[test]
    fn unsupported_expression_falls_back() {
        let expr = Expr::FunctionCall {
            name: vec!["lower".to_string()],
            args: vec![Expr::String("abc".to_string())],
            distinct: false,
            order_by: Vec::new(),
            within_group: Vec::new(),
            filter: None,
            over: None,
        };

        assert!(
            try_compile_expr(&expr)
                .expect("compilation should not error")
                .is_none()
        );
    }
}
