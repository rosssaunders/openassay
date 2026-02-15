use openassay::storage::tuple::ScalarValue;
use openassay::parser::ast::Expr;
use openassay::executor::exec_expr::eval_expr;

fn main() {
    println!("Testing numeric multiplication: 19.99 * 5");
    
    // Test that decimal literals parse as Numeric
    let float_literal = Expr::Float("19.99".to_string());
    let int_literal = Expr::Integer(5);
    
    let result = eval_expr(&float_literal, &[], &[], None);
    match result {
        Ok(ScalarValue::Numeric(decimal)) => {
            println!("19.99 parsed as Numeric: {}", decimal);
        }
        Ok(ScalarValue::Float(f)) => {
            println!("19.99 parsed as Float: {}", f);
        }
        Ok(other) => {
            println!("19.99 parsed as: {:?}", other);
        }
        Err(e) => {
            println!("Error parsing 19.99: {}", e.message);
        }
    }
    
    let result = eval_expr(&int_literal, &[], &[], None);
    match result {
        Ok(val) => {
            println!("5 parsed as: {:?}", val);
        }
        Err(e) => {
            println!("Error parsing 5: {}", e.message);
        }
    }
}