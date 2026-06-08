pub mod ast;
pub mod digest;
pub mod parser;
pub mod to_clickhouse;

use crate::error::HyperbytedbError;

pub fn parse(input: &str) -> Result<Vec<ast::Statement>, HyperbytedbError> {
    let stmts = parser::parse_query(input)?;
    for stmt in &stmts {
        if let ast::Statement::Select(s) = stmt {
            to_clickhouse::validate_select_into(s)?;
        }
    }
    Ok(stmts)
}
