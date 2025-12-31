use crate::error::Result;
use crate::lexer::Token;
use super::ast::*;
use super::Parser;

impl Parser {
    pub(super) fn parse_begin(&mut self) -> Result<Statement> {
        self.advance(); // BEGIN or START
        if *self.peek() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::Begin)
    }

    pub(super) fn parse_show(&mut self) -> Result<Statement> {
        self.expect(Token::Show)?;
        self.expect(Token::Tables)?;
        Ok(Statement::ShowTables)
    }

    pub(super) fn parse_describe(&mut self) -> Result<Statement> {
        self.advance(); // DESCRIBE or DESC
        let table_name = self.consume_identifier()?;
        Ok(Statement::Describe(table_name))
    }
}
