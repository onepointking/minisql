//! SQL Parser for MiniSQL
//!
//! Implements a hand-written recursive descent parser for a subset of SQL:
//! - CREATE TABLE
//! - SELECT (with WHERE)
//! - INSERT
//! - UPDATE (with WHERE)
//! - DELETE (with WHERE)
//! - BEGIN, COMMIT, ROLLBACK
//!
//! The parser produces an Abstract Syntax Tree (AST) that the executor can process.

use crate::error::Result;
use crate::lexer::{Lexer, Token};

pub mod ast;
pub use self::ast::*;

mod expressions;
mod select;
mod clauses;
mod dml;
mod ddl;
mod other;
mod utils;

//=============================================================================
// Parser
//=============================================================================

/// SQL Parser
pub struct Parser {
    pub(super) tokens: Vec<Token>,
    pub(super) pos: usize,
    /// Counter for placeholder parameters (?) in prepared statements
    pub(super) placeholder_count: usize,
    /// Current parsing context for better error messages
    pub(super) context_stack: Vec<utils::ParserContext>,
}

impl Parser {
    /// Parse a SQL string into a statement
    pub fn parse(sql: &str) -> Result<Statement> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser { tokens, pos: 0, placeholder_count: 0, context_stack: Vec::new() };
        parser.parse_statement()
    }

    /// Parse a SQL string for prepared statement usage
    /// Returns the statement and the number of placeholders found
    pub fn parse_prepared(sql: &str) -> Result<(Statement, usize)> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;
        let mut parser = Parser { tokens, pos: 0, placeholder_count: 0, context_stack: Vec::new() };
        let stmt = parser.parse_statement()?;
        Ok((stmt, parser.placeholder_count))
    }

    pub(super) fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    pub(super) fn advance(&mut self) {
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
    }

    /// Check if we've reached EOF unexpectedly
    pub(super) fn check_eof(&self, context: &str) -> Result<()> {
        if *self.peek() == Token::Eof {
            Err(self.error_with_context(format!("Unexpected end of input while parsing {}", context)))
        } else {
            Ok(())
        }
    }

    // `advance_if` removed - it was unused and duplicated convenience logic.

    pub(super) fn expect(&mut self, expected: Token) -> Result<()> {
        if *self.peek() == Token::Eof {
            return Err(self.error_with_context(
                format!("Expected {}, but reached end of input", self.token_brief(&expected))
            ));
        }
        if *self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(self.error_with_context(format!(
                "Expected {}, found {}",
                self.token_brief(&expected), self.token_brief(self.peek())
            )))
        }
    }

    pub(super) fn consume_identifier(&mut self) -> Result<String> {
        if *self.peek() == Token::Eof {
            return Err(self.error_with_context(
                "Expected identifier, but reached end of input".to_string()
            ));
        }
        match self.peek() {
            Token::Identifier(name) => {
                let result = name.clone();
                self.advance();
                Ok(result)
            }
            _ => Err(self.error_with_context(format!(
                "Expected identifier, found {}",
                self.token_brief(self.peek())
            ))),
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        self.push_context("SQL statement");
        let result = self.parse_statement_impl();
        self.pop_context();
        result
    }

    fn parse_statement_impl(&mut self) -> Result<Statement> {
        let stmt = match self.peek() {
            Token::Select => self.parse_select()?,
            Token::Insert => self.parse_insert()?,
            Token::Update => self.parse_update()?,
            Token::Delete => self.parse_delete()?,
            Token::Create => self.parse_create()?,
            Token::Drop => self.parse_drop()?,
            Token::Truncate => self.parse_truncate()?,
            Token::Alter => self.parse_alter()?,
            Token::Begin | Token::Start => self.parse_begin()?,
            Token::Commit => { self.advance(); Statement::Commit }
            Token::Rollback => { self.advance(); Statement::Rollback }
            Token::Checkpoint => { self.advance(); Statement::Checkpoint }
            Token::Vacuum => { self.advance(); Statement::Vacuum }
            Token::Show => self.parse_show()?,
            Token::Describe | Token::Desc => self.parse_describe()?,
            Token::Eof => return Err(self.error_with_context("Empty statement or unexpected end of input".to_string())),
            _ => return Err(self.error_with_context(format!(
                "Unexpected token: {:?}",
                self.peek()
            ))),
        };

        // Optional semicolon at the end
        if *self.peek() == Token::Semicolon {
            self.advance();
        }

        Ok(stmt)
    }

}

#[cfg(test)]
mod tests;








