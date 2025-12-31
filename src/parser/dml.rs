use crate::error::Result;
use crate::lexer::Token;
use super::ast::*;
use super::Parser;

impl Parser {
    pub(super) fn parse_insert(&mut self) -> Result<Statement> {
        self.push_context("INSERT statement");
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        let table_name = self.consume_identifier()?;

        // Optional column list
        let columns = if *self.peek() == Token::LeftParen {
            self.advance();
            let mut cols = Vec::new();
            loop {
                cols.push(self.consume_identifier()?);
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        // VALUES clause
        self.expect(Token::Values)?;
        let mut values = Vec::new();
        loop {
            self.check_eof("VALUES clause")?;
            self.expect(Token::LeftParen)?;
            let mut row_values = Vec::new();
            loop {
                row_values.push(self.parse_expression()?);
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.expect(Token::RightParen)?;
            values.push(row_values);

            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.pop_context();
        Ok(Statement::Insert(InsertStmt {
            table_name,
            columns,
            values,
        }))
    }

    pub(super) fn parse_update(&mut self) -> Result<Statement> {
        self.push_context("UPDATE statement");
        self.expect(Token::Update)?;
        let table_name = self.consume_identifier()?;
        self.expect(Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            self.check_eof("SET clause")?;
            let column = self.consume_identifier()?;
            self.expect(Token::Equal)?;
            let value = self.parse_expression()?;
            assignments.push((column, value));

            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        let where_clause = self.parse_where_clause()?;
        
        self.pop_context();
        Ok(Statement::Update(UpdateStmt {
            table_name,
            assignments,
            where_clause,
        }))
    }

    pub(super) fn parse_delete(&mut self) -> Result<Statement> {
        self.push_context("DELETE statement");
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        let table_name = self.consume_identifier()?;

        let where_clause = self.parse_where_clause()?;

        self.pop_context();
        Ok(Statement::Delete(DeleteStmt {
            table_name,
            where_clause,
        }))
    }
}
