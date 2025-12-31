use crate::error::Result;
use crate::lexer::Token;
use super::ast::*;
use super::Parser;

impl Parser {
    /// Parse optional WHERE clause
    pub(super) fn parse_where_clause(&mut self) -> Result<Option<Expr>> {
        if *self.peek() == Token::Where {
            self.push_context("WHERE clause");
            self.advance();
            let expr = self.parse_expression()?;
            self.pop_context();
            Ok(Some(expr))
        } else {
            Ok(None)
        }
    }

    /// Parse optional GROUP BY clause
    pub(super) fn parse_group_by_clause(&mut self) -> Result<Vec<Expr>> {
        let mut group_by = Vec::new();
        if *self.peek() == Token::Group {
            self.push_context("GROUP BY clause");
            self.advance();
            self.expect(Token::By)?;
            loop {
                group_by.push(self.parse_expression()?);
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.pop_context();
        }
        Ok(group_by)
    }

    /// Parse optional ORDER BY clause
    pub(super) fn parse_order_by_clause(&mut self) -> Result<Vec<OrderByClause>> {
        let mut order_by = Vec::new();
        if *self.peek() == Token::Order {
            self.push_context("ORDER BY clause");
            self.advance();
            self.expect(Token::By)?;
            
            loop {
                let expr = self.parse_expression()?;
                let direction = match self.peek() {
                    Token::Asc => {
                        self.advance();
                        SortOrder::Asc
                    }
                    Token::Desc => {
                        self.advance();
                        SortOrder::Desc
                    }
                    _ => SortOrder::Asc, // Default to ascending
                };
                
                order_by.push(OrderByClause { expr, direction });
                
                if *self.peek() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.pop_context();
        }
        Ok(order_by)
    }

    /// Parse optional LIMIT clause
    pub(super) fn parse_limit_clause(&mut self) -> Result<Option<u64>> {
        if *self.peek() == Token::Limit {
            self.push_context("LIMIT clause");
            self.advance();
            let limit = match self.peek().clone() {
                Token::NumberLiteral(n) => {
                    self.advance();
                    Some(n.parse().map_err(|_| self.error_with_context("Invalid LIMIT value".to_string()))?)
                }
                _ => {
                    self.pop_context();
                    return Err(self.error_with_context("Expected number after LIMIT".to_string()));
                }
            };
            self.pop_context();
            Ok(limit)
        } else {
            Ok(None)
        }
    }
}
