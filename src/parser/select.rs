use crate::error::{MiniSqlError, Result};
use crate::lexer::Token;
use super::ast::*;
use super::Parser;

impl Parser {
    pub(super) fn parse_select(&mut self) -> Result<Statement> {
        self.push_context("SELECT statement");
        self.expect(Token::Select)?;

        let columns = self.parse_select_columns()?;
        
        // FROM clause (optional in MySQL)
        let (from, joins) = if *self.peek() == Token::From {
            self.advance();
            let from = self.parse_table_ref()?;
            let joins = self.parse_join_clauses()?;
            (Some(from), joins)
        } else {
            (None, Vec::new())
        };

        // If there's no FROM clause, disallow standalone WHERE/GROUP/ORDER/LIMIT clauses
        if from.is_none() && (*self.peek() == Token::Where || *self.peek() == Token::Group || *self.peek() == Token::Order || *self.peek() == Token::Limit) {
            return Err(self.error_with_context("Expected FROM clause before WHERE/GROUP/ORDER/LIMIT".to_string()));
        }

        // Optional WHERE clause
        let where_clause = self.parse_where_clause()?;

        // Optional GROUP BY clause
        let group_by = self.parse_group_by_clause()?;

        // Optional ORDER BY clause
        let order_by = self.parse_order_by_clause()?;

        // Optional LIMIT clause
        let limit = self.parse_limit_clause()?;

        self.pop_context();
        
        Ok(Statement::Select(SelectStmt {
            columns,
            from,
            joins,
            where_clause,
            group_by,
            order_by,
            limit,
        }))
    }

    /// Parse the column list in a SELECT statement
    pub(super) fn parse_select_columns(&mut self) -> Result<Vec<SelectColumn>> {
        self.push_context("column list");
        let mut columns = Vec::new();
        
        loop {
            if *self.peek() == Token::Star {
                // Simple * (all columns)
                self.advance();
                columns.push(SelectColumn::Star);
            } else if matches!(self.peek(), Token::Identifier(_)) {
                // Check for table.* pattern before parsing as expression
                // We need to lookahead to see if it's identifier.* or just identifier
                let checkpoint = self.pos;
                let ident = self.consume_identifier()?;
                
                if *self.peek() == Token::Dot {
                    self.advance(); // consume .
                    if *self.peek() == Token::Star {
                        // It's table.*
                        self.advance(); // consume *
                        columns.push(SelectColumn::QualifiedStar { table: ident });
                        if *self.peek() == Token::Comma {
                            self.advance();
                            continue;
                        } else {
                            break;
                        }
                    } else {
                        // It's table.column or similar - restore position and parse as expression
                        self.pos = checkpoint;
                        let expr = self.parse_expression()?;
                        
                        // Check for AS or bare alias
                        let alias = if *self.peek() == Token::As {
                            self.advance();
                            Some(self.consume_identifier()?)
                        } else if matches!(self.peek(), Token::Identifier(_)) 
                            && !matches!(self.peek(), Token::Identifier(s) if s.to_uppercase() == "FROM" || s.to_uppercase() == "WHERE")
                        {
                            Some(self.consume_identifier()?)
                        } else {
                            None
                        };
                        columns.push(SelectColumn::Expr { expr, alias });
                    }
                } else {
                    // Just an identifier - restore position and parse as expression
                    self.pos = checkpoint;
                    let expr = self.parse_expression()?;
                    
                    // Check for AS or bare alias
                    let alias = if *self.peek() == Token::As {
                        self.advance();
                        Some(self.consume_identifier()?)
                    } else if matches!(self.peek(), Token::Identifier(_)) 
                        && !matches!(self.peek(), Token::Identifier(s) if s.to_uppercase() == "FROM" || s.to_uppercase() == "WHERE")
                    {
                        Some(self.consume_identifier()?)
                    } else {
                        None
                    };
                    columns.push(SelectColumn::Expr { expr, alias });
                }
            } else {
                // Some other expression (function call, literal, etc.)
                let expr = self.parse_expression()?;
                
                // Check for AS or bare alias
                let alias = if *self.peek() == Token::As {
                    self.advance();
                    Some(self.consume_identifier()?)
                } else if matches!(self.peek(), Token::Identifier(_)) 
                    && !matches!(self.peek(), Token::Identifier(s) if s.to_uppercase() == "FROM" || s.to_uppercase() == "WHERE")
                {
                    Some(self.consume_identifier()?)
                } else {
                    None
                };
                columns.push(SelectColumn::Expr { expr, alias });
            }

            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        
        self.pop_context();
        Ok(columns)
    }

    /// Parse optional JOIN clauses
    pub(super) fn parse_join_clauses(&mut self) -> Result<Vec<JoinClause>> {
        let mut joins = Vec::new();
        while self.is_join_token() {
            joins.push(self.parse_join_clause()?);
        }
        Ok(joins)
    }

    /// Check if the current token starts a JOIN clause
    pub(super) fn is_join_token(&self) -> bool {
        matches!(
            self.peek(),
            Token::Join | Token::Inner | Token::Left | Token::Right
        )
    }

    /// Parse a table reference with optional alias
    pub(super) fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.consume_identifier()?;
        
        // Optional alias: "table alias" or "table AS alias"
        let alias = if *self.peek() == Token::As {
            self.advance();
            Some(self.consume_identifier()?)
        } else if matches!(self.peek(), Token::Identifier(_))
            && !self.is_join_token()
            && !matches!(self.peek(), Token::Identifier(s) if 
                s.to_uppercase() == "WHERE" || 
                s.to_uppercase() == "ORDER" || 
                s.to_uppercase() == "LIMIT" ||
                s.to_uppercase() == "ON")
        {
            Some(self.consume_identifier()?)
        } else {
            None
        };

        Ok(TableRef { name, alias })
    }

    /// Parse a JOIN clause
    pub(super) fn parse_join_clause(&mut self) -> Result<JoinClause> {
        // Determine join type
        let join_type = match self.peek().clone() {
            Token::Inner => {
                self.advance();
                self.expect(Token::Join)?;
                JoinType::Inner
            }
            Token::Left => {
                self.advance();
                // Optional OUTER keyword
                if *self.peek() == Token::Outer {
                    self.advance();
                }
                self.expect(Token::Join)?;
                JoinType::Left
            }
            Token::Right => {
                return Err(MiniSqlError::Syntax(
                    "RIGHT JOIN is not supported. Use LEFT JOIN with reversed table order.".into()
                ));
            }
            Token::Join => {
                self.advance();
                JoinType::Inner // Plain JOIN is INNER JOIN
            }
            _ => {
                return Err(MiniSqlError::Syntax(format!(
                    "Expected JOIN keyword, found {:?}",
                    self.peek()
                )));
            }
        };

        // Parse table reference
        let table = self.parse_table_ref()?;

        // Parse ON condition
        self.expect(Token::On)?;
        let on_condition = self.parse_expression()?;

        Ok(JoinClause {
            join_type,
            table,
            on_condition,
        })
    }
}
