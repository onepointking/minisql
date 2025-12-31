use crate::error::Result;
use crate::lexer::Token;
use crate::types::Value;
use crate::parser::ast::{Expr, BinaryOperator};
use crate::parser::Parser;
use crate::error::MiniSqlError;

impl Parser {
    // Expression parsing with operator precedence
    pub(super) fn parse_expression(&mut self) -> Result<Expr> {
        self.parse_or_expression()
    }

    pub(super) fn parse_or_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expression()?;
        while *self.peek() == Token::Or {
            self.advance();
            let right = self.parse_and_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    pub(super) fn parse_and_expression(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expression()?;
        while *self.peek() == Token::And {
            self.advance();
            let right = self.parse_not_expression()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    pub(super) fn parse_not_expression(&mut self) -> Result<Expr> {
        if *self.peek() == Token::Not {
            self.advance();
            let expr = self.parse_not_expression()?;
            Ok(Expr::Not(Box::new(expr)))
        } else {
            self.parse_comparison()
        }
    }

    pub(super) fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_additive()?;

        // Handle IS NULL / IS NOT NULL
        if *self.peek() == Token::Is {
            self.advance();
            if *self.peek() == Token::Not {
                self.advance();
                self.expect(Token::Null)?;
                return Ok(Expr::IsNotNull(Box::new(left)));
            } else {
                self.expect(Token::Null)?;
                return Ok(Expr::IsNull(Box::new(left)));
            }
        }

        // Handle IN / NOT IN
        if *self.peek() == Token::Not {
            let checkpoint = self.pos;
            self.advance();
            if *self.peek() == Token::In {
                self.advance();
                self.expect(Token::LeftParen)?;
                let mut values = vec![];
                loop {
                    values.push(self.parse_additive()?);
                    if *self.peek() != Token::Comma {
                        break;
                    }
                    self.advance(); // consume comma
                }
                self.expect(Token::RightParen)?;
                return Ok(Expr::NotIn {
                    expr: Box::new(left),
                    values,
                });
            } else {
                // Not followed by IN, restore position
                self.pos = checkpoint;
            }
        }

        if *self.peek() == Token::In {
            self.advance();
            self.expect(Token::LeftParen)?;
            let mut values = vec![];
            loop {
                values.push(self.parse_additive()?);
                if *self.peek() != Token::Comma {
                    break;
                }
                self.advance(); // consume comma
            }
            self.expect(Token::RightParen)?;
            return Ok(Expr::In {
                expr: Box::new(left),
                values,
            });
        }

        let op = match self.peek() {
            Token::Equal => BinaryOperator::Equal,
            Token::NotEqual => BinaryOperator::NotEqual,
            Token::LessThan => BinaryOperator::LessThan,
            Token::LessThanEq => BinaryOperator::LessThanOrEqual,
            Token::GreaterThan => BinaryOperator::GreaterThan,
            Token::GreaterThanEq => BinaryOperator::GreaterThanOrEqual,
            Token::Like => BinaryOperator::Like,
            _ => return Ok(left),
        };

        self.advance();
        let right = self.parse_additive()?;
        Ok(Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        })
    }

    pub(super) fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinaryOperator::Plus,
                Token::Minus => BinaryOperator::Minus,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    pub(super) fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_json_access()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinaryOperator::Multiply,
                Token::Slash => BinaryOperator::Divide,
                _ => break,
            };
            self.advance();
            let right = self.parse_json_access()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    pub(super) fn parse_json_access(&mut self) -> Result<Expr> {
        let mut expr = self.parse_primary_expression()?;

        loop {
            match self.peek() {
                Token::Arrow => {
                    self.advance();
                    let key = match self.peek().clone() {
                        Token::StringLiteral(s) => {
                            self.advance();
                            s
                        }
                        Token::Identifier(s) => {
                            self.advance();
                            s
                        }
                        _ => return Err(MiniSqlError::Syntax(
                            "Expected key after ->".into()
                        )),
                    };
                    expr = Expr::JsonAccess {
                        expr: Box::new(expr),
                        key,
                        as_text: false,
                    };
                }
                Token::ArrowText => {
                    self.advance();
                    let key = match self.peek().clone() {
                        Token::StringLiteral(s) => {
                            self.advance();
                            s
                        }
                        Token::Identifier(s) => {
                            self.advance();
                            s
                        }
                        _ => return Err(MiniSqlError::Syntax(
                            "Expected key after ->>".into()
                        )),
                    };
                    expr = Expr::JsonAccess {
                        expr: Box::new(expr),
                        key,
                        as_text: true,
                    };
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    pub(super) fn parse_primary_expression(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            Token::NumberLiteral(n) => {
                self.advance();
                if n.contains('.') {
                    let f: f64 = n.parse().map_err(|_| {
                        self.error_with_context(format!("Invalid floating point number: {}", n))
                    })?;
                    Ok(Expr::Literal(Value::Float(f)))
                } else {
                    let i: i64 = n.parse().map_err(|_| {
                        self.error_with_context(format!("Invalid integer: {}", n))
                    })?;
                    Ok(Expr::Literal(Value::Integer(i)))
                }
            }
            Token::StringLiteral(s) => {
                self.advance();
                // SAFETY FIX: Don't automatically parse JSON based on { } or [ ] characters
                // JSON values should be explicitly created when inserting into JSON columns,
                // not automatically during parsing. This prevents legitimate strings like
                // "{hello}" or "[test]" from being incorrectly parsed as JSON.
                Ok(Expr::Literal(Value::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Value::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Value::Boolean(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Value::Null))
            }
            Token::Identifier(name) => {
                self.advance();
                // Check for function call
                if *self.peek() == Token::LeftParen {
                    self.advance();
                    
                    // Special case: COUNT(*) - empty args means count all rows
                    if name.to_uppercase() == "COUNT" && *self.peek() == Token::Star {
                        self.advance(); // consume *
                        self.expect(Token::RightParen)?;
                        return Ok(Expr::FunctionCall { name, args: vec![] });
                    }
                    
                    let mut args = Vec::new();
                    if *self.peek() != Token::RightParen {
                        loop {
                            args.push(self.parse_expression()?);
                            if *self.peek() == Token::Comma {
                                self.advance();
                            } else {
                                break;
                            }
                        }
                    }
                    self.expect(Token::RightParen)?;
                    Ok(Expr::FunctionCall { name, args })
                } else if *self.peek() == Token::Dot {
                    // Qualified column name: table.column
                    self.advance(); // consume .
                    let column_name = self.consume_identifier()?;
                    Ok(Expr::Column { table: Some(name), name: column_name })
                } else {
                    // Unqualified column name
                    Ok(Expr::Column { table: None, name })
                }
            }
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(expr)
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_primary_expression()?;
                Ok(Expr::BinaryOp {
                    left: Box::new(Expr::Literal(Value::Integer(0))),
                    op: BinaryOperator::Minus,
                    right: Box::new(expr),
                })
            }
            Token::Placeholder => {
                self.advance();
                let index = self.placeholder_count;
                self.placeholder_count += 1;
                Ok(Expr::Placeholder(index))
            }
            Token::Eof => Err(self.error_with_context(
                "Unexpected end of input in expression".to_string()
            )),
            other => Err(self.error_with_context(format!(
                "Unexpected token in expression: {:?}",
                other
            ))),
        }
    }
}
