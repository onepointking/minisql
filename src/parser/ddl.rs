use crate::error::Result;
use crate::lexer::Token;
use crate::types::DataType;
use super::ast::*;
use super::Parser;

impl Parser {
    pub(super) fn parse_create(&mut self) -> Result<Statement> {
        self.push_context("CREATE statement");
        self.expect(Token::Create)?;
        
        let result = match self.peek() {
            Token::Table => self.parse_create_table(),
            Token::Index => self.parse_create_index(),
            Token::Eof => Err(self.error_with_context("Expected TABLE or INDEX after CREATE".to_string())),
            _ => Err(self.error_with_context("Expected TABLE or INDEX after CREATE".to_string())),
        };
        
        self.pop_context();
        result
    }

    pub(super) fn parse_create_table(&mut self) -> Result<Statement> {
        self.push_context("CREATE TABLE statement");
        self.expect(Token::Table)?;

        let if_not_exists = if *self.peek() == Token::If {
            self.advance();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.consume_identifier()?;
        self.expect(Token::LeftParen)?;

        let mut columns = Vec::new();
        loop {
            let col_name = self.consume_identifier()?;
            let data_type = self.parse_data_type()?;

            let (nullable, primary_key, auto_increment, default) = self.parse_column_constraints()?;

            columns.push(ColumnDefAst {
                name: col_name,
                data_type,
                nullable,
                primary_key,
                auto_increment,
                default,
            });

            if *self.peek() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        self.expect(Token::RightParen)?;

        // Parse optional ENGINE clause
        let mut engine = None;
        if *self.peek() == Token::Engine {
            self.advance();
            self.expect(Token::Equal)?;
            
            let engine_name = self.consume_identifier()?;
            let engine_type = crate::engines::EngineType::from_name(&engine_name)
                .ok_or_else(|| self.error_with_context(format!(
                    "Unknown engine type: '{}'. Valid options: Granite, Sandstone",
                    engine_name
                )))?;
            engine = Some(engine_type);
        }

        self.pop_context();

        Ok(Statement::CreateTable(CreateTableStmt {
            table_name,
            columns,
            if_not_exists,
            engine,
        }))
    }

    /// Parse column constraints (NOT NULL, PRIMARY KEY, AUTO_INCREMENT, DEFAULT, etc.)
    /// Returns: (nullable, primary_key, auto_increment, default)
    pub(super) fn parse_column_constraints(&mut self) -> Result<(bool, bool, bool, Option<Expr>)> {
        let mut nullable = true;
        let mut primary_key = false;
        let mut auto_increment = false;
        let mut default = None;

        // Parse column constraints
        loop {
            match self.peek() {
                Token::Not => {
                    self.advance();
                    self.expect(Token::Null)?;
                    nullable = false;
                }
                Token::Null => {
                    self.advance();
                    nullable = true;
                }
                Token::Primary => {
                    self.advance();
                    self.expect(Token::Key)?;
                    primary_key = true;
                    nullable = false;
                }
                Token::AutoIncrement => {
                    self.advance();
                    auto_increment = true;
                }
                Token::Unique => {
                    // UNIQUE is parsed but for now just sets uniqueness implicitly
                    // (handled via primary key or explicit index creation)
                    self.advance();
                    // Check for optional KEY keyword
                    if *self.peek() == Token::Key {
                        self.advance();
                    }
                }
                Token::Default => {
                    self.advance();
                    default = Some(self.parse_primary_expression()?);
                }
                _ => break,
            }
        }

        Ok((nullable, primary_key, auto_increment, default))
    }

    pub(super) fn parse_create_index(&mut self) -> Result<Statement> {
        self.expect(Token::Index)?;
        
        let if_not_exists = if *self.peek() == Token::If {
            self.advance();
            self.expect(Token::Not)?;
            self.expect(Token::Exists)?;
            true
        } else {
            false
        };
        
        let index_name = self.consume_identifier()?;
        self.expect(Token::On)?;
        let table_name = self.consume_identifier()?;
        self.expect(Token::LeftParen)?;
        
        // Parse comma-separated list of column names for composite indexes
        let mut columns = Vec::new();
        columns.push(self.consume_identifier()?);
        
        while *self.peek() == Token::Comma {
            self.advance(); // consume comma
            columns.push(self.consume_identifier()?);
        }
        
        self.expect(Token::RightParen)?;
        
        Ok(Statement::CreateIndex(CreateIndexStmt {
            index_name,
            table_name,
            columns,
            if_not_exists,
        }))
    }

    pub(super) fn parse_drop(&mut self) -> Result<Statement> {
        self.push_context("DROP statement");
        self.expect(Token::Drop)?;
        
        let result = match self.peek() {
            Token::Table => {
                self.advance();
                // Optional IF EXISTS
                if *self.peek() == Token::If {
                    self.advance();
                    self.expect(Token::Exists)?;
                }
                let table_name = self.consume_identifier()?;
                Ok(Statement::DropTable(table_name))
            }
            Token::Index => {
                self.advance();
                // Optional IF EXISTS
                if *self.peek() == Token::If {
                    self.advance();
                    self.expect(Token::Exists)?;
                }
                let index_name = self.consume_identifier()?;
                Ok(Statement::DropIndex(index_name))
            }
            Token::Eof => Err(self.error_with_context("Expected TABLE or INDEX after DROP".to_string())),
            _ => Err(self.error_with_context("Expected TABLE or INDEX after DROP".to_string())),
        };
        
        self.pop_context();
        result
    }

    pub(super) fn parse_truncate(&mut self) -> Result<Statement> {
        self.expect(Token::Truncate)?;
        self.expect(Token::Table)?;
        let table_name = self.consume_identifier()?;
        Ok(Statement::TruncateTable(table_name))
    }

    /// Parse ALTER TABLE statement
    /// Currently supports: ALTER TABLE <name> ENGINE=<engine_type>
    pub(super) fn parse_alter(&mut self) -> Result<Statement> {
        self.push_context("ALTER TABLE statement");
        self.expect(Token::Alter)?;
        self.expect(Token::Table)?;
        
        let table_name = self.consume_identifier()?;
        
        // Currently only support ENGINE= clause
        self.expect(Token::Engine)?;
        self.expect(Token::Equal)?;
        
        let engine_name = self.consume_identifier()?;
        let engine_type = crate::engines::EngineType::from_name(&engine_name)
            .ok_or_else(|| self.error_with_context(format!(
                "Unknown engine type: '{}'. Valid options: Granite, Sandstone",
                engine_name
            )))?;
        
        self.pop_context();
        Ok(Statement::AlterTable(AlterTableStmt {
            table_name,
            action: AlterTableAction::ChangeEngine(engine_type),
        }))
    }

    pub(super) fn parse_data_type(&mut self) -> Result<DataType> {
        self.check_eof("data type")?;
        let dt = match self.peek().clone() {
            Token::Int | Token::Integer | Token::Bigint => {
                self.advance();
                DataType::Integer
            }
            Token::Float | Token::Double | Token::Real => {
                self.advance();
                DataType::Float
            }
            Token::Varchar => {
                self.advance();
                let len = if *self.peek() == Token::LeftParen {
                    self.advance();
                    let len = match self.peek().clone() {
                        Token::NumberLiteral(n) => {
                            self.advance();
                            Some(n.parse().map_err(|_| {
                                self.error_with_context("Invalid VARCHAR length".to_string())
                            })?)
                        }
                        _ => None,
                    };
                    self.expect(Token::RightParen)?;
                    len
                } else {
                    None
                };
                DataType::Varchar(len)
            }
            Token::Text => {
                self.advance();
                DataType::Text
            }
            Token::Boolean | Token::Bool => {
                self.advance();
                DataType::Boolean
            }
            Token::Json => {
                self.advance();
                DataType::Json
            }
            other => {
                return Err(self.error_with_context(format!(
                    "Expected data type, found {:?}",
                    other
                )))
            }
        };
        Ok(dt)
    }
}
