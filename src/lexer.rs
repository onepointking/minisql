use crate::error::{MiniSqlError, Result};

//=============================================================================
// Lexer (Tokenizer)
//=============================================================================

/// Token types for the lexer
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Drop,
    Truncate,
    If,
    Exists,
    Not,
    Null,
    And,
    Or,
    Is,
    Like,
    In,
    Begin,
    Start,
    Transaction,
    Commit,
    Rollback,
    Checkpoint,
    Vacuum,
    Primary,
    Key,
    Default,
    AutoIncrement,
    Unique,
    Show,
    Tables,
    Describe,
    Desc,
    Limit,
    Order,
    By,
    Asc,
    Index,
    On,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    As,
    Group,
    Having,
    Alter,
    Engine,
    
    // Data types
    Int,
    Integer,
    Bigint,
    Float,
    Double,
    Real,
    Varchar,
    Text,
    Boolean,
    Bool,
    Json,
    
    // Literals
    Identifier(String),
    StringLiteral(String),
    NumberLiteral(String),
    True,
    False,
    
    // Operators and punctuation
    Star,          // *
    Comma,         // ,
    LeftParen,     // (
    RightParen,    // )
    Semicolon,     // ;
    Equal,         // =
    NotEqual,      // <> or !=
    LessThan,      // <
    LessThanEq,    // <=
    GreaterThan,   // >
    GreaterThanEq, // >=
    Plus,          // +
    Minus,         // -
    Slash,         // /
    Arrow,         // ->
    ArrowText,     // ->>
    Dot,           // .
    Placeholder,   // ? (for prepared statements)
    
    // End of input
    Eof,
}

/// Lexer state
pub struct Lexer {
    input: Vec<char>,
    pos: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.chars().collect(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.pos).copied()
    }

    fn peek_ahead(&self, n: usize) -> Option<char> {
        self.input.get(self.pos + n).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else if c == '-' && self.peek_ahead(1) == Some('-') {
                // Line comment
                while let Some(c) = self.advance() {
                    if c == '\n' {
                        break;
                    }
                }
            } else if c == '/' && self.peek_ahead(1) == Some('*') {
                // Block comment
                self.advance(); // /
                self.advance(); // *
                while let Some(c) = self.advance() {
                    if c == '*' && self.peek() == Some('/') {
                        self.advance();
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut result = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                result.push(c);
                self.advance();
            } else {
                break;
            }
        }
        result
    }

    fn read_number(&mut self) -> String {
        let mut result = String::new();
        let mut has_dot = false;
        
        while let Some(c) = self.peek() {
            if c.is_ascii_digit() {
                result.push(c);
                self.advance();
            } else if c == '.' && !has_dot {
                has_dot = true;
                result.push(c);
                self.advance();
            } else {
                break;
            }
        }
        result
    }

    fn read_string(&mut self, quote: char) -> Result<String> {
        let mut result = String::new();
        self.advance(); // Skip opening quote
        
        while let Some(c) = self.advance() {
            if c == quote {
                // Check for escaped quote
                if self.peek() == Some(quote) {
                    result.push(quote);
                    self.advance();
                } else {
                    return Ok(result);
                }
            } else if c == '\\' {
                // Handle escape sequences
                match self.advance() {
                    Some('n') => result.push('\n'),
                    Some('r') => result.push('\r'),
                    Some('t') => result.push('\t'),
                    Some('\\') => result.push('\\'),
                    Some(q) if q == quote => result.push(quote),
                    Some(other) => {
                        result.push('\\');
                        result.push(other);
                    }
                    None => return Err(MiniSqlError::Syntax("Unterminated string".into())),
                }
            } else {
                result.push(c);
            }
        }
        
        Err(MiniSqlError::Syntax("Unterminated string".into()))
    }

    pub fn next_token(&mut self) -> Result<Token> {
        self.skip_whitespace();
        
        let c = match self.peek() {
            Some(c) => c,
            None => return Ok(Token::Eof),
        };

        // Single character tokens
        match c {
            '*' => { self.advance(); return Ok(Token::Star); }
            ',' => { self.advance(); return Ok(Token::Comma); }
            '(' => { self.advance(); return Ok(Token::LeftParen); }
            ')' => { self.advance(); return Ok(Token::RightParen); }
            ';' => { self.advance(); return Ok(Token::Semicolon); }
            '+' => { self.advance(); return Ok(Token::Plus); }
            '/' => { self.advance(); return Ok(Token::Slash); }
            '.' => { self.advance(); return Ok(Token::Dot); }
            '?' => { self.advance(); return Ok(Token::Placeholder); }
            _ => {}
        }

        // Multi-character tokens
        if c == '-' {
            self.advance();
            if self.peek() == Some('>') {
                self.advance();
                if self.peek() == Some('>') {
                    self.advance();
                    return Ok(Token::ArrowText);
                }
                return Ok(Token::Arrow);
            }
            return Ok(Token::Minus);
        }

        if c == '=' {
            self.advance();
            return Ok(Token::Equal);
        }

        if c == '!' {
            self.advance();
            if self.peek() == Some('=') {
                self.advance();
                return Ok(Token::NotEqual);
            }
            return Err(MiniSqlError::Syntax(format!("Unexpected character: !")));
        }

        if c == '<' {
            self.advance();
            if self.peek() == Some('=') {
                self.advance();
                return Ok(Token::LessThanEq);
            }
            if self.peek() == Some('>') {
                self.advance();
                return Ok(Token::NotEqual);
            }
            return Ok(Token::LessThan);
        }

        if c == '>' {
            self.advance();
            if self.peek() == Some('=') {
                self.advance();
                return Ok(Token::GreaterThanEq);
            }
            return Ok(Token::GreaterThan);
        }

        // String literals (single quotes) or identifiers (double quotes, backticks)
        if c == '\'' {
            let s = self.read_string(c)?;
            return Ok(Token::StringLiteral(s));
        }
        
        // Double quotes can be used for quoted identifiers (SQL standard)
        if c == '"' {
            self.advance(); // consume opening quote
            let mut ident = String::new();
            while let Some(ch) = self.peek() {
                if ch == '"' {
                    self.advance();
                    // Check for escaped double quote ("")
                    if self.peek() == Some('"') {
                        ident.push('"');
                        self.advance();
                    } else {
                        break;
                    }
                } else {
                    ident.push(ch);
                    self.advance();
                }
            }
            return Ok(Token::Identifier(ident));
        }

        // Numbers
        if c.is_ascii_digit() {
            let num = self.read_number();
            return Ok(Token::NumberLiteral(num));
        }

        // Identifiers and keywords
        if c.is_alphabetic() || c == '_' || c == '`' {
            let is_quoted = c == '`';
            if is_quoted {
                self.advance();
                let mut ident = String::new();
                while let Some(c) = self.peek() {
                    if c == '`' {
                        self.advance();
                        break;
                    }
                    ident.push(c);
                    self.advance();
                }
                return Ok(Token::Identifier(ident));
            }

            let ident = self.read_identifier();
            let upper = ident.to_uppercase();
            
            let token = match upper.as_str() {
                "SELECT" => Token::Select,
                "FROM" => Token::From,
                "WHERE" => Token::Where,
                "INSERT" => Token::Insert,
                "INTO" => Token::Into,
                "VALUES" => Token::Values,
                "UPDATE" => Token::Update,
                "SET" => Token::Set,
                "DELETE" => Token::Delete,
                "CREATE" => Token::Create,
                "TABLE" => Token::Table,
                "DROP" => Token::Drop,
                "TRUNCATE" => Token::Truncate,
                "IF" => Token::If,
                "EXISTS" => Token::Exists,
                "NOT" => Token::Not,
                "NULL" => Token::Null,
                "AND" => Token::And,
                "OR" => Token::Or,
                "IS" => Token::Is,
                "LIKE" => Token::Like,
                "IN" => Token::In,
                "BEGIN" => Token::Begin,
                "START" => Token::Start,
                "TRANSACTION" => Token::Transaction,
                "COMMIT" => Token::Commit,
                "ROLLBACK" => Token::Rollback,
                "CHECKPOINT" => Token::Checkpoint,
                "VACUUM" => Token::Vacuum,
                "PRIMARY" => Token::Primary,
                "KEY" => Token::Key,
                "DEFAULT" => Token::Default,
                "AUTO_INCREMENT" => Token::AutoIncrement,
                "UNIQUE" => Token::Unique,
                "SHOW" => Token::Show,
                "TABLES" => Token::Tables,
                "DESCRIBE" => Token::Describe,
                "DESC" => Token::Desc,
                "LIMIT" => Token::Limit,
                "ORDER" => Token::Order,
                "BY" => Token::By,
                "ASC" => Token::Asc,
                "INDEX" => Token::Index,
                "ON" => Token::On,
                "JOIN" => Token::Join,
                "INNER" => Token::Inner,
                "LEFT" => Token::Left,
                "RIGHT" => Token::Right,
                "OUTER" => Token::Outer,
                "AS" => Token::As,
                "GROUP" => Token::Group,
                "HAVING" => Token::Having,
                "ALTER" => Token::Alter,
                "ENGINE" => Token::Engine,
                "INT" => Token::Int,
                "INTEGER" => Token::Integer,
                "BIGINT" => Token::Bigint,
                "FLOAT" => Token::Float,
                "DOUBLE" => Token::Double,
                "REAL" => Token::Real,
                "VARCHAR" => Token::Varchar,
                "TEXT" => Token::Text,
                "BOOLEAN" => Token::Boolean,
                "BOOL" => Token::Bool,
                "JSON" => Token::Json,
                "TRUE" => Token::True,
                "FALSE" => Token::False,
                _ => Token::Identifier(ident),
            };
            
            return Ok(token);
        }

        Err(MiniSqlError::Syntax(format!("Unexpected character: {}", c)))
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_basic() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE id = 1;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Identifier("users".into()));
        assert_eq!(tokens[4], Token::Where);
        assert_eq!(tokens[5], Token::Identifier("id".into()));
        assert_eq!(tokens[6], Token::Equal);
        assert_eq!(tokens[7], Token::NumberLiteral("1".into()));
        assert_eq!(tokens[8], Token::Semicolon);
        assert_eq!(tokens[9], Token::Eof);
    }

    #[test]
    fn test_lexer_quoted_identifier() {
        let mut lexer = Lexer::new("SELECT `total count` FROM `my-table`;");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[1], Token::Identifier("total count".into()));
        assert_eq!(tokens[3], Token::Identifier("my-table".into()));
    }

    #[test]
    fn test_lexer_strings() {
        // Single quotes are for string literals
        let mut lexer = Lexer::new("INSERT INTO t VALUES ('hello', 'world', 'O''Reilly');");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[5], Token::StringLiteral("hello".into()));
        assert_eq!(tokens[7], Token::StringLiteral("world".into()));
        assert_eq!(tokens[9], Token::StringLiteral("O'Reilly".into()));
    }

    #[test]
    fn test_lexer_double_quoted_identifiers() {
        // Double quotes are for identifiers (SQL standard)
        let mut lexer = Lexer::new(r#"SELECT "column name" FROM "table-name";"#);
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[1], Token::Identifier("column name".into()));
        assert_eq!(tokens[3], Token::Identifier("table-name".into()));
    }

    #[test]
    fn test_lexer_operators() {
        let mut lexer = Lexer::new("= <> != < <= > >= -> ->>");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Equal);
        assert_eq!(tokens[1], Token::NotEqual);
        assert_eq!(tokens[2], Token::NotEqual);
        assert_eq!(tokens[3], Token::LessThan);
        assert_eq!(tokens[4], Token::LessThanEq);
        assert_eq!(tokens[5], Token::GreaterThan);
        assert_eq!(tokens[6], Token::GreaterThanEq);
        assert_eq!(tokens[7], Token::Arrow);
        assert_eq!(tokens[8], Token::ArrowText);
    }
}
