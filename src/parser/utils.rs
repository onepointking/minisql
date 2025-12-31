use crate::error::MiniSqlError;
use crate::lexer::Token;
use super::Parser;

/// Context for parser error messages
#[derive(Debug, Clone)]
pub(crate) struct ParserContext {
    /// What was being parsed (e.g., "SELECT statement", "WHERE clause", "column definition")
    pub(crate) context: String,
}

impl ParserContext {
    pub(crate) fn new(context: &str) -> Self {
        Self {
            context: context.to_string(),
        }
    }
}

impl Parser {
    /// Push a parsing context onto the stack
    pub(super) fn push_context(&mut self, context: &str) {
        self.context_stack.push(ParserContext::new(context));
    }

    /// Pop a parsing context from the stack
    pub(super) fn pop_context(&mut self) {
        self.context_stack.pop();
    }

    /// Build an error message with context
    pub(super) fn error_with_context(&self, msg: String) -> MiniSqlError {
        // Build a short human-friendly token context: current, previous and next tokens
        fn human_token(t: &Token) -> String {
            match t {
                Token::Identifier(s) => format!("identifier '{}'", s),
                Token::StringLiteral(s) => format!("string literal '{}'", s),
                Token::NumberLiteral(n) => format!("number '{}'", n),
                Token::Star => "'*'".into(),
                Token::Comma => "','".into(),
                Token::LeftParen => "'('".into(),
                Token::RightParen => "')'".into(),
                Token::Semicolon => "';'".into(),
                Token::Equal => "'='".into(),
                Token::NotEqual => "'<>' or '!='".into(),
                Token::LessThan => "'<'".into(),
                Token::LessThanEq => "'<='".into(),
                Token::GreaterThan => "'>'".into(),
                Token::GreaterThanEq => "'>='".into(),
                Token::Plus => "'+'".into(),
                Token::Minus => "'-'".into(),
                Token::Slash => "'/'".into(),
                Token::Arrow => "'->'".into(),
                Token::ArrowText => "'->>'".into(),
                Token::Dot => "'.'".into(),
                Token::Placeholder => "'?'".into(),
                Token::Eof => "end of input".into(),
                other => format!("{:?}", other),
            }
        }

        let found = human_token(self.peek());
        let prev = self.tokens.get(self.pos.saturating_sub(1)).map(|t| human_token(t)).unwrap_or_else(|| "start of input".into());
        let next = self.tokens.get(self.pos + 1).map(|t| human_token(t)).unwrap_or_else(|| "end of input".into());

        let context_info = if !self.context_stack.is_empty() {
            let contexts: Vec<_> = self.context_stack.iter()
                .map(|c| c.context.as_str())
                .collect();
            format!(" while parsing {}", contexts.join(" > "))
        } else {
            String::new()
        };

        MiniSqlError::Syntax(format!("{} (found {} at token position {}; prev: {}; next: {}){}",
            msg, found, self.pos, prev, next, context_info
        ))
    }

    /// Helper to produce a brief, human readable token description for error messages
    pub(super) fn token_brief(&self, t: &Token) -> String {
        match t {
            Token::Identifier(s) => format!("identifier '{}'", s),
            Token::StringLiteral(s) => format!("string literal '{}'", s),
            Token::NumberLiteral(n) => format!("number '{}'", n),
            Token::Star => "'*'".into(),
            Token::Dot => "'.'".into(),
            Token::Eof => "end of input".into(),
            other => format!("{:?}", other),
        }
    }
}
