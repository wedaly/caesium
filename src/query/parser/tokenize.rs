use std::num::{ParseFloatError, ParseIntError};

#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Symbol(String),
    Int(u64),
    Float(f64),
    LeftParen,
    RightParen,
    Comma,
}

#[derive(Debug)]
pub enum TokenizeError {
    UnexpectedChar(char),
    ParseIntError(ParseIntError),
    ParseFloatError(ParseFloatError),
}

impl From<ParseIntError> for TokenizeError {
    fn from(err: ParseIntError) -> TokenizeError {
        TokenizeError::ParseIntError(err)
    }
}

impl From<ParseFloatError> for TokenizeError {
    fn from(err: ParseFloatError) -> TokenizeError {
        TokenizeError::ParseFloatError(err)
    }
}

pub fn tokenize(s: &str) -> Result<Vec<Token>, TokenizeError> {
    let mut i = 0;
    let mut tokens = Vec::<Token>::new();
    while i < s.len() {
        let next_char = s[i..i + 1]
            .chars()
            .next()
            .expect("Could not retrieve next char");
        let slice = &s[i..];
        if next_char.is_ascii_whitespace() {
            i += 1;
        } else if next_char == ',' {
            i += tokenize_comma(&mut tokens);
        } else if next_char == '(' {
            i += tokenize_left_paren(&mut tokens);
        } else if next_char == ')' {
            i += tokenize_right_paren(&mut tokens);
        } else if next_char.is_ascii_digit() {
            i += tokenize_numeric(slice, &mut tokens)?;
        } else if next_char.is_ascii_alphabetic() {
            i += tokenize_symbol(slice, &mut tokens)?;
        } else {
            return Err(TokenizeError::UnexpectedChar(next_char));
        }
    }
    Ok(tokens)
}

fn tokenize_comma(tokens: &mut Vec<Token>) -> usize {
    tokens.push(Token::Comma);
    1
}

fn tokenize_left_paren(tokens: &mut Vec<Token>) -> usize {
    tokens.push(Token::LeftParen);
    1
}

fn tokenize_right_paren(tokens: &mut Vec<Token>) -> usize {
    tokens.push(Token::RightParen);
    1
}

fn tokenize_numeric(s: &str, tokens: &mut Vec<Token>) -> Result<usize, TokenizeError> {
    let mut i = 0;
    let mut found_decimal = false;
    for c in s.chars() {
        if c.is_ascii_digit() {
            i += 1;
        } else if c == '.' && !found_decimal {
            found_decimal = true;
            i += 1;
        } else if is_separator(c) {
            break;
        } else {
            return Err(TokenizeError::UnexpectedChar(c));
        }
    }
    debug_assert!(i > 0);
    if found_decimal {
        let value: f64 = s[..i].parse()?;
        tokens.push(Token::Float(value));
    } else {
        let value: u64 = s[..i].parse()?;
        tokens.push(Token::Int(value));
    }
    Ok(i)
}

fn tokenize_symbol(s: &str, tokens: &mut Vec<Token>) -> Result<usize, TokenizeError> {
    let mut i = 0;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || c == '.' || c == '-' || c == '_' {
            i += 1;
        } else if is_separator(c) {
            break;
        } else {
            return Err(TokenizeError::UnexpectedChar(c));
        }
    }
    debug_assert!(i > 0);
    tokens.push(Token::Symbol(s[..i].to_string()));
    Ok(i)
}

fn is_separator(c: char) -> bool {
    c.is_ascii_whitespace() || c == ',' || c == ')' || c == '('
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_tokenizes_symbols() {
        assert_tokenize(
            &"hello world",
            vec![
                Token::Symbol("hello".to_string()),
                Token::Symbol("world".to_string()),
            ],
        );
    }

    #[test]
    fn it_tokenizes_symbols_with_numbers() {
        assert_tokenize(&"server1234", vec![Token::Symbol("server1234".to_string())]);
    }

    #[test]
    fn it_tokenizes_symbols_with_periods() {
        assert_tokenize(
            &"region.us.server.abcd",
            vec![Token::Symbol("region.us.server.abcd".to_string())],
        );
    }

    #[test]
    fn it_tokenizes_symbols_with_hyphens() {
        assert_tokenize(&"us-west", vec![Token::Symbol("us-west".to_string())]);
    }

    #[test]
    fn it_tokenizes_symbols_with_underscores() {
        assert_tokenize(&"env_prod", vec![Token::Symbol("env_prod".to_string())]);
    }

    #[test]
    fn it_tokenizes_symbols_with_capitals() {
        assert_tokenize(&"FooBar", vec![Token::Symbol("FooBar".to_string())]);
    }

    #[test]
    fn it_tokenizes_floats() {
        assert_tokenize(&"10.2345", vec![Token::Float(10.2345f64)]);
    }

    #[test]
    fn it_tokenizes_parens() {
        assert_tokenize(
            &"foo(bar)",
            vec![
                Token::Symbol("foo".to_string()),
                Token::LeftParen,
                Token::Symbol("bar".to_string()),
                Token::RightParen,
            ],
        );
    }

    #[test]
    fn it_tokenizes_commas() {
        assert_tokenize(
            &"foo, bar, baz",
            vec![
                Token::Symbol("foo".to_string()),
                Token::Comma,
                Token::Symbol("bar".to_string()),
                Token::Comma,
                Token::Symbol("baz".to_string()),
            ],
        );
    }

    #[test]
    fn it_errors_if_float_with_too_many_decimal_points() {
        assert_error(&"123.45.67");
    }

    #[test]
    fn it_errors_if_float_has_invalid_chars() {
        assert_error(&"123abc");
    }

    #[test]
    fn it_errors_if_symbol_has_non_alphanumerics() {
        assert_error(&"foo%bar");
    }

    fn assert_tokenize(input: &str, expected: Vec<Token>) {
        let result = tokenize(input).expect("Could not tokenize string");
        assert_eq!(result, expected);
    }

    fn assert_error(input: &str) {
        match tokenize(input) {
            Ok(_) => panic!("Expected error"),
            Err(_) => {}
        }
    }
}
