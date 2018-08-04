use query::parser::ast::Expression;
use query::parser::tokenize::{tokenize, Token, TokenizeError};

#[derive(Debug)]
pub enum ParseError {
    TokenizeError(TokenizeError),
    UnexpectedToken(Token),
    UnexpectedEnd,
}

impl From<TokenizeError> for ParseError {
    fn from(err: TokenizeError) -> ParseError {
        ParseError::TokenizeError(err)
    }
}

pub fn parse(s: &str) -> Result<Box<Expression>, ParseError> {
    let tokens = tokenize(s)?;
    let (c, expr) = parse_expr(&tokens)?;
    if c < tokens.len() {
        let t = tokens[c].clone();
        Err(ParseError::UnexpectedToken(t))
    } else {
        Ok(expr)
    }
}

type ParseResult<T> = Result<(usize, T), ParseError>;

fn parse_expr(tokens: &[Token]) -> ParseResult<Box<Expression>> {
    match tokens.first() {
        Some(Token::Int(i)) => Ok((1, Box::new(Expression::IntLiteral(*i)))),
        Some(Token::Float(f)) => Ok((1, Box::new(Expression::FloatLiteral(*f)))),
        Some(Token::String(s)) => Ok((1, Box::new(Expression::StringLiteral(s.clone())))),
        Some(Token::Symbol(_)) => match tokens.get(1) {
            Some(Token::LeftParen) => parse_function_call(tokens),
            Some(t) => Err(ParseError::UnexpectedToken(t.clone())),
            None => Err(ParseError::UnexpectedEnd),
        },
        Some(t) => Err(ParseError::UnexpectedToken(t.clone())),
        None => Err(ParseError::UnexpectedEnd),
    }
}

fn parse_function_call(tokens: &[Token]) -> ParseResult<Box<Expression>> {
    let call_toks = (tokens.first(), tokens.get(1), tokens.last());
    match call_toks {
        (Some(Token::Symbol(name)), Some(Token::LeftParen), Some(Token::RightParen)) => {
            let (c, args) = parse_arg_list(&tokens[2..])?;
            let func = Box::new(Expression::FunctionCall(name.to_string(), args));
            Ok((c + 3, func))
        }
        (Some(t), Some(Token::LeftParen), Some(Token::RightParen)) => {
            Err(ParseError::UnexpectedToken(t.clone()))
        }
        (Some(Token::Symbol(_)), Some(t), Some(Token::RightParen)) => {
            Err(ParseError::UnexpectedToken(t.clone()))
        }
        (Some(Token::Symbol(_)), Some(Token::LeftParen), Some(t)) => {
            Err(ParseError::UnexpectedToken(t.clone()))
        }
        _ => Err(ParseError::UnexpectedEnd),
    }
}

fn parse_arg_list(tokens: &[Token]) -> ParseResult<Vec<Box<Expression>>> {
    let mut args = Vec::new();
    let mut c = 0;
    loop {
        match tokens.get(c) {
            Some(Token::RightParen) => {
                return Ok((c, args));
            }
            None => {
                return Err(ParseError::UnexpectedEnd);
            }
            _ => {}
        }

        let (consumed, arg) = parse_expr(&tokens[c..])?;
        c += consumed;
        args.push(arg);

        match tokens.get(c) {
            Some(Token::Comma) => {
                c += 1;
            }
            Some(Token::RightParen) => {} // Handled next iteration
            Some(t) => {
                return Err(ParseError::UnexpectedToken(t.clone()));
            }
            None => {} // Handled next iteration
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_string_literal() {
        let ast = parse(&"\"foo\"").expect("Could not parse input string");
        match *ast {
            Expression::StringLiteral(s) => assert_eq!(s, "foo"),
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_int_literal() {
        let ast = parse(&"23").expect("Could not parse input string");
        match *ast {
            Expression::IntLiteral(i) => assert_eq!(i, 23),
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_float_literal() {
        let ast = parse(&"23.45").expect("Could not parse input string");
        match *ast {
            Expression::FloatLiteral(f) => assert_eq!(f, 23.45f64),
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_function_call_no_args() {
        let ast = parse(&"foo()").expect("Could not parse input string");
        match { *ast } {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "foo");
                assert_eq!(args.len(), 0);
            }
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_function_call_literal_args() {
        let ast = parse(&"foo(\"bar\", 123.45)").expect("Could not parse input string");
        match { *ast } {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "foo");
                assert_eq!(args.len(), 2);
                let first_arg = args.first().unwrap();
                match **first_arg {
                    Expression::StringLiteral(ref s) => assert_eq!(s, "bar"),
                    _ => panic!("Expected string for first arg"),
                }

                let second_arg = args.get(1).unwrap();
                match **second_arg {
                    Expression::FloatLiteral(f) => assert_eq!(f, 123.45f64),
                    _ => panic!("Expected float for second arg"),
                }
            }
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_nested_function_calls() {
        let ast = parse(&"foo(bar())").expect("Could not parse input string");
        match { *ast } {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "foo");
                assert_eq!(args.len(), 1);
                let first_arg = args.first().unwrap();
                match **first_arg {
                    Expression::FunctionCall(ref name, ref args) => {
                        assert_eq!(name, "bar");
                        assert_eq!(args.len(), 0);
                    }
                    _ => panic!("Expected function call for second arg"),
                }
            }
            _ => panic!("Unexpected node type"),
        }
    }

    #[test]
    fn it_parses_two_function_calls_args() {
        let ast = parse(&"f(g(1), h())").expect("Could not parse input string");
        match { *ast } {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "f");
                assert_eq!(args.len(), 2);

                let first_arg = args.first().unwrap();
                match **first_arg {
                    Expression::FunctionCall(ref name, ref args) => {
                        assert_eq!(name, "g");
                        assert_eq!(args.len(), 1);
                    }
                    _ => panic!("Expected function call for first arg"),
                }

                let second_arg = args.get(1).unwrap();
                match **second_arg {
                    Expression::FunctionCall(ref name, ref args) => {
                        assert_eq!(name, "h");
                        assert_eq!(args.len(), 0);
                    }
                    _ => panic!("Expected function call for second arg"),
                }
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn it_allows_trailing_comma_in_function_call_args() {
        let ast = parse(&"f(1,2,)").expect("Could not parse input string");
        match { *ast } {
            Expression::FunctionCall(name, args) => {
                assert_eq!(name, "f");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("Expected function call"),
        }
    }

    #[test]
    fn it_rejects_empty_input() {
        assert_rejects("");
        assert_rejects(" ");
        assert_rejects("\n");
        assert_rejects("\t");
    }

    #[test]
    fn it_rejects_invalid_function_calls() {
        assert_rejects("(");
        assert_rejects(")");
        assert_rejects(",");
        assert_rejects("foo(");
        assert_rejects("foo)");
        assert_rejects("123()");
        assert_rejects("123ab()");
        assert_rejects("foo(,x)");
        assert_rejects("foo(123x)");
        assert_rejects("foo(,");
    }

    fn assert_rejects(input: &str) {
        if let Ok(_) = parse(input) {
            panic!("Expected parse error");
        }
    }
}
