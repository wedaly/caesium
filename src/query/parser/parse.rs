use query::parser::ast::Expression;
use query::parser::tokenize::{tokenize, Token, TokenizeError};

#[derive(Debug)]
pub enum ParseError {
    TokenizeError(TokenizeError),
    UnexpectedToken,
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
        Err(ParseError::UnexpectedToken)
    } else {
        Ok(expr)
    }
}

type ParseResult<T> = Result<(usize, T), ParseError>;

fn parse_expr(tokens: &[Token]) -> ParseResult<Box<Expression>> {
    match tokens.first() {
        Some(Token::Float(f)) => Ok((1, Box::new(Expression::FloatLiteral(*f)))),
        Some(Token::Symbol(s)) => match tokens.get(1) {
            Some(Token::LeftParen) => parse_function_call(tokens),
            _ => Ok((1, Box::new(Expression::StringLiteral(s.to_string())))),
        },
        Some(_) => Err(ParseError::UnexpectedToken),
        None => Err(ParseError::UnexpectedEnd),
    }
}

fn parse_function_call(tokens: &[Token]) -> ParseResult<Box<Expression>> {
    let call_toks = (tokens.first(), tokens.get(1), tokens.last());
    if let (Some(Token::Symbol(name)), Some(Token::LeftParen), Some(Token::RightParen)) = call_toks
    {
        let (c, args) = parse_arg_list(&tokens[2..tokens.len() - 1])?;
        let func = Box::new(Expression::FunctionCall(name.to_string(), args));
        Ok((c + 3, func))
    } else if let (Some(_), Some(_), Some(_)) = call_toks {
        Err(ParseError::UnexpectedToken)
    } else {
        Err(ParseError::UnexpectedEnd)
    }
}

fn parse_arg_list(tokens: &[Token]) -> ParseResult<Vec<Box<Expression>>> {
    let mut args = Vec::new();
    let mut c = 0;
    while c < tokens.len() {
        let (consumed, arg) = parse_expr(&tokens[c..])?;
        c += consumed;
        args.push(arg);

        if let Some(Token::Comma) = tokens.get(c) {
            if c < tokens.len() - 1 {
                c += 1;
                continue;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    if c < tokens.len() {
        Err(ParseError::UnexpectedToken)
    } else {
        Ok((c, args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_parses_string_literal() {
        let ast = parse(&"foo").expect("Could not parse input string");
        match *ast {
            Expression::StringLiteral(s) => assert_eq!(s, "foo"),
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
        let ast = parse(&"foo(bar, 123.45)").expect("Could not parse input string");
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
        assert_rejects("foo(x,)");
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
