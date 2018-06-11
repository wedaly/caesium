pub enum Expression {
    FunctionCall(String, Vec<Box<Expression>>),
    StringLiteral(String),
    FloatLiteral(f64),
}
