pub enum Expression {
    FunctionCall(String, Vec<Box<Expression>>),
    StringLiteral(String),
    IntLiteral(u64),
    FloatLiteral(f64),
}
