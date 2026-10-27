use serde_json::{Number, Value};

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{value_to_f64, value_to_string, DataRecord};

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Num(f64),
    Bool(bool),
    Null,
    LParen,
    RParen,
    Question,
    Colon,
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Not,
    And,
    Or,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone)]
pub enum Expr {
    Ident(String),
    Literal(Value),
    UnaryNot(Box<Expr>),
    UnaryNeg(Box<Expr>),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Ternary {
        cond: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Box<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
}

fn tokenize(input: &str) -> Result<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        match c {
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '?' => {
                tokens.push(Token::Question);
                i += 1;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '%' => {
                tokens.push(Token::Percent);
                i += 1;
            }
            '&' => {
                if i + 1 < chars.len() && chars[i + 1] == '&' {
                    tokens.push(Token::And);
                    i += 2;
                } else {
                    return Err(DataGenieError::Parse("Unexpected single '&'".to_string()));
                }
            }
            '|' => {
                if i + 1 < chars.len() && chars[i + 1] == '|' {
                    tokens.push(Token::Or);
                    i += 2;
                } else {
                    return Err(DataGenieError::Parse("Unexpected single '|'".to_string()));
                }
            }
            '=' => {
                if i + 2 < chars.len() && chars[i + 1] == '=' && chars[i + 2] == '=' {
                    tokens.push(Token::Eq);
                    i += 3;
                } else if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Eq);
                    i += 2;
                } else {
                    return Err(DataGenieError::Parse("Unexpected single '='".to_string()));
                }
            }
            '!' => {
                if i + 2 < chars.len() && chars[i + 1] == '=' && chars[i + 2] == '=' {
                    tokens.push(Token::Neq);
                    i += 3;
                } else if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Neq);
                    i += 2;
                } else {
                    tokens.push(Token::Not);
                    i += 1;
                }
            }
            '<' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Lte);
                    i += 2;
                } else {
                    tokens.push(Token::Lt);
                    i += 1;
                }
            }
            '>' => {
                if i + 1 < chars.len() && chars[i + 1] == '=' {
                    tokens.push(Token::Gte);
                    i += 2;
                } else {
                    tokens.push(Token::Gt);
                    i += 1;
                }
            }
            '\'' | '"' => {
                let quote = c;
                i += 1;
                let mut s = String::new();
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        i += 1;
                        s.push(chars[i]);
                    } else if chars[i] == quote {
                        closed = true;
                        i += 1;
                        break;
                    } else {
                        s.push(chars[i]);
                    }
                    i += 1;
                }
                if !closed {
                    return Err(DataGenieError::Parse(
                        "Unterminated string literal".to_string(),
                    ));
                }
                tokens.push(Token::Str(s));
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let n = num_str.parse::<f64>().map_err(|e| {
                    DataGenieError::Parse(format!("Invalid number '{}': {}", num_str, e))
                })?;
                tokens.push(Token::Num(n));
            }
            _ if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric()
                        || chars[i] == '_'
                        || chars[i] == '.'
                        || chars[i] == '$')
                {
                    i += 1;
                }
                let ident: String = chars[start..i].iter().collect();
                match ident.as_str() {
                    "true" => tokens.push(Token::Bool(true)),
                    "false" => tokens.push(Token::Bool(false)),
                    "null" | "undefined" => tokens.push(Token::Null),
                    "AND" | "and" => tokens.push(Token::And),
                    "OR" | "or" => tokens.push(Token::Or),
                    "NOT" | "not" => tokens.push(Token::Not),
                    _ => {
                        let cleaned = ident.strip_prefix("record.").unwrap_or(&ident).to_string();
                        tokens.push(Token::Ident(cleaned));
                    }
                }
            }
            _ => {
                return Err(DataGenieError::Parse(format!(
                    "Unexpected character '{}' in expression",
                    c
                )));
            }
        }
    }

    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        if self.pos < self.tokens.len() {
            let tok = self.tokens[self.pos].clone();
            self.pos += 1;
            Some(tok)
        } else {
            None
        }
    }

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_ternary()
    }

    fn parse_ternary(&mut self) -> Result<Expr> {
        let cond = self.parse_or()?;
        if matches!(self.peek(), Some(Token::Question)) {
            self.next();
            let then_branch = self.parse_ternary()?;
            if !matches!(self.next(), Some(Token::Colon)) {
                return Err(DataGenieError::Parse(
                    "Expected ':' in ternary expression".to_string(),
                ));
            }
            let else_branch = self.parse_ternary()?;
            Ok(Expr::Ternary {
                cond: Box::new(cond),
                then_branch: Box::new(then_branch),
                else_branch: Box::new(else_branch),
            })
        } else {
            Ok(cond)
        }
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while matches!(self.peek(), Some(Token::Or)) {
            self.next();
            let right = self.parse_and()?;
            left = Expr::Binary {
                op: BinOp::Or,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_equality()?;
        while matches!(self.peek(), Some(Token::And)) {
            self.next();
            let right = self.parse_equality()?;
            left = Expr::Binary {
                op: BinOp::And,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expr> {
        let mut left = self.parse_comparison()?;
        while let Some(tok) = self.peek() {
            let op = match tok {
                Token::Eq => BinOp::Eq,
                Token::Neq => BinOp::Neq,
                _ => break,
            };
            self.next();
            let right = self.parse_comparison()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let mut left = self.parse_additive()?;
        while let Some(tok) = self.peek() {
            let op = match tok {
                Token::Lt => BinOp::Lt,
                Token::Lte => BinOp::Lte,
                Token::Gt => BinOp::Gt,
                Token::Gte => BinOp::Gte,
                _ => break,
            };
            self.next();
            let right = self.parse_additive()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative()?;
        while let Some(tok) = self.peek() {
            let op = match tok {
                Token::Plus => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.next();
            let right = self.parse_multiplicative()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        while let Some(tok) = self.peek() {
            let op = match tok {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Rem,
                _ => break,
            };
            self.next();
            let right = self.parse_unary()?;
            left = Expr::Binary {
                op,
                left: Box::new(left),
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.peek() {
            Some(Token::Not) => {
                self.next();
                Ok(Expr::UnaryNot(Box::new(self.parse_unary()?)))
            }
            Some(Token::Minus) => {
                self.next();
                Ok(Expr::UnaryNeg(Box::new(self.parse_unary()?)))
            }
            Some(Token::Plus) => {
                self.next();
                self.parse_unary()
            }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.next() {
            Some(Token::Ident(name)) => Ok(Expr::Ident(name)),
            Some(Token::Str(s)) => Ok(Expr::Literal(Value::String(s))),
            Some(Token::Num(n)) => Ok(Expr::Literal(f64_to_json_value(n))),
            Some(Token::Bool(b)) => Ok(Expr::Literal(Value::Bool(b))),
            Some(Token::Null) => Ok(Expr::Literal(Value::Null)),
            Some(Token::LParen) => {
                let expr = self.parse_expr()?;
                if !matches!(self.next(), Some(Token::RParen)) {
                    return Err(DataGenieError::Parse("Expected closing ')'".to_string()));
                }
                Ok(expr)
            }
            other => Err(DataGenieError::Parse(format!(
                "Unexpected token in expression: {:?}",
                other
            ))),
        }
    }
}

fn f64_to_json_value(n: f64) -> Value {
    if n.fract() == 0.0 && n >= (i64::MIN as f64) && n <= (i64::MAX as f64) {
        Value::Number(Number::from(n as i64))
    } else {
        Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

pub fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0 && !f.is_nan()).unwrap_or(false),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if a == b {
        return true;
    }
    match (a, b) {
        (Value::Number(_), Value::String(_)) | (Value::String(_), Value::Number(_)) => {
            if let (Some(na), Some(nb)) = (value_to_f64(a), value_to_f64(b)) {
                (na - nb).abs() < f64::EPSILON
            } else {
                false
            }
        }
        (Value::Bool(ba), Value::String(sb)) | (Value::String(sb), Value::Bool(ba)) => {
            (sb == "true" && *ba) || (sb == "false" && !*ba)
        }
        _ => false,
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(na), Some(nb)) = (value_to_f64(a), value_to_f64(b)) {
        return na.partial_cmp(&nb);
    }
    if let (Value::String(sa), Value::String(sb)) = (a, b) {
        return sa.partial_cmp(sb);
    }
    None
}

impl Expr {
    pub fn parse(input: &str) -> Result<Self> {
        let tokens = tokenize(input)?;
        if tokens.is_empty() {
            return Err(DataGenieError::Parse("Empty expression".to_string()));
        }
        let mut parser = Parser::new(tokens);
        let expr = parser.parse_expr()?;
        if parser.pos < parser.tokens.len() {
            return Err(DataGenieError::Parse(format!(
                "Unexpected trailing tokens starting at {:?}",
                parser.tokens[parser.pos]
            )));
        }
        Ok(expr)
    }

    pub fn eval(&self, record: &DataRecord) -> Result<Value> {
        match self {
            Expr::Ident(name) => {
                if let Some(v) = record.get(name) {
                    Ok(v.clone())
                } else {
                    // Support nested dot lookup if present
                    if name.contains('.') {
                        let mut curr: Option<&Value> = None;
                        for (idx, part) in name.split('.').enumerate() {
                            if idx == 0 {
                                curr = record.get(part);
                            } else if let Some(Value::Object(map)) = curr {
                                curr = map.get(part);
                            } else {
                                curr = None;
                                break;
                            }
                        }
                        Ok(curr.cloned().unwrap_or(Value::Null))
                    } else {
                        Ok(Value::Null)
                    }
                }
            }
            Expr::Literal(v) => Ok(v.clone()),
            Expr::UnaryNot(inner) => {
                let v = inner.eval(record)?;
                Ok(Value::Bool(!is_truthy(&v)))
            }
            Expr::UnaryNeg(inner) => {
                let v = inner.eval(record)?;
                let n = value_to_f64(&v).ok_or_else(|| {
                    DataGenieError::Parse(format!("Cannot negate non-numeric value: {:?}", v))
                })?;
                Ok(f64_to_json_value(-n))
            }
            Expr::Ternary {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = cond.eval(record)?;
                if is_truthy(&c) {
                    then_branch.eval(record)
                } else {
                    else_branch.eval(record)
                }
            }
            Expr::Binary { op, left, right } => match op {
                BinOp::And => {
                    let l = left.eval(record)?;
                    if !is_truthy(&l) {
                        Ok(Value::Bool(false))
                    } else {
                        let r = right.eval(record)?;
                        Ok(Value::Bool(is_truthy(&r)))
                    }
                }
                BinOp::Or => {
                    let l = left.eval(record)?;
                    if is_truthy(&l) {
                        Ok(Value::Bool(true))
                    } else {
                        let r = right.eval(record)?;
                        Ok(Value::Bool(is_truthy(&r)))
                    }
                }
                BinOp::Eq => {
                    let l = left.eval(record)?;
                    let r = right.eval(record)?;
                    Ok(Value::Bool(values_equal(&l, &r)))
                }
                BinOp::Neq => {
                    let l = left.eval(record)?;
                    let r = right.eval(record)?;
                    Ok(Value::Bool(!values_equal(&l, &r)))
                }
                BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                    let l = left.eval(record)?;
                    let r = right.eval(record)?;
                    let ord = compare_values(&l, &r);
                    let res = match (op, ord) {
                        (BinOp::Lt, Some(std::cmp::Ordering::Less)) => true,
                        (
                            BinOp::Lte,
                            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal),
                        ) => true,
                        (BinOp::Gt, Some(std::cmp::Ordering::Greater)) => true,
                        (
                            BinOp::Gte,
                            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal),
                        ) => true,
                        _ => false,
                    };
                    Ok(Value::Bool(res))
                }
                BinOp::Add => {
                    let l = left.eval(record)?;
                    let r = right.eval(record)?;
                    // If both are numbers, add numerically; if either is string, concatenate
                    if let (Value::Number(_), Value::Number(_)) = (&l, &r) {
                        let na = value_to_f64(&l).unwrap_or(0.0);
                        let nb = value_to_f64(&r).unwrap_or(0.0);
                        Ok(f64_to_json_value(na + nb))
                    } else if matches!(l, Value::String(_)) || matches!(r, Value::String(_)) {
                        // However, if both strings look strictly numeric and neither is a literal string?
                        // In JS, string + anything is string concatenation.
                        Ok(Value::String(format!(
                            "{}{}",
                            value_to_string(&l),
                            value_to_string(&r)
                        )))
                    } else if let (Some(na), Some(nb)) = (value_to_f64(&l), value_to_f64(&r)) {
                        Ok(f64_to_json_value(na + nb))
                    } else {
                        Ok(Value::String(format!(
                            "{}{}",
                            value_to_string(&l),
                            value_to_string(&r)
                        )))
                    }
                }
                BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Rem => {
                    let l = left.eval(record)?;
                    let r = right.eval(record)?;
                    let na = value_to_f64(&l).ok_or_else(|| {
                        DataGenieError::Parse(format!("Left operand is not numeric: {:?}", l))
                    })?;
                    let nb = value_to_f64(&r).ok_or_else(|| {
                        DataGenieError::Parse(format!("Right operand is not numeric: {:?}", r))
                    })?;
                    let res = match op {
                        BinOp::Sub => na - nb,
                        BinOp::Mul => na * nb,
                        BinOp::Div => na / nb,
                        BinOp::Rem => na % nb,
                        _ => unreachable!(),
                    };
                    Ok(f64_to_json_value(res))
                }
            },
        }
    }
}
