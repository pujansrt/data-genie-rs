use serde_json::{Number, Value};

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, value_to_f64, value_to_string};

const MAX_EXPR_DEPTH: usize = 64;

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
                        let escaped = match chars[i] {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            '0' => '\0',
                            '\\' => '\\',
                            '\'' => '\'',
                            '"' => '"',
                            other => other,
                        };
                        s.push(escaped);
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
                    return Err(DataGenieError::Parse("Unterminated string literal".to_string()));
                }
                tokens.push(Token::Str(s));
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                let mut seen_dot = false;
                while i < chars.len() {
                    let ch = chars[i];
                    if ch.is_ascii_digit() {
                        i += 1;
                    } else if ch == '.' && !seen_dot {
                        seen_dot = true;
                        i += 1;
                    } else {
                        break;
                    }
                }
                if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                    let exp_pos = i;
                    i += 1;
                    if i < chars.len() && (chars[i] == '+' || chars[i] == '-') {
                        i += 1;
                    }
                    let digits_start = i;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                    if i == digits_start {
                        // Not a valid exponent; rollback so trailing identifier/operator fails cleanly
                        i = exp_pos;
                    }
                }
                let num_str: String = chars[start..i].iter().collect();
                let n = num_str
                    .parse::<f64>()
                    .map_err(|e| DataGenieError::Parse(format!("Invalid number '{}': {}", num_str, e)))?;
                if !n.is_finite() {
                    return Err(DataGenieError::Parse(format!("Number '{}' overflows finite float range", num_str)));
                }
                tokens.push(Token::Num(n));
            }
            _ if c.is_ascii_alphabetic() || c == '_' || c == '$' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.' || chars[i] == '$') {
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
                return Err(DataGenieError::Parse(format!("Unexpected character '{}' in expression", c)));
            }
        }
    }

    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0, depth: 0 }
    }

    fn enter_recursion(&mut self) -> Result<()> {
        self.depth += 1;
        if self.depth > MAX_EXPR_DEPTH {
            return Err(DataGenieError::Parse(format!(
                "Expression exceeds maximum nesting depth of {}",
                MAX_EXPR_DEPTH
            )));
        }
        Ok(())
    }

    fn leave_recursion(&mut self) {
        self.depth = self.depth.saturating_sub(1);
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
        self.enter_recursion()?;
        let res = (|| {
            let cond = self.parse_or()?;
            if matches!(self.peek(), Some(Token::Question)) {
                self.next();
                let then_branch = self.parse_ternary()?;
                if !matches!(self.next(), Some(Token::Colon)) {
                    return Err(DataGenieError::Parse("Expected ':' in ternary expression".to_string()));
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
        })();
        self.leave_recursion();
        res
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
        self.enter_recursion()?;
        let res = match self.peek() {
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
        };
        self.leave_recursion();
        res
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
            other => Err(DataGenieError::Parse(format!("Unexpected token in expression: {:?}", other))),
        }
    }
}

fn f64_to_json_value(n: f64) -> Value {
    if n.is_finite() && n.fract() == 0.0 && n >= (i64::MIN as f64) && n < (i64::MAX as f64) {
        Value::Number(Number::from(n as i64))
    } else {
        Number::from_f64(n).map(Value::Number).unwrap_or(Value::Null)
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
        (Value::Number(_), Value::Number(_))
        | (Value::Number(_), Value::String(_))
        | (Value::String(_), Value::Number(_)) => {
            if let (Some(na), Some(nb)) = (value_to_f64(a), value_to_f64(b)) {
                na == nb
            } else {
                false
            }
        }
        (Value::Bool(ba), Value::String(sb)) | (Value::String(sb), Value::Bool(ba)) => (sb == "true" && *ba) || (sb == "false" && !*ba),
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
        self.eval_with_depth(record, 0)
    }

    fn eval_with_depth(&self, record: &DataRecord, depth: usize) -> Result<Value> {
        if depth > MAX_EXPR_DEPTH {
            return Err(DataGenieError::Parse(format!(
                "Expression evaluation exceeds maximum depth of {}",
                MAX_EXPR_DEPTH
            )));
        }
        let next_depth = depth + 1;

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
                let v = inner.eval_with_depth(record, next_depth)?;
                Ok(Value::Bool(!is_truthy(&v)))
            }
            Expr::UnaryNeg(inner) => {
                let v = inner.eval_with_depth(record, next_depth)?;
                let n = value_to_f64(&v).ok_or_else(|| DataGenieError::Parse(format!("Cannot negate non-numeric value: {:?}", v)))?;
                Ok(f64_to_json_value(-n))
            }
            Expr::Ternary {
                cond,
                then_branch,
                else_branch,
            } => {
                let c = cond.eval_with_depth(record, next_depth)?;
                if is_truthy(&c) {
                    then_branch.eval_with_depth(record, next_depth)
                } else {
                    else_branch.eval_with_depth(record, next_depth)
                }
            }
            Expr::Binary { op, left, right } => match op {
                BinOp::And => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    if !is_truthy(&l) {
                        Ok(Value::Bool(false))
                    } else {
                        let r = right.eval_with_depth(record, next_depth)?;
                        Ok(Value::Bool(is_truthy(&r)))
                    }
                }
                BinOp::Or => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    if is_truthy(&l) {
                        Ok(Value::Bool(true))
                    } else {
                        let r = right.eval_with_depth(record, next_depth)?;
                        Ok(Value::Bool(is_truthy(&r)))
                    }
                }
                BinOp::Eq => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    let r = right.eval_with_depth(record, next_depth)?;
                    Ok(Value::Bool(values_equal(&l, &r)))
                }
                BinOp::Neq => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    let r = right.eval_with_depth(record, next_depth)?;
                    Ok(Value::Bool(!values_equal(&l, &r)))
                }
                BinOp::Lt | BinOp::Lte | BinOp::Gt | BinOp::Gte => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    let r = right.eval_with_depth(record, next_depth)?;
                    let ord = compare_values(&l, &r);
                    let res = matches!(
                        (op, ord),
                        (BinOp::Lt, Some(std::cmp::Ordering::Less))
                            | (BinOp::Lte, Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
                            | (BinOp::Gt, Some(std::cmp::Ordering::Greater))
                            | (BinOp::Gte, Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal))
                    );
                    Ok(Value::Bool(res))
                }
                BinOp::Add => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    let r = right.eval_with_depth(record, next_depth)?;
                    if let (Value::Number(_), Value::Number(_)) = (&l, &r) {
                        let na = value_to_f64(&l).unwrap_or(0.0);
                        let nb = value_to_f64(&r).unwrap_or(0.0);
                        let sum = na + nb;
                        if !sum.is_finite() {
                            return Err(DataGenieError::Parse("Numeric overflow in addition".to_string()));
                        }
                        Ok(f64_to_json_value(sum))
                    } else {
                        Ok(Value::String(format!("{}{}", value_to_string(&l), value_to_string(&r))))
                    }
                }
                BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Rem => {
                    let l = left.eval_with_depth(record, next_depth)?;
                    let r = right.eval_with_depth(record, next_depth)?;
                    let na = value_to_f64(&l).ok_or_else(|| DataGenieError::Parse(format!("Left operand is not numeric: {:?}", l)))?;
                    let nb = value_to_f64(&r).ok_or_else(|| DataGenieError::Parse(format!("Right operand is not numeric: {:?}", r)))?;
                    if matches!(op, BinOp::Div | BinOp::Rem) && nb == 0.0 {
                        return Err(DataGenieError::Parse("Division by zero in expression".to_string()));
                    }
                    let res = match op {
                        BinOp::Sub => na - nb,
                        BinOp::Mul => na * nb,
                        BinOp::Div => na / nb,
                        BinOp::Rem => na % nb,
                        _ => unreachable!(),
                    };
                    if !res.is_finite() {
                        return Err(DataGenieError::Parse("Non-finite arithmetic result in expression".to_string()));
                    }
                    Ok(f64_to_json_value(res))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record;
    use serde_json::json;

    #[test]
    fn test_expr_numeric_equality_scientific_notation_and_escapes() {
        let rec = record! {
            "int_val" => 10,
            "float_val" => 10.0,
            "zero" => 0.0,
            "tiny_str" => "1e-16",
            "nan_str" => "NaN",
            "inf_str" => "Infinity"
        };

        // Int vs float equality
        assert_eq!(Expr::parse("int_val == float_val").unwrap().eval(&rec).unwrap(), json!(true));
        // 0.0 != "1e-16"
        assert_eq!(Expr::parse("zero == tiny_str").unwrap().eval(&rec).unwrap(), json!(false));
        // "NaN" and "Infinity" strings are not treated as numbers
        assert!(value_to_f64(&json!("NaN")).is_none());
        assert!(value_to_f64(&json!("inf")).is_none());
        assert!(value_to_f64(&json!("Infinity")).is_none());
        assert_eq!(Expr::parse("inf_str > 100").unwrap().eval(&rec).unwrap(), json!(false));

        // Scientific notation
        assert_eq!(Expr::parse("1e3 + 2.5e2").unwrap().eval(&rec).unwrap(), json!(1250));
        assert_eq!(Expr::parse("1.5e-1").unwrap().eval(&rec).unwrap(), json!(0.15));

        // String escape decoding
        assert_eq!(
            Expr::parse(r#"'a\nb\tc\\d\'e'"#).unwrap().eval(&rec).unwrap(),
            json!("a\nb\tc\\d'e")
        );
    }

    #[test]
    fn test_expr_div_zero_depth_limit_and_i64_max_boundary() {
        let rec = DataRecord::new();

        // Division and remainder by zero error cleanly
        assert!(Expr::parse("10 / 0").unwrap().eval(&rec).is_err());
        assert!(Expr::parse("10 % 0").unwrap().eval(&rec).is_err());

        // Deep nesting returns Parse error instead of stack overflow
        let deep = format!("{}1{}", "(".repeat(200), ")".repeat(200));
        assert!(Expr::parse(&deep).is_err());

        // 2^63 must not saturate to i64::MAX (9223372036854775807)
        let two_pow_63 = 9223372036854775808.0_f64;
        let v = f64_to_json_value(two_pow_63);
        assert_ne!(v.as_i64(), Some(i64::MAX));
    }
}
