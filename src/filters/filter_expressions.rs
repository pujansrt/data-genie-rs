use std::sync::Arc;

use crate::core::error::Result;
use crate::core::expr::{Expr, is_truthy};
use crate::core::traits::DataRecord;

/// Evaluates boolean filter expressions (e.g., `"(status == 'OK' || status == 'ERROR') && age > 30"`)
/// against streaming `DataRecord` items.
#[derive(Debug, Clone)]
pub struct FilterExpression {
    expression: String,
    compiled: Option<Expr>,
    parse_error: Option<String>,
}

impl FilterExpression {
    pub fn new(expression: impl Into<String>) -> Self {
        let expr_str = expression.into();
        let (compiled, parse_error) = match Expr::parse(&expr_str) {
            Ok(ast) => (Some(ast), None),
            Err(err) => (None, Some(err.to_string())),
        };
        Self {
            expression: expr_str,
            compiled,
            parse_error,
        }
    }

    /// Fallible constructor that returns an error immediately if `expression` has invalid syntax.
    pub fn try_new(expression: impl Into<String>) -> Result<Self> {
        let expr_str = expression.into();
        let compiled = Expr::parse(&expr_str)?;
        Ok(Self {
            expression: expr_str,
            compiled: Some(compiled),
            parse_error: None,
        })
    }

    pub fn is_valid(&self) -> bool {
        self.compiled.is_some()
    }

    pub fn parse_error(&self) -> Option<&str> {
        self.parse_error.as_deref()
    }

    pub fn expression(&self) -> &str {
        &self.expression
    }

    pub fn create_record_filter(&self) -> Arc<dyn Fn(&DataRecord) -> bool + Send + Sync> {
        let compiled = self.compiled.clone();
        Arc::new(move |record: &DataRecord| {
            let Some(ast) = &compiled else {
                return false;
            };
            match ast.eval(record) {
                Ok(val) => is_truthy(&val),
                Err(_) => false,
            }
        })
    }
}
