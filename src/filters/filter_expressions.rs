use std::sync::Arc;

use crate::core::expr::{is_truthy, Expr};
use crate::core::interfaces::DataRecord;

/// Evaluates boolean filter expressions (e.g., `"(status == 'OK' || status == 'ERROR') && age > 30"`)
/// against streaming `DataRecord` items.
#[derive(Debug, Clone)]
pub struct FilterExpression {
    expression: String,
    compiled: Option<Expr>,
}

impl FilterExpression {
    pub fn new(expression: impl Into<String>) -> Self {
        let expr_str = expression.into();
        let compiled = Expr::parse(&expr_str).ok();
        Self {
            expression: expr_str,
            compiled,
        }
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
