use regex::Regex;
use serde_json::Value;
use std::sync::Arc;

use crate::core::interfaces::{value_to_f64, value_to_string, DataRecord};

pub type FieldFilterRule = Arc<dyn Fn(&Value) -> bool + Send + Sync>;

/// Builds a record filter rule targeting a single named field in a `DataRecord`.
#[derive(Clone)]
pub struct FieldFilter {
    field_name: String,
    rules: Vec<FieldFilterRule>,
}

impl FieldFilter {
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            rules: Vec::new(),
        }
    }

    pub fn add_rule<F>(&mut self, rule: F) -> &mut Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        self.rules.push(Arc::new(rule));
        self
    }

    pub fn add_shared_rule(&mut self, rule: FieldFilterRule) -> &mut Self {
        self.rules.push(rule);
        self
    }

    pub fn create_record_filter(&self) -> Arc<dyn Fn(&DataRecord) -> bool + Send + Sync> {
        let field_name = self.field_name.clone();
        let rules = self.rules.clone();
        Arc::new(move |record: &DataRecord| {
            let val = record.get(&field_name).unwrap_or(&Value::Null);
            for rule in &rules {
                if !rule(val) {
                    return false;
                }
            }
            true
        })
    }
}

/// Checks that a field value is not `Value::Null`.
pub fn is_not_null() -> FieldFilterRule {
    Arc::new(|value: &Value| !value.is_null())
}

/// Checks that a field value matches a type string (`"string"`, `"number"`, `"boolean"`, `"object"`, `"array"`).
pub fn is_type(expected_type: impl Into<String>) -> FieldFilterRule {
    let expected = expected_type.into();
    Arc::new(move |value: &Value| match expected.as_str() {
        "string" => value.is_string(),
        "number" => value.is_number(),
        "boolean" => value.is_boolean(),
        "object" => value.is_object(),
        "array" => value.is_array(),
        "null" => value.is_null(),
        _ => false,
    })
}

/// Checks that a field value matches one of the allowed JSON values.
pub fn value_match<I, V>(allowed_values: I) -> FieldFilterRule
where
    I: IntoIterator<Item = V>,
    V: Into<Value>,
{
    let allowed: Vec<Value> = allowed_values.into_iter().map(Into::into).collect();
    Arc::new(move |value: &Value| allowed.iter().any(|a| a == value))
}

/// Checks that a field value (converted to string) matches the given regular expression pattern.
pub fn pattern_match(pattern: impl AsRef<str>) -> FieldFilterRule {
    let re = Regex::new(pattern.as_ref()).expect("Invalid regex pattern in pattern_match");
    Arc::new(move |value: &Value| {
        if value.is_null() {
            return false;
        }
        let s = value_to_string(value);
        re.is_match(&s)
    })
}

/// Checks that a numeric field value is strictly greater than `threshold`.
pub fn greater_than(threshold: f64) -> FieldFilterRule {
    Arc::new(move |value: &Value| value_to_f64(value).map(|v| v > threshold).unwrap_or(false))
}

/// Checks that a numeric field value is strictly less than `threshold`.
pub fn less_than(threshold: f64) -> FieldFilterRule {
    Arc::new(move |value: &Value| value_to_f64(value).map(|v| v < threshold).unwrap_or(false))
}

/// Checks that a numeric field value lies inclusively between `min` and `max`.
pub fn between(min: f64, max: f64) -> FieldFilterRule {
    Arc::new(move |value: &Value| {
        value_to_f64(value)
            .map(|v| v >= min && v <= max)
            .unwrap_or(false)
    })
}

/// Checks if a field value is null or an empty string `""`.
pub fn is_empty() -> FieldFilterRule {
    Arc::new(|value: &Value| match value {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        _ => false,
    })
}
