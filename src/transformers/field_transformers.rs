use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::core::error::Result;
use crate::core::expr::Expr;
use crate::core::traits::{DataRecord, value_to_string};
use crate::transformers::transforming_reader::RecordTransformation;

/// Common type conversions and default-value transformations for one or more record fields.
#[derive(Debug, Clone)]
pub struct BasicFieldTransformer {
    field_names: Vec<String>,
}

impl BasicFieldTransformer {
    pub fn new<I, S>(field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            field_names: field_names.into_iter().map(Into::into).collect(),
        }
    }

    pub fn string_to_double(&self) -> RecordTransformation {
        let fields = self.field_names.clone();
        Arc::new(move |mut record: DataRecord| {
            for field in &fields {
                if let Some(Value::String(s)) = record.get(field) {
                    if let Ok(parsed) = s.trim().parse::<f64>() {
                        if let Some(num) = Number::from_f64(parsed) {
                            record.insert(field.clone(), Value::Number(num));
                        }
                    }
                }
            }
            record
        })
    }

    pub fn string_to_int(&self) -> RecordTransformation {
        let fields = self.field_names.clone();
        Arc::new(move |mut record: DataRecord| {
            for field in &fields {
                if let Some(Value::String(s)) = record.get(field) {
                    let trimmed = s.trim();
                    if let Ok(parsed) = trimmed.parse::<i64>() {
                        record.insert(field.clone(), Value::Number(Number::from(parsed)));
                    } else if let Ok(parsed_float) = trimmed.parse::<f64>()
                        && parsed_float.is_finite()
                        && parsed_float >= (i64::MIN as f64)
                        && parsed_float < (i64::MAX as f64)
                    {
                        record.insert(field.clone(), Value::Number(Number::from(parsed_float as i64)));
                    }
                }
            }
            record
        })
    }

    pub fn null_to_value(&self, default_value: impl Into<Value>) -> RecordTransformation {
        let fields = self.field_names.clone();
        let default_val = default_value.into();
        Arc::new(move |mut record: DataRecord| {
            for field in &fields {
                let is_null_or_missing = matches!(record.get(field), None | Some(Value::Null));
                if is_null_or_missing {
                    record.insert(field.clone(), default_val.clone());
                }
            }
            record
        })
    }

    pub fn date_parse(&self, format: impl Into<String>) -> RecordTransformation {
        let fields = self.field_names.clone();
        let fmt = format.into();
        Arc::new(move |mut record: DataRecord| {
            for field in &fields {
                let parsed_iso = if let Some(Value::String(s)) = record.get(field) {
                    let trimmed = s.trim();
                    if !fmt.is_empty() {
                        DateTime::parse_from_str(trimmed, &fmt)
                            .map(|dt| dt.with_timezone(&Utc).to_rfc3339())
                            .or_else(|_| NaiveDateTime::parse_from_str(trimmed, &fmt).map(|ndt| ndt.and_utc().to_rfc3339()))
                            .or_else(|_| NaiveDate::parse_from_str(trimmed, &fmt).map(|nd| nd.and_time(NaiveTime::MIN).and_utc().to_rfc3339()))
                            .ok()
                    } else {
                        DateTime::parse_from_rfc3339(trimmed)
                            .map(|dt| dt.with_timezone(&Utc).to_rfc3339())
                            .or_else(|_| NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").map(|nd| nd.and_time(NaiveTime::MIN).and_utc().to_rfc3339()))
                            .ok()
                    }
                } else {
                    None
                };

                if let Some(iso) = parsed_iso {
                    record.insert(field.clone(), Value::String(iso));
                }
            }
            record
        })
    }
}

/// Renames a field in a `DataRecord` while preserving key order.
#[derive(Debug, Clone)]
pub struct RenameField {
    old_name: String,
    new_name: String,
    allow_duplicate_field_names: bool,
}

impl RenameField {
    pub fn new(old_name: impl Into<String>, new_name: impl Into<String>) -> Self {
        Self {
            old_name: old_name.into(),
            new_name: new_name.into(),
            allow_duplicate_field_names: false,
        }
    }

    pub fn set_allow_duplicate_field_names(mut self, value: bool) -> Self {
        self.allow_duplicate_field_names = value;
        self
    }

    pub fn transform(&self) -> RecordTransformation {
        let old_name = self.old_name.clone();
        let new_name = self.new_name.clone();
        let allow_dup = self.allow_duplicate_field_names;

        Arc::new(move |record: DataRecord| {
            if old_name == new_name || !record.contains_key(&old_name) {
                return record;
            }
            if record.contains_key(&new_name) && !allow_dup {
                // Preserve existing target field when duplicates/overwrites are disallowed.
                return record;
            }
            let mut renamed = DataRecord::with_capacity(record.len());
            for (k, v) in record {
                if k == old_name {
                    renamed.insert(new_name.clone(), v);
                } else if k == new_name {
                    // Skip existing target key so the renamed field replaces it cleanly.
                    continue;
                } else {
                    renamed.insert(k, v);
                }
            }
            renamed
        })
    }
}

/// Sets a constant value on a specified field in every `DataRecord`.
#[derive(Debug, Clone)]
pub struct SetField {
    field_name: String,
    value: Value,
}

impl SetField {
    pub fn new(field_name: impl Into<String>, value: impl Into<Value>) -> Self {
        Self {
            field_name: field_name.into(),
            value: value.into(),
        }
    }

    pub fn transform(&self) -> RecordTransformation {
        let field_name = self.field_name.clone();
        let value = self.value.clone();
        Arc::new(move |mut record: DataRecord| {
            record.insert(field_name.clone(), value.clone());
            record
        })
    }
}

/// Computes a field dynamically using an expression evaluated against each `DataRecord`.
#[derive(Debug, Clone)]
pub struct SetCalculatedField {
    field_name: String,
    expression: String,
    compiled: Option<Expr>,
    parse_error: Option<String>,
}

impl SetCalculatedField {
    pub fn new(field_name: impl Into<String>, expression: impl Into<String>) -> Self {
        let expr_str = expression.into();
        let (compiled, parse_error) = match Expr::parse(&expr_str) {
            Ok(ast) => (Some(ast), None),
            Err(err) => (None, Some(err.to_string())),
        };
        Self {
            field_name: field_name.into(),
            expression: expr_str,
            compiled,
            parse_error,
        }
    }

    /// Fallible constructor that returns an error immediately if `expression` has invalid syntax.
    pub fn try_new(field_name: impl Into<String>, expression: impl Into<String>) -> Result<Self> {
        let expr_str = expression.into();
        let compiled = Expr::parse(&expr_str)?;
        Ok(Self {
            field_name: field_name.into(),
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

    pub fn transform(&self) -> RecordTransformation {
        let field_name = self.field_name.clone();
        let compiled = self.compiled.clone();
        Arc::new(move |mut record: DataRecord| {
            if let Some(ast) = &compiled
                && let Ok(val) = ast.eval(&record)
            {
                record.insert(field_name.clone(), val);
            }
            record
        })
    }
}

/// Removes one or more specified fields from each `DataRecord`.
#[derive(Debug, Clone)]
pub struct RemoveFields {
    field_names: Vec<String>,
}

impl RemoveFields {
    pub fn new<I, S>(field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            field_names: field_names.into_iter().map(Into::into).collect(),
        }
    }

    pub fn transform(&self) -> RecordTransformation {
        let fields = self.field_names.clone();
        Arc::new(move |mut record: DataRecord| {
            for field in &fields {
                record.shift_remove(field);
            }
            record
        })
    }
}

/// Projects only the specified fields (in the given order) into a new `DataRecord`.
#[derive(Debug, Clone)]
pub struct SelectFields {
    field_names: Vec<String>,
}

impl SelectFields {
    pub fn new<I, S>(field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            field_names: field_names.into_iter().map(Into::into).collect(),
        }
    }

    pub fn transform(&self) -> RecordTransformation {
        let fields = self.field_names.clone();
        Arc::new(move |record: DataRecord| {
            let mut new_record = DataRecord::new();
            for field in &fields {
                if let Some(val) = record.get(field) {
                    new_record.insert(field.clone(), val.clone());
                }
            }
            new_record
        })
    }
}

/// Creates or updates `output_field` by applying a custom mapping closure to `input_fields`.
pub struct MapFields {
    output_field: String,
    input_fields: Vec<String>,
    mapper: Arc<dyn Fn(&[Value]) -> Value + Send + Sync>,
}

impl MapFields {
    pub fn new<I, S, F>(output_field: impl Into<String>, input_fields: I, mapper: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(&[Value]) -> Value + Send + Sync + 'static,
    {
        Self {
            output_field: output_field.into(),
            input_fields: input_fields.into_iter().map(Into::into).collect(),
            mapper: Arc::new(mapper),
        }
    }

    pub fn transform(&self) -> RecordTransformation {
        let output_field = self.output_field.clone();
        let input_fields = self.input_fields.clone();
        let mapper = Arc::clone(&self.mapper);
        Arc::new(move |mut record: DataRecord| {
            let args: Vec<Value> = input_fields.iter().map(|f| record.get(f).cloned().unwrap_or(Value::Null)).collect();
            let result = mapper(&args);
            record.insert(output_field.clone(), result);
            record
        })
    }
}

/// Strategy used by `PIIMaskingTransformer` to anonymize sensitive fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MaskStrategy {
    Redact,
    Hash,
    Partial,
    Null,
}

impl MaskStrategy {
    pub fn from_str_name(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "hash" => Self::Hash,
            "partial" => Self::Partial,
            "null" => Self::Null,
            _ => Self::Redact,
        }
    }
}

/// Anonymizes sensitive PII fields (emails, credit cards, SSNs, names) during streaming.
#[derive(Debug, Clone, Default)]
pub struct PIIMaskingTransformer {
    fields: IndexMap<String, MaskStrategy>,
    salt: Option<String>,
}

impl PIIMaskingTransformer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Configures a salt/pepper string prepended during [`MaskStrategy::Hash`] SHA-256 hashing.
    pub fn with_salt(mut self, salt: impl Into<String>) -> Self {
        self.salt = Some(salt.into());
        self
    }

    pub fn mask(mut self, field_name: impl Into<String>, strategy: MaskStrategy) -> Self {
        self.fields.insert(field_name.into(), strategy);
        self
    }

    pub fn add_mask(&mut self, field_name: impl Into<String>, strategy: MaskStrategy) -> &mut Self {
        self.fields.insert(field_name.into(), strategy);
        self
    }

    pub fn transform(&self) -> RecordTransformation {
        let fields = self.fields.clone();
        let salt = self.salt.clone();
        Arc::new(move |mut record: DataRecord| {
            for (field, strategy) in &fields {
                if let Some(val) = record.get(field)
                    && !val.is_null()
                {
                    let masked = Self::apply_strategy(val, *strategy, salt.as_deref());
                    record.insert(field.clone(), masked);
                }
            }
            record
        })
    }

    fn apply_strategy(value: &Value, strategy: MaskStrategy, salt: Option<&str>) -> Value {
        match strategy {
            MaskStrategy::Redact => Value::String("[REDACTED]".to_string()),
            MaskStrategy::Null => Value::Null,
            MaskStrategy::Hash => {
                let val_str = value_to_string(value);
                let mut hasher = Sha256::new();
                if let Some(s) = salt {
                    hasher.update(s.as_bytes());
                    hasher.update(b":");
                }
                hasher.update(val_str.as_bytes());
                let digest = hasher.finalize();
                let mut hex_str = String::with_capacity(digest.len() * 2);
                for byte in digest {
                    use std::fmt::Write;
                    let _ = write!(&mut hex_str, "{:02x}", byte);
                }
                Value::String(hex_str)
            }
            MaskStrategy::Partial => {
                let val_str = value_to_string(value);
                Value::String(Self::apply_partial_mask(&val_str))
            }
        }
    }

    fn looks_like_email(val: &str) -> Option<(&str, &str)> {
        let (user, domain) = val.split_once('@')?;
        if user.is_empty()
            || domain.is_empty()
            || domain.contains('@')
            || user.chars().any(char::is_whitespace)
            || domain.chars().any(char::is_whitespace)
            || !domain.contains('.')
            || domain.starts_with('.')
            || domain.ends_with('.')
        {
            return None;
        }
        Some((user, domain))
    }

    fn apply_partial_mask(val: &str) -> String {
        if let Some((user, domain)) = Self::looks_like_email(val) {
            let chars: Vec<char> = user.chars().collect();
            if chars.len() <= 1 {
                return format!("*@{}", domain);
            }
            if chars.len() == 2 {
                return format!("{}*@{}", chars[0], domain);
            }
            return format!("{}****@{}", chars[0], domain);
        }

        let chars: Vec<char> = val.chars().collect();
        if chars.len() > 10 {
            let tail: String = chars[chars.len() - 4..].iter().collect();
            return format!("****{}", tail);
        }

        if chars.len() > 2 {
            let prefix: String = chars[..2].iter().collect();
            format!("{}****", prefix)
        } else {
            "****".to_string()
        }
    }
}
