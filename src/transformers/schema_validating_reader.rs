use async_stream::try_stream;
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::{Number, Value};
use std::sync::Arc;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter};

/// Trait representing a schema validator and type coercer (analogous to Zod's `.parse()`).
pub trait SchemaValidator: Send + Sync {
    fn parse(&self, record: DataRecord) -> Result<DataRecord>;
}

impl<F> SchemaValidator for F
where
    F: Fn(DataRecord) -> Result<DataRecord> + Send + Sync,
{
    fn parse(&self, record: DataRecord) -> Result<DataRecord> {
        (self)(record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaFieldType {
    String,
    Number,
    Integer,
    Boolean,
    Any,
}

/// Declarative field validator with optional coercion, nullability, and custom predicates.
#[derive(Clone)]
pub struct FieldSchema {
    field_type: SchemaFieldType,
    optional: bool,
    nullable: bool,
    coerce: bool,
    min_num: Option<f64>,
    max_num: Option<f64>,
    min_len: Option<usize>,
    custom_check: Option<Arc<dyn Fn(&Value) -> std::result::Result<(), String> + Send + Sync>>,
}

impl FieldSchema {
    pub fn string() -> Self {
        Self {
            field_type: SchemaFieldType::String,
            optional: false,
            nullable: false,
            coerce: false,
            min_num: None,
            max_num: None,
            min_len: None,
            custom_check: None,
        }
    }

    pub fn number() -> Self {
        Self {
            field_type: SchemaFieldType::Number,
            optional: false,
            nullable: false,
            coerce: false,
            min_num: None,
            max_num: None,
            min_len: None,
            custom_check: None,
        }
    }

    pub fn integer() -> Self {
        Self {
            field_type: SchemaFieldType::Integer,
            optional: false,
            nullable: false,
            coerce: false,
            min_num: None,
            max_num: None,
            min_len: None,
            custom_check: None,
        }
    }

    pub fn boolean() -> Self {
        Self {
            field_type: SchemaFieldType::Boolean,
            optional: false,
            nullable: false,
            coerce: false,
            min_num: None,
            max_num: None,
            min_len: None,
            custom_check: None,
        }
    }

    pub fn any() -> Self {
        Self {
            field_type: SchemaFieldType::Any,
            optional: true,
            nullable: true,
            coerce: false,
            min_num: None,
            max_num: None,
            min_len: None,
            custom_check: None,
        }
    }

    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn coerce(mut self) -> Self {
        self.coerce = true;
        self
    }

    pub fn min(mut self, min: f64) -> Self {
        self.min_num = Some(min);
        self
    }

    pub fn max(mut self, max: f64) -> Self {
        self.max_num = Some(max);
        self
    }

    pub fn min_length(mut self, min_len: usize) -> Self {
        self.min_len = Some(min_len);
        self
    }

    pub fn refine<F>(mut self, check: F, message: impl Into<String>) -> Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        let msg = message.into();
        self.custom_check = Some(Arc::new(move |v: &Value| if check(v) { Ok(()) } else { Err(msg.clone()) }));
        self
    }

    fn validate_value(&self, field_name: &str, value: Option<&Value>) -> Result<Option<Value>> {
        let Some(v) = value else {
            if self.optional {
                return Ok(None);
            }
            return Err(DataGenieError::Validation(format!("Field '{}' is required", field_name)));
        };

        if v.is_null() {
            if self.nullable || self.optional {
                return Ok(Some(Value::Null));
            }
            return Err(DataGenieError::Validation(format!("Field '{}' cannot be null", field_name)));
        }

        let coerced = match self.field_type {
            SchemaFieldType::Any => v.clone(),
            SchemaFieldType::String => match v {
                Value::String(s) => Value::String(s.clone()),
                other if self.coerce => Value::String(crate::core::traits::value_to_string(other)),
                _ => return Err(DataGenieError::Validation(format!("Field '{}' expected string, got {}", field_name, v))),
            },
            SchemaFieldType::Number => match v {
                Value::Number(n) => Value::Number(n.clone()),
                Value::String(s) if self.coerce => {
                    let parsed = s
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| DataGenieError::Validation(format!("Field '{}' could not coerce '{}' to number", field_name, s)))?;
                    let num = Number::from_f64(parsed).ok_or_else(|| DataGenieError::Validation(format!("Field '{}' invalid float '{}'", field_name, s)))?;
                    Value::Number(num)
                }
                _ => return Err(DataGenieError::Validation(format!("Field '{}' expected number, got {}", field_name, v))),
            },
            SchemaFieldType::Integer => match v {
                Value::Number(n) if n.is_i64() || n.is_u64() => Value::Number(n.clone()),
                Value::String(s) if self.coerce => {
                    let parsed = s
                        .trim()
                        .parse::<i64>()
                        .map_err(|_| DataGenieError::Validation(format!("Field '{}' could not coerce '{}' to integer", field_name, s)))?;
                    Value::Number(Number::from(parsed))
                }
                _ => return Err(DataGenieError::Validation(format!("Field '{}' expected integer, got {}", field_name, v))),
            },
            SchemaFieldType::Boolean => match v {
                Value::Bool(b) => Value::Bool(*b),
                Value::String(s) if self.coerce => match s.trim().to_lowercase().as_str() {
                    "true" | "1" | "yes" => Value::Bool(true),
                    "false" | "0" | "no" => Value::Bool(false),
                    _ => {
                        return Err(DataGenieError::Validation(format!(
                            "Field '{}' could not coerce '{}' to boolean",
                            field_name, s
                        )));
                    }
                },
                _ => return Err(DataGenieError::Validation(format!("Field '{}' expected boolean, got {}", field_name, v))),
            },
        };

        if let Some(min) = self.min_num {
            if let Some(n) = coerced.as_f64() {
                if n < min {
                    return Err(DataGenieError::Validation(format!(
                        "Field '{}' value {} is less than minimum {}",
                        field_name, n, min
                    )));
                }
            }
        }

        if let Some(max) = self.max_num {
            if let Some(n) = coerced.as_f64() {
                if n > max {
                    return Err(DataGenieError::Validation(format!(
                        "Field '{}' value {} is greater than maximum {}",
                        field_name, n, max
                    )));
                }
            }
        }

        if let Some(min_len) = self.min_len {
            if let Some(s) = coerced.as_str() {
                if s.len() < min_len {
                    return Err(DataGenieError::Validation(format!(
                        "Field '{}' length {} is less than minimum {}",
                        field_name,
                        s.len(),
                        min_len
                    )));
                }
            }
        }

        if let Some(ref check) = self.custom_check {
            if let Err(msg) = check(&coerced) {
                return Err(DataGenieError::Validation(format!("Field '{}': {}", field_name, msg)));
            }
        }

        Ok(Some(coerced))
    }
}

/// Zod-inspired declarative schema validator for `DataRecord` objects.
#[derive(Clone, Default)]
pub struct RecordSchema {
    fields: IndexMap<String, FieldSchema>,
    passthrough: bool,
}

impl RecordSchema {
    pub fn new() -> Self {
        Self {
            fields: IndexMap::new(),
            passthrough: true,
        }
    }

    pub fn field(mut self, name: impl Into<String>, schema: FieldSchema) -> Self {
        self.fields.insert(name.into(), schema);
        self
    }

    pub fn strict(mut self) -> Self {
        self.passthrough = false;
        self
    }
}

impl SchemaValidator for RecordSchema {
    fn parse(&self, mut record: DataRecord) -> Result<DataRecord> {
        let mut validated = DataRecord::new();

        for (field_name, field_schema) in &self.fields {
            let existing = record.shift_remove(field_name);
            if let Some(val) = field_schema.validate_value(field_name, existing.as_ref())? {
                validated.insert(field_name.clone(), val);
            }
        }

        if self.passthrough {
            for (k, v) in record {
                validated.insert(k, v);
            }
        }

        Ok(validated)
    }
}

/// Validates and coerces streaming records using a `SchemaValidator`, diverting failures to a DLQ if configured.
pub struct SchemaValidatingReader<R: DataReader> {
    reader: R,
    schema: Arc<dyn SchemaValidator>,
    dlq_writer: Option<SharedWriter>,
}

impl<R: DataReader> SchemaValidatingReader<R> {
    pub fn new<S: SchemaValidator + 'static>(reader: R, schema: S) -> Self {
        Self {
            reader,
            schema: Arc::new(schema),
            dlq_writer: None,
        }
    }

    pub fn from_arc(reader: R, schema: Arc<dyn SchemaValidator>) -> Self {
        Self {
            reader,
            schema,
            dlq_writer: None,
        }
    }

    pub fn with_dlq<W: Into<SharedWriter>>(mut self, writer: W) -> Self {
        let shared = writer.into();
        self.dlq_writer = Some(shared.clone());
        self.reader.set_dlq(shared);
        self
    }
}

impl<R: DataReader + 'static> DataReader for SchemaValidatingReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let schema = Arc::clone(&self.schema);
        let dlq_writer = self.dlq_writer.clone();

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    continue;
                };

                match schema.parse(record.clone()) {
                    Ok(validated) => {
                        yield Some(validated);
                    }
                    Err(err) => {
                        if let Some(ref dlq) = dlq_writer {
                            let mut dlq_rec = record;
                            dlq_rec.insert("_schema_error".to_string(), Value::String(err.to_string()));
                            dlq.write_dlq(dlq_rec).await?;
                        } else {
                            eprintln!("Schema validation failed for record. Skipping. {}", err);
                        }
                    }
                }
            }

            if let Some(ref dlq) = dlq_writer {
                dlq.close_dlq().await?;
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.dlq_writer = Some(writer.clone());
        self.reader.set_dlq(writer);
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.reader.set_ignore_errors(ignore);
    }
}
