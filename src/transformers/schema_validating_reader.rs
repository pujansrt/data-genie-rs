use async_stream::try_stream;
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::{Number, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

type FieldCustomCheck = Arc<dyn Fn(&Value) -> std::result::Result<(), String> + Send + Sync>;

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
    custom_checks: Vec<FieldCustomCheck>,
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
            custom_checks: Vec::new(),
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
            custom_checks: Vec::new(),
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
            custom_checks: Vec::new(),
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
            custom_checks: Vec::new(),
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
            custom_checks: Vec::new(),
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
        self.custom_checks
            .push(Arc::new(move |v: &Value| if check(v) { Ok(()) } else { Err(msg.clone()) }));
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
            if self.nullable {
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
                Value::Number(n) if self.coerce => {
                    if let Some(f) = n.as_f64()
                        && f.is_finite()
                        && f.fract() == 0.0
                        && f >= (i64::MIN as f64)
                        && f < (i64::MAX as f64)
                    {
                        Value::Number(Number::from(f as i64))
                    } else {
                        return Err(DataGenieError::Validation(format!("Field '{}' expected integer, got {}", field_name, v)));
                    }
                }
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
                Value::Number(n) if self.coerce && n.as_i64() == Some(1) => Value::Bool(true),
                Value::Number(n) if self.coerce && n.as_i64() == Some(0) => Value::Bool(false),
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

        if let Some(min) = self.min_num
            && let Some(n) = coerced.as_f64()
            && n < min
        {
            return Err(DataGenieError::Validation(format!(
                "Field '{}' value {} is less than minimum {}",
                field_name, n, min
            )));
        }

        if let Some(max) = self.max_num
            && let Some(n) = coerced.as_f64()
            && n > max
        {
            return Err(DataGenieError::Validation(format!(
                "Field '{}' value {} is greater than maximum {}",
                field_name, n, max
            )));
        }

        if let Some(min_len) = self.min_len
            && let Some(s) = coerced.as_str()
        {
            let char_len = s.chars().count();
            if char_len < min_len {
                return Err(DataGenieError::Validation(format!(
                    "Field '{}' length {} is less than minimum {}",
                    field_name, char_len, min_len
                )));
            }
        }

        for check in &self.custom_checks {
            if let Err(msg) = check(&coerced) {
                return Err(DataGenieError::Validation(format!("Field '{}': {}", field_name, msg)));
            }
        }

        Ok(Some(coerced))
    }
}

/// Zod-inspired declarative schema validator for `DataRecord` objects.
#[derive(Clone)]
pub struct RecordSchema {
    fields: IndexMap<String, FieldSchema>,
    passthrough: bool,
    reject_unknown_fields: bool,
}

impl Default for RecordSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordSchema {
    pub fn new() -> Self {
        Self {
            fields: IndexMap::new(),
            passthrough: true,
            reject_unknown_fields: false,
        }
    }

    pub fn field(mut self, name: impl Into<String>, schema: FieldSchema) -> Self {
        self.fields.insert(name.into(), schema);
        self
    }

    /// Strips any keys not explicitly declared in the schema.
    pub fn strict(mut self) -> Self {
        self.passthrough = false;
        self
    }

    /// Fails validation with `DataGenieError::Validation` if any undeclared keys are present.
    pub fn reject_unknown(mut self) -> Self {
        self.passthrough = false;
        self.reject_unknown_fields = true;
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

        if self.reject_unknown_fields && !record.is_empty() {
            let unknown_keys: Vec<&str> = record.keys().map(String::as_str).collect();
            return Err(DataGenieError::Validation(format!(
                "Unexpected unknown field(s) in record: {}",
                unknown_keys.join(", ")
            )));
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
    owns_dlq: bool,
    skipped_records: Arc<AtomicUsize>,
    local_dlq_records: Arc<AtomicUsize>,
    rejected_without_dlq: Arc<AtomicUsize>,
}

impl<R: DataReader> SchemaValidatingReader<R> {
    pub fn new<S: SchemaValidator + 'static>(reader: R, schema: S) -> Self {
        Self {
            reader,
            schema: Arc::new(schema),
            dlq_writer: None,
            owns_dlq: false,
            skipped_records: Arc::new(AtomicUsize::new(0)),
            local_dlq_records: Arc::new(AtomicUsize::new(0)),
            rejected_without_dlq: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn from_arc(reader: R, schema: Arc<dyn SchemaValidator>) -> Self {
        Self {
            reader,
            schema,
            dlq_writer: None,
            owns_dlq: false,
            skipped_records: Arc::new(AtomicUsize::new(0)),
            local_dlq_records: Arc::new(AtomicUsize::new(0)),
            rejected_without_dlq: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_dlq<W: Into<SharedWriter>>(mut self, writer: W) -> Self {
        let shared = writer.into();
        self.dlq_writer = Some(shared.clone());
        self.owns_dlq = true;
        self.reader.propagate_dlq(shared);
        self
    }
}

impl<R: DataReader + 'static> DataReader for SchemaValidatingReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let schema = Arc::clone(&self.schema);
        let dlq_writer = self.dlq_writer.clone();
        let owns_dlq = self.owns_dlq;
        let skipped_records = Arc::clone(&self.skipped_records);
        let local_dlq_records = Arc::clone(&self.local_dlq_records);
        let rejected_without_dlq = Arc::clone(&self.rejected_without_dlq);

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let record_opt = match item {
                    Ok(opt) => opt,
                    Err(err) => {
                        if owns_dlq
                            && let Some(ref dlq) = dlq_writer
                        {
                            let _ = dlq.close_dlq().await;
                        }
                        Err(err)?;
                        unreachable!()
                    }
                };
                let Some(record) = record_opt else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                    continue;
                };

                let dlq_backup = dlq_writer.as_ref().map(|_| record.clone());

                match schema.parse(record) {
                    Ok(validated) => {
                        yield Some(validated);
                    }
                    Err(err) => {
                        if let Some(ref dlq) = dlq_writer {
                            let mut dlq_rec = dlq_backup.unwrap();
                            dlq_rec.insert("_schema_error".to_string(), Value::String(err.to_string()));
                            dlq.write_dlq(dlq_rec).await?;
                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                        } else {
                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                            eprintln!("Schema validation failed for record. Skipping. {}", err);
                        }
                    }
                }
            }

            if owns_dlq
                && let Some(ref dlq) = dlq_writer
            {
                dlq.close_dlq().await?;
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.dlq_writer = Some(writer.clone());
        self.owns_dlq = true;
        self.reader.propagate_dlq(writer);
    }

    fn propagate_dlq(&mut self, writer: SharedWriter) {
        if self.dlq_writer.is_none() {
            self.dlq_writer = Some(writer.clone());
            self.owns_dlq = false;
            self.reader.propagate_dlq(writer);
        }
    }

    fn skipped_record_count(&self) -> usize {
        self.reader.skipped_record_count() + self.skipped_records.load(Ordering::Relaxed)
    }

    fn dlq_record_count(&self) -> usize {
        self.reader.dlq_record_count() + self.local_dlq_records.load(Ordering::Relaxed)
    }

    fn rejected_record_count(&self) -> usize {
        self.reader.rejected_record_count()
            + self.local_dlq_records.load(Ordering::Relaxed)
            + self.rejected_without_dlq.load(Ordering::Relaxed)
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.reader.set_ignore_errors(ignore);
    }
}
