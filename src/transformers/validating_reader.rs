use async_stream::try_stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

use crate::core::error::DataGenieError;
use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter};

/// Validation message recorded when a record fails a validation rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationMessage {
    pub record: DataRecord,
    pub field: Option<String>,
    pub message: String,
}

pub type RecordValidationRule = Arc<dyn Fn(&DataRecord, &mut Vec<ValidationMessage>) -> bool + Send + Sync>;

/// Validates streaming records against one or more `RecordValidationRule`s,
/// optionally throwing an error or diverting invalid/poison records to a Dead Letter Queue (DLQ).
pub struct ValidatingReader<R: DataReader> {
    reader: R,
    rules: Vec<RecordValidationRule>,
    messages: Arc<Mutex<Vec<ValidationMessage>>>,
    throw_exception_on_failure: bool,
    dlq_writer: Option<SharedWriter>,
}

impl<R: DataReader> ValidatingReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            rules: Vec::new(),
            messages: Arc::new(Mutex::new(Vec::new())),
            throw_exception_on_failure: false,
            dlq_writer: None,
        }
    }

    pub fn add<F>(mut self, rule: F) -> Self
    where
        F: Fn(&DataRecord, &mut Vec<ValidationMessage>) -> bool + Send + Sync + 'static,
    {
        self.rules.push(Arc::new(rule));
        self
    }

    pub fn add_rule(mut self, rule: RecordValidationRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn set_exception_on_failure(mut self, value: bool) -> Self {
        self.throw_exception_on_failure = value;
        self
    }

    pub fn with_dlq<W: Into<SharedWriter>>(mut self, writer: W) -> Self {
        let shared = writer.into();
        self.dlq_writer = Some(shared.clone());
        self.reader.set_dlq(shared);
        self
    }

    pub fn get_messages(&self) -> Vec<ValidationMessage> {
        self.messages.lock().unwrap().clone()
    }
}

impl<R: DataReader + 'static> DataReader for ValidatingReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let rules = self.rules.clone();
        let messages_store = Arc::clone(&self.messages);
        let throw_on_failure = self.throw_exception_on_failure;
        let dlq_writer = self.dlq_writer.clone();

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let record_opt = match item {
                    Ok(opt) => opt,
                    Err(err) => {
                        if let Some(ref dlq) = dlq_writer {
                            let mut err_rec = DataRecord::new();
                            err_rec.insert("_error".to_string(), Value::String(err.to_string()));
                            err_rec.insert("_type".to_string(), Value::String("upstream_error".to_string()));
                            dlq.write_dlq(err_rec).await?;
                            break;
                        } else {
                            if let Some(ref dlq) = dlq_writer {
                                let _ = dlq.close_dlq().await;
                            }
                            Err(err)?;
                            unreachable!()
                        }
                    }
                };

                let Some(record) = record_opt else {
                    continue;
                };

                let mut is_valid = true;
                let mut record_messages = Vec::new();

                for rule in &rules {
                    if !rule(&record, &mut record_messages) {
                        is_valid = false;
                    }
                }

                if !is_valid {
                    if throw_on_failure {
                        if let Some(ref dlq) = dlq_writer {
                            let _ = dlq.close_dlq().await;
                        }
                        Err(DataGenieError::Validation(format!(
                            "Validation failed for record: {}",
                            serde_json::to_string(&record).unwrap_or_default()
                        )))?;
                    }

                    {
                        let mut guard = messages_store.lock().unwrap();
                        guard.extend(record_messages.clone());
                    }

                    if let Some(ref dlq) = dlq_writer {
                        let mut dlq_rec = record.clone();
                        let errs: Vec<Value> = record_messages
                            .iter()
                            .map(|m| Value::String(m.message.clone()))
                            .collect();
                        dlq_rec.insert("_errors".to_string(), json!(errs));
                        dlq.write_dlq(dlq_rec).await?;
                        continue;
                    }
                }

                yield Some(record);
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

type FieldRuleFn = Arc<dyn Fn(&Value, &mut Vec<ValidationMessage>) -> bool + Send + Sync>;

/// Fluent builder for field-level validation rules.
#[derive(Clone)]
pub struct FieldValidator {
    field_name: String,
    rules: Vec<FieldRuleFn>,
}

impl FieldValidator {
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            rules: Vec::new(),
        }
    }

    pub fn add_rule<F>(mut self, rule: F, error_message: impl Into<String>) -> Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        let field = self.field_name.clone();
        let msg = error_message.into();
        self.rules.push(Arc::new(move |val: &Value, messages: &mut Vec<ValidationMessage>| {
            if !rule(val) {
                messages.push(ValidationMessage {
                    record: DataRecord::new(),
                    field: Some(field.clone()),
                    message: msg.clone(),
                });
                false
            } else {
                true
            }
        }));
        self
    }

    pub fn create_record_validation_rule(&self) -> RecordValidationRule {
        let field_name = self.field_name.clone();
        let rules = self.rules.clone();
        Arc::new(move |record: &DataRecord, messages: &mut Vec<ValidationMessage>| {
            let val = record.get(&field_name).unwrap_or(&Value::Null);
            let mut is_valid = true;
            for rule in &rules {
                if !rule(val, messages) {
                    is_valid = false;
                }
            }
            is_valid
        })
    }
}
