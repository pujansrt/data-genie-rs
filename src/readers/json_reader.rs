use async_stream::try_stream;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::error::DataGenieError;
use crate::core::traits::{DataReader, DataRecord, DataSource, RecordStream, SharedWriter};
use crate::core::transport_utils::{SourceInput, ensure_data_source};
use crate::transformers::SchemaValidator;

#[derive(Clone, Default)]
pub struct JsonReaderOptions {
    pub schema: Option<Arc<dyn SchemaValidator>>,
    pub ignore_errors: Option<bool>,
}

/// Reads an array of JSON objects from any `DataSource`, supporting DLQ and schema validation.
pub struct JsonReader {
    source: Arc<dyn DataSource>,
    schema: Option<Arc<dyn SchemaValidator>>,
    ignore_errors: bool,
    dlq_writer: Option<SharedWriter>,
    local_dlq_records: Arc<AtomicUsize>,
    rejected_without_dlq: Arc<AtomicUsize>,
}

impl JsonReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        Self {
            source: ensure_data_source(source),
            schema: None,
            ignore_errors: false,
            dlq_writer: None,
            local_dlq_records: Arc::new(AtomicUsize::new(0)),
            rejected_without_dlq: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: JsonReaderOptions) -> Self {
        Self {
            source: ensure_data_source(source),
            schema: options.schema,
            ignore_errors: options.ignore_errors.unwrap_or(false),
            dlq_writer: None,
            local_dlq_records: Arc::new(AtomicUsize::new(0)),
            rejected_without_dlq: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_schema<V: SchemaValidator + 'static>(mut self, schema: V) -> Self {
        self.schema = Some(Arc::new(schema));
        self
    }

    pub fn with_ignore_errors(mut self, value: bool) -> Self {
        self.ignore_errors = value;
        self
    }

    pub fn with_dlq<W: Into<SharedWriter>>(mut self, writer: W) -> Self {
        let shared = writer.into();
        self.set_dlq(shared);
        self
    }
}

impl DataReader for JsonReader {
    fn read(&mut self) -> RecordStream {
        let source = Arc::clone(&self.source);
        let schema = self.schema.clone();
        let ignore_errors = self.ignore_errors;
        let dlq_writer = self.dlq_writer.clone();
        let local_dlq_records = Arc::clone(&self.local_dlq_records);
        let rejected_without_dlq = Arc::clone(&self.rejected_without_dlq);

        Box::pin(try_stream! {
            let bytes = source.read_bytes().await?;
            let clean_bytes = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(&bytes);

            if clean_bytes.iter().all(|b| b.is_ascii_whitespace()) {
                return;
            }

            let parsed_res: std::result::Result<Vec<Value>, serde_json::Error> =
                serde_json::from_slice(clean_bytes);

            match parsed_res {
                Ok(items) => {
                    for item in items {
                        let record = match item {
                            Value::Object(map) => {
                                let mut rec = DataRecord::with_capacity(map.len());
                                for (k, v) in map {
                                    rec.insert(k, v);
                                }
                                rec
                            }
                            other => {
                                let msg = format!("Expected JSON object element in array, got {}", other);
                                if ignore_errors {
                                    if let Some(ref dlq) = dlq_writer {
                                        let mut dlq_rec = DataRecord::new();
                                        dlq_rec.insert("_raw".to_string(), other);
                                        dlq_rec.insert("_error".to_string(), Value::String(msg));
                                        dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                        dlq.write_dlq(dlq_rec).await?;
                                        local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                    }
                                    continue;
                                } else {
                                    Err(DataGenieError::Parse(msg))?;
                                    unreachable!()
                                }
                            }
                        };

                        if let Some(ref validator) = schema {
                            let raw_backup = if ignore_errors && dlq_writer.is_some() {
                                Some(record.clone())
                            } else {
                                None
                            };
                            match validator.parse(record) {
                                Ok(validated) => yield Some(validated),
                                Err(err) => {
                                    if ignore_errors {
                                        if let Some(ref dlq) = dlq_writer {
                                            let mut dlq_rec = DataRecord::new();
                                            dlq_rec.insert("_raw".to_string(), json!(raw_backup.unwrap_or_default()));
                                            dlq_rec.insert("_error".to_string(), Value::String(err.to_string()));
                                            dlq_rec.insert("_type".to_string(), Value::String("validation_error".to_string()));
                                            dlq.write_dlq(dlq_rec).await?;
                                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                        }
                                        continue;
                                    } else {
                                        Err(err)?;
                                    }
                                }
                            }
                        } else {
                            yield Some(record);
                        }
                    }
                }
                Err(err) => {
                    if ignore_errors {
                        if let Some(ref dlq) = dlq_writer {
                            let mut dlq_rec = DataRecord::new();
                            let raw_preview: String = String::from_utf8_lossy(clean_bytes).chars().take(512).collect();
                            dlq_rec.insert("_raw".to_string(), Value::String(raw_preview));
                            dlq_rec.insert("_error".to_string(), Value::String(err.to_string()));
                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                            dlq.write_dlq(dlq_rec).await?;
                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                        } else {
                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        Err(DataGenieError::Json(err))?;
                    }
                }
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.dlq_writer = Some(writer);
        self.ignore_errors = true;
    }

    fn propagate_dlq(&mut self, writer: SharedWriter) {
        if self.dlq_writer.is_none() {
            self.dlq_writer = Some(writer);
            self.ignore_errors = true;
        }
    }

    fn dlq_record_count(&self) -> usize {
        self.local_dlq_records.load(Ordering::Relaxed)
    }

    fn rejected_record_count(&self) -> usize {
        self.dlq_record_count() + self.rejected_without_dlq.load(Ordering::Relaxed)
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.ignore_errors = ignore;
    }
}
