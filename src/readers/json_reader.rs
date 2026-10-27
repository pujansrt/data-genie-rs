use async_stream::try_stream;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::core::error::DataGenieError;
use crate::core::interfaces::{DataReader, DataRecord, DataSource, RecordStream, SharedWriter};
use crate::core::transport_utils::{ensure_data_source, SourceInput};
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
}

impl JsonReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        Self {
            source: ensure_data_source(source),
            schema: None,
            ignore_errors: false,
            dlq_writer: None,
        }
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: JsonReaderOptions) -> Self {
        Self {
            source: ensure_data_source(source),
            schema: options.schema,
            ignore_errors: options.ignore_errors.unwrap_or(false),
            dlq_writer: None,
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

        Box::pin(try_stream! {
            let bytes = source.read_bytes().await?;
            let parsed_res: std::result::Result<Vec<DataRecord>, serde_json::Error> =
                serde_json::from_slice(&bytes);

            match parsed_res {
                Ok(records) => {
                    for record in records {
                        if let Some(ref validator) = schema {
                            match validator.parse(record.clone()) {
                                Ok(validated) => yield Some(validated),
                                Err(err) => {
                                    if ignore_errors {
                                        if let Some(ref dlq) = dlq_writer {
                                            let mut dlq_rec = DataRecord::new();
                                            dlq_rec.insert("_raw".to_string(), json!(record));
                                            dlq_rec.insert("_error".to_string(), Value::String(err.to_string()));
                                            dlq_rec.insert("_type".to_string(), Value::String("validation_error".to_string()));
                                            dlq.write_dlq(dlq_rec).await?;
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
                            dlq_rec.insert("_error".to_string(), Value::String(err.to_string()));
                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                            dlq.write_dlq(dlq_rec).await?;
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

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.ignore_errors = ignore;
    }
}
