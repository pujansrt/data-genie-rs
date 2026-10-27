use async_stream::try_stream;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::core::error::DataGenieError;
use crate::core::interfaces::{DataReader, DataRecord, DataSource, RecordStream, SharedWriter};
use crate::core::transport_utils::{ensure_data_source, SourceInput};
use crate::transformers::SchemaValidator;

#[derive(Clone, Default)]
pub struct CSVReaderOptions {
    pub schema: Option<Arc<dyn SchemaValidator>>,
    pub has_field_names_in_first_row: Option<bool>,
    pub delimiter: Option<char>,
    pub ignore_errors: Option<bool>,
}

/// Streaming CSV reader with Dead Letter Queue (DLQ) support, header inference, and schema validation.
pub struct CSVReader {
    source: Arc<dyn DataSource>,
    has_field_names_in_first_row: bool,
    field_separator: u8,
    schema: Option<Arc<dyn SchemaValidator>>,
    ignore_errors: bool,
    dlq_writer: Option<SharedWriter>,
}

impl CSVReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        Self {
            source: ensure_data_source(source),
            has_field_names_in_first_row: true,
            field_separator: b',',
            schema: None,
            ignore_errors: false,
            dlq_writer: None,
        }
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: CSVReaderOptions) -> Self {
        let mut reader = Self::new(source);
        reader.schema = options.schema;
        if let Some(has_headers) = options.has_field_names_in_first_row {
            reader.has_field_names_in_first_row = has_headers;
        }
        if let Some(delim) = options.delimiter {
            reader.field_separator = delim as u8;
        }
        if let Some(ignore) = options.ignore_errors {
            reader.ignore_errors = ignore;
        }
        reader
    }

    pub fn set_field_names_in_first_row(mut self, value: bool) -> Self {
        self.has_field_names_in_first_row = value;
        self
    }

    pub fn set_field_separator(mut self, separator: char) -> Self {
        self.field_separator = separator as u8;
        self
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

    #[inline]
    fn parse_csv_line<'a>(
        line: &'a str,
        delimiter: u8,
        str_record: &'a mut csv::StringRecord,
    ) -> std::result::Result<Vec<&'a str>, csv::Error> {
        let delim_char = delimiter as char;
        // Fast path for unquoted CSV lines (avoids constructing a full csv::Reader state machine per row)
        if !line.as_bytes().contains(&b'"') {
            return Ok(line.split(delim_char).collect());
        }

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(delimiter)
            .flexible(true)
            .from_reader(line.as_bytes());
        str_record.clear();
        if rdr.read_record(str_record)? {
            Ok(str_record.iter().collect())
        } else {
            Ok(Vec::new())
        }
    }
}

impl DataReader for CSVReader {
    fn read(&mut self) -> RecordStream {
        let source = Arc::clone(&self.source);
        let has_headers = self.has_field_names_in_first_row;
        let delimiter = self.field_separator;
        let schema = self.schema.clone();
        let ignore_errors = self.ignore_errors;
        let dlq_writer = self.dlq_writer.clone();

        Box::pin(try_stream! {
            let async_reader = source.get_reader().await?;
            let mut reader = BufReader::with_capacity(256 * 1024, async_reader);

            let mut headers: Option<Vec<String>> = None;
            let mut expected_col_count: Option<usize> = None;
            let mut line_buf = String::with_capacity(256);
            let mut reusable_record = csv::StringRecord::new();

            loop {
                line_buf.clear();
                let bytes_read = match reader.read_line(&mut line_buf).await {
                    Ok(n) => n,
                    Err(e) => {
                        if ignore_errors {
                            if let Some(ref dlq) = dlq_writer {
                                let mut rec = DataRecord::new();
                                rec.insert("_error".to_string(), Value::String(e.to_string()));
                                rec.insert("_type".to_string(), Value::String("fatal_parse_error".to_string()));
                                dlq.write_dlq(rec).await?;
                            }
                            break;
                        } else {
                            Err(DataGenieError::Io(e))?;
                            unreachable!()
                        }
                    }
                };

                if bytes_read == 0 {
                    break;
                }

                let line = line_buf.trim_end_matches(&['\r', '\n'][..]);
                if line.trim().is_empty() {
                    continue;
                }

                let fields = match Self::parse_csv_line(line, delimiter, &mut reusable_record) {
                    Ok(f) if f.is_empty() => continue,
                    Ok(f) => f,
                    Err(err) => {
                        if ignore_errors {
                            if let Some(ref dlq) = dlq_writer {
                                let mut rec = DataRecord::new();
                                rec.insert("_raw".to_string(), Value::String(line.to_string()));
                                rec.insert("_error".to_string(), Value::String(err.to_string()));
                                rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                dlq.write_dlq(rec).await?;
                            }
                            continue;
                        } else {
                            Err(DataGenieError::Csv(err))?;
                            unreachable!()
                        }
                    }
                };

                if has_headers && headers.is_none() {
                    expected_col_count = Some(fields.len());
                    headers = Some(fields.into_iter().map(str::to_string).collect());
                    continue;
                }

                if expected_col_count.is_none() {
                    expected_col_count = Some(fields.len());
                }

                let expected = expected_col_count.unwrap_or(fields.len());
                if fields.len() != expected {
                    let msg = format!(
                        "Inconsistent column count: expected {}, got {}",
                        expected,
                        fields.len()
                    );
                    if ignore_errors {
                        if let Some(ref dlq) = dlq_writer {
                            let mut raw_obj = DataRecord::new();
                            if let Some(ref hdrs) = headers {
                                for (idx, val) in fields.iter().enumerate() {
                                    let key = hdrs.get(idx).cloned().unwrap_or_else(|| format!("col_{}", idx));
                                    raw_obj.insert(key, Value::String((*val).to_string()));
                                }
                            }
                            let mut dlq_rec = DataRecord::new();
                            dlq_rec.insert("_raw".to_string(), json!(raw_obj));
                            dlq_rec.insert("_error".to_string(), Value::String(msg));
                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                            dlq.write_dlq(dlq_rec).await?;
                        }
                        continue;
                    } else {
                        Err(DataGenieError::Parse(msg))?;
                    }
                }

                let mut record = DataRecord::with_capacity(fields.len());
                if let Some(ref hdrs) = headers {
                    for (idx, val) in fields.into_iter().enumerate() {
                        let key = hdrs.get(idx).cloned().unwrap_or_else(|| idx.to_string());
                        record.insert(key, Value::String(val.to_string()));
                    }
                } else {
                    for (idx, val) in fields.into_iter().enumerate() {
                        record.insert(idx.to_string(), Value::String(val.to_string()));
                    }
                }

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
