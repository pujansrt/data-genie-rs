use async_stream::try_stream;
use serde_json::Value;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::core::error::DataGenieError;
use crate::core::interfaces::{DataReader, DataRecord, DataSource, RecordStream};
use crate::core::transport_utils::{ensure_data_source, SourceInput};
use crate::transformers::SchemaValidator;

#[derive(Clone, Default)]
pub struct FixedWidthReaderOptions {
    pub schema: Option<Arc<dyn SchemaValidator>>,
    pub field_widths: Vec<usize>,
    pub field_names: Vec<String>,
    pub has_field_names_in_first_row: Option<bool>,
}

/// Streaming reader for Fixed-Width text files.
pub struct FixedWidthReader {
    source: Arc<dyn DataSource>,
    field_widths: Vec<usize>,
    field_names: Vec<String>,
    has_field_names_in_first_row: bool,
    schema: Option<Arc<dyn SchemaValidator>>,
}

impl FixedWidthReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        Self {
            source: ensure_data_source(source),
            field_widths: Vec::new(),
            field_names: Vec::new(),
            has_field_names_in_first_row: false,
            schema: None,
        }
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: FixedWidthReaderOptions) -> Self {
        Self {
            source: ensure_data_source(source),
            field_widths: options.field_widths,
            field_names: options.field_names,
            has_field_names_in_first_row: options.has_field_names_in_first_row.unwrap_or(false),
            schema: options.schema,
        }
    }

    pub fn set_field_widths<I: IntoIterator<Item = usize>>(mut self, widths: I) -> Self {
        self.field_widths = widths.into_iter().collect();
        self
    }

    pub fn set_field_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.field_names = names.into_iter().map(Into::into).collect();
        self
    }

    pub fn set_field_names_in_first_row(mut self, value: bool) -> Self {
        self.has_field_names_in_first_row = value;
        self
    }

    pub fn with_schema<V: SchemaValidator + 'static>(mut self, schema: V) -> Self {
        self.schema = Some(Arc::new(schema));
        self
    }

    fn split_line(line: &str, widths: &[usize]) -> Vec<String> {
        let chars: Vec<char> = line.chars().collect();
        let mut values = Vec::with_capacity(widths.len());
        let mut pos = 0;
        for &w in widths {
            let end = (pos + w).min(chars.len());
            if pos < chars.len() {
                let s: String = chars[pos..end].iter().collect();
                values.push(s.trim().to_string());
            } else {
                values.push(String::new());
            }
            pos += w;
        }
        values
    }
}

impl DataReader for FixedWidthReader {
    fn read(&mut self) -> RecordStream {
        let source = Arc::clone(&self.source);
        let field_widths = self.field_widths.clone();
        let initial_field_names = self.field_names.clone();
        let has_headers = self.has_field_names_in_first_row;
        let schema = self.schema.clone();

        Box::pin(try_stream! {
            if field_widths.is_empty() {
                Err(DataGenieError::Config(
                    "Field widths must be defined before reading.".to_string(),
                ))?;
            }

            let async_reader = source.get_reader().await?;
            let mut lines = BufReader::new(async_reader).lines();

            let mut initialized = false;
            let mut field_names = initial_field_names;

            while let Some(line) = lines.next_line().await? {
                if line.trim().is_empty() {
                    continue;
                }

                if !initialized && has_headers {
                    field_names = Self::split_line(&line, &field_widths);
                    initialized = true;
                    continue;
                }

                if !initialized && field_names.is_empty() {
                    field_names = (0..field_widths.len())
                        .map(|i| format!("field{}", i))
                        .collect();
                    initialized = true;
                }

                let values = Self::split_line(&line, &field_widths);
                let mut record = DataRecord::new();
                for (i, name) in field_names.iter().enumerate() {
                    let val = values.get(i).cloned().unwrap_or_default();
                    record.insert(name.clone(), Value::String(val));
                }

                if let Some(ref validator) = schema {
                    let validated = validator.parse(record)?;
                    yield Some(validated);
                } else {
                    yield Some(record);
                }
            }
        })
    }
}
