use async_trait::async_trait;
use serde_json::Value;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

/// Streaming CSV writer that automatically writes headers from the first record unless disabled.
pub struct CSVWriter {
    sink: Box<dyn DataSink>,
    field_names: Vec<String>,
    header_written: bool,
    output_stream: Option<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
}

impl CSVWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            field_names: Vec::new(),
            header_written: false,
            output_stream: None,
        }
    }

    pub fn set_field_names_in_first_row(mut self, value: bool) -> Self {
        self.header_written = !value;
        self
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        if self.output_stream.is_none() {
            self.output_stream = Some(self.sink.get_writer().await?);
        }
        Ok(())
    }

    fn format_csv_row(fields: &[String]) -> Result<Vec<u8>> {
        let mut wtr = csv::WriterBuilder::new().has_headers(false).from_writer(Vec::new());
        wtr.write_record(fields)?;
        wtr.flush()?;
        Ok(wtr.into_inner().unwrap_or_default())
    }
}

#[async_trait]
impl DataWriter for CSVWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if record.is_empty() {
            return Ok(());
        }
        self.ensure_stream().await?;

        if !self.header_written && self.field_names.is_empty() {
            self.field_names = record.keys().cloned().collect();
            let header_bytes = Self::format_csv_row(&self.field_names)?;
            if let Some(ref mut stream) = self.output_stream {
                stream.write_all(&header_bytes).await?;
            }
            self.header_written = true;
        } else if self.field_names.is_empty() {
            self.field_names = record.keys().cloned().collect();
        }

        let row_values: Vec<String> = self
            .field_names
            .iter()
            .map(|k| record.get(k).map(value_to_string).unwrap_or_default())
            .collect();
        let row_bytes = Self::format_csv_row(&row_values)?;
        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(&row_bytes).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let Some(mut stream) = self.output_stream.take() {
            stream.flush().await?;
            stream.shutdown().await?;
        }
        self.sink.finalize().await?;
        Ok(())
    }
}

impl CSVWriter {
    /// Helper for writing an arbitrary JSON value if needed; ignores `Value::Null`.
    pub async fn write_value(&mut self, val: Value) -> Result<()> {
        if let Value::Object(map) = val {
            let mut rec = DataRecord::new();
            for (k, v) in map {
                rec.insert(k, v);
            }
            self.write(rec).await?;
        }
        Ok(())
    }
}
