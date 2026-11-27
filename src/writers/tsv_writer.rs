use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{ByteWriter, DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

/// Streaming Tab-Separated Values (TSV) writer.
pub struct TSVWriter {
    sink: Box<dyn DataSink>,
    field_names: Vec<String>,
    header_written: bool,
    output_stream: Option<ByteWriter>,
    is_closed: bool,
}

impl TSVWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            field_names: Vec::new(),
            header_written: false,
            output_stream: None,
            is_closed: false,
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

    fn sanitize_field(value: &str) -> String {
        if !value.bytes().any(|b| matches!(b, b'\t' | b'\r' | b'\n')) {
            return value.to_string();
        }
        value
            .chars()
            .map(|c| match c {
                '\t' | '\r' | '\n' => ' ',
                other => other,
            })
            .collect()
    }

    async fn write_line(&mut self, line: &str) -> Result<()> {
        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(line.as_bytes()).await?;
            stream.write_all(b"\n").await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for TSVWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        if record.is_empty() {
            return Ok(());
        }
        self.ensure_stream().await?;

        if !self.header_written {
            self.field_names = record.keys().cloned().collect();
            let header = self
                .field_names
                .iter()
                .map(|name| Self::sanitize_field(name))
                .collect::<Vec<_>>()
                .join("\t");
            self.write_line(&header).await?;
            self.header_written = true;
        } else if self.field_names.is_empty() {
            self.field_names = record.keys().cloned().collect();
        }

        let row = self
            .field_names
            .iter()
            .map(|name| {
                record
                    .get(name)
                    .map(|v| Self::sanitize_field(&value_to_string(v)))
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\t");
        self.write_line(&row).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        if let Some(mut stream) = self.output_stream.take() {
            stream.flush().await?;
            stream.shutdown().await?;
        }
        self.sink.finalize().await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        self.output_stream = None;
        self.sink.abort().await?;
        Ok(())
    }
}
