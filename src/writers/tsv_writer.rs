use async_trait::async_trait;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

/// Streaming Tab-Separated Values (TSV) writer.
pub struct TSVWriter {
    sink: Box<dyn DataSink>,
    field_names: Vec<String>,
    header_written: bool,
    output_stream: Option<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
}

impl TSVWriter {
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
        if record.is_empty() {
            return Ok(());
        }
        self.ensure_stream().await?;

        if !self.header_written {
            self.field_names = record.keys().cloned().collect();
            let header = self.field_names.join("\t");
            self.write_line(&header).await?;
            self.header_written = true;
        } else if self.field_names.is_empty() {
            self.field_names = record.keys().cloned().collect();
        }

        let row = self
            .field_names
            .iter()
            .map(|name| record.get(name).map(value_to_string).unwrap_or_default())
            .collect::<Vec<_>>()
            .join("\t");
        self.write_line(&row).await?;
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
