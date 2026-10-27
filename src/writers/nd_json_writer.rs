use async_trait::async_trait;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::error::Result;
use crate::core::interfaces::{DataRecord, DataSink, DataWriter};
use crate::core::transport_utils::{ensure_data_sink, SinkInput};

/// Streaming Newline-Delimited JSON (NDJSON) writer.
pub struct NDJsonWriter {
    sink: Box<dyn DataSink>,
    output_stream: Option<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
}

impl NDJsonWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            output_stream: None,
        }
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        if self.output_stream.is_none() {
            self.output_stream = Some(self.sink.get_writer().await?);
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for NDJsonWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.ensure_stream().await?;
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');

        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(&line).await?;
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
