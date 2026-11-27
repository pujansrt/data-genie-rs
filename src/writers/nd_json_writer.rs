use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{ByteWriter, DataRecord, DataSink, DataWriter};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

/// Streaming Newline-Delimited JSON (NDJSON) writer.
pub struct NDJsonWriter {
    sink: Box<dyn DataSink>,
    output_stream: Option<ByteWriter>,
    is_closed: bool,
}

impl NDJsonWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            output_stream: None,
            is_closed: false,
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
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        self.ensure_stream().await?;
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');

        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(&line).await?;
        }
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
