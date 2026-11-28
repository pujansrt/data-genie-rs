use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataWriter};

/// Writes each `DataRecord` as JSON to stdout and prints a completion banner on close.
#[derive(Debug, Default, Clone)]
pub struct ConsoleWriter;

impl ConsoleWriter {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl DataWriter for ConsoleWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        let mut line = serde_json::to_vec(&record)?;
        line.push(b'\n');
        let mut stdout = tokio::io::stdout();
        stdout.write_all(&line).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let mut stdout = tokio::io::stdout();
        stdout.write_all(b"--- End of data stream ---\n").await?;
        stdout.flush().await?;
        Ok(())
    }
}
