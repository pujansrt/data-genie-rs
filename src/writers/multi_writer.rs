use async_trait::async_trait;

use crate::core::error::Result;
use crate::core::interfaces::{DataRecord, DataWriter};

/// Fan-out writer that writes every record to multiple underlying `DataWriter` sinks.
pub struct MultiWriter {
    writers: Vec<Box<dyn DataWriter>>,
}

impl MultiWriter {
    pub fn new(writers: Vec<Box<dyn DataWriter>>) -> Self {
        Self { writers }
    }

    pub fn add<W: DataWriter + 'static>(mut self, writer: W) -> Self {
        self.writers.push(Box::new(writer));
        self
    }
}

#[async_trait]
impl DataWriter for MultiWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        for writer in &mut self.writers {
            writer.write(record.clone()).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        for writer in &mut self.writers {
            writer.close().await?;
        }
        Ok(())
    }
}
