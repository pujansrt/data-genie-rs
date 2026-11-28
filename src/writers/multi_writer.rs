use async_trait::async_trait;

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataWriter};

/// Fan-out writer that writes every record to multiple underlying `DataWriter` sinks.
pub struct MultiWriter {
    writers: Vec<Box<dyn DataWriter>>,
}

impl MultiWriter {
    /// Creates a fan-out writer starting with one writer.
    ///
    /// Use [`add`](Self::add) to attach additional writers without boxing them manually.
    pub fn from_writer<W: DataWriter + 'static>(writer: W) -> Self {
        Self {
            writers: vec![Box::new(writer)],
        }
    }

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
        if let Some((last, rest)) = self.writers.split_last_mut() {
            for writer in rest {
                writer.write(record.clone()).await?;
            }
            last.write(record).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let mut first_err = None;
        for writer in &mut self.writers {
            if let Err(err) = writer.close().await
                && first_err.is_none()
            {
                first_err = Some(err);
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        let mut first_err = None;
        for writer in &mut self.writers {
            if let Err(err) = writer.abort().await
                && first_err.is_none()
            {
                first_err = Some(err);
            }
        }
        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
