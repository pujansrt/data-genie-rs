use async_trait::async_trait;

use crate::core::error::Result;
use crate::core::interfaces::{DataRecord, DataWriter};

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
        println!("{}", serde_json::to_string(&record)?);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        println!("--- End of data stream ---");
        Ok(())
    }
}
