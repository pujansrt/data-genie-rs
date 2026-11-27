use async_trait::async_trait;
use std::sync::{Arc, Mutex};

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataWriter};

/// Stores written records in a thread-safe shared in-memory `Vec<DataRecord>`.
/// Cloning `MemoryWriter` shares the underlying buffer so tests and DLQ inspectors can read emitted records after a job completes.
#[derive(Debug, Default, Clone)]
pub struct MemoryWriter {
    records: Arc<Mutex<Vec<DataRecord>>>,
}

impl MemoryWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_records(&self) -> Vec<DataRecord> {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn len(&self) -> usize {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    pub fn clear(&self) {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }
}

#[async_trait]
impl DataWriter for MemoryWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.records.lock().unwrap_or_else(|e| e.into_inner()).push(record);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
