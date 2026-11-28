use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataWriter};

type AsyncBatchCallback = Arc<dyn Fn(Vec<DataRecord>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Buffers `DataRecord` items into batches of `batch_size` before invoking a callback.
pub struct BatchCallbackWriter {
    batch_size: usize,
    batch: Vec<DataRecord>,
    callback: AsyncBatchCallback,
    is_closed: bool,
}

impl BatchCallbackWriter {
    pub fn new<F>(batch_size: usize, callback: F) -> Self
    where
        F: Fn(Vec<DataRecord>) + Send + Sync + 'static,
    {
        let cb = Arc::new(callback);
        let effective_size = batch_size.max(1);
        Self {
            batch_size: effective_size,
            batch: Vec::with_capacity(effective_size),
            callback: Arc::new(move |batch: Vec<DataRecord>| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move {
                    cb_ref(batch);
                    Ok(())
                })
            }),
            is_closed: false,
        }
    }

    pub fn new_async<F, Fut>(batch_size: usize, callback: F) -> Self
    where
        F: Fn(Vec<DataRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let cb = Arc::new(callback);
        let effective_size = batch_size.max(1);
        Self {
            batch_size: effective_size,
            batch: Vec::with_capacity(effective_size),
            callback: Arc::new(move |batch: Vec<DataRecord>| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move { cb_ref(batch).await })
            }),
            is_closed: false,
        }
    }

    async fn flush(&mut self, from_write: bool) -> Result<()> {
        if !self.batch.is_empty() {
            // Keep self.batch intact until the callback succeeds so retries never lose earlier
            // buffered records. On write() failure, pop only the just-pushed record so retrying
            // write(record) replays the exact batch without duplicates.
            let payload = self.batch.clone();
            match (self.callback)(payload).await {
                Ok(()) => {
                    self.batch.clear();
                }
                Err(err) => {
                    if from_write {
                        self.batch.pop();
                    }
                    return Err(err);
                }
            }
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for BatchCallbackWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        self.batch.push(record);
        if self.batch.len() >= self.batch_size {
            self.flush(true).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.flush(false).await?;
        self.is_closed = true;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.is_closed = true;
        self.batch.clear();
        Ok(())
    }
}
