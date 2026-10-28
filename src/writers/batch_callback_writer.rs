use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataWriter};

type AsyncBatchCallback = Arc<dyn Fn(Vec<DataRecord>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Buffers `DataRecord` items into batches of `batch_size` before invoking a callback.
pub struct BatchCallbackWriter {
    batch_size: usize,
    batch: Vec<DataRecord>,
    callback: AsyncBatchCallback,
}

impl BatchCallbackWriter {
    pub fn new<F>(batch_size: usize, callback: F) -> Self
    where
        F: Fn(Vec<DataRecord>) + Send + Sync + 'static,
    {
        let cb = Arc::new(callback);
        Self {
            batch_size: batch_size.max(1),
            batch: Vec::new(),
            callback: Arc::new(move |batch: Vec<DataRecord>| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move {
                    cb_ref(batch);
                    Ok(())
                })
            }),
        }
    }

    pub fn new_async<F, Fut>(batch_size: usize, callback: F) -> Self
    where
        F: Fn(Vec<DataRecord>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let cb = Arc::new(callback);
        Self {
            batch_size: batch_size.max(1),
            batch: Vec::new(),
            callback: Arc::new(move |batch: Vec<DataRecord>| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move { cb_ref(batch).await })
            }),
        }
    }

    async fn flush(&mut self) -> Result<()> {
        if !self.batch.is_empty() {
            let current_batch = std::mem::take(&mut self.batch);
            (self.callback)(current_batch).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for BatchCallbackWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.batch.push(record);
        if self.batch.len() >= self.batch_size {
            self.flush().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.flush().await
    }
}
