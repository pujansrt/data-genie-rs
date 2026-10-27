use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::error::Result;
use crate::core::interfaces::{DataRecord, DataWriter};

type AsyncCallback =
    Arc<dyn Fn(DataRecord) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// Executes a custom closure or async callback for each `DataRecord` emitted by a pipeline.
pub struct CallbackWriter {
    callback: AsyncCallback,
}

impl CallbackWriter {
    /// Creates a `CallbackWriter` from an infallible synchronous closure.
    pub fn new<F>(callback: F) -> Self
    where
        F: Fn(DataRecord) + Send + Sync + 'static,
    {
        let cb = Arc::new(callback);
        Self {
            callback: Arc::new(move |rec: DataRecord| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move {
                    cb_ref(rec);
                    Ok(())
                })
            }),
        }
    }

    /// Creates a `CallbackWriter` from a fallible or asynchronous closure.
    pub fn new_async<F, Fut>(callback: F) -> Self
    where
        F: Fn(DataRecord) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let cb = Arc::new(callback);
        Self {
            callback: Arc::new(move |rec: DataRecord| {
                let cb_ref = Arc::clone(&cb);
                Box::pin(async move { cb_ref(rec).await })
            }),
        }
    }
}

#[async_trait]
impl DataWriter for CallbackWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        (self.callback)(record).await
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}
