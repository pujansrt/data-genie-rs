use async_trait::async_trait;
use std::future::Future;
use std::pin::Pin;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataWriter};

type SyncCallback = Box<dyn FnMut(DataRecord) + Send>;
type AsyncCallback = Box<dyn FnMut(DataRecord) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send>;

enum CallbackKind {
    Sync(SyncCallback),
    Async(AsyncCallback),
}

/// Executes a custom closure or async callback for each `DataRecord` emitted by a pipeline.
pub struct CallbackWriter {
    callback: CallbackKind,
    is_closed: bool,
}

impl CallbackWriter {
    /// Creates a `CallbackWriter` from an infallible synchronous closure (zero per-record future boxing).
    pub fn new<F>(callback: F) -> Self
    where
        F: FnMut(DataRecord) + Send + 'static,
    {
        Self {
            callback: CallbackKind::Sync(Box::new(callback)),
            is_closed: false,
        }
    }

    /// Creates a `CallbackWriter` from a fallible asynchronous closure.
    pub fn new_async<F, Fut>(mut callback: F) -> Self
    where
        F: FnMut(DataRecord) -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self {
            callback: CallbackKind::Async(Box::new(move |rec: DataRecord| Box::pin(callback(rec)))),
            is_closed: false,
        }
    }
}

#[async_trait]
impl DataWriter for CallbackWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        match &mut self.callback {
            CallbackKind::Sync(cb) => {
                cb(record);
                Ok(())
            }
            CallbackKind::Async(cb) => cb(record).await,
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.is_closed = true;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        self.is_closed = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::block_on;
    use crate::record;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_callback_writer_sync_fn_mut_async_and_lifecycle() {
        block_on(async {
            // Sync FnMut captures local mutable state directly without Arc<Mutex<...>>
            let mut seen_ids: Vec<i64> = Vec::new();
            let collected = Arc::new(Mutex::new(Vec::new()));
            let out_ref = Arc::clone(&collected);
            let mut sync_writer = CallbackWriter::new(move |rec| {
                let id = rec.get("id").and_then(|v| v.as_i64()).unwrap();
                seen_ids.push(id);
                *out_ref.lock().unwrap() = seen_ids.clone();
            });
            sync_writer.write(record! { "id" => 1 }).await.unwrap();
            sync_writer.write(record! { "id" => 2 }).await.unwrap();
            sync_writer.close().await.unwrap();
            sync_writer.close().await.unwrap(); // idempotent
            assert!(sync_writer.write(record! { "id" => 3 }).await.is_err());
            assert_eq!(*collected.lock().unwrap(), vec![1, 2]);

            // Async callback happy path + error propagation + abort
            let async_seen = Arc::new(Mutex::new(Vec::<i64>::new()));
            let async_ref = Arc::clone(&async_seen);
            let mut async_writer = CallbackWriter::new_async(move |rec| {
                let target = Arc::clone(&async_ref);
                async move {
                    let id = rec.get("id").and_then(|v| v.as_i64()).unwrap();
                    if id < 0 {
                        return Err(DataGenieError::Validation("negative id".to_string()));
                    }
                    target.lock().unwrap().push(id);
                    Ok(())
                }
            });
            async_writer.write(record! { "id" => 10 }).await.unwrap();
            assert!(matches!(
                async_writer.write(record! { "id" => -1 }).await,
                Err(DataGenieError::Validation(_))
            ));
            async_writer.abort().await.unwrap();
            assert!(async_writer.write(record! { "id" => 20 }).await.is_err());
            assert_eq!(*async_seen.lock().unwrap(), vec![10]);
        });
    }
}
