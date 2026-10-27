use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{DataRecord, DataWriter};

#[derive(Debug, Clone)]
pub struct ParallelWriterOptions {
    pub concurrency: usize,
    pub batch_size: usize,
}

impl Default for ParallelWriterOptions {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|n| (n.get() / 2).max(1))
            .unwrap_or(2);
        Self {
            concurrency: cpus,
            batch_size: 100,
        }
    }
}

/// Offloads batch record processing/writing across a pool of concurrent Tokio worker tasks
/// with bounded channel backpressure.
pub struct ParallelWriter {
    options: ParallelWriterOptions,
    buffer: Vec<DataRecord>,
    current_worker_index: usize,
    senders: Vec<mpsc::Sender<Vec<DataRecord>>>,
    handles: Vec<JoinHandle<Result<()>>>,
}

impl ParallelWriter {
    /// Creates a `ParallelWriter` using a factory closure that constructs a dedicated `DataWriter` per worker.
    pub fn new<F, W>(options: ParallelWriterOptions, mut writer_factory: F) -> Self
    where
        F: FnMut(usize) -> W,
        W: DataWriter + 'static,
    {
        let concurrency = options.concurrency.max(1);
        let mut senders = Vec::with_capacity(concurrency);
        let mut handles = Vec::with_capacity(concurrency);

        for idx in 0..concurrency {
            let mut worker_writer = writer_factory(idx);
            let (tx, mut rx) = mpsc::channel::<Vec<DataRecord>>(4);
            senders.push(tx);

            let handle = tokio::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    for record in chunk {
                        worker_writer.write(record).await?;
                    }
                }
                worker_writer.close().await?;
                Ok(())
            });
            handles.push(handle);
        }

        Self {
            options: ParallelWriterOptions {
                concurrency,
                batch_size: options.batch_size.max(1),
            },
            buffer: Vec::new(),
            current_worker_index: 0,
            senders,
            handles,
        }
    }

    /// Creates a `ParallelWriter` that runs a thread-safe handler closure across concurrent workers.
    pub fn from_handler<H>(options: ParallelWriterOptions, handler: H) -> Self
    where
        H: Fn(Vec<DataRecord>) -> Result<()> + Send + Sync + 'static,
    {
        let shared_handler = Arc::new(handler);
        let concurrency = options.concurrency.max(1);
        let mut senders = Vec::with_capacity(concurrency);
        let mut handles = Vec::with_capacity(concurrency);

        for _ in 0..concurrency {
            let h = Arc::clone(&shared_handler);
            let (tx, mut rx) = mpsc::channel::<Vec<DataRecord>>(4);
            senders.push(tx);

            let handle = tokio::spawn(async move {
                while let Some(chunk) = rx.recv().await {
                    h(chunk)?;
                }
                Ok(())
            });
            handles.push(handle);
        }

        Self {
            options: ParallelWriterOptions {
                concurrency,
                batch_size: options.batch_size.max(1),
            },
            buffer: Vec::new(),
            current_worker_index: 0,
            senders,
            handles,
        }
    }

    async fn dispatch(&mut self) -> Result<()> {
        if self.buffer.is_empty() || self.senders.is_empty() {
            return Ok(());
        }
        let chunk = std::mem::take(&mut self.buffer);
        let sender = &self.senders[self.current_worker_index];
        self.current_worker_index = (self.current_worker_index + 1) % self.senders.len();

        sender
            .send(chunk)
            .await
            .map_err(|e| DataGenieError::Other(format!("Parallel worker channel closed: {}", e)))?;
        Ok(())
    }
}

#[async_trait]
impl DataWriter for ParallelWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.buffer.push(record);
        if self.buffer.len() >= self.options.batch_size {
            self.dispatch().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.dispatch().await?;
        // Drop all senders so workers finish their loops and close their writers
        self.senders.clear();

        let handles = std::mem::take(&mut self.handles);
        for handle in handles {
            let worker_res = handle
                .await
                .map_err(|e| DataGenieError::Other(format!("Parallel worker panicked: {}", e)))?;
            worker_res?;
        }
        Ok(())
    }
}
