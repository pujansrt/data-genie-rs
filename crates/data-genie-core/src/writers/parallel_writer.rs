use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataWriter};

type SyncBatchHandler = Arc<dyn Fn(Vec<DataRecord>) -> Result<()> + Send + Sync>;

enum WorkerBlueprint {
    Writers(Vec<Box<dyn DataWriter>>),
    Handler(SyncBatchHandler),
    Started,
}

#[derive(Debug, Clone)]
pub struct ParallelWriterOptions {
    pub concurrency: usize,
    pub batch_size: usize,
}

impl Default for ParallelWriterOptions {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map(|n| (n.get() / 2).max(1)).unwrap_or(2);
        Self {
            concurrency: cpus,
            batch_size: 100,
        }
    }
}

/// Offloads batch record processing/writing across a pool of concurrent Tokio worker tasks
/// with bounded channel backpressure.
///
/// Workers are spawned lazily on the first async `write()` so `ParallelWriter` can be constructed
/// safely outside a Tokio runtime (for example, before calling `Job::run_blocking`).
pub struct ParallelWriter {
    options: ParallelWriterOptions,
    buffer: Vec<DataRecord>,
    current_worker_index: usize,
    blueprint: WorkerBlueprint,
    senders: Vec<mpsc::Sender<Vec<DataRecord>>>,
    handles: Vec<JoinHandle<Result<()>>>,
    aborted: Arc<AtomicBool>,
    is_closed: bool,
}

impl ParallelWriter {
    /// Creates a `ParallelWriter` using a factory closure that constructs a dedicated `DataWriter` per worker.
    pub fn new<F, W>(options: ParallelWriterOptions, mut writer_factory: F) -> Self
    where
        F: FnMut(usize) -> W,
        W: DataWriter + 'static,
    {
        let concurrency = options.concurrency.max(1);
        let mut writers: Vec<Box<dyn DataWriter>> = Vec::with_capacity(concurrency);
        for idx in 0..concurrency {
            writers.push(Box::new(writer_factory(idx)));
        }

        Self {
            options: ParallelWriterOptions {
                concurrency,
                batch_size: options.batch_size.max(1),
            },
            buffer: Vec::new(),
            current_worker_index: 0,
            blueprint: WorkerBlueprint::Writers(writers),
            senders: Vec::new(),
            handles: Vec::new(),
            aborted: Arc::new(AtomicBool::new(false)),
            is_closed: false,
        }
    }

    /// Creates a `ParallelWriter` that runs a thread-safe handler closure across concurrent workers.
    pub fn from_handler<H>(options: ParallelWriterOptions, handler: H) -> Self
    where
        H: Fn(Vec<DataRecord>) -> Result<()> + Send + Sync + 'static,
    {
        let concurrency = options.concurrency.max(1);
        Self {
            options: ParallelWriterOptions {
                concurrency,
                batch_size: options.batch_size.max(1),
            },
            buffer: Vec::new(),
            current_worker_index: 0,
            blueprint: WorkerBlueprint::Handler(Arc::new(handler)),
            senders: Vec::new(),
            handles: Vec::new(),
            aborted: Arc::new(AtomicBool::new(false)),
            is_closed: false,
        }
    }

    fn ensure_started(&mut self) {
        let blueprint = std::mem::replace(&mut self.blueprint, WorkerBlueprint::Started);
        match blueprint {
            WorkerBlueprint::Writers(writers) => {
                self.senders.reserve(writers.len());
                self.handles.reserve(writers.len());
                for mut worker_writer in writers {
                    let (tx, mut rx) = mpsc::channel::<Vec<DataRecord>>(4);
                    let aborted = Arc::clone(&self.aborted);
                    self.senders.push(tx);

                    let handle = tokio::spawn(async move {
                        let mut write_err: Option<DataGenieError> = None;
                        while let Some(chunk) = rx.recv().await {
                            if aborted.load(Ordering::Acquire) {
                                break;
                            }
                            for record in chunk {
                                if aborted.load(Ordering::Acquire) {
                                    break;
                                }
                                if let Err(err) = worker_writer.write(record).await {
                                    write_err = Some(err);
                                    break;
                                }
                            }
                            if write_err.is_some() || aborted.load(Ordering::Acquire) {
                                break;
                            }
                        }

                        if let Some(err) = write_err {
                            let _ = worker_writer.abort().await;
                            return Err(err);
                        }
                        if aborted.load(Ordering::Acquire) {
                            let _ = worker_writer.abort().await;
                            return Ok(());
                        }
                        worker_writer.close().await?;
                        Ok(())
                    });
                    self.handles.push(handle);
                }
            }
            WorkerBlueprint::Handler(shared_handler) => {
                let concurrency = self.options.concurrency;
                self.senders.reserve(concurrency);
                self.handles.reserve(concurrency);
                for _ in 0..concurrency {
                    let h = Arc::clone(&shared_handler);
                    let (tx, mut rx) = mpsc::channel::<Vec<DataRecord>>(4);
                    let aborted = Arc::clone(&self.aborted);
                    self.senders.push(tx);

                    let handle = tokio::spawn(async move {
                        while let Some(chunk) = rx.recv().await {
                            if aborted.load(Ordering::Acquire) {
                                break;
                            }
                            h(chunk)?;
                        }
                        Ok(())
                    });
                    self.handles.push(handle);
                }
            }
            WorkerBlueprint::Started => {}
        }
    }

    async fn join_all_workers(&mut self) -> Result<()> {
        self.senders.clear();
        let handles = std::mem::take(&mut self.handles);
        let mut first_err: Option<DataGenieError> = None;

        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if first_err.is_none() {
                        first_err = Some(err);
                    }
                }
                Err(join_err) => {
                    if first_err.is_none() {
                        first_err = Some(DataGenieError::Other(format!("Parallel worker panicked: {}", join_err)));
                    }
                }
            }
        }

        match first_err {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    async fn dispatch(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.ensure_started();
        if self.senders.is_empty() {
            return Err(DataGenieError::Other("ParallelWriter has no active workers".to_string()));
        }

        let chunk = std::mem::take(&mut self.buffer);
        let sender = &self.senders[self.current_worker_index];
        self.current_worker_index = (self.current_worker_index + 1) % self.senders.len();

        if let Err(send_err) = sender.send(chunk).await {
            // A worker exited early (likely due to an error or panic). Signal remaining
            // workers to abort and collect all handles so we surface the real worker error.
            self.aborted.store(true, Ordering::Release);
            self.is_closed = true;
            self.join_all_workers().await?;
            return Err(DataGenieError::Other(format!("Parallel worker channel closed: {}", send_err)));
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for ParallelWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        self.buffer.push(record);
        if self.buffer.len() >= self.options.batch_size {
            self.dispatch().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;

        // If no records were ever dispatched, close any unstarted writers directly without spawning.
        if self.buffer.is_empty() && !matches!(self.blueprint, WorkerBlueprint::Started) {
            let blueprint = std::mem::replace(&mut self.blueprint, WorkerBlueprint::Started);
            if let WorkerBlueprint::Writers(mut writers) = blueprint {
                let mut first_err: Option<DataGenieError> = None;
                for writer in &mut writers {
                    if let Err(err) = writer.close().await
                        && first_err.is_none()
                    {
                        first_err = Some(err);
                    }
                }
                if let Some(err) = first_err {
                    return Err(err);
                }
            }
            return Ok(());
        }

        let dispatch_res = self.dispatch().await;
        // Always close all senders and await every worker handle so no task is detached.
        let join_res = self.join_all_workers().await;

        // Prefer an actual worker error over a secondary channel-closed dispatch error.
        join_res?;
        dispatch_res
    }

    async fn abort(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        self.aborted.store(true, Ordering::Release);
        self.buffer.clear();

        if !matches!(self.blueprint, WorkerBlueprint::Started) {
            let blueprint = std::mem::replace(&mut self.blueprint, WorkerBlueprint::Started);
            if let WorkerBlueprint::Writers(mut writers) = blueprint {
                for writer in &mut writers {
                    let _ = writer.abort().await;
                }
            }
            return Ok(());
        }

        let _ = self.join_all_workers().await;
        Ok(())
    }
}
