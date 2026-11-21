use async_trait::async_trait;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use serde_json::Value;
use std::borrow::Cow;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, BufReader};
use tokio::sync::Mutex;

use crate::core::error::Result;

/// Default buffer size (256 KB) for streaming byte readers and writers to minimize OS syscalls.
pub const DEFAULT_IO_BUFFER_CAPACITY: usize = 256 * 1024;

/// Any asynchronous byte reader returned by a [`DataSource`] (file, memory buffer, HTTP, S3, etc.).
///
/// ### Why `Pin<Box<dyn AsyncRead + Send + Unpin>>`?
/// - `dyn AsyncRead` — allows any concrete async byte reader behind one common type.
/// - `Box<...>` — stores the reader on the heap because different readers have different sizes.
/// - `Pin<...>` — required by Rust's async I/O polling model so the reader cannot move in memory while polled.
/// - `+ Send` — safe to move across Tokio's multi-threaded worker pool.
/// - `+ Unpin` — lets callers use `.read()` / `.read_to_end()` directly without manual pinning.
///
/// **How to return one:** wrap your concrete reader with `Box::pin(reader)`.
pub type ByteReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Any asynchronous *buffered* byte reader returned by [`DataSource::get_buf_reader`].
///
/// Same as [`ByteReader`], except it also implements [`AsyncBufRead`] so line-oriented readers
/// (`CSVReader`, `NDJsonReader`, `FixedWidthReader`) can call `.read_line()` / `.lines()` directly
/// without wrapping an already-buffered source in a second `BufReader`.
///
/// **How to return one:** wrap your buffered reader (e.g. `BufReader`, `Cursor`, `StreamReader`) with `Box::pin(reader)`.
pub type BufferedByteReader = Pin<Box<dyn AsyncBufRead + Send + Unpin>>;

/// Any asynchronous byte writer returned by a [`DataSink`] (file, memory buffer, HTTP, S3, etc.).
///
/// ### Why `Pin<Box<dyn AsyncWrite + Send + Unpin>>`?
/// - `dyn AsyncWrite` — allows any concrete async byte writer behind one common type.
/// - `Box<...>` — heap-allocated so all writer implementations share a single size.
/// - `Pin<...>` — guarantees the writer stays at a fixed memory address while Tokio polls writes.
/// - `+ Send + Unpin` — safe across Tokio worker threads and ergonomic for `.write_all()` / `.flush()`.
///
/// **How to return one:** wrap your concrete writer with `Box::pin(writer)`.
pub type ByteWriter = Pin<Box<dyn AsyncWrite + Send + Unpin>>;

/// Ordered map of field names to JSON values representing a single streaming record.
/// Preserves insertion order for deterministic CSV/TSV/SQL/FixedWidth headers.
pub type DataRecord = IndexMap<String, Value>;

/// Convenience macro to construct a `DataRecord` with preserved key ordering.
#[macro_export]
macro_rules! record {
    () => {
        $crate::DataRecord::new()
    };
    ($($k:expr => $v:expr),+ $(,)?) => {{
        let mut map = $crate::DataRecord::new();
        $(
            map.insert(($k).to_string(), serde_json::json!($v));
        )+
        map
    }};
}

/// Convert any JSON Value into a clean string representation (without quotes around strings).
pub fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Parse a numeric value from a JSON Value (works for both `Value::Number` and numeric strings).
pub fn value_to_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Asynchronous stream of [`DataRecord`] items (like an async `Iterator`).
///
/// Yielding `Ok(None)` represents a null/empty record, which `Job` and [`DataWriter`] skip gracefully.
///
/// ### Why `Pin<Box<dyn Stream<...> + Send>>`?
/// - `dyn Stream<Item = ...>` — type-erases the async generator returned by each [`DataReader`].
/// - `Pin<Box<...>>` — async streams hold self-referential state across `.await` points, so they must be pinned on the heap before calling `.next().await`.
/// - `+ Send` — safe to poll across Tokio worker threads.
///
/// **How to return one:** `Box::pin(async_stream::try_stream! { yield Some(record); })`.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Option<DataRecord>>> + Send>>;

/// Thread-safe, cheaply cloneable handle to a shared [`DataWriter`] (used heavily for Dead Letter Queues).
///
/// Internally wraps `Box<dyn DataWriter>` in an `Arc<tokio::sync::Mutex<...>>` so multiple pipeline stages
/// (e.g. `CSVReader` and `ValidatingReader`) can safely write rejected records to the same destination.
#[derive(Clone)]
pub struct SharedWriter {
    inner: Arc<Mutex<Box<dyn DataWriter>>>,
}

impl SharedWriter {
    pub fn new<W: DataWriter + 'static>(writer: W) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Box::new(writer))),
        }
    }

    pub async fn write_dlq(&self, record: DataRecord) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.write(record).await
    }

    pub async fn close_dlq(&self) -> Result<()> {
        let mut guard = self.inner.lock().await;
        guard.close().await
    }

    /// Synchronously flushes and closes the underlying DLQ writer without requiring an async context.
    pub fn close_dlq_blocking(&self) -> Result<()> {
        crate::core::job::block_on(self.close_dlq())
    }
}

impl<W: DataWriter + 'static> From<W> for SharedWriter {
    fn from(writer: W) -> Self {
        SharedWriter::new(writer)
    }
}

/// Represents a raw byte source (File, S3, HTTP, Memory, or custom).
///
/// Implementers only need to provide [`get_reader`](Self::get_reader) and [`name`](Self::name).
/// Buffered reading ([`get_buf_reader`](Self::get_buf_reader)) and full-buffer loading
/// ([`read_bytes`](Self::read_bytes)) have defaults that can be overridden for zero-copy
/// or single-buffer fast paths.
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Returns an asynchronous byte reader.
    async fn get_reader(&self) -> Result<ByteReader>;

    /// Returns a buffered asynchronous byte reader suitable for line-by-line streaming.
    ///
    /// Defaults to wrapping [`get_reader`](Self::get_reader) in a 256 KB [`BufReader`].
    /// Sources already backed by an in-memory buffer or buffered stream should override this
    /// to avoid redundant buffering.
    async fn get_buf_reader(&self) -> Result<BufferedByteReader> {
        let reader = self.get_reader().await?;
        Ok(Box::pin(BufReader::with_capacity(DEFAULT_IO_BUFFER_CAPACITY, reader)))
    }

    /// Identifier for logging and error messages.
    fn name(&self) -> Cow<'_, str>;

    /// Returns the local filesystem path if backed directly by a local file.
    fn as_file_path(&self) -> Option<&Path> {
        None
    }

    /// Reads the entire source into memory as bytes.
    async fn read_bytes(&self) -> Result<Vec<u8>> {
        let mut reader = self.get_reader().await?;
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await?;
        Ok(buf)
    }
}

/// Represents a raw byte sink (File, S3, HTTP, Memory, or custom).
///
/// Implementers only need to provide [`get_writer`](Self::get_writer) and [`name`](Self::name).
#[async_trait]
pub trait DataSink: Send + Sync {
    /// Returns an asynchronous byte writer.
    async fn get_writer(&self) -> Result<ByteWriter>;

    /// Finalizes the sink (flushing buffers, completing multipart uploads, etc.).
    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Identifier for logging and error messages.
    fn name(&self) -> Cow<'_, str>;

    /// Returns the local filesystem path if backed directly by a local file.
    fn as_file_path(&self) -> Option<&Path> {
        None
    }
}

/// Streaming record reader interface.
pub trait DataReader: Send {
    /// Returns an async stream of records.
    fn read(&mut self) -> RecordStream;

    /// Sets a Dead Letter Queue (DLQ) writer and propagates it to any upstream reader.
    fn set_dlq(&mut self, _writer: SharedWriter) {}

    /// Configures whether parsing/validation errors should be ignored (diverted to DLQ if set).
    fn set_ignore_errors(&mut self, _ignore: bool) {}
}

/// Streaming record writer interface.
#[async_trait]
pub trait DataWriter: Send {
    /// Writes a single record to the destination.
    async fn write(&mut self, record: DataRecord) -> Result<()>;

    /// Writes a single optional record, gracefully skipping `None` or completely empty/null records.
    async fn write_opt(&mut self, record: Option<DataRecord>) -> Result<()> {
        if let Some(rec) = record {
            self.write(rec).await?;
        }
        Ok(())
    }

    /// Consumes an entire `RecordStream`, skipping `None` records and writing each valid record.
    async fn write_all(&mut self, records: &mut RecordStream) -> Result<()> {
        while let Some(item) = records.next().await {
            if let Some(record) = item? {
                self.write(record).await?;
            }
        }
        Ok(())
    }

    /// Flushes and closes the underlying resource.
    async fn close(&mut self) -> Result<()>;
}

#[async_trait]
impl DataWriter for Box<dyn DataWriter> {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        (**self).write(record).await
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }
}

/// Driver-agnostic SQL database connection interface.
#[async_trait]
pub trait SqlConnection: Send + Sync {
    /// Executes a SQL query with positional parameters and returns rows as `Vec<DataRecord>`.
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<DataRecord>>;

    /// Starts a database transaction if supported.
    async fn begin_transaction(&self) -> Result<()> {
        Ok(())
    }

    /// Commits the current database transaction if supported.
    async fn commit(&self) -> Result<()> {
        Ok(())
    }

    /// Rolls back the current database transaction if supported.
    async fn rollback(&self) -> Result<()> {
        Ok(())
    }

    /// Whether this connection supports explicit transactions.
    fn supports_transactions(&self) -> bool {
        true
    }
}

/// Pluggable logger trait for job execution observability.
pub trait Logger: Send + Sync {
    fn info(&self, message: &str);
    fn warn(&self, message: &str);
    fn error(&self, message: &str);
    fn debug(&self, message: &str) {
        let _ = message;
    }
}

/// Default stdout/stderr console logger.
#[derive(Debug, Clone, Default)]
pub struct ConsoleLogger;

impl Logger for ConsoleLogger {
    fn info(&self, message: &str) {
        println!("{}", message);
    }

    fn warn(&self, message: &str) {
        eprintln!("WARN: {}", message);
    }

    fn error(&self, message: &str) {
        eprintln!("ERROR: {}", message);
    }

    fn debug(&self, message: &str) {
        println!("DEBUG: {}", message);
    }
}
