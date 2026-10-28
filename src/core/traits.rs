use async_trait::async_trait;
use futures::{Stream, StreamExt};
use indexmap::IndexMap;
use serde_json::Value;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use tokio::sync::Mutex;

use crate::core::error::Result;

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

/// Asynchronous stream of `DataRecord` items.
/// Yielding `Ok(None)` represents a null/empty record, which `Job` and `DataWriter` skip gracefully.
pub type RecordStream = Pin<Box<dyn Stream<Item = Result<Option<DataRecord>>> + Send>>;

/// Thread-safe handle to a shared `DataWriter` (used heavily for Dead Letter Queues).
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

/// Represents a raw byte source (File, S3, HTTP, Memory, etc.)
#[async_trait]
pub trait DataSource: Send + Sync {
    /// Returns an asynchronous byte reader.
    async fn get_reader(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>>;

    /// Identifier for logging and error messages.
    fn name(&self) -> &str;

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

/// Represents a raw byte sink (File, S3, HTTP, Memory, etc.)
#[async_trait]
pub trait DataSink: Send + Sync {
    /// Returns an asynchronous byte writer.
    async fn get_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send + Unpin>>>;

    /// Finalizes the sink (flushing buffers, completing multipart uploads, etc.).
    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Identifier for logging and error messages.
    fn name(&self) -> &str;

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
