use async_trait::async_trait;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};

use crate::core::error::Result;
use crate::core::traits::{DataSink, DataSource};

/// In-memory `DataSource` backed by a byte buffer or string.
#[derive(Debug, Clone)]
pub struct MemorySource {
    data: Arc<Vec<u8>>,
    identifier: String,
}

impl MemorySource {
    pub fn new<B: Into<Vec<u8>>>(data: B) -> Self {
        Self {
            data: Arc::new(data.into()),
            identifier: "memory-source".to_string(),
        }
    }

    pub fn with_name<B: Into<Vec<u8>>, S: Into<String>>(data: B, identifier: S) -> Self {
        Self {
            data: Arc::new(data.into()),
            identifier: identifier.into(),
        }
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.data
    }
}

#[async_trait]
impl DataSource for MemorySource {
    async fn get_reader(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>> {
        let cursor = Cursor::new((*self.data).clone());
        Ok(Box::pin(cursor))
    }

    fn name(&self) -> &str {
        &self.identifier
    }

    async fn read_bytes(&self) -> Result<Vec<u8>> {
        Ok((*self.data).clone())
    }
}

/// Shared internal buffer writer implementing `AsyncWrite`.
pub struct SharedVecWriter {
    pub(crate) buffer: Arc<Mutex<Vec<u8>>>,
}

impl AsyncWrite for SharedVecWriter {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let mut guard = self.buffer.lock().unwrap();
        guard.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// In-memory `DataSink` that collects streamed bytes into a thread-safe shared buffer.
#[derive(Debug, Clone)]
pub struct MemorySink {
    buffer: Arc<Mutex<Vec<u8>>>,
    identifier: String,
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl MemorySink {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            identifier: "memory-sink".to_string(),
        }
    }

    pub fn with_name<S: Into<String>>(identifier: S) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            identifier: identifier.into(),
        }
    }

    pub fn clear(&self) {
        self.buffer.lock().unwrap().clear();
    }

    pub fn get_data(&self) -> Vec<u8> {
        self.buffer.lock().unwrap().clone()
    }

    pub fn get_text(&self) -> String {
        String::from_utf8_lossy(&self.buffer.lock().unwrap()).to_string()
    }
}

#[async_trait]
impl DataSink for MemorySink {
    async fn get_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send + Unpin>>> {
        Ok(Box::pin(SharedVecWriter {
            buffer: Arc::clone(&self.buffer),
        }))
    }

    async fn finalize(&mut self) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        &self.identifier
    }
}
