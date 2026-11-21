use async_trait::async_trait;
use std::borrow::Cow;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

use crate::core::error::Result;
use crate::core::traits::{BufferedByteReader, ByteReader, ByteWriter, DataSink, DataSource};

/// In-memory [`DataSource`] backed by a shared byte buffer with zero-copy streaming reads.
#[derive(Debug, Clone)]
pub struct MemorySource {
    data: Arc<[u8]>,
    identifier: String,
}

impl MemorySource {
    pub fn new<B: Into<Vec<u8>>>(data: B) -> Self {
        Self::with_name(data, "memory-source")
    }

    pub fn with_name<B: Into<Vec<u8>>, S: Into<String>>(data: B, identifier: S) -> Self {
        Self {
            data: Arc::from(data.into()),
            identifier: identifier.into(),
        }
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.data
    }

    fn cursor(&self) -> Cursor<Arc<[u8]>> {
        Cursor::new(Arc::clone(&self.data))
    }
}

#[async_trait]
impl DataSource for MemorySource {
    async fn get_reader(&self) -> Result<ByteReader> {
        Ok(Box::pin(self.cursor()))
    }

    async fn get_buf_reader(&self) -> Result<BufferedByteReader> {
        Ok(Box::pin(self.cursor()))
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.identifier)
    }

    async fn read_bytes(&self) -> Result<Vec<u8>> {
        Ok(self.data.to_vec())
    }
}

/// Shared internal buffer writer implementing [`AsyncWrite`].
pub struct SharedVecWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl SharedVecWriter {
    pub(crate) fn new(buffer: Arc<Mutex<Vec<u8>>>) -> Self {
        Self { buffer }
    }
}

impl AsyncWrite for SharedVecWriter {
    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let mut guard = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
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

/// In-memory [`DataSink`] that collects streamed bytes into a thread-safe shared buffer.
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
        Self::with_name("memory-sink")
    }

    pub fn with_name<S: Into<String>>(identifier: S) -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            identifier: identifier.into(),
        }
    }

    pub fn clear(&self) {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    pub fn get_data(&self) -> Vec<u8> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn get_text(&self) -> String {
        String::from_utf8_lossy(&self.buffer.lock().unwrap_or_else(|e| e.into_inner())).to_string()
    }
}

#[async_trait]
impl DataSink for MemorySink {
    async fn get_writer(&self) -> Result<ByteWriter> {
        Ok(Box::pin(SharedVecWriter::new(Arc::clone(&self.buffer))))
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.identifier)
    }
}
