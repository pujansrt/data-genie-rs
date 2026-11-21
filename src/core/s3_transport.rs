use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

use crate::core::error::Result;
use crate::core::memory_transport::SharedVecWriter;
use crate::core::traits::{BufferedByteReader, ByteReader, ByteWriter, DataSink, DataSource};

/// S3-compatible [`DataSource`] built on `object_store`.
/// Supports real AWS S3 buckets as well as custom `Arc<dyn ObjectStore>` (e.g., `InMemory` for tests or MinIO).
#[derive(Debug, Clone)]
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    key: String,
}

impl S3Source {
    /// Creates a new `S3Source` backed by an existing `ObjectStore` client.
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// Creates a new `S3Source` using AWS S3 environment configuration.
    pub fn from_env(bucket: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket).build()?;
        Ok(Self::new(Arc::new(s3), bucket, key))
    }

    /// Creates an in-memory S3 mock store for testing.
    pub fn in_memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    async fn open_stream_reader(&self) -> Result<impl AsyncBufRead + Send + Unpin + use<>> {
        let path = ObjectPath::from(self.key.as_str());
        let get_result = self.store.get(&path).await?;
        let stream = get_result.into_stream().map_err(std::io::Error::other);
        Ok(StreamReader::new(stream))
    }
}

#[async_trait]
impl DataSource for S3Source {
    async fn get_reader(&self) -> Result<ByteReader> {
        Ok(Box::pin(self.open_stream_reader().await?))
    }

    async fn get_buf_reader(&self) -> Result<BufferedByteReader> {
        Ok(Box::pin(self.open_stream_reader().await?))
    }

    async fn read_bytes(&self) -> Result<Vec<u8>> {
        let path = ObjectPath::from(self.key.as_str());
        let get_result = self.store.get(&path).await?;
        Ok(get_result.bytes().await?.to_vec())
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Owned(format!("s3://{}/{}", self.bucket, self.key))
    }
}

/// S3-compatible [`DataSink`] built on `object_store`.
#[derive(Debug, Clone)]
pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    key: String,
    content_type: String,
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl S3Sink {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            store,
            bucket: bucket.into(),
            key: key.into(),
            content_type: "application/octet-stream".to_string(),
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn from_env(bucket: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let bucket = bucket.into();
        let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket).build()?;
        Ok(Self::new(Arc::new(s3), bucket, key))
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn content_type(&self) -> &str {
        &self.content_type
    }
}

#[async_trait]
impl DataSink for S3Sink {
    async fn get_writer(&self) -> Result<ByteWriter> {
        Ok(Box::pin(SharedVecWriter::new(Arc::clone(&self.buffer))))
    }

    async fn finalize(&mut self) -> Result<()> {
        let data = {
            let mut guard = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
            Bytes::from(std::mem::take(&mut *guard))
        };
        let path = ObjectPath::from(self.key.as_str());
        self.store.put(&path, PutPayload::from(data)).await?;
        Ok(())
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Owned(format!("s3://{}/{}", self.bucket, self.key))
    }
}
