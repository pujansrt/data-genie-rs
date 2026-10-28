use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::StreamReader;

use crate::core::error::Result;
use crate::core::memory_transport::SharedVecWriter;
use crate::core::traits::{DataSink, DataSource};

/// S3-compatible `DataSource` built on `object_store`.
/// Supports real AWS S3 buckets as well as custom `Arc<dyn ObjectStore>` (e.g., `InMemory` for tests or MinIO).
#[derive(Debug, Clone)]
pub struct S3Source {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    key: String,
    uri: String,
}

impl S3Source {
    /// Creates a new `S3Source` backed by an existing `ObjectStore` client.
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        let bucket = bucket.into();
        let key = key.into();
        let uri = format!("s3://{}/{}", bucket, key);
        Self { store, bucket, key, uri }
    }

    /// Creates a new `S3Source` using AWS S3 environment configuration.
    pub fn from_env(bucket: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let bucket_str = bucket.into();
        let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket_str).build()?;
        Ok(Self::new(Arc::new(s3), bucket_str, key))
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
}

#[async_trait]
impl DataSource for S3Source {
    async fn get_reader(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>> {
        let path = ObjectPath::from(self.key.as_str());
        let get_result = self.store.get(&path).await?;
        let stream = get_result.into_stream().map_err(std::io::Error::other);
        let reader = StreamReader::new(stream);
        Ok(Box::pin(reader))
    }

    fn name(&self) -> &str {
        &self.uri
    }
}

/// S3-compatible `DataSink` built on `object_store`.
#[derive(Debug, Clone)]
pub struct S3Sink {
    store: Arc<dyn ObjectStore>,
    bucket: String,
    key: String,
    content_type: String,
    uri: String,
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl S3Sink {
    pub fn new(store: Arc<dyn ObjectStore>, bucket: impl Into<String>, key: impl Into<String>) -> Self {
        let bucket = bucket.into();
        let key = key.into();
        let uri = format!("s3://{}/{}", bucket, key);
        Self {
            store,
            bucket,
            key,
            content_type: "application/octet-stream".to_string(),
            uri,
            buffer: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = content_type.into();
        self
    }

    pub fn from_env(bucket: impl Into<String>, key: impl Into<String>) -> Result<Self> {
        let bucket_str = bucket.into();
        let s3 = AmazonS3Builder::from_env().with_bucket_name(&bucket_str).build()?;
        Ok(Self::new(Arc::new(s3), bucket_str, key))
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
    async fn get_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send + Unpin>>> {
        Ok(Box::pin(SharedVecWriter {
            buffer: Arc::clone(&self.buffer),
        }))
    }

    async fn finalize(&mut self) -> Result<()> {
        let data = {
            let guard = self.buffer.lock().unwrap();
            Bytes::from(guard.clone())
        };
        let path = ObjectPath::from(self.key.as_str());
        self.store.put(&path, PutPayload::from(data)).await?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.uri
    }
}
