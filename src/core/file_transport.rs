use async_trait::async_trait;
use std::borrow::Cow;
use std::path::{Path, PathBuf};
use tokio::fs::{self, File};
use tokio::io::BufWriter;

use crate::core::error::Result;
use crate::core::traits::{ByteReader, ByteWriter, DEFAULT_IO_BUFFER_CAPACITY, DataSink, DataSource};

/// Local filesystem [`DataSource`] backed by `tokio::fs::File`.
#[derive(Debug, Clone)]
pub struct FileSource {
    file_path: PathBuf,
}

impl FileSource {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            file_path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

#[async_trait]
impl DataSource for FileSource {
    async fn get_reader(&self) -> Result<ByteReader> {
        let file = File::open(&self.file_path).await?;
        Ok(Box::pin(file))
    }

    fn name(&self) -> Cow<'_, str> {
        self.file_path.to_string_lossy()
    }

    fn as_file_path(&self) -> Option<&Path> {
        Some(&self.file_path)
    }

    async fn read_bytes(&self) -> Result<Vec<u8>> {
        Ok(fs::read(&self.file_path).await?)
    }
}

/// Local filesystem [`DataSink`] that creates parent directories automatically and streams bytes to disk with a 256 KB write buffer.
#[derive(Debug, Clone)]
pub struct FileSink {
    file_path: PathBuf,
}

impl FileSink {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        Self {
            file_path: path.as_ref().to_path_buf(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

#[async_trait]
impl DataSink for FileSink {
    async fn get_writer(&self) -> Result<ByteWriter> {
        if let Some(parent) = self.file_path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).await?;
        }
        let file = File::create(&self.file_path).await?;
        Ok(Box::pin(BufWriter::with_capacity(DEFAULT_IO_BUFFER_CAPACITY, file)))
    }

    fn name(&self) -> Cow<'_, str> {
        self.file_path.to_string_lossy()
    }

    fn as_file_path(&self) -> Option<&Path> {
        Some(&self.file_path)
    }
}
