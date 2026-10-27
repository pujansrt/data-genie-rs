use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use tokio::fs::{self, File};
use tokio::io::{AsyncRead, AsyncWrite, BufReader, BufWriter};

use crate::core::error::Result;
use crate::core::interfaces::{DataSink, DataSource};

const IO_BUFFER_CAPACITY: usize = 256 * 1024; // 256 KB buffer to minimize OS syscalls & threadpool hops

/// Local filesystem `DataSource` streaming bytes asynchronously with a 256KB read buffer.
#[derive(Debug, Clone)]
pub struct FileSource {
    file_path: PathBuf,
    display_name: String,
}

impl FileSource {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file_path = path.as_ref().to_path_buf();
        let display_name = file_path.to_string_lossy().to_string();
        Self {
            file_path,
            display_name,
        }
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

#[async_trait]
impl DataSource for FileSource {
    async fn get_reader(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>> {
        let file = File::open(&self.file_path).await?;
        Ok(Box::pin(BufReader::with_capacity(IO_BUFFER_CAPACITY, file)))
    }

    fn name(&self) -> &str {
        &self.display_name
    }

    fn as_file_path(&self) -> Option<&Path> {
        Some(&self.file_path)
    }
}

/// Local filesystem `DataSink` creating parent directories automatically and streaming bytes to disk with a 256KB write buffer.
#[derive(Debug, Clone)]
pub struct FileSink {
    file_path: PathBuf,
    display_name: String,
}

impl FileSink {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file_path = path.as_ref().to_path_buf();
        let display_name = file_path.to_string_lossy().to_string();
        Self {
            file_path,
            display_name,
        }
    }

    pub fn path(&self) -> &Path {
        &self.file_path
    }
}

#[async_trait]
impl DataSink for FileSink {
    async fn get_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send + Unpin>>> {
        if let Some(parent) = self.file_path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).await?;
            }
        }
        let file = File::create(&self.file_path).await?;
        Ok(Box::pin(BufWriter::with_capacity(IO_BUFFER_CAPACITY, file)))
    }

    fn name(&self) -> &str {
        &self.display_name
    }

    fn as_file_path(&self) -> Option<&Path> {
        Some(&self.file_path)
    }
}
