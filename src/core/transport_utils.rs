use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::file_transport::{FileSink, FileSource};
#[cfg(feature = "http")]
use crate::core::http_transport::{HttpSink, HttpSource};
use crate::core::memory_transport::{MemorySink, MemorySource};
#[cfg(feature = "s3")]
use crate::core::s3_transport::{S3Sink, S3Source};
use crate::core::traits::{DataSink, DataSource};

/// Flexible input wrapper allowing readers to accept file paths (`&str`, `String`, `PathBuf`),
/// raw byte buffers (`Vec<u8>`, `&[u8]`), or any `DataSource` (`MemorySource`, `FileSource`, `HttpSource`, `S3Source`).
#[derive(Clone)]
pub struct SourceInput(pub Arc<dyn DataSource>);

impl SourceInput {
    pub fn into_inner(self) -> Arc<dyn DataSource> {
        self.0
    }
}

/// Ensures any supported input is converted into an `Arc<dyn DataSource>`.
pub fn ensure_data_source<S: Into<SourceInput>>(input: S) -> Arc<dyn DataSource> {
    input.into().into_inner()
}

impl From<&str> for SourceInput {
    fn from(s: &str) -> Self {
        SourceInput(Arc::new(FileSource::new(s)))
    }
}

impl From<String> for SourceInput {
    fn from(s: String) -> Self {
        SourceInput(Arc::new(FileSource::new(s)))
    }
}

impl From<&String> for SourceInput {
    fn from(s: &String) -> Self {
        SourceInput(Arc::new(FileSource::new(s.as_str())))
    }
}

impl From<&Path> for SourceInput {
    fn from(p: &Path) -> Self {
        SourceInput(Arc::new(FileSource::new(p)))
    }
}

impl From<PathBuf> for SourceInput {
    fn from(p: PathBuf) -> Self {
        SourceInput(Arc::new(FileSource::new(p)))
    }
}

impl From<&PathBuf> for SourceInput {
    fn from(p: &PathBuf) -> Self {
        SourceInput(Arc::new(FileSource::new(p.as_path())))
    }
}

impl From<Vec<u8>> for SourceInput {
    fn from(bytes: Vec<u8>) -> Self {
        SourceInput(Arc::new(MemorySource::new(bytes)))
    }
}

impl From<&[u8]> for SourceInput {
    fn from(bytes: &[u8]) -> Self {
        SourceInput(Arc::new(MemorySource::new(bytes.to_vec())))
    }
}

impl<const N: usize> From<&[u8; N]> for SourceInput {
    fn from(bytes: &[u8; N]) -> Self {
        SourceInput(Arc::new(MemorySource::new(bytes.to_vec())))
    }
}

impl From<FileSource> for SourceInput {
    fn from(src: FileSource) -> Self {
        SourceInput(Arc::new(src))
    }
}

impl From<MemorySource> for SourceInput {
    fn from(src: MemorySource) -> Self {
        SourceInput(Arc::new(src))
    }
}

impl From<&MemorySource> for SourceInput {
    fn from(src: &MemorySource) -> Self {
        SourceInput(Arc::new(src.clone()))
    }
}

#[cfg(feature = "http")]
impl From<HttpSource> for SourceInput {
    fn from(src: HttpSource) -> Self {
        SourceInput(Arc::new(src))
    }
}

#[cfg(feature = "s3")]
impl From<S3Source> for SourceInput {
    fn from(src: S3Source) -> Self {
        SourceInput(Arc::new(src))
    }
}

impl From<Arc<dyn DataSource>> for SourceInput {
    fn from(src: Arc<dyn DataSource>) -> Self {
        SourceInput(src)
    }
}

/// Flexible sink wrapper allowing writers to accept file paths or any `DataSink`.
pub struct SinkInput(pub Box<dyn DataSink>);

impl SinkInput {
    pub fn into_inner(self) -> Box<dyn DataSink> {
        self.0
    }
}

/// Ensures any supported sink input is converted into a `Box<dyn DataSink>`.
pub fn ensure_data_sink<S: Into<SinkInput>>(input: S) -> Box<dyn DataSink> {
    input.into().into_inner()
}

impl From<&str> for SinkInput {
    fn from(s: &str) -> Self {
        SinkInput(Box::new(FileSink::new(s)))
    }
}

impl From<String> for SinkInput {
    fn from(s: String) -> Self {
        SinkInput(Box::new(FileSink::new(s)))
    }
}

impl From<&String> for SinkInput {
    fn from(s: &String) -> Self {
        SinkInput(Box::new(FileSink::new(s.as_str())))
    }
}

impl From<&Path> for SinkInput {
    fn from(p: &Path) -> Self {
        SinkInput(Box::new(FileSink::new(p)))
    }
}

impl From<PathBuf> for SinkInput {
    fn from(p: PathBuf) -> Self {
        SinkInput(Box::new(FileSink::new(p)))
    }
}

impl From<&PathBuf> for SinkInput {
    fn from(p: &PathBuf) -> Self {
        SinkInput(Box::new(FileSink::new(p.as_path())))
    }
}

impl From<FileSink> for SinkInput {
    fn from(sink: FileSink) -> Self {
        SinkInput(Box::new(sink))
    }
}

impl From<MemorySink> for SinkInput {
    fn from(sink: MemorySink) -> Self {
        SinkInput(Box::new(sink))
    }
}

impl From<&MemorySink> for SinkInput {
    fn from(sink: &MemorySink) -> Self {
        SinkInput(Box::new(sink.clone()))
    }
}

#[cfg(feature = "http")]
impl From<HttpSink> for SinkInput {
    fn from(sink: HttpSink) -> Self {
        SinkInput(Box::new(sink))
    }
}

#[cfg(feature = "s3")]
impl From<S3Sink> for SinkInput {
    fn from(sink: S3Sink) -> Self {
        SinkInput(Box::new(sink))
    }
}

impl From<Box<dyn DataSink>> for SinkInput {
    fn from(sink: Box<dyn DataSink>) -> Self {
        SinkInput(sink)
    }
}
