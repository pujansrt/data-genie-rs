use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::core::file_transport::{FileSink, FileSource};
use crate::core::memory_transport::{MemorySink, MemorySource};
use crate::core::traits::{DataSink, DataSource};

/// Flexible input wrapper allowing readers to accept file paths (`&str`, `String`, `PathBuf`),
/// raw byte buffers (`Vec<u8>`, `&[u8]`), or any [`DataSource`] implementation (built-in or custom).
#[derive(Clone)]
pub struct SourceInput(pub Arc<dyn DataSource>);

impl SourceInput {
    pub fn new<S: DataSource + 'static>(source: S) -> Self {
        Self(Arc::new(source))
    }

    pub fn into_inner(self) -> Arc<dyn DataSource> {
        self.0
    }
}

/// Converts any supported source input into an `Arc<dyn DataSource>`.
pub fn ensure_data_source<S: Into<SourceInput>>(input: S) -> Arc<dyn DataSource> {
    input.into().into_inner()
}

macro_rules! impl_path_conversions {
    ($target:ident, $ctor:path, $($ty:ty),+ $(,)?) => {
        $(
            impl From<$ty> for $target {
                fn from(path: $ty) -> Self {
                    Self::new($ctor(path))
                }
            }
        )+
    };
}

impl_path_conversions!(SourceInput, FileSource::new, &str, String, &String, &Path, PathBuf, &PathBuf);

impl From<Vec<u8>> for SourceInput {
    fn from(bytes: Vec<u8>) -> Self {
        Self::new(MemorySource::new(bytes))
    }
}

impl From<&[u8]> for SourceInput {
    fn from(bytes: &[u8]) -> Self {
        Self::new(MemorySource::new(bytes.to_vec()))
    }
}

impl<const N: usize> From<&[u8; N]> for SourceInput {
    fn from(bytes: &[u8; N]) -> Self {
        Self::new(MemorySource::new(bytes.to_vec()))
    }
}

impl<S: DataSource + 'static> From<S> for SourceInput {
    fn from(source: S) -> Self {
        Self::new(source)
    }
}

impl From<&MemorySource> for SourceInput {
    fn from(source: &MemorySource) -> Self {
        Self::new(source.clone())
    }
}

impl From<&FileSource> for SourceInput {
    fn from(source: &FileSource) -> Self {
        Self::new(source.clone())
    }
}

impl<S: DataSource + 'static> From<Arc<S>> for SourceInput {
    fn from(source: Arc<S>) -> Self {
        Self(source)
    }
}

impl From<Arc<dyn DataSource>> for SourceInput {
    fn from(source: Arc<dyn DataSource>) -> Self {
        Self(source)
    }
}

/// Flexible sink wrapper allowing writers to accept file paths or any [`DataSink`] implementation (built-in or custom).
pub struct SinkInput(pub Box<dyn DataSink>);

impl SinkInput {
    pub fn new<S: DataSink + 'static>(sink: S) -> Self {
        Self(Box::new(sink))
    }

    pub fn into_inner(self) -> Box<dyn DataSink> {
        self.0
    }
}

/// Converts any supported sink input into a `Box<dyn DataSink>`.
pub fn ensure_data_sink<S: Into<SinkInput>>(input: S) -> Box<dyn DataSink> {
    input.into().into_inner()
}

impl_path_conversions!(SinkInput, FileSink::new, &str, String, &String, &Path, PathBuf, &PathBuf);

impl<S: DataSink + 'static> From<S> for SinkInput {
    fn from(sink: S) -> Self {
        Self::new(sink)
    }
}

impl From<&MemorySink> for SinkInput {
    fn from(sink: &MemorySink) -> Self {
        Self::new(sink.clone())
    }
}

impl From<&FileSink> for SinkInput {
    fn from(sink: &FileSink) -> Self {
        Self::new(sink.clone())
    }
}

impl From<Box<dyn DataSink>> for SinkInput {
    fn from(sink: Box<dyn DataSink>) -> Self {
        Self(sink)
    }
}
