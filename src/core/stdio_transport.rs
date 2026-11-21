use async_trait::async_trait;
use std::borrow::Cow;
use tokio::io::BufWriter;

use crate::core::error::Result;
use crate::core::traits::{ByteReader, ByteWriter, DEFAULT_IO_BUFFER_CAPACITY, DataSink, DataSource};

/// Byte source backed by the process standard input stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdinSource;

#[async_trait]
impl DataSource for StdinSource {
    async fn get_reader(&self) -> Result<ByteReader> {
        Ok(Box::pin(tokio::io::stdin()))
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("stdin")
    }
}

/// Byte sink backed by the process standard output stream.
#[derive(Debug, Clone, Copy, Default)]
pub struct StdoutSink;

#[async_trait]
impl DataSink for StdoutSink {
    async fn get_writer(&self) -> Result<ByteWriter> {
        Ok(Box::pin(BufWriter::with_capacity(DEFAULT_IO_BUFFER_CAPACITY, tokio::io::stdout())))
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed("stdout")
    }
}
