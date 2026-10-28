use async_trait::async_trait;
use serde::Serialize;
use serde_json::ser::{Formatter, Serializer};
use std::io;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataSink, DataWriter};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

const WRITE_BUFFER_THRESHOLD: usize = 256 * 1024; // 256KB batch buffer

/// Custom zero-allocation `serde_json::ser::Formatter` that formats each record
/// at a base indentation of 2 spaces (matching `JSON.stringify(record, null, 2).replace(/\n/g, '\n  ')`).
struct RecordPrettyFormatter {
    current_indent: usize,
    has_value: bool,
}

impl RecordPrettyFormatter {
    #[inline]
    fn new() -> Self {
        Self {
            current_indent: 1, // Base indent = 1 level (2 spaces) inside root JSON array `[`
            has_value: false,
        }
    }

    #[inline]
    fn write_indent<W: ?Sized + io::Write>(writer: &mut W, levels: usize) -> io::Result<()> {
        const SPACES: &[u8] = b"                                ";
        let mut remaining = levels * 2;
        while remaining > 0 {
            let chunk = remaining.min(SPACES.len());
            writer.write_all(&SPACES[..chunk])?;
            remaining -= chunk;
        }
        Ok(())
    }
}

impl Formatter for RecordPrettyFormatter {
    #[inline]
    fn begin_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"[")
    }

    #[inline]
    fn end_array<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.current_indent -= 1;
        if self.has_value {
            writer.write_all(b"\n")?;
            Self::write_indent(writer, self.current_indent)?;
        }
        writer.write_all(b"]")
    }

    #[inline]
    fn begin_array_value<W: ?Sized + io::Write>(&mut self, writer: &mut W, first: bool) -> io::Result<()> {
        if first {
            writer.write_all(b"\n")?;
        } else {
            writer.write_all(b",\n")?;
        }
        Self::write_indent(writer, self.current_indent)
    }

    #[inline]
    fn end_array_value<W: ?Sized + io::Write>(&mut self, _writer: &mut W) -> io::Result<()> {
        self.has_value = true;
        Ok(())
    }

    #[inline]
    fn begin_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"{")
    }

    #[inline]
    fn end_object<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        self.current_indent -= 1;
        if self.has_value {
            writer.write_all(b"\n")?;
            Self::write_indent(writer, self.current_indent)?;
        }
        writer.write_all(b"}")
    }

    #[inline]
    fn begin_object_key<W: ?Sized + io::Write>(&mut self, writer: &mut W, first: bool) -> io::Result<()> {
        if first {
            writer.write_all(b"\n")?;
        } else {
            writer.write_all(b",\n")?;
        }
        Self::write_indent(writer, self.current_indent)
    }

    #[inline]
    fn begin_object_value<W: ?Sized + io::Write>(&mut self, writer: &mut W) -> io::Result<()> {
        writer.write_all(b": ")
    }

    #[inline]
    fn end_object_value<W: ?Sized + io::Write>(&mut self, _writer: &mut W) -> io::Result<()> {
        self.has_value = true;
        Ok(())
    }
}

/// Streaming JSON array writer (`[\n  ...\n]` or `[]` when empty) that avoids buffering records in memory.
pub struct JsonWriter {
    sink: Box<dyn DataSink>,
    output_stream: Option<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
    write_buf: Vec<u8>,
    first_record: bool,
    is_closed: bool,
}

impl JsonWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            output_stream: None,
            write_buf: Vec::with_capacity(WRITE_BUFFER_THRESHOLD + 4096),
            first_record: true,
            is_closed: false,
        }
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        if self.output_stream.is_none() {
            let writer = self.sink.get_writer().await?;
            self.write_buf.extend_from_slice(b"[\n");
            self.output_stream = Some(writer);
        }
        Ok(())
    }
}

#[async_trait]
impl DataWriter for JsonWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        self.ensure_stream().await?;

        if self.first_record {
            self.write_buf.extend_from_slice(b"  ");
            self.first_record = false;
        } else {
            self.write_buf.extend_from_slice(b",\n  ");
        }

        let mut ser = Serializer::with_formatter(&mut self.write_buf, RecordPrettyFormatter::new());
        record.serialize(&mut ser)?;

        if self.write_buf.len() >= WRITE_BUFFER_THRESHOLD {
            if let Some(ref mut stream) = self.output_stream {
                stream.write_all(&self.write_buf).await?;
                self.write_buf.clear();
            }
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;

        if let Some(mut stream) = self.output_stream.take() {
            self.write_buf.extend_from_slice(b"\n]");
            stream.write_all(&self.write_buf).await?;
            self.write_buf.clear();
            stream.flush().await?;
            stream.shutdown().await?;
        } else {
            let mut stream = self.sink.get_writer().await?;
            stream.write_all(b"[]").await?;
            stream.flush().await?;
            stream.shutdown().await?;
        }

        self.sink.finalize().await?;
        Ok(())
    }
}
