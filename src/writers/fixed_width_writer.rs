use async_trait::async_trait;
use tokio::io::AsyncWriteExt;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{ByteWriter, DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

/// Streaming Fixed-Width text writer with configurable column widths and optional header row.
pub struct FixedWidthWriter {
    sink: Box<dyn DataSink>,
    output_stream: Option<ByteWriter>,
    field_widths: Vec<usize>,
    field_names: Vec<String>,
    has_field_names_in_first_row: bool,
    header_written: bool,
    initialized_field_names: bool,
    is_closed: bool,
}

impl FixedWidthWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            output_stream: None,
            field_widths: Vec::new(),
            field_names: Vec::new(),
            has_field_names_in_first_row: false,
            header_written: false,
            initialized_field_names: false,
            is_closed: false,
        }
    }

    pub fn set_field_widths<I: IntoIterator<Item = usize>>(mut self, widths: I) -> Result<Self> {
        let collected: Vec<usize> = widths.into_iter().collect();
        if collected.is_empty() || collected.contains(&0) {
            return Err(DataGenieError::Config(
                "Field widths must be non-empty positive numbers.".to_string(),
            ));
        }
        self.field_widths = collected;
        Ok(self)
    }

    pub fn with_field_widths<I: IntoIterator<Item = usize>>(mut self, widths: I) -> Self {
        let collected: Vec<usize> = widths.into_iter().collect();
        assert!(
            !collected.is_empty() && collected.iter().all(|&w| w > 0),
            "Field widths must be non-empty positive numbers."
        );
        self.field_widths = collected;
        self
    }

    pub fn set_field_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.field_names = names.into_iter().map(Into::into).collect();
        self.initialized_field_names = true;
        self
    }

    pub fn set_field_names_in_first_row(mut self, value: bool) -> Self {
        self.has_field_names_in_first_row = value;
        self
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        if self.output_stream.is_none() {
            self.output_stream = Some(self.sink.get_writer().await?);
        }
        Ok(())
    }

    fn format_field(value: &str, width: usize) -> String {
        let mut out = String::with_capacity(width);
        let mut count = 0usize;
        for c in value.chars() {
            if count == width {
                break;
            }
            out.push(match c {
                '\r' | '\n' => ' ',
                other => other,
            });
            count += 1;
        }
        for _ in count..width {
            out.push(' ');
        }
        out
    }
}

#[async_trait]
impl DataWriter for FixedWidthWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }

        if self.field_widths.is_empty() {
            return Err(DataGenieError::Config(
                "Field widths must be defined using set_field_widths() before writing.".to_string(),
            ));
        }

        if !self.initialized_field_names {
            self.field_names = record.keys().cloned().collect();
            self.initialized_field_names = true;
        }

        if self.field_names.len() != self.field_widths.len() {
            return Err(DataGenieError::Config(format!(
                "Number of field names ({}) must match number of field widths ({}).",
                self.field_names.len(),
                self.field_widths.len()
            )));
        }

        self.ensure_stream().await?;

        if self.has_field_names_in_first_row && !self.header_written {
            let mut header_line: String = self
                .field_names
                .iter()
                .zip(self.field_widths.iter())
                .map(|(name, &w)| Self::format_field(name, w))
                .collect();
            header_line.push('\n');
            if let Some(ref mut stream) = self.output_stream {
                stream.write_all(header_line.as_bytes()).await?;
            }
            self.header_written = true;
        }

        let mut data_line: String = self
            .field_names
            .iter()
            .zip(self.field_widths.iter())
            .map(|(name, &w)| {
                let val_str = record.get(name).map(value_to_string).unwrap_or_default();
                Self::format_field(&val_str, w)
            })
            .collect();
        data_line.push('\n');

        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(data_line.as_bytes()).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        if let Some(mut stream) = self.output_stream.take() {
            stream.flush().await?;
            stream.shutdown().await?;
        }
        self.sink.finalize().await?;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        self.output_stream = None;
        self.sink.abort().await?;
        Ok(())
    }
}
