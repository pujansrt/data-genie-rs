use async_trait::async_trait;
use serde_json::Value;
use std::pin::Pin;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

#[derive(Debug, Clone)]
pub struct XMLWriterOptions {
    pub root_tag: String,
    pub record_tag: String,
    pub add_declaration: bool,
}

impl Default for XMLWriterOptions {
    fn default() -> Self {
        Self {
            root_tag: "root".to_string(),
            record_tag: "record".to_string(),
            add_declaration: true,
        }
    }
}

/// Streaming XML writer that emits well-formed, entity-escaped XML records in constant memory.
pub struct XMLWriter {
    sink: Box<dyn DataSink>,
    root_tag: String,
    record_tag: String,
    add_declaration: bool,
    is_first_record: bool,
    output_stream: Option<Pin<Box<dyn AsyncWrite + Send + Unpin>>>,
    is_closed: bool,
}

impl XMLWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self::with_options(sink, XMLWriterOptions::default())
    }

    pub fn with_options<S: Into<SinkInput>>(sink: S, options: XMLWriterOptions) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            root_tag: options.root_tag,
            record_tag: options.record_tag,
            add_declaration: options.add_declaration,
            is_first_record: true,
            output_stream: None,
            is_closed: false,
        }
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        if self.output_stream.is_none() {
            self.output_stream = Some(self.sink.get_writer().await?);
        }
        Ok(())
    }

    fn escape_xml(unsafe_str: &str) -> String {
        let mut escaped = String::with_capacity(unsafe_str.len());
        for c in unsafe_str.chars() {
            match c {
                '<' => escaped.push_str("&lt;"),
                '>' => escaped.push_str("&gt;"),
                '&' => escaped.push_str("&amp;"),
                '"' => escaped.push_str("&quot;"),
                '\'' => escaped.push_str("&apos;"),
                other => escaped.push(other),
            }
        }
        escaped
    }
}

#[async_trait]
impl DataWriter for XMLWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.ensure_stream().await?;

        if self.is_first_record {
            let mut header = String::new();
            if self.add_declaration {
                header.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
            }
            header.push_str(&format!("<{}>\n", self.root_tag));
            if let Some(ref mut stream) = self.output_stream {
                stream.write_all(header.as_bytes()).await?;
            }
            self.is_first_record = false;
        }

        let mut xml = format!("  <{}>\n", self.record_tag);
        for (key, value) in &record {
            if !matches!(value, Value::Null) {
                let escaped = Self::escape_xml(&value_to_string(value));
                xml.push_str(&format!("    <{}>{}</{}>\n", key, escaped, key));
            }
        }
        xml.push_str(&format!("  </{}>\n", self.record_tag));

        if let Some(ref mut stream) = self.output_stream {
            stream.write_all(xml.as_bytes()).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;

        self.ensure_stream().await?;

        if let Some(mut stream) = self.output_stream.take() {
            if !self.is_first_record {
                let footer = format!("</{}>\n", self.root_tag);
                stream.write_all(footer.as_bytes()).await?;
            } else {
                let mut empty_doc = String::new();
                if self.add_declaration {
                    empty_doc.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
                }
                empty_doc.push_str(&format!("<{}></{}>\n", self.root_tag, self.root_tag));
                stream.write_all(empty_doc.as_bytes()).await?;
            }
            stream.flush().await?;
            stream.shutdown().await?;
        }

        self.sink.finalize().await?;
        Ok(())
    }
}
