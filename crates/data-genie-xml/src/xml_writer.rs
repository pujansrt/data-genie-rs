use async_trait::async_trait;
use serde_json::Value;
use tokio::io::AsyncWriteExt;

use data_genie_core::core::error::{DataGenieError, Result};
use data_genie_core::core::traits::{ByteWriter, DataRecord, DataSink, DataWriter, value_to_string};
use data_genie_core::core::transport_utils::{SinkInput, ensure_data_sink};

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
    output_stream: Option<ByteWriter>,
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

    fn is_valid_xml_name(name: &str) -> bool {
        let mut chars = name.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !(first.is_alphabetic() || first == '_' || first == ':') {
            return false;
        }
        chars.all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
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
                // Strip illegal XML 1.0 control characters (retain \t, \n, \r)
                '\u{0000}'..='\u{0008}' | '\u{000B}' | '\u{000C}' | '\u{000E}'..='\u{001F}' => {}
                other => escaped.push(other),
            }
        }
        escaped
    }
}

#[async_trait]
impl DataWriter for XMLWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        if !Self::is_valid_xml_name(&self.root_tag) {
            return Err(DataGenieError::Validation(format!("Invalid XML root tag name: {:?}", self.root_tag)));
        }
        if !Self::is_valid_xml_name(&self.record_tag) {
            return Err(DataGenieError::Validation(format!("Invalid XML record tag name: {:?}", self.record_tag)));
        }
        for (key, value) in &record {
            if !matches!(value, Value::Null) && !Self::is_valid_xml_name(key) {
                return Err(DataGenieError::Validation(format!("Invalid XML element name for record field: {:?}", key)));
            }
        }

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
