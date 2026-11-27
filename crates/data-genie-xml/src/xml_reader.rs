use async_stream::try_stream;
use indexmap::IndexMap;
use quick_xml::Reader;
use quick_xml::events::{BytesStart, Event};
use serde_json::{Map, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use data_genie_core::core::error::DataGenieError;
use data_genie_core::core::traits::{DataReader, DataRecord, DataSource, RecordStream, SharedWriter};
use data_genie_core::core::transport_utils::{SourceInput, ensure_data_source};

#[derive(Debug, Clone)]
pub struct XMLReaderOptions {
    pub ignore_errors: bool,
    /// Path to the record element, e.g. `"library/book"`, `"records/record"`, or `"//book"` for deep search.
    pub record_path: String,
    /// Include XML attributes prefixed with `'@'`. Defaults to `true`.
    pub include_attributes: bool,
    /// Trim whitespace from text nodes. Defaults to `true`.
    pub trim_whitespace: bool,
    /// Strip namespace prefixes from tags and attributes (e.g., `ns:book` -> `book`). Defaults to `false`.
    pub strip_namespaces: bool,
    /// Optional dot-path mapping to flatten nested fields (e.g. `"title" => "title.#text"`).
    pub field_mapping: Option<IndexMap<String, String>>,
}

impl XMLReaderOptions {
    pub fn new(record_path: impl Into<String>) -> Self {
        Self {
            ignore_errors: false,
            record_path: record_path.into(),
            include_attributes: true,
            trim_whitespace: true,
            strip_namespaces: false,
            field_mapping: None,
        }
    }

    pub fn with_include_attributes(mut self, val: bool) -> Self {
        self.include_attributes = val;
        self
    }

    pub fn with_trim_whitespace(mut self, val: bool) -> Self {
        self.trim_whitespace = val;
        self
    }

    pub fn with_strip_namespaces(mut self, val: bool) -> Self {
        self.strip_namespaces = val;
        self
    }

    pub fn with_field_mapping(mut self, mapping: IndexMap<String, String>) -> Self {
        self.field_mapping = Some(mapping);
        self
    }

    pub fn with_ignore_errors(mut self, val: bool) -> Self {
        self.ignore_errors = val;
        self
    }
}

/// Streaming pull-based XML reader built on `quick-xml` with XPath-style record boundaries and DLQ support.
pub struct XMLReader {
    source: Arc<dyn DataSource>,
    record_path: String,
    is_deep_search: bool,
    include_attributes: bool,
    trim_whitespace: bool,
    strip_namespaces: bool,
    field_mapping: Option<IndexMap<String, String>>,
    ignore_errors: bool,
    dlq_writer: Option<SharedWriter>,
    local_dlq_records: Arc<AtomicUsize>,
    rejected_without_dlq: Arc<AtomicUsize>,
}

impl XMLReader {
    pub fn new<S: Into<SourceInput>>(source: S, options: XMLReaderOptions) -> Self {
        let is_deep_search = options.record_path.starts_with("//");
        let raw_cleaned = options.record_path.trim_start_matches("//").trim_start_matches('/').trim_end_matches('/');
        let cleaned_path = if options.strip_namespaces {
            raw_cleaned.split('/').map(|seg| Self::clean_name(seg, true)).collect::<Vec<_>>().join("/")
        } else {
            raw_cleaned.to_string()
        };

        Self {
            source: ensure_data_source(source),
            record_path: cleaned_path,
            is_deep_search,
            include_attributes: options.include_attributes,
            trim_whitespace: options.trim_whitespace,
            strip_namespaces: options.strip_namespaces,
            field_mapping: options.field_mapping,
            ignore_errors: options.ignore_errors,
            dlq_writer: None,
            local_dlq_records: Arc::new(AtomicUsize::new(0)),
            rejected_without_dlq: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn with_dlq<W: Into<SharedWriter>>(mut self, writer: W) -> Self {
        let shared = writer.into();
        self.set_dlq(shared);
        self
    }

    fn clean_name(raw: &str, strip_namespaces: bool) -> String {
        if strip_namespaces {
            raw.rsplit(':').next().unwrap_or(raw).to_string()
        } else {
            raw.to_string()
        }
    }

    fn attach_attributes(obj: &mut Map<String, Value>, e: &BytesStart<'_>, strip_namespaces: bool) -> std::result::Result<(), String> {
        for attr_res in e.attributes() {
            let attr = attr_res.map_err(|err| err.to_string())?;
            let key_raw: &str = attr.key.as_ref();
            if strip_namespaces && (key_raw == "xmlns" || key_raw.starts_with("xmlns:")) {
                continue;
            }
            let key = Self::clean_name(key_raw, strip_namespaces);
            let val = attr
                .normalized_value(quick_xml::XmlVersion::Implicit1_0)
                .map_err(|err| err.to_string())?
                .into_owned();
            obj.insert(format!("@{}", key), Value::String(val));
        }
        Ok(())
    }

    fn get_nested_value(obj: &Value, path: &str) -> Value {
        let mut current = obj;
        for part in path.split('.') {
            match current {
                Value::Object(map) => {
                    if let Some(next) = map.get(part) {
                        current = next;
                    } else {
                        return Value::Null;
                    }
                }
                _ => return Value::Null,
            }
        }
        current.clone()
    }

    fn insert_child_value(parent: &mut Map<String, Value>, tag_name: &str, child_val: Value) {
        if let Some(existing) = parent.get_mut(tag_name) {
            if let Value::Array(arr) = existing {
                arr.push(child_val);
            } else {
                let prev = existing.take();
                *existing = Value::Array(vec![prev, child_val]);
            }
        } else {
            parent.insert(tag_name.to_string(), child_val);
        }
    }
}

impl DataReader for XMLReader {
    fn read(&mut self) -> RecordStream {
        let source = Arc::clone(&self.source);
        let record_path = self.record_path.clone();
        let is_deep_search = self.is_deep_search;
        let include_attributes = self.include_attributes;
        let trim_whitespace = self.trim_whitespace;
        let strip_namespaces = self.strip_namespaces;
        let field_mapping = self.field_mapping.clone();
        let ignore_errors = self.ignore_errors;
        let dlq_writer = self.dlq_writer.clone();
        let local_dlq_records = Arc::clone(&self.local_dlq_records);
        let rejected_without_dlq = Arc::clone(&self.rejected_without_dlq);

        Box::pin(try_stream! {
            let xml_bytes = source.read_bytes().await?;
            let mut reader = Reader::from_reader(xml_bytes.as_slice());
            reader.config_mut().trim_text(false);
            reader.config_mut().check_end_names = true;

            let mut buf = Vec::new();
            let mut tag_stack: Vec<String> = Vec::new();
            // Stack of (tag_name, attributes/children map, text_buffer)
            let mut node_stack: Vec<(String, Map<String, Value>, String)> = Vec::new();
            let mut in_record = false;

            loop {
                buf.clear();
                match reader.read_event_into(&mut buf) {
                    Ok(Event::Start(e)) => {
                        let qname = e.name();
                        let tag_name = Self::clean_name(qname.as_ref(), strip_namespaces);
                        tag_stack.push(tag_name.clone());
                        let current_path = tag_stack.join("/");

                        let is_match = if is_deep_search {
                            current_path == record_path || current_path.ends_with(&format!("/{}", record_path))
                        } else {
                            current_path == record_path
                        };

                        if (is_match && !in_record) || in_record {
                            in_record = true;
                            let mut obj = Map::new();
                            if include_attributes {
                                if let Err(attr_err) = Self::attach_attributes(&mut obj, &e, strip_namespaces) {
                                    if ignore_errors {
                                        if let Some(ref dlq) = dlq_writer {
                                            let mut dlq_rec = DataRecord::new();
                                            dlq_rec.insert("_error".to_string(), Value::String(attr_err));
                                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                            dlq.write_dlq(dlq_rec).await?;
                                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                        }
                                        break;
                                    } else {
                                        Err(DataGenieError::Xml(attr_err))?;
                                    }
                                }
                            }
                            node_stack.push((tag_name, obj, String::new()));
                        }
                    }
                    Ok(Event::Empty(e)) => {
                        let qname = e.name();
                        let tag_name = Self::clean_name(qname.as_ref(), strip_namespaces);
                        tag_stack.push(tag_name.clone());
                        let current_path = tag_stack.join("/");

                        let is_match = if is_deep_search {
                            current_path == record_path || current_path.ends_with(&format!("/{}", record_path))
                        } else {
                            current_path == record_path
                        };

                        if is_match && !in_record {
                            let mut obj = Map::new();
                            if include_attributes {
                                if let Err(attr_err) = Self::attach_attributes(&mut obj, &e, strip_namespaces) {
                                    if ignore_errors {
                                        if let Some(ref dlq) = dlq_writer {
                                            let mut dlq_rec = DataRecord::new();
                                            dlq_rec.insert("_error".to_string(), Value::String(attr_err));
                                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                            dlq.write_dlq(dlq_rec).await?;
                                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                        }
                                        break;
                                    } else {
                                        Err(DataGenieError::Xml(attr_err))?;
                                    }
                                }
                            }
                            let record = map_to_record(obj, field_mapping.as_ref());
                            yield Some(record);
                        } else if in_record {
                            let mut obj = Map::new();
                            if include_attributes
                                && let Err(attr_err) = Self::attach_attributes(&mut obj, &e, strip_namespaces)
                            {
                                if ignore_errors {
                                    if let Some(ref dlq) = dlq_writer {
                                        let mut dlq_rec = DataRecord::new();
                                        dlq_rec.insert("_error".to_string(), Value::String(attr_err));
                                        dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                        dlq.write_dlq(dlq_rec).await?;
                                        local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                    } else {
                                        rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                    }
                                    tag_stack.pop();
                                    continue;
                                } else {
                                    Err(DataGenieError::Xml(attr_err))?;
                                }
                            }
                            if let Some((_, parent_map, _)) = node_stack.last_mut() {
                                let child_val = if obj.is_empty() {
                                    Value::String(String::new())
                                } else {
                                    Value::Object(obj)
                                };
                                Self::insert_child_value(parent_map, &tag_name, child_val);
                            }
                        }

                        tag_stack.pop();
                    }
                    Ok(Event::Text(e)) => {
                        if in_record {
                            let txt = e.xml10_content();
                            if let Some((_, _, text_buf)) = node_stack.last_mut() {
                                text_buf.push_str(&txt);
                            }
                        }
                    }
                    Ok(Event::GeneralRef(e)) => {
                        if in_record {
                            let resolved = match e.resolve_char_ref() {
                                Ok(Some(ch)) => Ok(ch.to_string()),
                                Ok(None) => {
                                    let name = e.xml10_content();
                                    match quick_xml::escape::resolve_predefined_entity(&name) {
                                        Some(s) => Ok(s.to_string()),
                                        None => Err(format!("Unknown XML entity reference: &{};", name)),
                                    }
                                }
                                Err(err) => Err(err.to_string()),
                            };
                            match resolved {
                                Ok(txt) => {
                                    if let Some((_, _, text_buf)) = node_stack.last_mut() {
                                        text_buf.push_str(&txt);
                                    }
                                }
                                Err(ref_err) => {
                                    if ignore_errors {
                                        if let Some(ref dlq) = dlq_writer {
                                            let mut dlq_rec = DataRecord::new();
                                            dlq_rec.insert("_error".to_string(), Value::String(ref_err));
                                            dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                            dlq.write_dlq(dlq_rec).await?;
                                            local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                        }
                                    } else {
                                        Err(DataGenieError::Xml(ref_err))?;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Event::CData(e)) => {
                        if in_record {
                            let txt = e.xml10_content();
                            if let Some((_, _, text_buf)) = node_stack.last_mut() {
                                text_buf.push_str(&txt);
                            }
                        }
                    }
                    Ok(Event::End(_)) => {
                        let current_path = tag_stack.join("/");
                        let is_match = if is_deep_search {
                            current_path == record_path || current_path.ends_with(&format!("/{}", record_path))
                        } else {
                            current_path == record_path
                        };

                        if in_record {
                            if let Some((tag_name, mut current_map, raw_text)) = node_stack.pop() {
                                let content = if trim_whitespace {
                                    raw_text.trim().to_string()
                                } else {
                                    raw_text
                                };

                                if is_match && node_stack.is_empty() {
                                    if !content.is_empty() {
                                        current_map.insert("#text".to_string(), Value::String(content));
                                    }
                                    let record = map_to_record(current_map, field_mapping.as_ref());
                                    yield Some(record);
                                    in_record = false;
                                } else if let Some((_, parent_map, _)) = node_stack.last_mut() {
                                    let child_val = if current_map.is_empty() {
                                        Value::String(content)
                                    } else {
                                        if !content.is_empty() {
                                            current_map.insert("#text".to_string(), Value::String(content));
                                        }
                                        Value::Object(current_map)
                                    };
                                    Self::insert_child_value(parent_map, &tag_name, child_val);
                                }
                            }
                        }

                        tag_stack.pop();
                    }
                    Ok(Event::Eof) => {
                        if !tag_stack.is_empty() {
                            let msg = format!("Unclosed XML tags at EOF: {}", tag_stack.join("/"));
                            if ignore_errors {
                                if let Some(ref dlq) = dlq_writer {
                                    let mut dlq_rec = DataRecord::new();
                                    dlq_rec.insert("_error".to_string(), Value::String(msg));
                                    dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                    dlq.write_dlq(dlq_rec).await?;
                                    local_dlq_records.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                                }
                            } else {
                                Err(DataGenieError::Xml(msg))?;
                            }
                        }
                        break;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if ignore_errors {
                            if let Some(ref dlq) = dlq_writer {
                                let mut dlq_rec = DataRecord::new();
                                dlq_rec.insert("_error".to_string(), Value::String(err.to_string()));
                                dlq_rec.insert("_type".to_string(), Value::String("parse_error".to_string()));
                                dlq.write_dlq(dlq_rec).await?;
                                local_dlq_records.fetch_add(1, Ordering::Relaxed);
                            } else {
                                rejected_without_dlq.fetch_add(1, Ordering::Relaxed);
                            }
                            break;
                        } else {
                            Err(DataGenieError::Xml(err.to_string()))?;
                        }
                    }
                }
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.dlq_writer = Some(writer);
        self.ignore_errors = true;
    }

    fn propagate_dlq(&mut self, writer: SharedWriter) {
        if self.dlq_writer.is_none() {
            self.dlq_writer = Some(writer);
            self.ignore_errors = true;
        }
    }

    fn dlq_record_count(&self) -> usize {
        self.local_dlq_records.load(Ordering::Relaxed)
    }

    fn rejected_record_count(&self) -> usize {
        self.dlq_record_count() + self.rejected_without_dlq.load(Ordering::Relaxed)
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.ignore_errors = ignore;
    }
}

fn map_to_record(map: Map<String, Value>, field_mapping: Option<&IndexMap<String, String>>) -> DataRecord {
    if let Some(mapping) = field_mapping {
        let root_val = Value::Object(map);
        let mut record = DataRecord::new();
        for (target, path) in mapping {
            let val = XMLReader::get_nested_value(&root_val, path);
            record.insert(target.clone(), val);
        }
        record
    } else {
        let mut record = DataRecord::new();
        for (k, v) in map {
            record.insert(k, v);
        }
        record
    }
}
