use async_stream::try_stream;
use calamine::{open_workbook_from_rs, Data, Reader as CalamineReader, Xlsx};
use serde_json::{json, Number, Value};
use std::io::Cursor;
use std::sync::Arc;

use crate::core::interfaces::{DataReader, DataRecord, DataSource, RecordStream};
use crate::core::transport_utils::{ensure_data_source, SourceInput};
use crate::transformers::SchemaValidator;

#[derive(Clone)]
pub struct XlsxReaderOptions {
    pub sheet_name: Option<String>,
    /// 1-based sheet index (default `1` to match ExcelJS).
    pub sheet_index: usize,
    pub has_field_names_in_first_row: bool,
    pub schema: Option<Arc<dyn SchemaValidator>>,
}

impl Default for XlsxReaderOptions {
    fn default() -> Self {
        Self {
            sheet_name: None,
            sheet_index: 1,
            has_field_names_in_first_row: true,
            schema: None,
        }
    }
}

/// Excel (`.xlsx`) workbook reader built on `calamine`.
pub struct XlsxReader {
    source: Arc<dyn DataSource>,
    options: XlsxReaderOptions,
}

impl XlsxReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        Self::with_options(source, XlsxReaderOptions::default())
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: XlsxReaderOptions) -> Self {
        Self {
            source: ensure_data_source(source),
            options,
        }
    }

    fn cell_to_value(cell: &Data) -> Value {
        match cell {
            Data::Empty => Value::Null,
            Data::String(s) => Value::String(s.clone()),
            Data::Int(i) => json!(*i),
            Data::Float(f) => {
                if f.fract() == 0.0 && *f >= (i64::MIN as f64) && *f <= (i64::MAX as f64) {
                    json!(*f as i64)
                } else {
                    Number::from_f64(*f)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                }
            }
            Data::Bool(b) => Value::Bool(*b),
            Data::DateTime(dt) => Value::String(dt.to_string()),
            Data::DateTimeIso(s) => Value::String(s.clone()),
            Data::DurationIso(s) => Value::String(s.clone()),
            Data::Error(e) => Value::String(format!("{:?}", e)),
        }
    }
}

impl DataReader for XlsxReader {
    fn read(&mut self) -> RecordStream {
        let source = Arc::clone(&self.source);
        let options = self.options.clone();

        Box::pin(try_stream! {
            let bytes = source.read_bytes().await?;
            let cursor = Cursor::new(bytes);
            let mut workbook: Xlsx<_> = open_workbook_from_rs(cursor)?;

            let sheet_names = workbook.sheet_names().to_vec();
            let target_sheet = if let Some(ref name) = options.sheet_name {
                if sheet_names.contains(name) {
                    Some(name.clone())
                } else {
                    None
                }
            } else {
                let idx = options.sheet_index.saturating_sub(1);
                sheet_names.get(idx).cloned()
            };

            let Some(sheet_name) = target_sheet else {
                return;
            };

            let range = workbook.worksheet_range(&sheet_name)?;
            let mut rows_iter = range.rows();

            let mut field_names: Vec<String> = Vec::new();
            if options.has_field_names_in_first_row {
                if let Some(first_row) = rows_iter.next() {
                    field_names = first_row
                        .iter()
                        .map(|c| match Self::cell_to_value(c) {
                            Value::String(s) => s,
                            other => other.to_string(),
                        })
                        .collect();
                } else {
                    return;
                }
            }

            for row in rows_iter {
                let mut record = DataRecord::new();
                if !field_names.is_empty() {
                    for (idx, name) in field_names.iter().enumerate() {
                        let cell_val = row
                            .get(idx)
                            .map(Self::cell_to_value)
                            .unwrap_or(Value::Null);
                        record.insert(name.clone(), cell_val);
                    }
                } else {
                    for (idx, cell) in row.iter().enumerate() {
                        record.insert(format!("col{}", idx + 1), Self::cell_to_value(cell));
                    }
                }

                if let Some(ref validator) = options.schema {
                    yield Some(validator.parse(record)?);
                } else {
                    yield Some(record);
                }
            }
        })
    }
}
