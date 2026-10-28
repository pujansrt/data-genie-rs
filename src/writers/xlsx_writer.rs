use async_trait::async_trait;
use rust_xlsxwriter::Workbook;
use serde_json::Value;
use tokio::io::AsyncWriteExt;

use crate::core::error::Result;
use crate::core::traits::{DataRecord, DataSink, DataWriter, value_to_string};
use crate::core::transport_utils::{SinkInput, ensure_data_sink};

#[derive(Debug, Clone)]
pub struct XlsxWriterOptions {
    pub sheet_name: String,
    pub use_styles: bool,
}

impl Default for XlsxWriterOptions {
    fn default() -> Self {
        Self {
            sheet_name: "Sheet1".to_string(),
            use_styles: false,
        }
    }
}

/// Excel (`.xlsx`) workbook writer built on `rust_xlsxwriter`.
pub struct XlsxWriter {
    sink: Box<dyn DataSink>,
    options: XlsxWriterOptions,
    field_names: Vec<String>,
    records: Vec<DataRecord>,
}

impl XlsxWriter {
    pub fn new<S: Into<SinkInput>>(sink: S) -> Self {
        Self::with_options(sink, XlsxWriterOptions::default())
    }

    pub fn with_options<S: Into<SinkInput>>(sink: S, options: XlsxWriterOptions) -> Self {
        Self {
            sink: ensure_data_sink(sink),
            options,
            field_names: Vec::new(),
            records: Vec::new(),
        }
    }
}

#[async_trait]
impl DataWriter for XlsxWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.field_names.is_empty() {
            self.field_names = record.keys().cloned().collect();
        }
        self.records.push(record);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.records.is_empty() && self.field_names.is_empty() {
            return Ok(());
        }

        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name(&self.options.sheet_name)?;

        for (col_idx, name) in self.field_names.iter().enumerate() {
            worksheet.write_string(0, col_idx as u16, name)?;
        }

        for (row_idx, record) in self.records.iter().enumerate() {
            let excel_row = (row_idx + 1) as u32;
            for (col_idx, field_name) in self.field_names.iter().enumerate() {
                let excel_col = col_idx as u16;
                match record.get(field_name) {
                    None | Some(Value::Null) => {}
                    Some(Value::Bool(b)) => {
                        worksheet.write_boolean(excel_row, excel_col, *b)?;
                    }
                    Some(Value::Number(n)) => {
                        if let Some(f) = n.as_f64() {
                            worksheet.write_number(excel_row, excel_col, f)?;
                        } else {
                            worksheet.write_string(excel_row, excel_col, n.to_string())?;
                        }
                    }
                    Some(other) => {
                        worksheet.write_string(excel_row, excel_col, value_to_string(other))?;
                    }
                }
            }
        }

        let xlsx_bytes = workbook.save_to_buffer()?;

        if let Some(path) = self.sink.as_file_path() {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)?;
                }
            }
            std::fs::write(path, &xlsx_bytes)?;
        } else {
            let mut writer = self.sink.get_writer().await?;
            writer.write_all(&xlsx_bytes).await?;
            writer.flush().await?;
            writer.shutdown().await?;
            self.sink.finalize().await?;
        }

        Ok(())
    }
}
