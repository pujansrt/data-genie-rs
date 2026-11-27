use async_trait::async_trait;
use rust_xlsxwriter::{Format, Workbook};
use serde_json::Value;
use tokio::io::AsyncWriteExt;

const EXCEL_MAX_COLUMNS: usize = 16_384;
const EXCEL_MAX_ROWS: usize = 1_048_576;

use data_genie_core::core::error::{DataGenieError, Result};
use data_genie_core::core::traits::{DataRecord, DataSink, DataWriter, value_to_string};
use data_genie_core::core::transport_utils::{SinkInput, ensure_data_sink};

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
    is_closed: bool,
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
            is_closed: false,
        }
    }
}

#[async_trait]
impl DataWriter for XlsxWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        if record.is_empty() {
            return Ok(());
        }
        if self.field_names.is_empty() {
            if record.len() > EXCEL_MAX_COLUMNS {
                return Err(DataGenieError::Validation(format!(
                    "XLSX supports at most {} columns, got {}",
                    EXCEL_MAX_COLUMNS,
                    record.len()
                )));
            }
            self.field_names = record.keys().cloned().collect();
        }
        if self.records.len() + 1 >= EXCEL_MAX_ROWS {
            return Err(DataGenieError::Validation(format!(
                "XLSX supports at most {} rows including header",
                EXCEL_MAX_ROWS
            )));
        }
        self.records.push(record);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;

        if self.records.is_empty() && self.field_names.is_empty() {
            return Ok(());
        }

        let mut workbook = Workbook::new();
        let worksheet = workbook.add_worksheet();
        worksheet.set_name(&self.options.sheet_name).map_err(|e| DataGenieError::Xlsx(e.to_string()))?;

        let header_format = if self.options.use_styles { Some(Format::new().set_bold()) } else { None };

        for (col_idx, name) in self.field_names.iter().enumerate() {
            let excel_col = u16::try_from(col_idx).map_err(|_| DataGenieError::Validation(format!("XLSX column index {} exceeds u16 limit", col_idx)))?;
            if let Some(ref fmt) = header_format {
                worksheet
                    .write_string_with_format(0, excel_col, name, fmt)
                    .map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
            } else {
                worksheet.write_string(0, excel_col, name).map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
            }
        }

        for (row_idx, record) in self.records.iter().enumerate() {
            let excel_row = u32::try_from(row_idx + 1).map_err(|_| DataGenieError::Validation(format!("XLSX row index {} exceeds u32 limit", row_idx + 1)))?;
            for (col_idx, field_name) in self.field_names.iter().enumerate() {
                let excel_col = u16::try_from(col_idx).map_err(|_| DataGenieError::Validation(format!("XLSX column index {} exceeds u16 limit", col_idx)))?;
                match record.get(field_name) {
                    None | Some(Value::Null) => {}
                    Some(Value::Bool(b)) => {
                        worksheet
                            .write_boolean(excel_row, excel_col, *b)
                            .map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
                    }
                    Some(Value::Number(n)) => {
                        if let Some(f) = n.as_f64() {
                            worksheet
                                .write_number(excel_row, excel_col, f)
                                .map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
                        } else {
                            worksheet
                                .write_string(excel_row, excel_col, n.to_string())
                                .map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
                        }
                    }
                    Some(other) => {
                        worksheet
                            .write_string(excel_row, excel_col, value_to_string(other))
                            .map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
                    }
                }
            }
        }

        let xlsx_bytes = workbook.save_to_buffer().map_err(|e| DataGenieError::Xlsx(e.to_string()))?;
        self.records.clear();

        if let Some(path) = self.sink.as_file_path() {
            if let Some(parent) = path.parent()
                && !parent.as_os_str().is_empty()
            {
                std::fs::create_dir_all(parent)?;
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

    async fn abort(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        self.records.clear();
        self.sink.abort().await?;
        Ok(())
    }
}
