use std::sync::Arc;

use crate::core::interfaces::{DataReader, DataSource, RecordStream, SharedWriter};
use crate::core::transport_utils::{ensure_data_source, SourceInput};
use crate::readers::csv_reader::{CSVReader, CSVReaderOptions};
use crate::transformers::SchemaValidator;

#[derive(Clone, Default)]
pub struct TSVReaderOptions {
    pub schema: Option<Arc<dyn SchemaValidator>>,
    pub has_field_names_in_first_row: Option<bool>,
}

/// Streaming Tab-Separated Values (TSV) reader.
pub struct TSVReader {
    inner: CSVReader,
}

impl TSVReader {
    pub fn new<S: Into<SourceInput>>(source: S) -> Self {
        let src: Arc<dyn DataSource> = ensure_data_source(source);
        let inner = CSVReader::new(src).set_field_separator('\t');
        Self { inner }
    }

    pub fn with_options<S: Into<SourceInput>>(source: S, options: TSVReaderOptions) -> Self {
        let csv_opts = CSVReaderOptions {
            schema: options.schema,
            has_field_names_in_first_row: options.has_field_names_in_first_row,
            delimiter: Some('\t'),
            ignore_errors: None,
        };
        Self {
            inner: CSVReader::with_options(source, csv_opts),
        }
    }

    pub fn set_field_names_in_first_row(mut self, value: bool) -> Self {
        self.inner = self.inner.set_field_names_in_first_row(value);
        self
    }

    pub fn with_schema<V: SchemaValidator + 'static>(mut self, schema: V) -> Self {
        self.inner = self.inner.with_schema(schema);
        self
    }
}

impl DataReader for TSVReader {
    fn read(&mut self) -> RecordStream {
        self.inner.read()
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.inner.set_dlq(writer);
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.inner.set_ignore_errors(ignore);
    }
}
