use async_stream::try_stream;
use std::sync::Arc;

use crate::core::traits::{DataReader, DataRecord, RecordStream};
use crate::transformers::SchemaValidator;

/// Streams records from a shared in-memory collection of `DataRecord`s.
pub struct MemoryReader {
    records: Arc<Vec<DataRecord>>,
    schema: Option<Arc<dyn SchemaValidator>>,
}

impl MemoryReader {
    /// Creates a reader from any iterable collection of records, such as a `Vec` or array.
    pub fn new(records: impl IntoIterator<Item = DataRecord>) -> Self {
        Self {
            records: Arc::new(records.into_iter().collect()),
            schema: None,
        }
    }

    pub fn with_schema<V: SchemaValidator + 'static>(mut self, schema: V) -> Self {
        self.schema = Some(Arc::new(schema));
        self
    }
}

impl DataReader for MemoryReader {
    fn read(&mut self) -> RecordStream {
        let records = Arc::clone(&self.records);
        let schema = self.schema.clone();

        Box::pin(try_stream! {
            for record in records.iter().cloned() {
                if let Some(ref validator) = schema {
                    let validated = validator.parse(record)?;
                    yield Some(validated);
                } else {
                    yield Some(record);
                }
            }
        })
    }
}
