use async_stream::try_stream;
use std::sync::Arc;

use crate::core::interfaces::{DataReader, DataRecord, RecordStream};
use crate::transformers::SchemaValidator;

/// Streams records from an in-memory `Vec<DataRecord>`.
pub struct MemoryReader {
    records: Vec<DataRecord>,
    schema: Option<Arc<dyn SchemaValidator>>,
}

impl MemoryReader {
    pub fn new(records: Vec<DataRecord>) -> Self {
        Self {
            records,
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
        let records = self.records.clone();
        let schema = self.schema.clone();

        Box::pin(try_stream! {
            for record in records {
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
