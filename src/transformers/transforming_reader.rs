use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

use crate::core::interfaces::{DataReader, DataRecord, RecordStream, SharedWriter};

pub type RecordTransformation = Arc<dyn Fn(DataRecord) -> DataRecord + Send + Sync>;

/// Streaming reader decorator that applies a sequence of `RecordTransformation` functions
/// to each record (optionally guarded by a condition predicate).
pub struct TransformingReader<R: DataReader> {
    reader: R,
    transformations: Vec<RecordTransformation>,
    condition: Arc<dyn Fn(&DataRecord) -> bool + Send + Sync>,
}

impl<R: DataReader> TransformingReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            transformations: Vec::new(),
            condition: Arc::new(|_| true),
        }
    }

    pub fn add<F>(mut self, transformation: F) -> Self
    where
        F: Fn(DataRecord) -> DataRecord + Send + Sync + 'static,
    {
        self.transformations.push(Arc::new(transformation));
        self
    }

    pub fn add_transform(mut self, transformation: RecordTransformation) -> Self {
        self.transformations.push(transformation);
        self
    }

    pub fn push_transform(&mut self, transformation: RecordTransformation) -> &mut Self {
        self.transformations.push(transformation);
        self
    }

    pub fn set_condition<F>(mut self, condition: F) -> Self
    where
        F: Fn(&DataRecord) -> bool + Send + Sync + 'static,
    {
        self.condition = Arc::new(condition);
        self
    }
}

impl<R: DataReader + 'static> DataReader for TransformingReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let transformations = self.transformations.clone();
        let condition = Arc::clone(&self.condition);

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let Some(mut record) = item? else {
                    continue;
                };
                if condition(&record) {
                    for transform in &transformations {
                        record = transform(record);
                    }
                }
                yield Some(record);
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.reader.set_dlq(writer);
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.reader.set_ignore_errors(ignore);
    }
}
