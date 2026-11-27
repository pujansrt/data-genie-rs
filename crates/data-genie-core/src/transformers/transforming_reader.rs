use async_stream::try_stream;
use futures_util::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter};

pub type RecordTransformation = Arc<dyn Fn(DataRecord) -> DataRecord + Send + Sync>;

/// Streaming reader decorator that applies a sequence of `RecordTransformation` functions
/// to each record (optionally guarded by a condition predicate).
pub struct TransformingReader<R: DataReader> {
    reader: R,
    transformations: Vec<RecordTransformation>,
    condition: Arc<dyn Fn(&DataRecord) -> bool + Send + Sync>,
    skipped_records: Arc<AtomicUsize>,
}

impl<R: DataReader> TransformingReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            transformations: Vec::new(),
            condition: Arc::new(|_| true),
            skipped_records: Arc::new(AtomicUsize::new(0)),
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
        let skipped_records = Arc::clone(&self.skipped_records);

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let Some(mut record) = item? else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
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

    fn propagate_dlq(&mut self, writer: SharedWriter) {
        self.reader.propagate_dlq(writer);
    }

    fn skipped_record_count(&self) -> usize {
        self.reader.skipped_record_count() + self.skipped_records.load(Ordering::Relaxed)
    }

    fn dlq_record_count(&self) -> usize {
        self.reader.dlq_record_count()
    }

    fn rejected_record_count(&self) -> usize {
        self.reader.rejected_record_count()
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.reader.set_ignore_errors(ignore);
    }
}
