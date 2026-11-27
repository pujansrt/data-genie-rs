use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter};

pub type RecordFilterRule = Arc<dyn Fn(&DataRecord) -> bool + Send + Sync>;

/// Streaming reader decorator that yields only records passing all registered filter predicates.
pub struct FilteringReader<R: DataReader> {
    reader: R,
    rules: Vec<RecordFilterRule>,
    skipped_records: Arc<AtomicUsize>,
}

impl<R: DataReader> FilteringReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            rules: Vec::new(),
            skipped_records: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add<F>(mut self, rule: F) -> Self
    where
        F: Fn(&DataRecord) -> bool + Send + Sync + 'static,
    {
        self.rules.push(Arc::new(rule));
        self
    }

    pub fn add_rule(mut self, rule: RecordFilterRule) -> Self {
        self.rules.push(rule);
        self
    }

    pub fn push_rule(&mut self, rule: RecordFilterRule) -> &mut Self {
        self.rules.push(rule);
        self
    }
}

impl<R: DataReader + 'static> DataReader for FilteringReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let rules = self.rules.clone();
        let skipped_records = Arc::clone(&self.skipped_records);

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let passed = rules.iter().all(|rule| rule(&record));
                if passed {
                    yield Some(record);
                } else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                }
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
