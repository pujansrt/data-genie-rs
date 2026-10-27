use async_stream::try_stream;
use futures::StreamExt;
use std::sync::Arc;

use crate::core::interfaces::{DataReader, DataRecord, RecordStream, SharedWriter};

pub type RecordFilterRule = Arc<dyn Fn(&DataRecord) -> bool + Send + Sync>;

/// Streaming reader decorator that yields only records passing all registered filter predicates.
pub struct FilteringReader<R: DataReader> {
    reader: R,
    rules: Vec<RecordFilterRule>,
}

impl<R: DataReader> FilteringReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            rules: Vec::new(),
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

        Box::pin(try_stream! {
            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    continue;
                };
                let passed = rules.iter().all(|rule| rule(&record));
                if passed {
                    yield Some(record);
                }
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
