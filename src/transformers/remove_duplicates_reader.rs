use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Mutex;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter};
use serde_json::Value;

/// Pluggable key-store interface for `RemoveDuplicatesReader` (supports in-memory, disk, or Redis stores).
#[async_trait]
pub trait DuplicateKeyStore: Send + Sync {
    async fn has(&self, key: &str) -> Result<bool>;
    async fn add(&self, key: String) -> Result<()>;
    fn is_memory_store(&self) -> bool {
        false
    }

    /// Atomically checks whether `key` is absent and inserts it if so, returning `Ok(true)` if
    /// newly inserted or `Ok(false)` if already seen. Memory-backed stores enforce `max_keys`.
    async fn insert_if_absent(&self, key: String, max_keys: Option<usize>) -> Result<bool> {
        let _ = max_keys;
        if self.has(&key).await? {
            Ok(false)
        } else {
            self.add(key).await?;
            Ok(true)
        }
    }
}

/// Default in-memory `HashSet` store for deduplication.
#[derive(Debug, Default, Clone)]
pub struct MemoryKeyStore {
    seen: Arc<Mutex<HashSet<String>>>,
}

impl MemoryKeyStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl DuplicateKeyStore for MemoryKeyStore {
    async fn has(&self, key: &str) -> Result<bool> {
        let guard = self.seen.lock().await;
        Ok(guard.contains(key))
    }

    async fn add(&self, key: String) -> Result<()> {
        let mut guard = self.seen.lock().await;
        guard.insert(key);
        Ok(())
    }

    fn is_memory_store(&self) -> bool {
        true
    }

    async fn insert_if_absent(&self, key: String, max_keys: Option<usize>) -> Result<bool> {
        let mut guard = self.seen.lock().await;
        if guard.contains(&key) {
            return Ok(false);
        }
        if let Some(limit) = max_keys
            && guard.len() >= limit
        {
            return Err(DataGenieError::MemoryLimit(format!(
                "Memory limit reached: deduplication saw more than {} unique keys. Use a disk-backed KeyStore.",
                limit
            )));
        }
        guard.insert(key);
        Ok(true)
    }
}

fn build_composite_key(record: &DataRecord, field_names: &[String]) -> String {
    let mut parts: Vec<Value> = Vec::with_capacity(field_names.len());
    for f in field_names {
        match record.get(f) {
            Some(v) => parts.push(Value::Array(vec![Value::Bool(true), v.clone()])),
            None => parts.push(Value::Array(vec![Value::Bool(false)])),
        }
    }
    serde_json::to_string(&parts).unwrap_or_default()
}

/// Deduplicates streaming records based on one or more composite key fields.
pub struct RemoveDuplicatesReader<R: DataReader> {
    reader: R,
    field_names: Vec<String>,
    store: Arc<dyn DuplicateKeyStore>,
    max_keys: usize,
    skipped_records: Arc<AtomicUsize>,
}

impl<R: DataReader> RemoveDuplicatesReader<R> {
    pub fn new<I, S>(reader: R, field_names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            reader,
            field_names: field_names.into_iter().map(Into::into).collect(),
            store: Arc::new(MemoryKeyStore::new()),
            max_keys: 1_000_000,
            skipped_records: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set_key_store<K: DuplicateKeyStore + 'static>(mut self, store: K) -> Self {
        self.store = Arc::new(store);
        self
    }

    pub fn set_max_keys(mut self, limit: usize) -> Self {
        self.max_keys = limit;
        self
    }
}

impl<R: DataReader + 'static> DataReader for RemoveDuplicatesReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let field_names = self.field_names.clone();
        let store = Arc::clone(&self.store);
        let max_keys = self.max_keys;
        let skipped_records = Arc::clone(&self.skipped_records);

        Box::pin(try_stream! {
            if field_names.is_empty() {
                Err(DataGenieError::Config(
                    "RemoveDuplicatesReader requires at least one key field name.".to_string(),
                ))?;
            }

            let limit_opt = if store.is_memory_store() { Some(max_keys) } else { None };
            let mut local_inserted: usize = 0;

            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                    continue;
                };

                let key = build_composite_key(&record, &field_names);

                if store.insert_if_absent(key, limit_opt).await? {
                    if let Some(limit) = limit_opt {
                        // Also enforce limit for custom memory stores that only override has/add.
                        if local_inserted >= limit {
                            Err(DataGenieError::MemoryLimit(format!(
                                "Memory limit reached: deduplication saw more than {} unique keys. Use a disk-backed KeyStore.",
                                limit
                            )))?;
                        }
                        local_inserted += 1;
                    }
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
