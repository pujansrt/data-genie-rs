use async_stream::try_stream;
use async_trait::async_trait;
use futures::StreamExt;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataReader, RecordStream, SharedWriter, value_to_string};

/// Pluggable key-store interface for `RemoveDuplicatesReader` (supports in-memory, disk, or Redis stores).
#[async_trait]
pub trait DuplicateKeyStore: Send + Sync {
    async fn has(&self, key: &str) -> Result<bool>;
    async fn add(&self, key: String) -> Result<()>;
    fn is_memory_store(&self) -> bool {
        false
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
}

/// Deduplicates streaming records based on one or more composite key fields.
pub struct RemoveDuplicatesReader<R: DataReader> {
    reader: R,
    field_names: Vec<String>,
    store: Arc<dyn DuplicateKeyStore>,
    max_keys: usize,
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

        Box::pin(try_stream! {
            let mut current_key_count: usize = 0;

            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    continue;
                };

                let key = field_names
                    .iter()
                    .map(|f| record.get(f).map(value_to_string).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("\0");

                if !store.has(&key).await? {
                    if current_key_count >= max_keys && store.is_memory_store() {
                        Err(DataGenieError::MemoryLimit(format!(
                            "Memory limit reached: deduplication saw more than {} unique keys. Use a disk-backed KeyStore.",
                            max_keys
                        )))?;
                    }
                    store.add(key).await?;
                    current_key_count += 1;
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
