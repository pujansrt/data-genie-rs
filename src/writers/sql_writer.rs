use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{DataRecord, DataWriter, SqlConnection};

/// SQL dialect selector for bulk inserts and idempotent upserts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    Postgres,
    MySql,
    Sqlite,
    Oracle,
}

impl SqlDialect {
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "mysql" => Self::MySql,
            "sqlite" => Self::Sqlite,
            "oracle" => Self::Oracle,
            _ => Self::Postgres,
        }
    }
}

/// Driver-agnostic SQL database writer supporting batched bulk inserts, transactions,
/// and dialect-aware idempotent upserts (`Postgres`, `SQLite`, `MySQL`, `Oracle`).
pub struct SQLWriter {
    db_client: Arc<dyn SqlConnection>,
    table_name: String,
    field_names: Vec<String>,
    initialized_field_names: bool,
    batch_size: usize,
    buffer: Vec<DataRecord>,
    use_transaction: bool,
    transaction_started: bool,
    dialect: SqlDialect,
    conflict_key: Option<String>,
}

impl SQLWriter {
    pub fn new(db_client: Arc<dyn SqlConnection>, table_name: impl Into<String>) -> Self {
        Self {
            db_client,
            table_name: table_name.into(),
            field_names: Vec::new(),
            initialized_field_names: false,
            batch_size: 1,
            buffer: Vec::new(),
            use_transaction: false,
            transaction_started: false,
            dialect: SqlDialect::Postgres,
            conflict_key: None,
        }
    }

    pub fn set_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn set_upsert(mut self, key: impl Into<String>) -> Self {
        self.conflict_key = Some(key.into());
        self
    }

    pub fn set_batch_size(mut self, size: usize) -> Result<Self> {
        if size < 1 {
            return Err(DataGenieError::Config(
                "Batch size must be at least 1".to_string(),
            ));
        }
        self.batch_size = size;
        Ok(self)
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        assert!(size >= 1, "Batch size must be at least 1");
        self.batch_size = size;
        self
    }

    pub fn set_field_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.field_names = names.into_iter().map(Into::into).collect();
        self.initialized_field_names = true;
        self
    }

    pub fn set_use_transaction(mut self, value: bool) -> Self {
        self.use_transaction = value;
        self
    }

    async fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        if self.dialect == SqlDialect::Oracle {
            return self.flush_oracle().await;
        }

        let columns = self
            .field_names
            .iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(", ");

        let mut rows = Vec::with_capacity(self.buffer.len());
        let mut values = Vec::with_capacity(self.buffer.len() * self.field_names.len());
        let mut param_index = 1usize;

        for record in &self.buffer {
            let placeholders = self
                .field_names
                .iter()
                .map(|_| {
                    let p = format!("${}", param_index);
                    param_index += 1;
                    p
                })
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(format!("({})", placeholders));

            for name in &self.field_names {
                values.push(record.get(name).cloned().unwrap_or(Value::Null));
            }
        }

        let mut sql = format!(
            "INSERT INTO \"{}\" ({}) VALUES {}",
            self.table_name,
            columns,
            rows.join(", ")
        );

        if let Some(ref conflict_key) = self.conflict_key {
            match self.dialect {
                SqlDialect::Postgres | SqlDialect::Sqlite => {
                    let updates = self
                        .field_names
                        .iter()
                        .filter(|&n| n != conflict_key)
                        .map(|n| format!("\"{}\" = EXCLUDED.\"{}\"", n, n))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&format!(
                        " ON CONFLICT (\"{}\") DO UPDATE SET {}",
                        conflict_key, updates
                    ));
                }
                SqlDialect::MySql => {
                    let updates = self
                        .field_names
                        .iter()
                        .filter(|&n| n != conflict_key)
                        .map(|n| format!("\"{}\" = VALUES(\"{}\")", n, n))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", updates));
                }
                SqlDialect::Oracle => {}
            }
        }

        match self.db_client.query(&sql, &values).await {
            Ok(_) => {
                self.buffer.clear();
                Ok(())
            }
            Err(err) => {
                if self.use_transaction
                    && self.transaction_started
                    && self.db_client.supports_transactions()
                {
                    let _ = self.db_client.rollback().await;
                    self.transaction_started = false;
                }
                Err(err)
            }
        }
    }

    async fn flush_oracle(&mut self) -> Result<()> {
        let columns = self
            .field_names
            .iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(", ");
        let mut values = Vec::new();
        let mut param_index = 1usize;

        let sql = if let (Some(ref conflict_key), 1) = (&self.conflict_key, self.buffer.len()) {
            let record = &self.buffer[0];
            let updates = self
                .field_names
                .iter()
                .filter(|&n| n != conflict_key)
                .map(|n| format!("target.\"{}\" = src.\"{}\"", n, n))
                .collect::<Vec<_>>()
                .join(", ");
            let insert_values = self
                .field_names
                .iter()
                .map(|n| format!("src.\"{}\"", n))
                .collect::<Vec<_>>()
                .join(", ");
            let src_values = self
                .field_names
                .iter()
                .map(|_| {
                    let p = format!(":{}", param_index);
                    param_index += 1;
                    p
                })
                .collect::<Vec<_>>()
                .join(", ");

            for n in &self.field_names {
                values.push(record.get(n).cloned().unwrap_or(Value::Null));
            }

            format!(
                "MERGE INTO \"{}\" target USING (SELECT {} FROM DUAL) src ON (target.\"{}\" = src.\"{}\") WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                self.table_name, src_values, conflict_key, conflict_key, updates, columns, insert_values
            )
        } else {
            let mut s = "INSERT ALL ".to_string();
            for record in &self.buffer {
                let row_values = self
                    .field_names
                    .iter()
                    .map(|_| {
                        let p = format!(":{}", param_index);
                        param_index += 1;
                        p
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                s.push_str(&format!(
                    "INTO \"{}\" ({}) VALUES ({}) ",
                    self.table_name, columns, row_values
                ));
                for n in &self.field_names {
                    values.push(record.get(n).cloned().unwrap_or(Value::Null));
                }
            }
            s.push_str("SELECT * FROM DUAL");
            s
        };

        self.db_client.query(&sql, &values).await?;
        self.buffer.clear();
        Ok(())
    }
}

#[async_trait]
impl DataWriter for SQLWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.use_transaction
            && !self.transaction_started
            && self.db_client.supports_transactions()
        {
            self.db_client.begin_transaction().await?;
            self.transaction_started = true;
        }

        if !self.initialized_field_names {
            self.field_names = record.keys().cloned().collect();
            self.initialized_field_names = true;
        }

        self.buffer.push(record);

        if self.buffer.len() >= self.batch_size {
            self.flush().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let flush_res = self.flush().await;
        if let Err(err) = flush_res {
            if self.use_transaction
                && self.transaction_started
                && self.db_client.supports_transactions()
            {
                let _ = self.db_client.rollback().await;
                self.transaction_started = false;
            }
            return Err(err);
        }

        if self.use_transaction
            && self.transaction_started
            && self.db_client.supports_transactions()
        {
            self.db_client.commit().await?;
            self.transaction_started = false;
        }
        Ok(())
    }
}
