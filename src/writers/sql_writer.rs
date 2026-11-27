use async_trait::async_trait;
use serde_json::Value;
use std::sync::Arc;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataWriter, SqlConnection};

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

/// Quotes a single SQL identifier according to the target dialect, escaping embedded quote characters.
pub(crate) fn quote_sql_identifier(ident: &str, dialect: SqlDialect) -> Result<String> {
    if ident.trim().is_empty() || ident.contains('\0') {
        return Err(DataGenieError::Config(format!("Invalid SQL identifier: {:?}", ident)));
    }
    Ok(match dialect {
        SqlDialect::MySql => format!("`{}`", ident.replace('`', "``")),
        SqlDialect::Postgres | SqlDialect::Sqlite | SqlDialect::Oracle => {
            format!("\"{}\"", ident.replace('"', "\"\""))
        }
    })
}

/// Quotes a potentially schema/table-qualified SQL identifier (e.g. `public.users` -> `"public"."users"`).
pub(crate) fn quote_sql_qualified_identifier(ident: &str, dialect: SqlDialect) -> Result<String> {
    let trimmed = ident.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return Err(DataGenieError::Config(format!("Invalid SQL identifier: {:?}", ident)));
    }
    // Split on '.' only when there are no embedded quotes/backticks/whitespace so literal
    // weird identifiers or injection payloads are escaped as a single quoted identifier.
    if trimmed.contains('.')
        && !trimmed.contains('"')
        && !trimmed.contains('`')
        && !trimmed.chars().any(char::is_whitespace)
    {
        let mut quoted_parts = Vec::new();
        for part in trimmed.split('.') {
            if part.is_empty() {
                return Err(DataGenieError::Config(format!("Invalid qualified SQL identifier: {:?}", ident)));
            }
            quoted_parts.push(quote_sql_identifier(part, dialect)?);
        }
        return Ok(quoted_parts.join("."));
    }
    quote_sql_identifier(ident, dialect)
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
    is_closed: bool,
    failed: bool,
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
            is_closed: false,
            failed: false,
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
            return Err(DataGenieError::Config("Batch size must be at least 1".to_string()));
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

    async fn rollback_if_started(&mut self) {
        if self.use_transaction && self.transaction_started && self.db_client.supports_transactions() {
            let _ = self.db_client.rollback().await;
            self.transaction_started = false;
        }
    }

    async fn handle_flush_error(&mut self, from_write: bool) {
        if self.use_transaction && self.transaction_started && self.db_client.supports_transactions() {
            self.rollback_if_started().await;
            self.buffer.clear();
            self.failed = true;
        } else if from_write {
            // Remove only the just-pushed record so non-transactional retry decorators
            // can safely retry the failed write without duplicating it in the batch.
            self.buffer.pop();
        }
    }

    async fn flush(&mut self, from_write: bool) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        if self.field_names.is_empty() {
            let err = DataGenieError::Config("SQLWriter requires at least one field name".to_string());
            self.handle_flush_error(from_write).await;
            return Err(err);
        }

        if let Some(ref conflict_key) = self.conflict_key
            && !self.field_names.iter().any(|n| n == conflict_key)
        {
            let err = DataGenieError::Config(format!(
                "Upsert conflict key '{}' is not present in SQLWriter field names",
                conflict_key
            ));
            self.handle_flush_error(from_write).await;
            return Err(err);
        }

        if self.dialect == SqlDialect::Oracle {
            return self.flush_oracle(from_write).await;
        }

        let quoted_table = match quote_sql_qualified_identifier(&self.table_name, self.dialect) {
            Ok(t) => t,
            Err(err) => {
                self.handle_flush_error(from_write).await;
                return Err(err);
            }
        };

        let quoted_fields: Vec<String> = match self
            .field_names
            .iter()
            .map(|n| quote_sql_identifier(n, self.dialect))
            .collect::<Result<Vec<_>>>()
        {
            Ok(f) => f,
            Err(err) => {
                self.handle_flush_error(from_write).await;
                return Err(err);
            }
        };

        let columns = quoted_fields.join(", ");
        let mut rows = Vec::with_capacity(self.buffer.len());
        let mut values = Vec::with_capacity(self.buffer.len() * self.field_names.len());
        let mut param_index = 1usize;

        for record in &self.buffer {
            let placeholders = self
                .field_names
                .iter()
                .map(|_| match self.dialect {
                    SqlDialect::MySql => "?".to_string(),
                    SqlDialect::Postgres | SqlDialect::Sqlite => {
                        let p = format!("${}", param_index);
                        param_index += 1;
                        p
                    }
                    SqlDialect::Oracle => unreachable!(),
                })
                .collect::<Vec<_>>()
                .join(", ");
            rows.push(format!("({})", placeholders));

            for name in &self.field_names {
                values.push(record.get(name).cloned().unwrap_or(Value::Null));
            }
        }

        let mut sql = format!("INSERT INTO {} ({}) VALUES {}", quoted_table, columns, rows.join(", "));

        if let Some(ref conflict_key) = self.conflict_key {
            let quoted_key = quote_sql_identifier(conflict_key, self.dialect)?;
            match self.dialect {
                SqlDialect::Postgres | SqlDialect::Sqlite => {
                    let updates: Vec<String> = self
                        .field_names
                        .iter()
                        .zip(quoted_fields.iter())
                        .filter(|(raw, _)| raw.as_str() != conflict_key.as_str())
                        .map(|(_, q)| format!("{} = EXCLUDED.{}", q, q))
                        .collect();
                    if updates.is_empty() {
                        sql.push_str(&format!(" ON CONFLICT ({}) DO NOTHING", quoted_key));
                    } else {
                        sql.push_str(&format!(" ON CONFLICT ({}) DO UPDATE SET {}", quoted_key, updates.join(", ")));
                    }
                }
                SqlDialect::MySql => {
                    let updates: Vec<String> = self
                        .field_names
                        .iter()
                        .zip(quoted_fields.iter())
                        .filter(|(raw, _)| raw.as_str() != conflict_key.as_str())
                        .map(|(_, q)| format!("{} = VALUES({})", q, q))
                        .collect();
                    if updates.is_empty() {
                        sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {} = {}", quoted_key, quoted_key));
                    } else {
                        sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {}", updates.join(", ")));
                    }
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
                self.handle_flush_error(from_write).await;
                Err(err)
            }
        }
    }

    async fn flush_oracle(&mut self, from_write: bool) -> Result<()> {
        let quoted_table = match quote_sql_qualified_identifier(&self.table_name, SqlDialect::Oracle) {
            Ok(t) => t,
            Err(err) => {
                self.handle_flush_error(from_write).await;
                return Err(err);
            }
        };

        let quoted_fields: Vec<String> = match self
            .field_names
            .iter()
            .map(|n| quote_sql_identifier(n, SqlDialect::Oracle))
            .collect::<Result<Vec<_>>>()
        {
            Ok(f) => f,
            Err(err) => {
                self.handle_flush_error(from_write).await;
                return Err(err);
            }
        };

        let columns = quoted_fields.join(", ");
        let mut values = Vec::with_capacity(self.buffer.len() * self.field_names.len());
        let mut param_index = 1usize;

        let sql = if let Some(ref conflict_key) = self.conflict_key {
            let quoted_key = quote_sql_identifier(conflict_key, SqlDialect::Oracle)?;
            let updates: Vec<String> = self
                .field_names
                .iter()
                .zip(quoted_fields.iter())
                .filter(|(raw, _)| raw.as_str() != conflict_key.as_str())
                .map(|(_, q)| format!("target.{} = src.{}", q, q))
                .collect();
            let insert_values = quoted_fields
                .iter()
                .map(|q| format!("src.{}", q))
                .collect::<Vec<_>>()
                .join(", ");

            let mut using_rows = Vec::with_capacity(self.buffer.len());
            for record in &self.buffer {
                let select_exprs = quoted_fields
                    .iter()
                    .map(|q| {
                        let p = format!(":{} AS {}", param_index, q);
                        param_index += 1;
                        p
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                using_rows.push(format!("SELECT {} FROM DUAL", select_exprs));

                for n in &self.field_names {
                    values.push(record.get(n).cloned().unwrap_or(Value::Null));
                }
            }

            let using_clause = using_rows.join(" UNION ALL ");
            if updates.is_empty() {
                format!(
                    "MERGE INTO {} target USING ({}) src ON (target.{} = src.{}) WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    quoted_table, using_clause, quoted_key, quoted_key, columns, insert_values
                )
            } else {
                format!(
                    "MERGE INTO {} target USING ({}) src ON (target.{} = src.{}) WHEN MATCHED THEN UPDATE SET {} WHEN NOT MATCHED THEN INSERT ({}) VALUES ({})",
                    quoted_table,
                    using_clause,
                    quoted_key,
                    quoted_key,
                    updates.join(", "),
                    columns,
                    insert_values
                )
            }
        } else if self.buffer.len() == 1 {
            let record = &self.buffer[0];
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
            for n in &self.field_names {
                values.push(record.get(n).cloned().unwrap_or(Value::Null));
            }
            format!("INSERT INTO {} ({}) VALUES ({})", quoted_table, columns, row_values)
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
                s.push_str(&format!("INTO {} ({}) VALUES ({}) ", quoted_table, columns, row_values));
                for n in &self.field_names {
                    values.push(record.get(n).cloned().unwrap_or(Value::Null));
                }
            }
            s.push_str("SELECT 1 FROM DUAL");
            s
        };

        match self.db_client.query(&sql, &values).await {
            Ok(_) => {
                self.buffer.clear();
                Ok(())
            }
            Err(err) => {
                self.handle_flush_error(from_write).await;
                Err(err)
            }
        }
    }
}

#[async_trait]
impl DataWriter for SQLWriter {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        if self.is_closed {
            return Err(DataGenieError::Other("Writer is already closed".to_string()));
        }
        if self.failed {
            return Err(DataGenieError::Other(
                "SQLWriter is in a failed state after a previous transaction error".to_string(),
            ));
        }

        if !self.initialized_field_names {
            if record.is_empty() {
                return Err(DataGenieError::Config(
                    "SQLWriter cannot infer field names from an empty record".to_string(),
                ));
            }
            self.field_names = record.keys().cloned().collect();
            self.initialized_field_names = true;
        } else if self.field_names.is_empty() {
            return Err(DataGenieError::Config(
                "SQLWriter requires at least one field name".to_string(),
            ));
        }

        if self.use_transaction && !self.transaction_started && self.db_client.supports_transactions() {
            self.db_client.begin_transaction().await?;
            self.transaction_started = true;
        }

        self.buffer.push(record);

        if self.buffer.len() >= self.batch_size {
            self.flush(true).await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }

        if self.failed {
            self.rollback_if_started().await;
            self.buffer.clear();
            self.is_closed = true;
            return Ok(());
        }

        if let Err(err) = self.flush(false).await {
            if self.use_transaction {
                self.rollback_if_started().await;
                self.buffer.clear();
                self.failed = true;
                self.is_closed = true;
            }
            return Err(err);
        }

        if self.use_transaction && self.transaction_started && self.db_client.supports_transactions() {
            if let Err(err) = self.db_client.commit().await {
                self.rollback_if_started().await;
                self.failed = true;
                self.is_closed = true;
                return Err(err);
            }
            self.transaction_started = false;
        }

        self.is_closed = true;
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        if self.is_closed {
            return Ok(());
        }
        self.is_closed = true;
        self.failed = true;
        self.buffer.clear();
        self.rollback_if_started().await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::job::{Job, block_on};
    use crate::core::traits::{DataReader, RecordStream};
    use crate::record;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum DbEvent {
        Begin,
        Query(String, Vec<Value>),
        Commit,
        Rollback,
    }

    struct RecordingSqlConn {
        events: Mutex<Vec<DbEvent>>,
        fail_on_query_call: Mutex<Option<usize>>,
        query_calls: Mutex<usize>,
    }

    impl RecordingSqlConn {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                fail_on_query_call: Mutex::new(None),
                query_calls: Mutex::new(0),
            }
        }

        fn fail_at(call_number: usize) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                fail_on_query_call: Mutex::new(Some(call_number)),
                query_calls: Mutex::new(0),
            }
        }

        fn events(&self) -> Vec<DbEvent> {
            self.events.lock().unwrap().clone()
        }

        fn queries(&self) -> Vec<(String, Vec<Value>)> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter_map(|e| match e {
                    DbEvent::Query(sql, params) => Some((sql.clone(), params.clone())),
                    _ => None,
                })
                .collect()
        }
    }

    #[async_trait]
    impl SqlConnection for RecordingSqlConn {
        async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<DataRecord>> {
            let mut count = self.query_calls.lock().unwrap();
            *count += 1;
            let current = *count;
            drop(count);

            self.events
                .lock()
                .unwrap()
                .push(DbEvent::Query(sql.to_string(), params.to_vec()));

            if *self.fail_on_query_call.lock().unwrap() == Some(current) {
                return Err(DataGenieError::Other(format!("simulated db error on query {}", current)));
            }
            Ok(Vec::new())
        }

        async fn begin_transaction(&self) -> Result<()> {
            self.events.lock().unwrap().push(DbEvent::Begin);
            Ok(())
        }

        async fn commit(&self) -> Result<()> {
            self.events.lock().unwrap().push(DbEvent::Commit);
            Ok(())
        }

        async fn rollback(&self) -> Result<()> {
            self.events.lock().unwrap().push(DbEvent::Rollback);
            Ok(())
        }
    }

    #[test]
    fn test_sql_dialect_from_name() {
        assert_eq!(SqlDialect::from_name("postgres"), SqlDialect::Postgres);
        assert_eq!(SqlDialect::from_name("POSTGRES"), SqlDialect::Postgres);
        assert_eq!(SqlDialect::from_name("mysql"), SqlDialect::MySql);
        assert_eq!(SqlDialect::from_name("MySQL"), SqlDialect::MySql);
        assert_eq!(SqlDialect::from_name("sqlite"), SqlDialect::Sqlite);
        assert_eq!(SqlDialect::from_name("oracle"), SqlDialect::Oracle);
        assert_eq!(SqlDialect::from_name("unknown"), SqlDialect::Postgres);
    }

    #[test]
    fn test_postgres_and_sqlite_insert_and_upserts() {
        block_on(async {
            for dialect in [SqlDialect::Postgres, SqlDialect::Sqlite] {
                let conn = Arc::new(RecordingSqlConn::new());
                let mut writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "public.users")
                    .set_dialect(dialect)
                    .set_upsert("id")
                    .set_batch_size(2)
                    .unwrap();

                writer.write(record! { "id" => 1, "email" => "a@test.com" }).await.unwrap();
                writer.write(record! { "id" => 2, "email" => "b@test.com" }).await.unwrap();
                writer.close().await.unwrap();

                let queries = conn.queries();
                assert_eq!(queries.len(), 1);
                assert_eq!(
                    queries[0].0,
                    "INSERT INTO \"public\".\"users\" (\"id\", \"email\") VALUES ($1, $2), ($3, $4) ON CONFLICT (\"id\") DO UPDATE SET \"email\" = EXCLUDED.\"email\""
                );
                assert_eq!(queries[0].1, vec![json!(1), json!("a@test.com"), json!(2), json!("b@test.com")]);

                // Single-column upsert uses DO NOTHING instead of invalid empty DO UPDATE SET
                let conn_single = Arc::new(RecordingSqlConn::new());
                let mut single_writer = SQLWriter::new(Arc::clone(&conn_single) as Arc<dyn SqlConnection>, "tags")
                    .set_dialect(dialect)
                    .set_upsert("tag");
                single_writer.write(record! { "tag" => "rust" }).await.unwrap();
                single_writer.close().await.unwrap();
                assert_eq!(
                    conn_single.queries()[0].0,
                    "INSERT INTO \"tags\" (\"tag\") VALUES ($1) ON CONFLICT (\"tag\") DO NOTHING"
                );
            }
        });
    }

    #[test]
    fn test_mysql_placeholders_backticks_and_upserts() {
        block_on(async {
            let conn = Arc::new(RecordingSqlConn::new());
            let mut writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "app.users")
                .set_dialect(SqlDialect::MySql)
                .set_upsert("id")
                .with_batch_size(2);

            writer.write(record! { "id" => 1, "name" => "Alice" }).await.unwrap();
            writer.write(record! { "id" => 2, "name" => "Bob" }).await.unwrap();
            writer.close().await.unwrap();

            let queries = conn.queries();
            assert_eq!(queries.len(), 1);
            assert_eq!(
                queries[0].0,
                "INSERT INTO `app`.`users` (`id`, `name`) VALUES (?, ?), (?, ?) ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)"
            );
            assert_eq!(queries[0].1, vec![json!(1), json!("Alice"), json!(2), json!("Bob")]);

            // Single-column MySQL upsert emits valid no-op update
            let conn_single = Arc::new(RecordingSqlConn::new());
            let mut single_writer = SQLWriter::new(Arc::clone(&conn_single) as Arc<dyn SqlConnection>, "tags")
                .set_dialect(SqlDialect::MySql)
                .set_upsert("id");
            single_writer.write(record! { "id" => 7 }).await.unwrap();
            single_writer.close().await.unwrap();
            assert_eq!(
                conn_single.queries()[0].0,
                "INSERT INTO `tags` (`id`) VALUES (?) ON DUPLICATE KEY UPDATE `id` = `id`"
            );
        });
    }

    #[test]
    fn test_oracle_inserts_and_single_and_multi_row_merge_upserts() {
        block_on(async {
            // 1. Multi-row MERGE upsert (batch_size > 1) with column aliases and UNION ALL
            let conn = Arc::new(RecordingSqlConn::new());
            let mut writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "users")
                .set_dialect(SqlDialect::Oracle)
                .set_upsert("id")
                .with_batch_size(2);

            writer.write(record! { "id" => 1, "email" => "a@ora.io" }).await.unwrap();
            writer.write(record! { "id" => 2, "email" => "b@ora.io" }).await.unwrap();
            writer.close().await.unwrap();

            let queries = conn.queries();
            assert_eq!(queries.len(), 1);
            assert_eq!(
                queries[0].0,
                "MERGE INTO \"users\" target USING (SELECT :1 AS \"id\", :2 AS \"email\" FROM DUAL UNION ALL SELECT :3 AS \"id\", :4 AS \"email\" FROM DUAL) src ON (target.\"id\" = src.\"id\") WHEN MATCHED THEN UPDATE SET target.\"email\" = src.\"email\" WHEN NOT MATCHED THEN INSERT (\"id\", \"email\") VALUES (src.\"id\", src.\"email\")"
            );
            assert_eq!(queries[0].1, vec![json!(1), json!("a@ora.io"), json!(2), json!("b@ora.io")]);

            // 2. Single-column Oracle MERGE omits WHEN MATCHED
            let conn_single = Arc::new(RecordingSqlConn::new());
            let mut single_writer = SQLWriter::new(Arc::clone(&conn_single) as Arc<dyn SqlConnection>, "keys")
                .set_dialect(SqlDialect::Oracle)
                .set_upsert("id");
            single_writer.write(record! { "id" => 99 }).await.unwrap();
            single_writer.close().await.unwrap();
            assert_eq!(
                conn_single.queries()[0].0,
                "MERGE INTO \"keys\" target USING (SELECT :1 AS \"id\" FROM DUAL) src ON (target.\"id\" = src.\"id\") WHEN NOT MATCHED THEN INSERT (\"id\") VALUES (src.\"id\")"
            );

            // 3. Oracle plain single-row INSERT and multi-row INSERT ALL
            let conn_plain = Arc::new(RecordingSqlConn::new());
            let mut plain_writer = SQLWriter::new(Arc::clone(&conn_plain) as Arc<dyn SqlConnection>, "events")
                .set_dialect(SqlDialect::Oracle)
                .with_batch_size(2);
            plain_writer.write(record! { "id" => 1, "v" => "x" }).await.unwrap();
            plain_writer.write(record! { "id" => 2, "v" => "y" }).await.unwrap();
            plain_writer.write(record! { "id" => 3, "v" => "z" }).await.unwrap();
            plain_writer.close().await.unwrap();

            let plain_queries = conn_plain.queries();
            assert_eq!(plain_queries.len(), 2);
            assert_eq!(
                plain_queries[0].0,
                "INSERT ALL INTO \"events\" (\"id\", \"v\") VALUES (:1, :2) INTO \"events\" (\"id\", \"v\") VALUES (:3, :4) SELECT 1 FROM DUAL"
            );
            assert_eq!(
                plain_queries[1].0,
                "INSERT INTO \"events\" (\"id\", \"v\") VALUES (:1, :2)"
            );
        });
    }

    #[test]
    fn test_sql_identifier_injection_escaping_and_validation() {
        block_on(async {
            // Double-quote escaping in Postgres
            let conn_pg = Arc::new(RecordingSqlConn::new());
            let mut pg = SQLWriter::new(Arc::clone(&conn_pg) as Arc<dyn SqlConnection>, "users\"; DROP TABLE x; --")
                .set_dialect(SqlDialect::Postgres)
                .set_upsert("id\"key");
            pg.write(record! { "id\"key" => 1, "col\"2" => "v" }).await.unwrap();
            pg.close().await.unwrap();
            let pg_sql = &conn_pg.queries()[0].0;
            assert!(pg_sql.starts_with("INSERT INTO \"users\"\"; DROP TABLE x; --\" (\"id\"\"key\", \"col\"\"2\")"));
            assert!(pg_sql.contains("ON CONFLICT (\"id\"\"key\") DO UPDATE SET \"col\"\"2\" = EXCLUDED.\"col\"\"2\""));

            // Backtick escaping in MySQL
            let conn_my = Arc::new(RecordingSqlConn::new());
            let mut my = SQLWriter::new(Arc::clone(&conn_my) as Arc<dyn SqlConnection>, "users`; DROP TABLE x; --")
                .set_dialect(SqlDialect::MySql);
            my.write(record! { "a`b" => 1 }).await.unwrap();
            my.close().await.unwrap();
            assert_eq!(
                conn_my.queries()[0].0,
                "INSERT INTO `users``; DROP TABLE x; --` (`a``b`) VALUES (?)"
            );

            // Config validation checks
            let conn_err = Arc::new(RecordingSqlConn::new());
            assert!(SQLWriter::new(Arc::clone(&conn_err) as Arc<dyn SqlConnection>, "t").set_batch_size(0).is_err());

            let mut empty_fields = SQLWriter::new(Arc::clone(&conn_err) as Arc<dyn SqlConnection>, "t").set_field_names(Vec::<String>::new());
            assert!(empty_fields.write(record! { "a" => 1 }).await.is_err());

            let mut empty_rec = SQLWriter::new(Arc::clone(&conn_err) as Arc<dyn SqlConnection>, "t");
            assert!(empty_rec.write(DataRecord::new()).await.is_err());

            let mut bad_key = SQLWriter::new(Arc::clone(&conn_err) as Arc<dyn SqlConnection>, "t")
                .set_field_names(["id", "name"])
                .set_upsert("missing_key");
            assert!(bad_key.write(record! { "id" => 1, "name" => "a" }).await.is_err());
        });
    }

    #[test]
    fn test_sql_writer_transaction_rollback_and_no_uncommitted_replay() {
        block_on(async {
            // 1. Postgres & Oracle: query error rolls back, clears buffer, and close() does NOT replay or commit
            for dialect in [SqlDialect::Postgres, SqlDialect::Oracle] {
                let conn = Arc::new(RecordingSqlConn::fail_at(2));
                let mut writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "orders")
                    .set_dialect(dialect)
                    .with_batch_size(1)
                    .set_use_transaction(true);

                writer.write(record! { "id" => 1 }).await.unwrap();
                assert!(writer.write(record! { "id" => 2 }).await.is_err());

                // Calling close() after a failed transactional write must NOT execute query #3 or commit!
                writer.close().await.unwrap();
                // Double close is a no-op
                writer.close().await.unwrap();
                // Write after close fails
                assert!(writer.write(record! { "id" => 3 }).await.is_err());

                let events = conn.events();
                assert_eq!(events.iter().filter(|e| matches!(e, DbEvent::Begin)).count(), 1);
                assert_eq!(events.iter().filter(|e| matches!(e, DbEvent::Query(_, _))).count(), 2);
                assert_eq!(events.iter().filter(|e| matches!(e, DbEvent::Rollback)).count(), 1);
                assert_eq!(events.iter().filter(|e| matches!(e, DbEvent::Commit)).count(), 0);
            }

            // 2. Upstream reader error mid-job calls abort() -> rolls back transaction without committing partial batch
            struct FailingReader;
            impl DataReader for FailingReader {
                fn read(&mut self) -> RecordStream {
                    Box::pin(async_stream::try_stream! {
                        yield Some(record! { "id" => 1 });
                        yield Some(record! { "id" => 2 });
                        Err(DataGenieError::Other("upstream disk failure".to_string()))?;
                    })
                }
            }

            let conn = Arc::new(RecordingSqlConn::new());
            let writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "orders")
                .with_batch_size(10) // stays buffered when reader fails
                .set_use_transaction(true);

            let res = Job::run(FailingReader, writer).await;
            assert!(res.is_err());

            let events = conn.events();
            assert_eq!(events, vec![DbEvent::Begin, DbEvent::Rollback]);
        });
    }

    #[test]
    fn test_non_transactional_sql_writer_with_retrying_writer_no_duplicates() {
        use crate::writers::retrying_writer::{RetryOptions, RetryingWriter};

        block_on(async {
            // Query #1 (first flush of batch [1, 2]) fails once, then retry succeeds with exact 2 rows (not 3!)
            let conn = Arc::new(RecordingSqlConn::fail_at(1));
            let sql_writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "items")
                .set_dialect(SqlDialect::Postgres)
                .with_batch_size(2);
            let retrying = RetryingWriter::with_options(
                sql_writer,
                RetryOptions {
                    max_retries: 2,
                    initial_delay_ms: 1,
                    backoff_factor: 1,
                    circuit_breaker_threshold: 5,
                    circuit_reset_timeout_ms: 1000,
                },
            );

            let input = vec![
                record! { "id" => 1 },
                record! { "id" => 2 },
                record! { "id" => 3 },
            ];
            Job::run(crate::readers::MemoryReader::new(input), retrying).await.unwrap();

            let queries = conn.queries();
            // Call 1 (failed): [1, 2], Call 2 (retry): [1, 2], Call 3 (close flush): [3]
            assert_eq!(queries.len(), 3);
            assert_eq!(queries[0].1, vec![json!(1), json!(2)]);
            assert_eq!(queries[1].1, vec![json!(1), json!(2)]);
            assert_eq!(queries[2].1, vec![json!(3)]);
        });
    }
}
