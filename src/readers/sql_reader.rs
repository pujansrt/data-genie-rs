use async_stream::try_stream;
use serde_json::Value;
use std::sync::Arc;

use crate::core::error::DataGenieError;
use crate::core::traits::{DataReader, RecordStream, SqlConnection};
use crate::transformers::SchemaValidator;
use crate::writers::sql_writer::{SqlDialect, quote_sql_qualified_identifier};

/// Ensures an open transaction is rolled back if a `SQLReader` stream is dropped before completion.
struct TransactionRollbackGuard {
    db_client: Arc<dyn SqlConnection>,
    active: bool,
}

impl TransactionRollbackGuard {
    fn new(db_client: Arc<dyn SqlConnection>) -> Self {
        Self { db_client, active: false }
    }

    fn mark_active(&mut self) {
        self.active = true;
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for TransactionRollbackGuard {
    fn drop(&mut self) {
        if self.active {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let client = Arc::clone(&self.db_client);
                handle.spawn(async move {
                    let _ = client.rollback().await;
                });
            }
        }
    }
}

/// Driver-agnostic SQL database reader supporting single-query execution,
/// chunked pagination, dialect-aware quoting, and optional transactions.
pub struct SQLReader {
    db_client: Arc<dyn SqlConnection>,
    query: String,
    params: Vec<Value>,
    use_transaction: bool,
    chunk_size: usize,
    order_by_column: Option<String>,
    dialect: SqlDialect,
    schema: Option<Arc<dyn SchemaValidator>>,
}

impl SQLReader {
    pub fn new(db_client: Arc<dyn SqlConnection>, query: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            db_client,
            query: query.into(),
            params,
            use_transaction: false,
            chunk_size: 0,
            order_by_column: None,
            dialect: SqlDialect::Postgres,
            schema: None,
        }
    }

    pub fn set_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    pub fn set_order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by_column = Some(column.into());
        self
    }

    pub fn set_dialect(mut self, dialect: SqlDialect) -> Self {
        self.dialect = dialect;
        self
    }

    pub fn set_use_transaction(mut self, value: bool) -> Self {
        self.use_transaction = value;
        self
    }

    pub fn with_schema<V: SchemaValidator + 'static>(mut self, schema: V) -> Self {
        self.schema = Some(Arc::new(schema));
        self
    }
}

impl DataReader for SQLReader {
    fn read(&mut self) -> RecordStream {
        let db_client = Arc::clone(&self.db_client);
        let query = self.query.clone();
        let params = self.params.clone();
        let use_transaction = self.use_transaction;
        let chunk_size = self.chunk_size;
        let order_by_column = self.order_by_column.clone();
        let dialect = self.dialect;
        let schema = self.schema.clone();

        Box::pin(try_stream! {
            let quoted_order_col = if chunk_size > 0 {
                let raw_col = order_by_column.as_deref().map(str::trim).unwrap_or("");
                if raw_col.is_empty() {
                    Err(DataGenieError::Config(
                        "order_by column must be set (and non-empty) when using chunk_size for pagination.".to_string(),
                    ))?;
                }
                Some(quote_sql_qualified_identifier(raw_col, dialect)?)
            } else {
                None
            };

            let base_query = query.trim().trim_end_matches(';').trim_end();
            if base_query.is_empty() {
                Err(DataGenieError::Config("SQLReader query must not be empty.".to_string()))?;
            }

            let mut tx_guard = TransactionRollbackGuard::new(Arc::clone(&db_client));
            if use_transaction && db_client.supports_transactions() {
                db_client.begin_transaction().await?;
                tx_guard.mark_active();
            }

            if chunk_size > 0 {
                let order_col = quoted_order_col.as_deref().unwrap();
                let mut offset = 0usize;
                loop {
                    let paginated_query = match dialect {
                        SqlDialect::Oracle => format!(
                            "SELECT * FROM ({}\n) _dg_sub ORDER BY {} OFFSET {} ROWS FETCH NEXT {} ROWS ONLY",
                            base_query, order_col, offset, chunk_size
                        ),
                        SqlDialect::Postgres | SqlDialect::MySql | SqlDialect::Sqlite => format!(
                            "SELECT * FROM ({}\n) _dg_sub ORDER BY {} LIMIT {} OFFSET {}",
                            base_query, order_col, chunk_size, offset
                        ),
                    };
                    let rows = match db_client.query(&paginated_query, &params).await {
                        Ok(r) => r,
                        Err(err) => {
                            if tx_guard.active {
                                let _ = db_client.rollback().await;
                                tx_guard.disarm();
                            }
                            Err(err)?;
                            unreachable!()
                        }
                    };

                    if rows.is_empty() {
                        break;
                    }

                    let row_count = rows.len();
                    for row in rows {
                        if let Some(ref validator) = schema {
                            match validator.parse(row) {
                                Ok(v) => yield Some(v),
                                Err(e) => {
                                    if tx_guard.active {
                                        let _ = db_client.rollback().await;
                                        tx_guard.disarm();
                                    }
                                    Err(e)?;
                                }
                            }
                        } else {
                            yield Some(row);
                        }
                    }

                    offset += chunk_size;
                    if row_count < chunk_size {
                        break;
                    }
                }
            } else {
                let rows = match db_client.query(base_query, &params).await {
                    Ok(r) => r,
                    Err(err) => {
                        if tx_guard.active {
                            let _ = db_client.rollback().await;
                            tx_guard.disarm();
                        }
                        Err(err)?;
                        unreachable!()
                    }
                };

                for row in rows {
                    if let Some(ref validator) = schema {
                        match validator.parse(row) {
                            Ok(v) => yield Some(v),
                            Err(e) => {
                                if tx_guard.active {
                                    let _ = db_client.rollback().await;
                                    tx_guard.disarm();
                                }
                                Err(e)?;
                            }
                        }
                    } else {
                        yield Some(row);
                    }
                }
            }

            if tx_guard.active {
                tx_guard.disarm();
                db_client.commit().await?;
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::Result;
    use crate::core::job::block_on;
    use crate::core::traits::DataRecord;
    use crate::record;
    use async_trait::async_trait;
    use futures::StreamExt;
    use std::sync::Mutex;

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum ReaderDbEvent {
        Begin,
        Query(String),
        Commit,
        Rollback,
    }

    struct MockReaderConn {
        events: Mutex<Vec<ReaderDbEvent>>,
        pages: Vec<Vec<DataRecord>>,
        call_idx: Mutex<usize>,
        fail_on_call: Option<usize>,
    }

    impl MockReaderConn {
        fn new(pages: Vec<Vec<DataRecord>>) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                pages,
                call_idx: Mutex::new(0),
                fail_on_call: None,
            }
        }

        fn failing(call_number: usize) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                pages: Vec::new(),
                call_idx: Mutex::new(0),
                fail_on_call: Some(call_number),
            }
        }

        fn events(&self) -> Vec<ReaderDbEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SqlConnection for MockReaderConn {
        async fn query(&self, sql: &str, _params: &[Value]) -> Result<Vec<DataRecord>> {
            let mut idx = self.call_idx.lock().unwrap();
            let current = *idx;
            *idx += 1;
            drop(idx);

            self.events.lock().unwrap().push(ReaderDbEvent::Query(sql.to_string()));
            if self.fail_on_call == Some(current + 1) {
                return Err(DataGenieError::Other("query error".to_string()));
            }
            Ok(self.pages.get(current).cloned().unwrap_or_default())
        }

        async fn begin_transaction(&self) -> Result<()> {
            self.events.lock().unwrap().push(ReaderDbEvent::Begin);
            Ok(())
        }

        async fn commit(&self) -> Result<()> {
            self.events.lock().unwrap().push(ReaderDbEvent::Commit);
            Ok(())
        }

        async fn rollback(&self) -> Result<()> {
            self.events.lock().unwrap().push(ReaderDbEvent::Rollback);
            Ok(())
        }
    }

    #[test]
    fn test_sql_reader_subquery_wrapping_comments_semicolons_and_dialects() {
        block_on(async {
            // Trailing semicolon and trailing `--` comment must not break pagination
            let conn = Arc::new(MockReaderConn::new(vec![vec![record! { "id" => 1 }]]));
            let mut reader = SQLReader::new(
                Arc::clone(&conn) as Arc<dyn SqlConnection>,
                "SELECT id FROM users ORDER BY id -- trailing comment",
                vec![],
            )
            .set_chunk_size(10)
            .set_order_by("users.id");

            let items: Vec<_> = reader.read().collect().await;
            assert_eq!(items.len(), 1);
            assert!(items[0].is_ok());

            let events = conn.events();
            let ReaderDbEvent::Query(q) = &events[0] else {
                panic!("expected query event");
            };
            assert_eq!(
                q,
                "SELECT * FROM (SELECT id FROM users ORDER BY id -- trailing comment\n) _dg_sub ORDER BY \"users\".\"id\" LIMIT 10 OFFSET 0"
            );

            // MySQL backticks + trailing semicolon stripped
            let conn_my = Arc::new(MockReaderConn::new(vec![vec![record! { "id" => 1 }]]));
            let mut my_reader = SQLReader::new(Arc::clone(&conn_my) as Arc<dyn SqlConnection>, "SELECT * FROM users;  ", vec![])
                .set_dialect(SqlDialect::MySql)
                .set_chunk_size(5)
                .set_order_by("id");
            let _: Vec<_> = my_reader.read().collect().await;
            let ReaderDbEvent::Query(my_q) = &conn_my.events()[0] else {
                panic!("expected query");
            };
            assert_eq!(
                my_q,
                "SELECT * FROM (SELECT * FROM users\n) _dg_sub ORDER BY `id` LIMIT 5 OFFSET 0"
            );

            // Oracle OFFSET ... ROWS FETCH NEXT ... ROWS ONLY
            let conn_ora = Arc::new(MockReaderConn::new(vec![vec![record! { "id" => 1 }]]));
            let mut ora_reader = SQLReader::new(Arc::clone(&conn_ora) as Arc<dyn SqlConnection>, "SELECT * FROM users", vec![])
                .set_dialect(SqlDialect::Oracle)
                .set_chunk_size(5)
                .set_order_by("id");
            let _: Vec<_> = ora_reader.read().collect().await;
            let ReaderDbEvent::Query(ora_q) = &conn_ora.events()[0] else {
                panic!("expected query");
            };
            assert_eq!(
                ora_q,
                "SELECT * FROM (SELECT * FROM users\n) _dg_sub ORDER BY \"id\" OFFSET 0 ROWS FETCH NEXT 5 ROWS ONLY"
            );
        });
    }

    #[test]
    fn test_sql_reader_injection_escaping_and_empty_order_by_rejected() {
        block_on(async {
            // Injection payload in order_by is escaped inside quotes
            let conn = Arc::new(MockReaderConn::new(vec![vec![]]));
            let mut reader = SQLReader::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "SELECT * FROM users", vec![])
                .set_chunk_size(10)
                .set_order_by("id\"; DROP TABLE users; --");
            let items: Vec<_> = reader.read().collect().await;
            assert!(items.is_empty());
            let ReaderDbEvent::Query(q) = &conn.events()[0] else {
                panic!("expected query");
            };
            assert!(q.contains("ORDER BY \"id\"\"; DROP TABLE users; --\" LIMIT 10 OFFSET 0"));

            // Whitespace-only order_by rejected
            let conn2 = Arc::new(MockReaderConn::new(vec![]));
            let mut bad_reader = SQLReader::new(Arc::clone(&conn2) as Arc<dyn SqlConnection>, "SELECT * FROM users", vec![])
                .set_chunk_size(10)
                .set_order_by("   ");
            let first = bad_reader.read().next().await.unwrap();
            assert!(matches!(first, Err(DataGenieError::Config(_))));
        });
    }

    #[test]
    fn test_sql_reader_transaction_commit_error_rollback_and_early_drop_rollback() {
        block_on(async {
            // 1. Happy path commits
            let conn_ok = Arc::new(MockReaderConn::new(vec![vec![record! { "id" => 1 }]]));
            let mut ok_reader = SQLReader::new(Arc::clone(&conn_ok) as Arc<dyn SqlConnection>, "SELECT * FROM t", vec![])
                .set_use_transaction(true);
            let items: Vec<_> = ok_reader.read().collect().await;
            assert_eq!(items.len(), 1);
            assert_eq!(
                conn_ok.events(),
                vec![
                    ReaderDbEvent::Begin,
                    ReaderDbEvent::Query("SELECT * FROM t".to_string()),
                    ReaderDbEvent::Commit,
                ]
            );

            // 2. Query error rolls back
            let conn_err = Arc::new(MockReaderConn::failing(1));
            let mut err_reader = SQLReader::new(Arc::clone(&conn_err) as Arc<dyn SqlConnection>, "SELECT * FROM t", vec![])
                .set_use_transaction(true);
            let first = err_reader.read().next().await.unwrap();
            assert!(first.is_err());
            assert_eq!(
                conn_err.events(),
                vec![
                    ReaderDbEvent::Begin,
                    ReaderDbEvent::Query("SELECT * FROM t".to_string()),
                    ReaderDbEvent::Rollback,
                ]
            );

            // 3. Early stream drop (e.g. taking 1 row out of 2) triggers drop-guard rollback
            let conn_drop = Arc::new(MockReaderConn::new(vec![vec![record! { "id" => 1 }, record! { "id" => 2 }]]));
            let mut drop_reader = SQLReader::new(Arc::clone(&conn_drop) as Arc<dyn SqlConnection>, "SELECT * FROM t", vec![])
                .set_use_transaction(true);
            {
                let mut stream = drop_reader.read();
                let first = stream.next().await.unwrap().unwrap();
                assert!(first.is_some());
                // Drop stream without exhausting it
            }
            // Give the spawned rollback task a moment to complete
            tokio::task::yield_now().await;
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            let events = conn_drop.events();
            assert!(events.contains(&ReaderDbEvent::Begin));
            assert!(events.contains(&ReaderDbEvent::Rollback));
            assert!(!events.contains(&ReaderDbEvent::Commit));
        });
    }
}
