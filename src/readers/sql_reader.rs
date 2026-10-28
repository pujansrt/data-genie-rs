use async_stream::try_stream;
use serde_json::Value;
use std::sync::Arc;

use crate::core::error::DataGenieError;
use crate::core::traits::{DataReader, RecordStream, SqlConnection};
use crate::transformers::SchemaValidator;

/// Driver-agnostic SQL database reader supporting single-query execution,
/// `LIMIT`/`OFFSET` chunked pagination, and optional transactions.
pub struct SQLReader {
    db_client: Arc<dyn SqlConnection>,
    query: String,
    params: Vec<Value>,
    use_transaction: bool,
    chunk_size: usize,
    order_by_column: Option<String>,
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
        let schema = self.schema.clone();

        Box::pin(try_stream! {
            if chunk_size > 0 && order_by_column.is_none() {
                Err(DataGenieError::Config(
                    "orderByColumn must be set when using chunkSize for pagination.".to_string(),
                ))?;
            }

            let mut tx_started = false;
            if use_transaction && db_client.supports_transactions() {
                db_client.begin_transaction().await?;
                tx_started = true;
            }

            if chunk_size > 0 {
                let order_col = order_by_column.as_deref().unwrap();
                let mut offset = 0usize;
                loop {
                    let paginated_query = format!(
                        "{} ORDER BY \"{}\" LIMIT {} OFFSET {}",
                        query, order_col, chunk_size, offset
                    );
                    let rows = match db_client.query(&paginated_query, &params).await {
                        Ok(r) => r,
                        Err(err) => {
                            if tx_started {
                                let _ = db_client.rollback().await;
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
                                    if tx_started {
                                        let _ = db_client.rollback().await;
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
                let rows = match db_client.query(&query, &params).await {
                    Ok(r) => r,
                    Err(err) => {
                        if tx_started {
                            let _ = db_client.rollback().await;
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
                                if tx_started {
                                    let _ = db_client.rollback().await;
                                }
                                Err(e)?;
                            }
                        }
                    } else {
                        yield Some(row);
                    }
                }
            }

            if tx_started {
                db_client.commit().await?;
            }
        })
    }
}
