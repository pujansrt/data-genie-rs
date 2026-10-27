# Streaming CSV to PostgreSQL

Stream millions of CSV rows into PostgreSQL in batches without exhausting memory using `CSVReader` and `SQLWriter`.

## Implementation

Implement the lightweight `SqlClient` trait for your database connection (e.g., `tokio_postgres` or `sqlx`) and pass it to `SQLWriter`:

```rust
use async_trait::async_trait;
use data_genie::{CSVReader, Job, Record, Result, SQLWriter, SqlClient, SqlDialect};
use serde_json::Value;
use std::sync::Arc;

struct PostgresAdapter {
    // Wrap your tokio_postgres::Client or sqlx::PgPool here
}

#[async_trait]
impl SqlClient for PostgresAdapter {
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<u64> {
        println!("Executing batch SQL: {} ({} params)", sql, params.len());
        Ok(1)
    }

    async fn query(&self, _sql: &str, _params: &[Value]) -> Result<Vec<Record>> {
        Ok(vec![])
    }
}

fn main() -> Result<()> {
    let client = Arc::new(PostgresAdapter {});
    let reader = CSVReader::new("large_data.csv");

    let writer = SQLWriter::new(client, "users")
        .with_dialect(SqlDialect::Postgres)
        .with_batch_size(500);

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## Why This Works
1. **Streaming (`O(1)` RAM)**: `CSVReader` streams records one by one.
2. **Automatic Batching**: `SQLWriter` groups records into parameterized multi-row `INSERT` batches.
3. **Backpressure**: The reader pauses automatically while the database commits each batch.
