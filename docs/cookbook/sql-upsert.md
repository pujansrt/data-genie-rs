# Idempotent Data Loading (SQL Upsert)

Prevent duplicate rows when re-running ETL jobs by configuring `SQLWriter` with `.with_upsert(conflict_key)`.

## Implementation

```rust
use data_genie::{CSVReader, Job, SQLWriter, SqlDialect};
use std::sync::Arc;

fn run_sync(db: Arc<dyn data_genie::SqlClient>) -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");

    let writer = SQLWriter::new(db, "users")
        .with_dialect(SqlDialect::Postgres) // Postgres, MySql, Sqlite, Oracle
        .with_upsert("email")               // Conflict key column
        .with_batch_size(100)
        .with_transaction(true);

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## Dialect-Specific SQL Generation

`SQLWriter` automatically generates native upsert statements for your target database:
- **PostgreSQL / SQLite**: `INSERT INTO "users" (...) VALUES (...) ON CONFLICT ("email") DO UPDATE SET ...`
- **MySQL**: `INSERT INTO users (...) VALUES (...) ON DUPLICATE KEY UPDATE ...`
- **Oracle**: `MERGE INTO "users" t USING (...) s ON (t."email" = s."email") WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`
