# Writers

Writers persist processed `Record` streams to a destination sink.

`StdoutSink` writes formatted bytes to process standard output. Pass it to a format writer, such as `NDJsonWriter::new(StdoutSink)`. For a direct Rust job, use `StderrLogger` so informational logs do not mix with output. In YAML pipelines, set `write.path: "-"`; logs are routed to standard error automatically.

```rust
let options = JobOptions::new().with_logger(StderrLogger);
let mut job = Job::with_options(reader, NDJsonWriter::new(StdoutSink), options);
job.execute().await?;
```

## `CSVWriter`, `TSVWriter` & `FixedWidthWriter`
- **`CSVWriter::new("output.csv")`**: Writes CSV files with automatic header detection.
- **`TSVWriter::new("output.tsv")`**: Writes tab-separated files.
- **`FixedWidthWriter::new("output.fw", fields)`**: Writes fixed-width formatted rows.

## `JsonWriter`, `NDJsonWriter` & `XMLWriter`
- **`JsonWriter::new("output.json")`**: Streams a formatted JSON array incrementally without buffering all records in memory.
- **`NDJsonWriter::new("output.ndjson")`**: Writes one JSON object per line.
- **`XMLWriter::new("output.xml", "root", "record")`**: Streams XML elements.

## `ParquetWriter` *(feature: `parquet`)*
Writes Arrow/Parquet files with configurable batch size and schema. Buffers the encoded Parquet file in memory until `close()`.
- **Constructor**: `ParquetWriter::new("output.parquet")`
- **Builder Methods**: `.with_schema(schema)`, `.with_batch_size(1024)`

## `XlsxWriter` *(feature: `xlsx`)*
Writes Excel `.xlsx` spreadsheets. Buffers records in memory and builds the workbook on `close()`.
- **Constructor**: `XlsxWriter::new("output.xlsx")`

## `SQLWriter`
Bulk inserts or upserts records into SQL databases via the `SqlClient` trait.
- **Constructor**: `SQLWriter::new(client, "users")`
- **Builder Methods**:
  - `.with_dialect(SqlDialect::Postgres)` (`Postgres`, `MySql`, `Sqlite`, `Oracle`)
  - `.with_upsert("email")` (Idempotent `ON CONFLICT` / `ON DUPLICATE KEY` / `MERGE INTO`)
  - `.with_batch_size(500)`
  - `.with_transaction(true)`

## `CallbackWriter` & `BatchCallbackWriter`
- **`CallbackWriter::new(|record| async move { ... })`**: Invokes an async closure for each record.
- **`BatchCallbackWriter::new(100, |batch| async move { ... })`**: Invokes an async closure per batch of records.

## `MultiWriter`
Broadcasts every record to multiple writers sequentially.
- **Builder**: `MultiWriter::from_writer(w1).add(w2)`
- **Boxed constructor**: `MultiWriter::new(vec![Box::new(w1), Box::new(w2)])`

## `ParallelWriter`
Distributes record batches across a pool of background Tokio worker tasks.
- **Constructor**: `ParallelWriter::new(concurrency, batch_size, || Box::new(writer_factory()))`

## `RetryingWriter`
Wraps any writer with exponential backoff retries, circuit breaker protection, and an optional DLQ fallback.
- **Constructor**: `RetryingWriter::new(inner_writer)`
