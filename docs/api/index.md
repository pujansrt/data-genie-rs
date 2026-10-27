# API Reference

Data-Genie is organized into four core modules: **Readers**, **Writers**, **Transformers/Filters**, and **Transports**.

Full rustdoc API documentation is also available on **[docs.rs/data-genie](https://docs.rs/data-genie)**.

## Core Types & Traits

### `Record`
An ordered map representing a single row/document of data (`IndexMap<String, serde_json::Value>`):
```rust
pub type Record = indexmap::IndexMap<String, serde_json::Value>;
```

### `DataReader`
Async trait implemented by all sources and middleware readers:
```rust
#[async_trait]
pub trait DataReader: Send {
    async fn read(&mut self) -> Result<RecordStream>;
    fn set_dlq(&mut self, _dlq: SharedWriter) {}
}
```

### `DataWriter`
Async trait implemented by all sinks:
```rust
#[async_trait]
pub trait DataWriter: Send {
    async fn write(&mut self, record: Record) -> Result<()>;
    async fn write_all(&mut self, records: Vec<Record>) -> Result<()>;
    async fn flush(&mut self) -> Result<()>;
    async fn close(&mut self) -> Result<()>;
}
```

### `Job<R, W>`
The pipeline orchestrator:
- **Execution**:
  - `Job::run_blocking(reader, writer)` / `job.execute_blocking()`: Runs synchronously (spins up an internal Tokio runtime).
  - `Job::run(reader, writer).await` / `job.execute().await`: Runs inside an existing async runtime.
- **Utilities**:
  - `Job::preview_blocking(reader, limit)` / `Job::preview(reader, limit).await`: Prints the first `limit` records.
  - `Job::infer_schema_blocking(reader, sample_size, name)` / `Job::infer_schema(...).await`: Infers Rust structs, TypeScript interfaces, Zod schemas, and SQL DDL.
- **Lifecycle Callbacks**:
  - `job.on_start(|start_time| ...)`
  - `job.on_progress(|metrics| ...)`
  - `job.on_record(|record| ...)`
  - `job.on_error(|err, record| ...)`
  - `job.on_complete(|metrics| ...)`

---

## Modules

- **[Readers](/api/readers)**: Extract data from CSV, TSV, Fixed-Width, JSON, NDJSON, XML, Parquet, Excel, SQL, and HTTP.
- **[Writers](/api/writers)**: Load data into files, S3, databases, memory, or parallel worker pools.
- **[Transformers](/api/transformers)**: Middleware for mapping, filtering, aggregating, deduplicating, and validating records.
