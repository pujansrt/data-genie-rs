# Data Genie

**`data-genie`** is a high-performance, streaming-first **ETL (Extract, Transform, Load) Engine** written in **Rust**. Designed to process massive datasets (GBs to TBs) with a **constant `O(1)` memory footprint**, it combines Tokio's asynchronous streaming runtime with strong Rust type safety, integrated Dead Letter Queues (DLQ), and multi-transport support.

---

## Core Philosophy

- **O(1) Memory Complexity**: Data is streamed record-by-record via `futures::Stream` (`RecordStream`).
- **Transport Agnostic**: Works seamlessly across Local Files (`FileSource`/`FileSink`), AWS S3 (`S3Source`/`S3Sink`), HTTP APIs (`HttpSource`/`HttpSink`), and Memory (`MemorySource`/`MemorySink`).
- **Fault Tolerance**: Integrated Dead Letter Queues (DLQ) with automatic upstream propagation, exponential backoff (`RetryingWriter`), and Circuit Breakers.
- **Declarative & Programmatic**: Run pipelines programmatically in Rust or declaratively via YAML (`data-genie run pipeline.yaml`).

---

## Supported Formats & Connectors

| Category | Readers | Writers |
| :--- | :--- | :--- |
| **Text / Tabular** | `CSVReader`, `TSVReader`, `FixedWidthReader` | `CSVWriter`, `TSVWriter`, `FixedWidthWriter` |
| **Structured / Document** | `JsonReader`, `NDJsonReader`, `XMLReader` | `JsonWriter`, `NDJsonWriter`, `XMLWriter` |
| **Columnar & Spreadsheets** | `ParquetReader`, `XlsxReader` | `ParquetWriter`, `XlsxWriter` |
| **Databases & APIs** | `SQLReader`, `HttpReader` | `SQLWriter` (Postgres, MySQL, SQLite, Oracle Upserts) |
| **Composite & Utility** | `MemoryReader` | `MemoryWriter`, `ConsoleWriter`, `MultiWriter`, `RetryingWriter`, `CallbackWriter`, `BatchCallbackWriter`, `ParallelWriter` |

---

## Quick Start

### 1. Synchronous Execution (No `#[tokio::main]` Needed)

You do not need `#[tokio::main]` or `tokio` in your own `Cargo.toml`. Call `job.execute_blocking()` or `Job::run_blocking(reader, writer)` directly inside a standard `fn main()`:

```rust
use std::io::{self, Write};
use std::time::Instant;
use data_genie::{CSVReader, Job, JobOptions, JsonWriter, SharedWriter};

fn main() -> data_genie::Result<()> {
    let dlq = SharedWriter::new(JsonWriter::new("examples/output/errors.json"));
    let reader = CSVReader::new("examples/input/heavy_data.csv").with_dlq(dlq.clone());
    let writer = JsonWriter::new("examples/output/output.json");

    let start_time = Instant::now();
    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    job.on_progress(|m| {
        print!(
            "\r⏳ Processed: {} | {:.0} rec/sec",
            m.record_count, m.records_per_second
        );
        let _ = io::stdout().flush();
    });

    // Automatically spins up an internal runtime and runs to completion
    let metrics = job.execute_blocking()?;
    dlq.close_dlq_blocking()?;

    let elapsed_mins = start_time.elapsed().as_secs_f64() / 60.0;
    println!("\n--- Performance Report ---");
    println!("Total Time: {:.2} minutes", elapsed_mins);
    println!("Avg Speed: {:.0} records/sec", metrics.records_per_second);
    println!("Final Count: {}", metrics.record_count);

    Ok(())
}
```

### 2. Asynchronous Execution (With `#[tokio::main]` or Existing Async Runtime)

If your application already runs inside an async runtime (such as `#[tokio::main]`, Axum, or Actix), use `.execute().await` or `Job::run(reader, writer).await`:

```rust
use std::io::{self, Write};
use std::time::Instant;
use data_genie::{CSVReader, Job, JobOptions, JsonWriter, SharedWriter};

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let dlq = SharedWriter::new(JsonWriter::new("examples/output/errors.json"));
    let reader = CSVReader::new("examples/input/heavy_data.csv").with_dlq(dlq.clone());
    let writer = JsonWriter::new("examples/output/output.json");

    let start_time = Instant::now();
    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    job.on_progress(|m| {
        print!(
            "\r⏳ Processed: {} | {:.0} rec/sec",
            m.record_count, m.records_per_second
        );
        let _ = io::stdout().flush();
    });

    let metrics = job.execute().await?;
    dlq.close_dlq().await?;

    let elapsed_mins = start_time.elapsed().as_secs_f64() / 60.0;
    println!("\n--- Performance Report ---");
    println!("Total Time: {:.2} minutes", elapsed_mins);
    println!("Avg Speed: {:.0} records/sec", metrics.records_per_second);
    println!("Final Count: {}", metrics.record_count);

    Ok(())
}
```

### 3. Transformations, Filtering & DLQ Propagation

```rust
use data_genie::{
    CSVReader, FilterExpression, FilteringReader, Job, JsonWriter,
    RenameField, SelectFields, SetCalculatedField, TransformingReader, ValidatingReader,
};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("examples/input/users.csv");

    // Filter records with expression
    let expr = FilterExpression::new("age > 30");
    let filtered = FilteringReader::new(reader).add_rule(expr.create_record_filter());

    // Chain field transformations
    let transformed = TransformingReader::new(filtered)
        .add_transform(RenameField::new("fname", "firstName").transform())
        .add_transform(SetCalculatedField::new("tier", "age >= 50 ? 'GOLD' : 'STANDARD'").transform())
        .add_transform(SelectFields::new(["firstName", "email", "age", "tier"]).transform());

    // Attach DLQ on ValidatingReader (automatically propagates upstream to CSVReader)
    let validated = ValidatingReader::new(transformed)
        .with_dlq(JsonWriter::new("output/errors.json"));

    let writer = JsonWriter::new("output/users_filtered.json");
    let metrics = Job::run_blocking(validated, writer)?;

    println!("Processed {} records at {:.0} rec/sec", metrics.record_count, metrics.records_per_second);
    Ok(())
}
```

### 4. Optional Cargo Features

By default, `data-genie` compiles with a minimal dependency footprint (`csv`, `serde_json`, `quick-xml`, `serde_yaml`, `regex`, `sha2`). Enable heavy format or cloud connectors only when needed:

```toml
[dependencies]
data-genie = { version = "2.1", features = ["parquet", "xlsx", "s3", "http"] }
# Or enable everything:
# data-genie = { version = "2.1", features = ["full"] }
```

### 5. CLI Usage

```bash
# Run a declarative YAML pipeline
cargo run --bin data-genie -- run tests/declarative-pipeline.yaml

# Preview first 5 records of a file
cargo run --bin data-genie -- preview examples/input/users.csv --limit 5

# Infer Rust, TypeScript, Zod, and SQL schemas from a dataset
cargo run --bin data-genie -- infer-schema examples/input/users.csv
```

---

## Running Tests

```bash
cargo test
```
