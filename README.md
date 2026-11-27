# Data Genie

**`data-genie`** is a high-performance, streaming-first **ETL (Extract, Transform, Load) Engine** written in **Rust**. Streaming pipelines process records
incrementally, so memory usage does not grow with the total number of input records. It combines Tokio's asynchronous streaming runtime with strong Rust type
safety, integrated Dead Letter Queues (DLQ), and multi-transport support.

![](./docs/demo.gif)

---

## Core Philosophy

- **Streaming Memory Use**: `RecordStream` processes records one at a time. Streaming memory depends on buffers, the current record, and pipeline state rather
  than total input size.
- **Modular Workspace & Lean Defaults**: `data-genie` is a thin umbrella crate backed by `data-genie-core`. Heavy format, YAML, and cloud connectors live in
  dedicated workspace crates behind optional Cargo features (`default = []`), so you only compile what you use.
- **Standard Streams**: Use `StdinSource` and `StdoutSink`, or a quoted `"-"` YAML path, to connect pipelines to shell commands.
- **Transport Agnostic**: Works seamlessly across Local Files (`FileSource`/`FileSink`), AWS S3 (`S3Source`/`S3Sink`), HTTP APIs (`HttpSource`/`HttpSink`),
  and Memory (`MemorySource`/`MemorySink`).
- **Fault Tolerance**: Integrated Dead Letter Queues (DLQ) with automatic upstream propagation, atomic `abort()` on job failure, exponential backoff
  (`RetryingWriter`), and Circuit Breakers.
- **Programmatic & Declarative Support**: Run code programmatically in **Rust** or declaratively via **YAML** (with the `yaml` feature or `data-genie-cli`).

---

## Memory Use

Here is a measured baseline for the built-in streaming path. A release build read a CSV file with one string field per data row (rows were approximately 10 KB;
the final row was shortened to reach the exact file size) and wrote NDJSON to `/dev/null`. Peak resident set size (RSS) was measured with macOS
`/usr/bin/time -l` on an arm64 machine using Rust 1.95.0.

| Input size                    | Records processed | Peak RSS |
|:------------------------------|------------------:|---------:|
| 100 KiB (102,400 bytes)       |                11 |  7.1 MiB |
| 100 MiB (104,857,600 bytes)   |            10,486 |  7.9 MiB |
| 10 GiB (10,737,418,240 bytes) |         1,073,742 |  7.9 MiB |

Reproduce with `cargo build --release -p data-genie-cli --bin data-genie`, then run `data-genie run pipeline.yaml` under `/usr/bin/time -l`. The pipeline
should read the generated CSV and write to `/dev/null`; keep the row size approximately 10 KB to make results comparable. These figures are a baseline for that
setup, not a guarantee for every pipeline.

Memory remains independent of the number of records for the streaming path, but it is not a fixed amount for every workload. A single very large record,
full-file formats or operations, grouping and deduplication state, parallel batches, and in-memory sources or sinks can increase memory use. For example,
`GroupByReader` retains aggregate state until input is exhausted, and `MemoryWriter` retains all output records.

---

## Installation & Optional Cargo Features

This crate uses **Rust edition 2024**. You only ever need to install **`data-genie`** — optional connectors are enabled through Cargo feature flags (`default = []`), so you never compile dependencies you don't use.

### 1. Core only (JSON, NDJSON, CSV, TSV, FixedWidth, SQL)

Built-in formats (`CSV`, `TSV`, `JSON`, `NDJSON`, `FixedWidth`, `SQL`, `Memory`, `File`, `Stdin`/`Stdout`, filters, transformers, DLQ) require **no feature flags**:

```shell
cargo add data-genie
```

```toml
# Cargo.toml
[dependencies]
data-genie = "0.3"
```

### 2. Core + selected connectors (e.g. S3 & Parquet, or HTTP & Excel)

Pick only the features your project needs:

```shell
# Example: AWS S3 + Apache Parquet
cargo add data-genie --features s3,parquet

# Example: REST APIs + Excel
cargo add data-genie --features http,xlsx

# Example: XML + Declarative YAML pipelines
cargo add data-genie --features xml,yaml
```

```toml
# Cargo.toml
[dependencies]
data-genie = { version = "0.3", features = ["s3", "parquet"] }
```

### 3. Everything (`full`)

Enable all optional formats, declarative YAML pipelines, and cloud/HTTP connectors at once:

```shell
cargo add data-genie --features full
```

```toml
# Cargo.toml
[dependencies]
data-genie = { version = "0.3", features = ["full"] }
```

### Feature Reference

| Feature   | Workspace Crate      | Enables                                                                                                              |
|:----------|:---------------------|:---------------------------------------------------------------------------------------------------------------------|
| *(core)*  | `data-genie-core`    | CSV, TSV, JSON, NDJSON, FixedWidth, SQL, Memory, File/Stdio, Filters, Transformers, Validation/DLQ, Schema Inference |
| `xml`     | `data-genie-xml`     | `XMLReader`, `XMLWriter`                                                                                             |
| `yaml`    | `data-genie-yaml`    | `PipelineParser`, declarative YAML pipeline validation & execution                                                   |
| `parquet` | `data-genie-parquet` | `ParquetReader`, `ParquetWriter`, `ParquetSchema`, `ParquetFieldSchema`, `ParquetColumnType`                         |
| `xlsx`    | `data-genie-xlsx`    | `XlsxReader`, `XlsxWriter`                                                                                           |
| `http`    | `data-genie-http`    | `HttpSource`, `HttpSink`, `HttpReader` (paginated REST APIs)                                                         |
| `s3`      | `data-genie-s3`      | `S3Source`, `S3Sink` (AWS S3 / S3-compatible object stores)                                                          |
| `full`    | *(all of the above)* | Enables `xml`, `yaml`, `parquet`, `xlsx`, `http`, `s3`                                                               |

---

## Quick Start

See the [standalone examples](examples/README.md) for small, complete programs with embedded data and individual run commands:

```shell
cargo run --example csv_to_json
cargo run --example filter_transform
cargo run --example validation_dlq
```

### 1. Synchronous Execution

Call `Job::run_blocking(reader, writer)` directly without `#[tokio::main]`:

```rust
use data_genie::{CSVReader, Job, JsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("examples/input/users.csv");
    let writer = JsonWriter::new("examples/output/users.json");

    let metrics = Job::run_blocking(reader, writer)?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```

### 2. Asynchronous Execution

If your application already runs inside an async runtime (such as `#[tokio::main]`, Axum, or Actix), use `Job::run(reader, writer).await`:

```rust
use data_genie::{CSVReader, Job, JsonWriter};

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("examples/input/users.csv");
    let writer = JsonWriter::new("examples/output/users.json");

    let metrics = Job::run(reader, writer).await?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```

### 3. Transformations, Filtering & DLQ

```rust
use data_genie::{
    CSVReader, FilterExpression, FilteringReader, Job, JsonWriter,
    RenameField, SelectFields, TransformingReader, ValidatingReader,
};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("examples/input/users.csv");

    let filtered = FilteringReader::new(reader)
        .add_rule(FilterExpression::new("age > 30").create_record_filter());

    let transformed = TransformingReader::new(filtered)
        .add_transform(RenameField::new("fname", "firstName").transform())
        .add_transform(SelectFields::new(["firstName", "email", "age"]).transform());

    // Dead Letter Queue automatically propagates upstream to CSVReader
    let validated = ValidatingReader::new(transformed)
        .with_dlq(JsonWriter::new("examples/output/errors.json"));

    Job::run_blocking(validated, JsonWriter::new("examples/output/users.json"))?;
    Ok(())
}
```

### 4. Using Optional Connectors (e.g. S3 + Parquet)

With `features = ["s3", "parquet"]`, every connector is re-exported directly from `data_genie` — no extra crate imports required:

```rust
use data_genie::{
    Job, ParquetFieldSchema, ParquetReader, ParquetSchema, ParquetWriter, S3Sink, S3Source,
};

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let source = S3Source::from_env("my-raw-bucket", "events/input.parquet")?;
    let sink = S3Sink::from_env("my-clean-bucket", "events/output.parquet")?
        .with_content_type("application/vnd.apache.parquet");

    let schema = ParquetSchema::new()
        .add_field("id", ParquetFieldSchema::int64())
        .add_field("email", ParquetFieldSchema::utf8());

    let reader = ParquetReader::new(source);
    let writer = ParquetWriter::with_schema(sink, schema);

    Job::run(reader, writer).await?;
    Ok(())
}
```

---

## Supported Formats & Connectors

| Category                    | Readers                                          | Writers                                                                                                                     | Feature    |
|:----------------------------|:-------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------|:-----------|
| **Text / Tabular**          | `CSVReader`, `TSVReader`, `FixedWidthReader`     | `CSVWriter`, `TSVWriter`, `FixedWidthWriter`                                                                                | *(core)*   |
| **Structured / JSON**       | `JsonReader`, `NDJsonReader`                     | `JsonWriter`, `NDJsonWriter`                                                                                                | *(core)*   |
| **XML**                     | `XMLReader`                                      | `XMLWriter`                                                                                                                 | `xml`      |
| **Declarative YAML**        | `PipelineParser`                                 | `PipelineParser`                                                                                                            | `yaml`     |
| **Columnar & Spreadsheets** | `ParquetReader`, `XlsxReader`                    | `ParquetWriter`, `XlsxWriter`                                                                                               | `parquet`, `xlsx` |
| **Databases & APIs**        | `SQLReader`, `HttpReader` (`http`)               | `SQLWriter` (Postgres, MySQL, SQLite, Oracle Upserts)                                                                       | *(core)* / `http` |
| **Cloud & HTTP Transports** | `S3Source` (`s3`), `HttpSource` (`http`)         | `S3Sink` (`s3`), `HttpSink` (`http`)                                                                                        | `s3`, `http` |
| **Composite & Utility**     | `MemoryReader`                                   | `MemoryWriter`, `ConsoleWriter`, `MultiWriter`, `RetryingWriter`, `CallbackWriter`, `BatchCallbackWriter`, `ParallelWriter` | *(core)*   |

---

## CLI Usage

The CLI is packaged separately in `data-genie-cli` (so library users don't pull CLI/YAML dependencies):

```shell
# Install the `data-genie` binary
cargo install data-genie-cli

# Validate a declarative YAML pipeline without running it
data-genie validate pipeline.yaml

# Run a declarative YAML pipeline
data-genie run pipeline.yaml

# Preview first 5 records of a file
data-genie preview users.csv

# Infer Rust, TypeScript, Zod, and SQL schemas from a dataset
data-genie infer-schema users.csv
```

[Interactive Pipeline Builder (YAML)](https://pujansrt.github.io/data-genie-rs/guide/declarative-pipelines.html#interactive-pipeline-builder)

---

## Documentation & Contributing

- **Docs & Guides**: [pujansrt.github.io/data-genie-rs](https://pujansrt.github.io/data-genie-rs/) | [docs.rs/data-genie](https://docs.rs/data-genie)
- **Developer Onboarding & Architecture**: Read [Dev.md](Dev.md) for a complete walkthrough of the workspace structure, streaming architecture, testing/coverage commands, and step-by-step contribution recipes.
- **Contribution Guidelines**: See [CONTRIBUTING.md](CONTRIBUTING.md).
- **License**: [MIT](LICENSE)
