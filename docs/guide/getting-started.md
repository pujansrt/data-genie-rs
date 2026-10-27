# Getting Started

## Installation

### As a Rust Library
Add `data-genie` to your `Cargo.toml`:
```toml
[dependencies]
data-genie = "0.1"

# Or enable optional format/cloud features (parquet, xlsx, s3, http, or full):
# data-genie = { version = "0.1", features = ["full"] }
```

### As a CLI Tool
Install the `data-genie` CLI binary via Cargo:
```bash
cargo install data-genie --features full
```

## Choose Your Path

Data-Genie supports two ways of building ETL pipelines:

1. **Programmatic (Rust API)**: Full type-safe control in Rust (`Job::run_blocking` or `Job::run`).
2. **Declarative (YAML CLI)**: Define pipelines in YAML (`data-genie run pipeline.yaml`).

## Interactive Rust Code Generator

Use the interactive builder below to generate your Rust ETL pipeline:

<script setup>
import CodeGenerator from '../.vitepress/components/CodeGenerator.vue'
</script>

<CodeGenerator />

## Quick Start: CSV to JSON

### Synchronous (No `#[tokio::main]` Needed)

```rust
use data_genie::{CSVReader, Job, JsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");
    let writer = JsonWriter::new("output.json");

    let metrics = Job::run_blocking(reader, writer)?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```

### Asynchronous (With `#[tokio::main]`)

```rust
use data_genie::{CSVReader, Job, JsonWriter};

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");
    let writer = JsonWriter::new("output.json");

    let metrics = Job::run(reader, writer).await?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```

## Optional Cargo Features

By default, `data-genie` compiles with a fast, minimal dependency tree (`csv`, `serde_json`, `quick-xml`, `serde_yaml`, `regex`, `sha2`). Enable optional connectors only when needed:

- **`parquet`**: Apache Parquet support (`ParquetReader`, `ParquetWriter`)
- **`xlsx`**: Excel support (`XlsxReader`, `XlsxWriter`)
- **`s3`**: AWS S3 streaming (`S3Source`, `S3Sink`)
- **`http`**: HTTP streaming (`HttpSource`, `HttpSink`, `HttpReader`)
- **`full`**: Enables all optional features above

## Previewing Data & Inferring Schemas

Preview the first few rows or automatically infer Rust, TypeScript, Zod, and SQL schemas:

```rust
use data_genie::{CSVReader, Job};

fn main() -> data_genie::Result<()> {
    // Preview first 5 records in the terminal
    Job::preview_blocking(CSVReader::new("users.csv"), 5)?;

    // Infer Rust struct, TypeScript interface, Zod schema, and SQL DDL
    let schema = Job::infer_schema_blocking(CSVReader::new("users.csv"), 1000, "Users")?;
    println!("{}", schema.rust_struct);
    println!("{}", schema.sql);
    Ok(())
}
```
