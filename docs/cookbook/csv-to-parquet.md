# CSV to Parquet

Parquet is a compressed, columnar storage format ideal for analytics. Enable the `parquet` feature in `Cargo.toml`:

```toml
[dependencies]
data-genie = { version = "0.1", features = ["parquet"] }
```

## Implementation

`ParquetWriter` can infer the schema from the first batch automatically or use an explicit `ParquetSchema`:

```rust
use data_genie::{CSVReader, Job, ParquetColumnType, ParquetSchema, ParquetWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");

    let schema = ParquetSchema::new()
        .add_column("id", ParquetColumnType::Int64)
        .add_column("username", ParquetColumnType::Utf8)
        .add_column("email", ParquetColumnType::Utf8)
        .add_column("is_active", ParquetColumnType::Boolean);

    let writer = ParquetWriter::new("users.parquet").with_schema(schema);

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## Supported Parquet Column Types
- `ParquetColumnType::Utf8`: UTF-8 strings
- `ParquetColumnType::Int64`: 64-bit signed integers
- `ParquetColumnType::Float64`: 64-bit floating-point numbers
- `ParquetColumnType::Boolean`: `true` / `false` booleans
