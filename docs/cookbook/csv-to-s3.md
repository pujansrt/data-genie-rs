# CSV to S3 (JSON or Parquet)

Stream local CSV files directly into AWS S3 (or S3-compatible object storage like MinIO/R2) by enabling the `s3` feature:

```toml
[dependencies]
data-genie = { version = "0.1", features = ["s3", "parquet"] }
```

## Implementation (CSV to S3 JSON)

```rust
use data_genie::{CSVReader, Job, JsonWriter, S3Config, S3Sink};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("local_data.csv");

    let s3_config = S3Config::new("us-east-1");
    let sink = S3Sink::new("my-bucket", "exports/data.json", s3_config)?;
    let writer = JsonWriter::from_sink(Box::new(sink));

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## Implementation (CSV to S3 Parquet)

```rust
use data_genie::{CSVReader, Job, ParquetWriter, S3Config, S3Sink};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("local_data.csv");

    let sink = S3Sink::new("my-bucket", "exports/data.parquet", S3Config::new("us-east-1"))?;
    let writer = ParquetWriter::from_sink(Box::new(sink));

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```
