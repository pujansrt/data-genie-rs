# S3 to JSON (Local or S3-to-S3)

Stream objects directly from AWS S3 without downloading full files to disk first. Enable the `s3` feature in `Cargo.toml`:

```toml
[dependencies]
data-genie = { version = "0.1", features = ["s3"] }
```

## S3 to Local JSON

```rust
use data_genie::{Job, JsonReader, JsonWriter, S3Config, S3Source};

fn main() -> data_genie::Result<()> {
    let source = S3Source::new("raw-data-bucket", "daily-export.json", S3Config::new("us-east-1"))?;
    let reader = JsonReader::from_source(Box::new(source));
    let writer = JsonWriter::new("local-backup.json");

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## S3-to-S3 Format Conversion

Stream CSV from one S3 bucket and write JSON directly to another S3 bucket with zero local disk usage:

```rust
use data_genie::{CSVReader, Job, JsonWriter, S3Config, S3Sink, S3Source};

fn main() -> data_genie::Result<()> {
    let config = S3Config::new("us-east-1");
    let source = S3Source::new("source-bucket", "data.csv", config.clone())?;
    let sink = S3Sink::new("dest-bucket", "data.json", config)?;

    let reader = CSVReader::from_source(Box::new(source));
    let writer = JsonWriter::from_sink(Box::new(sink));

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```
