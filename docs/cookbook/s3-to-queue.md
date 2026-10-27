# Streaming S3 Objects to a Message Broker

Combine `S3Source` and `ParquetReader` with `CallbackWriter` or `BatchCallbackWriter` to publish records to Kafka, RabbitMQ, or NATS as they stream from S3.

## Per-Record Publishing (`CallbackWriter`)

```rust
use data_genie::{CallbackWriter, Job, ParquetReader, S3Config, S3Source};

fn main() -> data_genie::Result<()> {
    let source = S3Source::new("my-bucket", "exports/users.parquet", S3Config::new("us-east-1"))?;
    let reader = ParquetReader::from_source(Box::new(source));

    let writer = CallbackWriter::new(|record| async move {
        let payload = serde_json::to_vec(&record).unwrap();
        // Publish `payload` to RabbitMQ / Kafka / NATS here
        let _ = payload;
        Ok(())
    });

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## High-Throughput Batch Publishing (`BatchCallbackWriter`)

```rust
use data_genie::{BatchCallbackWriter, Job, ParquetReader, S3Config, S3Source};

fn main() -> data_genie::Result<()> {
    let source = S3Source::new("my-bucket", "exports/users.parquet", S3Config::new("us-east-1"))?;
    let reader = ParquetReader::from_source(Box::new(source));

    let writer = BatchCallbackWriter::new(100, |batch| async move {
        // Send 100 records in a single broker batch
        println!("Published batch of {} messages", batch.len());
        Ok(())
    });

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```
