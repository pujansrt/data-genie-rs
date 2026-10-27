# Memory and Callbacks

Use in-memory transports for unit tests or small datasets, and `CallbackWriter` / `BatchCallbackWriter` to execute custom async Rust closures per record or per batch.

## Memory Source & Sink

```rust
use data_genie::{CSVReader, Job, JsonWriter, MemorySink, MemorySource};

fn main() -> data_genie::Result<()> {
    let source = MemorySource::from_str("id,name\n1,Alice\n2,Bob");
    let sink = MemorySink::new();
    let buffer = sink.buffer();

    let reader = CSVReader::from_source(Box::new(source));
    let writer = JsonWriter::from_sink(Box::new(sink));

    Job::run_blocking(reader, writer)?;

    let json_output = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
    println!("{}", json_output);
    Ok(())
}
```

## Callback Writer & Batch Callback Writer

```rust
use data_genie::{BatchCallbackWriter, CSVReader, CallbackWriter, Job};

fn main() -> data_genie::Result<()> {
    // Per-record async callback
    let writer = CallbackWriter::new(|record| async move {
        println!("Record: {:?}", record);
        Ok(())
    });
    Job::run_blocking(CSVReader::new("users.csv"), writer)?;

    // Batch async callback (groups of 100 records)
    let batch_writer = BatchCallbackWriter::new(100, |batch| async move {
        println!("Flushing batch of {} records", batch.len());
        Ok(())
    });
    Job::run_blocking(CSVReader::new("users.csv"), batch_writer)?;
    Ok(())
}
```

## Job Lifecycle Callbacks

```rust
use data_genie::{CSVReader, Job, JobOptions, JsonWriter};

fn main() -> data_genie::Result<()> {
    let mut job = Job::with_options(
        CSVReader::new("users.csv"),
        JsonWriter::new("output.json"),
        JobOptions::new().with_progress(false),
    );

    job.on_start(|start_time| println!("Started at {}", start_time));
    job.on_progress(|m| println!("Processed {} records ({:.0} rec/s)", m.record_count, m.records_per_second));
    job.on_complete(|m| println!("Finished {} records in {} ms", m.record_count, m.duration_ms));

    job.execute_blocking()?;
    Ok(())
}
```
