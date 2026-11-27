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

`MemorySource` holds serialized bytes and is useful when a format reader should parse an in-memory payload. `MemoryReader` accepts already-parsed records. It stores records in shared immutable memory and clones records as they are yielded, avoiding a second full collection copy when `read()` starts. The original collection still remains in memory for the lifetime of the reader.

```rust
use data_genie::{MemoryReader, record};

let reader = MemoryReader::new([
    record!("name" => "Alice"),
    record!("name" => "Bob"),
]);
```

## Callback Writer & Batch Callback Writer

```rust
use data_genie::{BatchCallbackWriter, CSVReader, CallbackWriter, Job};

fn main() -> data_genie::Result<()> {
    // Per-record async callback
    let writer = CallbackWriter::new_async(|record| async move {
        println!("Record: {:?}", record);
        Ok(())
    });
    Job::run_blocking(CSVReader::new("users.csv"), writer)?;

    // Batch async callback (groups of 100 records)
    let batch_writer = BatchCallbackWriter::new_async(100, |batch| async move {
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

`JobMetrics` also reports records skipped by filters/deduplication, rejected by schema validation, and written to a DLQ. To include logical JSON-encoded record sizes, opt in to an extra serialization pass:

```rust
use data_genie::{CSVReader, Job, JobOptions, JsonWriter};

let options = JobOptions::new().with_record_byte_metrics(true);
let mut job = Job::with_options(CSVReader::new("input.csv"), JsonWriter::new("output.json"), options);
let metrics = job.execute_blocking()?;
println!("{} logical bytes", metrics.logical_record_bytes_written);
```

These byte counts describe records at the job boundary; they are not raw transport bytes or the exact encoded size of every output format. Byte accounting is disabled by default to avoid the extra serialization work.
