# Parallel Processing (Multi-Threading)

Scale throughput across multiple Tokio worker tasks using `ParallelWriter`.

## How `ParallelWriter` Works

`ParallelWriter` buffers incoming records into batches of `batch_size` and dispatches them across `concurrency` background Tokio worker tasks in round-robin order.

```rust
use data_genie::{CSVReader, CallbackWriter, Job, ParallelWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("massive_data.csv");

    // 4 concurrent workers, 500 records per batch
    let writer = ParallelWriter::new(4, 500, || {
        Box::new(CallbackWriter::new(|record| async move {
            // Executed concurrently across Tokio worker threads
            let _ = record;
            Ok(())
        }))
    });

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

## `MultiWriter` vs. `ParallelWriter`

| Feature | `MultiWriter` | `ParallelWriter` |
| :--- | :--- | :--- |
| **Goal** | **Broadcast** every record to multiple sinks | **Partition** batches across concurrent workers |
| **Routing** | Record goes to Sink A **AND** Sink B | Batch goes to Worker 1 **OR** Worker 2 |
| **Best For** | Simultaneous archiving + indexing | High-throughput database/API ingestion |
