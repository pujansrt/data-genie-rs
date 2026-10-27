# Building a Real-time Dashboard

Use `Job` lifecycle callbacks (`on_progress`, `on_error`, `on_complete`) to push real-time ETL metrics over a channel or Server-Sent Events (SSE) stream.

## Example

```rust
use data_genie::{CSVReader, Job, JobOptions, JsonWriter};
use std::sync::mpsc;

fn main() -> data_genie::Result<()> {
    let (tx, rx) = mpsc::channel();

    let reader = CSVReader::new("massive_data.csv");
    let writer = JsonWriter::new("output.json");
    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    let progress_tx = tx.clone();
    job.on_progress(move |metrics| {
        let _ = progress_tx.send(serde_json::json!({
            "type": "PROGRESS",
            "record_count": metrics.record_count,
            "records_per_second": metrics.records_per_second,
        }));
    });

    job.on_complete(move |metrics| {
        let _ = tx.send(serde_json::json!({
            "type": "COMPLETE",
            "record_count": metrics.record_count,
            "duration_ms": metrics.duration_ms,
        }));
    });

    job.execute_blocking()?;

    for event in rx.try_iter() {
        println!("SSE event -> data: {}", event);
    }
    Ok(())
}
```
