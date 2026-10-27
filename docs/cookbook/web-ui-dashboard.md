# Building a Real-time Dashboard

Use `Job` lifecycle callbacks (`on_progress`, `on_error`, `on_complete`) with an async Rust web server (such as Axum) to stream real-time ETL metrics over Server-Sent Events (SSE).

## Backend Example (Rust + Tokio Channel)

```rust
use data_genie::{CSVReader, Job, JobOptions, JsonWriter};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel();

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

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            println!("SSE event -> data: {}", event);
        }
    });

    job.execute().await?;
    Ok(())
}
```
