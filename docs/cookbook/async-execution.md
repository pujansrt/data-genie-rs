# Running Pipelines Asynchronously (Tokio)

All recipes in the Cookbook use synchronous execution (`Job::run_blocking` or `job.execute_blocking()`) by default so you don't need `#[tokio::main]` in standalone scripts.

If your application already runs inside an async runtime (such as `#[tokio::main]`, **Axum**, **Actix Web**, or **Tonic**), replace the blocking call with `.await`:

| Synchronous (Default in Cookbook) | Asynchronous (`#[tokio::main]`) |
| :--- | :--- |
| `Job::run_blocking(reader, writer)?` | `Job::run(reader, writer).await?` |
| `job.execute_blocking()?` | `job.execute().await?` |
| `Job::preview_blocking(reader, 5)?` | `Job::preview(reader, 5).await?` |
| `Job::infer_schema_blocking(reader, 1000, "Row")?` | `Job::infer_schema(reader, 1000, "Row").await?` |
| `PipelineParser::run_file_blocking("pipeline.yaml")?` | `PipelineParser::run_file("pipeline.yaml").await?` |

## Example

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
