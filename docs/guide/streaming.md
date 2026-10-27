# Streaming & Memory

The primary goal of Data-Genie is to maintain a **constant `O(1)` memory footprint**, regardless of dataset size, using Rust's `futures::Stream` (`RecordStream`).

## The Memory Problem

Loading an entire file into a `Vec` in memory scales linearly with file size:

```rust
// ❌ Dangerous for multi-GB files
let content = std::fs::read_to_string("huge_file.json")?;
let records: Vec<serde_json::Value> = serde_json::from_str(&content)?;
```

If `huge_file.json` is 10 GB and your container has 512 MB of RAM, the process will crash with an Out-Of-Memory (OOM) error.

## The Data-Genie Solution

Data-Genie uses pull-based asynchronous streaming (`Pin<Box<dyn Stream<Item = Result<Record>> + Send>>`). It reads one record at a time, transforms it in-flight, writes it to the sink, and drops it immediately:

```rust
use data_genie::{Job, JsonReader, JsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = JsonReader::new("huge_file.json");
    let writer = JsonWriter::new("output.json");

    // Memory usage remains constant even for 100GB+ files
    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

### Consuming `RecordStream` Directly
Every `DataReader` produces a `RecordStream` via `.read().await?`, so you can also iterate over records manually with `futures::StreamExt`:

```rust
use data_genie::{CSVReader, DataReader};
use futures::StreamExt;

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let mut reader = CSVReader::new("users.csv");
    let mut stream = reader.read().await?;

    while let Some(record) = stream.next().await {
        println!("{:?}", record?);
    }
    Ok(())
}
```

## Automatic Backpressure
Because Rust futures and `Stream` are pull-driven, backpressure is automatic. If your destination sink (such as a database or remote API) writes slower than your disk reads, the reader naturally waits until the writer is ready for the next item.
