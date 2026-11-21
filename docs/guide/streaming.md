# Streaming & Memory

Streaming readers and writers process records incrementally, so memory use does not grow with total record count. It is bounded by I/O buffers, the largest current record, and any state retained by pipeline stages. Grouping, deduplication, batching, in-memory readers/writers, and some whole-document formats can use memory proportional to data size.

In one release-mode CSV-to-NDJSON run on macOS arm64, peak RSS was 7.1 MiB for a 100 KiB input, 7.9 MiB for 100 MiB, and 7.9 MiB for 10 GiB. The input used approximately 10 KiB rows and output went to `/dev/null`; these are setup-specific measurements, not a guarantee. The [README memory table](https://github.com/pujansrt/data-genie-rs#memory-use) gives the exact sizes and setup.

## The Memory Problem

Loading an entire file into a `Vec` in memory scales linearly with file size:

```rust
// ❌ Dangerous for multi-GB files
let content = std::fs::read_to_string("huge_file.json")?;
let records: Vec<serde_json::Value> = serde_json::from_str(&content)?;
```

If the parsed data expands beyond available memory, the process can run out of memory.

## The Data-Genie Solution

For formats with streaming readers, Data-Genie uses pull-based asynchronous streams. CSV and NDJSON are read line by line, then records are transformed and written before the next record is pulled:

```rust
use data_genie::{CSVReader, Job, NDJsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("huge_file.csv");
    let writer = NDJsonWriter::new("output.ndjson");

    // Memory usage does not grow with the number of CSV rows.
    Job::run_blocking(reader, writer)?;
    Ok(())
}
```

### Consuming `RecordStream` Directly
Every `DataReader` produces a `RecordStream` via `.read()`, so you can also iterate over records manually with `futures::StreamExt`:

```rust
use data_genie::{CSVReader, DataReader};
use futures::StreamExt;

#[tokio::main]
async fn main() -> data_genie::Result<()> {
    let mut reader = CSVReader::new("users.csv");
    let mut stream = reader.read();

    while let Some(record) = stream.next().await {
        println!("{:?}", record?);
    }
    Ok(())
}
```

## Automatic Backpressure
Because Rust futures and `Stream` are pull-driven, backpressure is automatic. If your destination sink (such as a database or remote API) writes slower than your disk reads, the reader naturally waits until the writer is ready for the next item.
