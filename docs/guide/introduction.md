# What is Data-Genie?

Data-Genie (`data-genie`) is a high-performance, streaming-first **ETL (Extract, Transform, Load) Engine** written in **Rust**.

It is designed for engineers who need to process massive datasets (gigabytes or terabytes) without worrying about "Out of Memory" (OOM) errors. By leveraging Rust's zero-cost abstractions and Tokio's asynchronous `Stream` ecosystem (`RecordStream`), Data-Genie maintains a **constant `O(1)` memory footprint** whether you are processing 100 rows or 100 million rows.

You can run pipelines synchronously via `Job::run_blocking` (no `#[tokio::main]` required) or asynchronously inside any async runtime via `Job::run`.

## Why Data-Genie?

Loading entire files into memory (`std::fs::read_to_string` or `serde_json::from_slice`) works for small datasets but fails on multi-gigabyte datasets.

| Feature | Data-Genie (`data-genie`) | Traditional In-Memory Approach |
| :--- | :--- | :--- |
| **Memory Usage** | Constant `O(1)` | Linear `O(N)` (proportional to file size) |
| **Latency** | Starts writing immediately | Waits for full read/parse |
| **Scalability** | Unlimited stream size | Limited by available RAM |

## Key Features

- **Streaming First**: `O(1)` memory complexity across readers, transformers, filters, and writers.
- **Multi-Format**: CSV, TSV, Fixed-Width, JSON, NDJSON, XML, Parquet, Excel (`.xlsx`), and SQL.
- **Transport Agnostic**: Local FS (`FileSource`/`FileSink`), AWS S3 (`S3Source`/`S3Sink`), HTTP (`HttpSource`/`HttpSink`), and Memory (`MemorySource`/`MemorySink`).
- **Schema Validation & DLQ**: Built-in `SchemaValidatingReader` and `ValidatingReader` with automatic upstream Dead Letter Queue propagation.
- **Fault Tolerant**: `RetryingWriter` with exponential backoff and circuit breakers.
