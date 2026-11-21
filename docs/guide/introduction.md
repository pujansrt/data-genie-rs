# What is Data-Genie?

Data-Genie (`data-genie`) is a high-performance, streaming-first **ETL (Extract, Transform, Load) Engine** written in **Rust**.

It is designed for engineers who need to process large datasets. Streaming readers and writers process records incrementally, so memory use does not grow with the total number of records. Actual memory depends on record size and pipeline state; operations such as grouping and in-memory readers retain data. See the [streaming guide](/guide/streaming) for format-specific details.

You can run pipelines synchronously via `Job::run_blocking` (no `#[tokio::main]` required) or asynchronously inside any async runtime via `Job::run`.

## Why Data-Genie?

Loading entire files into memory (`std::fs::read_to_string` or `serde_json::from_slice`) works for small datasets but fails on multi-gigabyte datasets.

| Feature | Data-Genie (`data-genie`) | Traditional In-Memory Approach |
| :--- | :--- | :--- |
| **Memory Usage** | Bounded by buffers, record size, and retained pipeline state for streaming paths | Grows with the amount of data loaded |
| **Latency** | Streaming paths can write before the full input is read | Usually waits for the full read/parse |
| **Scalability** | Input size can grow without retaining all records | Limited by available RAM |

## Key Features

- **Streaming First**: Memory on streaming paths depends on buffers, the current record, and pipeline state rather than total input size.
- **Multi-Format**: CSV, TSV, Fixed-Width, JSON, NDJSON, XML, Parquet, Excel (`.xlsx`), and SQL.
- **Transport Agnostic**: Local FS (`FileSource`/`FileSink`), AWS S3 (`S3Source`/`S3Sink`), HTTP (`HttpSource`/`HttpSink`), and Memory (`MemorySource`/`MemorySink`).
- **Schema Validation & DLQ**: Built-in `SchemaValidatingReader` and `ValidatingReader` with automatic upstream Dead Letter Queue propagation.
- **Fault Tolerant**: `RetryingWriter` with exponential backoff and circuit breakers.
