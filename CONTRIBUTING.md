# Contributing to Data-Genie

First off, thank you for considering contributing to **Data-Genie**! It's people like you that make Data-Genie such a great high-performance streaming ETL engine in Rust.

## How Can I Contribute?

### Reporting Bugs
* Check the [GitHub Issues](https://github.com/pujansrt/data-genie-rs/issues) to see if the bug has already been reported.
* Use the **Bug Report** template to provide as much detail as possible (including Rust version `rustc --version` and enabled Cargo features).

### Suggesting Enhancements
* Open an issue using the **Feature Request** template.
* Explain why the feature would be useful to most users and whether it belongs in core or behind an optional Cargo feature flag.

### Pull Requests
1. Fork the repo and create your feature branch.
2. Add unit or integration tests in `tests/integration_tests.rs` for any new functionality.
3. Ensure the test suite passes across default and optional features:
   ```bash
   cargo test
   cargo test --all-features
   ```
4. Format your code with `rustfmt` and lint with `clippy`:
   ```bash
   cargo fmt --all
   cargo clippy --all-targets --all-features
   ```
5. Verify documentation compiles cleanly:
   ```bash
   cargo doc --no-deps --all-features
   ```
6. Submit a Pull Request!

---

## Architecture & Engineering Guidelines

When contributing new Readers, Writers, Filters, or Transformers, please adhere to these core principles:
* **`O(1)` Streaming Memory Complexity**: Data must be processed incrementally via `RecordStream` (`futures::Stream`). Avoid loading entire files into memory (`Vec<DataRecord>`) unless an operation inherently requires materialization (e.g., `GroupByReader`).
* **Dead Letter Queue (DLQ) Propagation**:
  * Wrapper readers (`ValidatingReader`, `TransformingReader`, `FilteringReader`, etc.) must propagate `set_dlq` and `set_ignore_errors` upstream to their inner `DataReader`.
  * Readers should not close a shared DLQ writer mid-stream; the owner or outer `ValidatingReader` is responsible for closing the DLQ when the stream finishes.
* **Minimal Core Dependencies**: Keep default dependencies lightweight. Heavy format or cloud connectors (`parquet`, `xlsx`, `s3`, `http`) should be gated behind optional Cargo `[features]` in `Cargo.toml`.

---

## Example Contribution Ideas

Here are some great places to start:
* **Add a new Filter:** Implement a new field filter in `src/filters/field_filters.rs`.
* **Add a new Transformer:** Add a field transformer in `src/transformers/field_transformers.rs`.
* **Documentation & Examples:** Expand Rust doc comments (`///`) or add runnable examples (`examples/example.rs`, `tests/stress-test.rs`, `docs/`).
* **More Sources & Sinks** (behind optional Cargo feature flags):
  * **Cloud Storage:** Google Cloud Storage (GCS) and Azure Blob Storage via `object_store`.
  * **Message Queues:** Kafka, RabbitMQ, NATS, or AWS SQS streaming sources/sinks.
  * **NoSQL & Analytical Databases:** MongoDB, Redis, ClickHouse, or DuckDB writers.
* **Multi-file Glob Support:** Allow `FileSource` or a `CompositeReader` to expand glob patterns (e.g., `input/data/*.csv`) and stream matching files as one continuous `RecordStream`.
* **Stream Join & Lookup Enrichment:** Implement a `LookupTransformer` that enriches a high-volume stream against a reference dataset in `O(1)` lookup time.

---

## Development Setup

### Prerequisites
* **Rust** (stable toolchain, edition 2021+): install via [rustup.rs](https://rustup.rs/)

### Common Commands
```bash
# Build library and CLI (minimal default features)
cargo build

# Build with all optional connectors (parquet, xlsx, s3, http)
cargo build --all-features

# Run integration test suite
cargo test

# Run examples & stress benchmark
cargo run --release --example example
cargo run --release --example stress-test

# Generate and open Rust API documentation
cargo doc --no-deps --all-features --open
```

---
*By contributing to this project, you agree to abide by its terms.*
