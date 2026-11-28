# Developer Guide (`Dev.md`)

Welcome to **`data-genie-rs`**! This guide is written for anyone opening this repository for the first time. It explains the directory layout, how the streaming
ETL engine works under the hood, how Cargo feature flags keep the core lean, how to run tests and coverage locally, and step-by-step recipes for contributing.

---

## Table of Contents

1. [Prerequisites & First Run](#1-prerequisites--first-run)
2. [Project Structure & Feature Flags](#2-project-structure--feature-flags)
3. [Core Mental Model & Architecture](#3-core-mental-model--architecture)
4. [Module Map (`src/`)](#4-module-map-src)
5. [Engineering Invariants (Must-Know Rules)](#5-engineering-invariants-must-know-rules)
6. [Common Development Commands](#6-common-development-commands)
7. [Step-by-Step Contribution Recipes](#7-step-by-step-contribution-recipes)
8. [Dependency Hygiene & Publishing](#8-dependency-hygiene--publishing)
9. [Pull Request Checklist](#9-pull-request-checklist)

---

## 1. Prerequisites & First Run

- **Rust toolchain (Edition 2024)**: Install or update via [rustup.rs](https://rustup.rs/) (`rustup update stable`).
- **Components**: `rustfmt` and `clippy` (`rustup component add rustfmt clippy`).
- **Optional (for code coverage reports)**: `cargo install cargo-llvm-cov`.

Verify your environment right after cloning:

```shell
# 1. Run default (core-only) tests
cargo test

# 2. Run the full test suite with all optional features + CLI tests
cargo test --all-features

# 3. Run a standalone example
cargo run --example csv_to_json
```

---

## 2. Project Structure & Feature Flags

`data-genie` is a **single Cargo crate** (`lib` + optional `bin`) using Cargo `[features]` (`default = []`) to keep default builds fast and lightweight while
keeping maintenance and publishing simple.

```text
data-genie-rs/
├── Cargo.toml                  # Single crate manifest (features: xml, yaml, cli, parquet, xlsx, http, s3, full)
├── rustfmt.toml                # Formatting rules (edition 2024, max_width = 160)
├── .cargo/config.toml          # Cargo aliases (`cargo cov`, `cargo cov-html`, `cargo stress-test`)
├── src/
│   ├── lib.rs                  # Public crate exports (core + #[cfg(feature = "...")] connectors)
│   ├── main.rs                 # `data-genie` CLI binary (requires feature `yaml` / `cli`)
│   ├── core/                   # Traits, errors, Job, Expr, transports, PipelineParser
│   ├── readers/                # Source readers (CSV, TSV, JSON, NDJSON, FixedWidth, SQL, Memory, XML, Parquet, XLSX, HTTP)
│   ├── writers/                # Sink & composite writers (CSV, TSV, JSON, NDJSON, FixedWidth, SQL, XML, Parquet, XLSX, etc.)
│   ├── filters/                # FieldFilter, FilterExpression, FilteringReader
│   └── transformers/           # Field transformers, validation/DLQ, GroupBy, RemoveDuplicates
├── tests/
│   ├── integration_tests.rs    # End-to-end tests (core + feature-gated connector & CLI tests)
│   └── stress-test.rs          # Throughput / stress benchmark (`cargo stress-test`)
├── examples/                   # Standalone runnable examples + `coverage_report.rs`
└── docs/                       # VitePress documentation site
```

### Feature Flag Design Rationale: Why JSON & CSV Stay in Core

New contributors often ask why `json` and `csv` are always enabled in core while `xml`, `yaml`, `parquet`, `xlsx`, `http`, and `s3` are optional features:

1. **JSON adds zero extra dependencies:** Every record is `DataRecord = IndexMap<String, serde_json::Value>`, and `serde_json` is already required across the
   engine (`record!`, `Expr`, `JobMetrics`, `ConsoleWriter`, `RemoveDuplicatesReader`, `SQLWriter`). Gating JSON behind a feature would save 0 dependencies and
   add friction.
2. **CSV is tiny; optional features gate heavy or niche stacks:** `csv` adds only two small pure-Rust crates (`csv` + `csv-core`; `itoa`, `ryu`, and
   `serde_core` are already in core via `serde_json`), and `tsv`, `fixed_width`, and `sql` add zero extra dependencies. By contrast, optional features isolate
   heavy or specialized dependencies:
    - `parquet` → Apache `parquet` + `arrow` (~100+ transitive crates)
    - `http` → `reqwest` + `rustls` + `hyper`
    - `s3` → `object_store` + AWS signing/HTTP stack
    - `xlsx` → `calamine` + `rust_xlsxwriter` (ZIP/Excel XML stack)
    - `yaml` / `cli` → `serde_yaml_ng` (`unsafe-libyaml`) + `xml`
    - `xml` → `quick-xml`
3. **Rule of thumb for new contributions:** Put lightweight, zero-dependency or core-engine features directly in core. Gate any new format, cloud storage
   provider, message broker, or database driver that introduces non-trivial third-party dependencies behind an optional Cargo feature
   in [Cargo.toml](Cargo.toml).

---

## 3. Core Mental Model & Architecture

Every pipeline in `data-genie` follows one simple streaming abstraction:

```text
┌────────────┐    bytes    ┌────────────┐   RecordStream   ┌────────────────────────┐   DataRecord   ┌────────────┐    bytes    ┌──────────┐
│ DataSource │ ──────────► │ DataReader │ ───────────────► │ Decorator Reader(s)    │ ─────────────► │ DataWriter │ ──────────► │ DataSink │
│ (File/S3/  │             │ (CSV/JSON/ │  (1 record at    │ (Filter / Transform /  │                │ (JSON/CSV/ │             │ (File/S3/│
│ HTTP/Mem/  │             │ Parquet/…) │   a time)        │  Validate / Dedup / …) │                │ Parquet/…) │             │ HTTP/Mem/│
│ Stdin)     │             └────────────┘                  └────────────────────────┘                └────────────┘             │ Stdout)  │
└────────────┘                                                         ▲                                                        └──────────┘
                                                                       │
                                                       Job::execute() / Job::run()
                                                       drives the stream & handles
                                                       metrics, DLQ close, and abort()
```

### The 5 Building Blocks

| Concept                  | Trait / Type                                                                    | Role                                                                                                                                                                                                                                                                                               |
|--------------------------|---------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Record**               | `DataRecord = IndexMap<String, serde_json::Value>`                              | Ordered key-value map representing one row. Insertion order is preserved so CSV/TSV/SQL/FixedWidth headers stay deterministic.                                                                                                                                                                     |
| **Stream**               | `RecordStream = Pin<Box<dyn Stream<Item = Result<Option<DataRecord>>> + Send>>` | Lazy async stream yielding one record at a time. `Ok(Some(rec))` is a record; `Ok(None)` is a skipped/filtered slot; `Err(e)` fails the job.                                                                                                                                                       |
| **Transport**            | `DataSource` & `DataSink`                                                       | Abstract byte I/O (local files, memory buffers, stdin/stdout, S3 objects, HTTP bodies). `SourceInput` / `SinkInput` let users pass a `&str`, `PathBuf`, or custom `DataSource`/`DataSink` seamlessly.                                                                                              |
| **Readers & Decorators** | `DataReader`                                                                    | Both **source readers** (`CSVReader`, `JsonReader`, `ParquetReader`) and **pipeline stages** (`FilteringReader`, `TransformingReader`, `ValidatingReader`, `SchemaValidatingReader`, `GroupByReader`, `RemoveDuplicatesReader`) implement `DataReader`, so stages compose by wrapping one another. |
| **Writers**              | `DataWriter`                                                                    | Consumes records via `async fn write(&mut self, record)`, flushes/commits in `close()`, and rolls back/discards partial output in `abort()`.                                                                                                                                                       |

---

## 4. Module Map (`src/`)

```text
src/
├── lib.rs                          # Crate root re-exports + `#[doc(hidden)] pub use serde_json;` for `record!`
├── main.rs                         # `data-genie` CLI (`run`, `validate`, `preview`, `infer-schema`) [feature: yaml]
├── core/
│   ├── traits.rs                   # DataRecord, RecordStream, DataSource, DataSink, DataReader, DataWriter,
│   │                               # SharedWriter, SqlConnection, Logger, `record!` macro, value helpers
│   ├── error.rs                    # Unified `DataGenieError` enum and `Result<T>`
│   ├── job.rs                      # `Job` orchestrator, `JobMetrics`, `JobOptions`, `Job::preview`,
│   │                               # `Job::infer_schema`, `block_on`, `block_on_send`
│   ├── expr.rs                     # Expression lexer/parser/evaluator (FilterExpression & SetCalculatedField)
│   ├── file_transport.rs           # FileSource & FileSink
│   ├── memory_transport.rs         # MemorySource, MemorySink & SharedVecWriter
│   ├── stdio_transport.rs          # StdinSource & StdoutSink
│   ├── transport_utils.rs          # SourceInput, SinkInput, ensure_data_source(), ensure_data_sink()
│   ├── pipeline_parser.rs          # Declarative YAML parser & runner [feature: yaml]
│   ├── http_transport.rs           # HttpSource & HttpSink [feature: http]
│   └── s3_transport.rs             # S3Source & S3Sink [feature: s3]
├── readers/
│   ├── csv_reader.rs               # Streaming RFC 4180 CSV reader (BOM handling, multiline fields, DLQ)
│   ├── tsv_reader.rs               # Streaming TSV reader
│   ├── fixed_width_reader.rs       # Fixed-width column parser
│   ├── json_reader.rs              # Streaming JSON array reader
│   ├── nd_json_reader.rs           # Line-delimited JSON (NDJSON/JSONL) reader
│   ├── memory_reader.rs            # In-memory `DataReader` backed by `Arc<[DataRecord]>`
│   ├── sql_reader.rs               # Paginated SQL reader over `SqlConnection`
│   ├── xml_reader.rs               # Streaming pull XML reader (`quick-xml`) [feature: xml]
│   ├── parquet_reader.rs           # Apache Parquet reader [feature: parquet]
│   ├── xlsx_reader.rs              # Excel `.xlsx` reader (`calamine`) [feature: xlsx]
│   └── http_reader.rs              # Paginated REST API reader (`reqwest`) [feature: http]
├── writers/
│   ├── csv_writer.rs, tsv_writer.rs, fixed_width_writer.rs, json_writer.rs, nd_json_writer.rs
│   ├── sql_writer.rs               # Dialect-aware batched INSERT/UPSERT (Postgres, MySQL, SQLite, Oracle)
│   ├── memory_writer.rs            # Thread-safe in-memory collector (`Arc<Mutex<Vec<DataRecord>>>`)
│   ├── console_writer.rs           # Pretty-printed stdout writer
│   ├── callback_writer.rs          # Sync (`FnMut`) and Async per-record callback writer
│   ├── batch_callback_writer.rs    # Batched async callback writer
│   ├── multi_writer.rs             # Fan-out writer to multiple downstream `DataWriter`s
│   ├── parallel_writer.rs          # Worker-pool writer with lazy worker spawning
│   ├── retrying_writer.rs          # Exponential backoff + Closed/Open/HalfOpen circuit breaker
│   ├── xml_writer.rs               # Streaming XML writer [feature: xml]
│   ├── parquet_writer.rs           # Apache Arrow/Parquet writer [feature: parquet]
│   └── xlsx_writer.rs              # Excel `.xlsx` writer (`rust_xlsxwriter`) [feature: xlsx]
├── filters/
│   ├── field_filters.rs            # Built-in predicates (`greater_than`, `between`, `pattern_match`, etc.)
│   ├── filter_expressions.rs       # Expression-based record filter (`age > 30 && status == 'ACTIVE'`)
│   └── filtering_reader.rs         # `FilteringReader` decorator
└── transformers/
    ├── field_transformers.rs       # BasicFieldTransformer, RenameField, SelectFields, RemoveFields,
    │                               # MapFields, SetField, SetCalculatedField, PIIMaskingTransformer
    ├── transforming_reader.rs      # `TransformingReader` decorator
    ├── validating_reader.rs        # Rule-based `ValidatingReader` + DLQ routing
    ├── schema_validating_reader.rs # `SchemaValidator` trait, `RecordSchema`, `FieldSchema`, coercion
    ├── group_by_reader.rs          # Streaming incremental `O(G)` aggregation (count, sum, min, max, avg)
    └── remove_duplicates_reader.rs # Deduplication with `DuplicateKeyStore` & `max_keys` guard
```

---

## 5. Engineering Invariants (Must-Know Rules)

Before writing code, internalize these five invariants — they are tested explicitly in `tests/integration_tests.rs`:

1. **`O(1)` Streaming Memory by Default**
    - Build readers with `async_stream::try_stream!` and yield records incrementally.
    - Never collect all records into a `Vec<DataRecord>` unless the operation inherently requires materialization (e.g., `GroupByReader`, `XlsxWriter` workbook
      generation).

2. **Safe Writer Lifecycle (`write` / `close` / `abort`)**
    - Every `DataWriter` must track `is_closed: bool`.
    - `close()` must be **idempotent** (calling `close()` twice must be a no-op — never overwrite an already finalized S3/HTTP/file output with 0 bytes!).
    - Calling `write()` after `close()` or `abort()` must return an `Err(...)`.
    - On any job error, `Job::execute()` calls `writer.abort().await` instead of `close()`. Writers must discard buffered data and call
      `self.sink.abort().await` so partial/corrupted files or uploads are not committed.

3. **DLQ Ownership & Upstream Propagation**
    - Wrapper readers (`FilteringReader`, `TransformingReader`, `ValidatingReader`, `SchemaValidatingReader`, etc.) must implement
      `propagate_dlq(&mut self, writer: SharedWriter)` and `set_ignore_errors(&mut self, ignore: bool)` to forward configuration upstream.
    - Only the reader that **owns** the DLQ (attached via `.with_dlq(...)` or `.set_dlq(...)`) closes it when the stream finishes via
      `SharedWriter::close_dlq()` (which is idempotent).
    - `dlq_record_count()` must return `local_dlq_records + inner.dlq_record_count()` so multi-stage DLQ counts sum accurately without double-counting.

4. **Feature-Gated Error Conversions**
    - `DataGenieError` in `src/core/error.rs` uses `String` variants (`Xml(String)`, `Yaml(String)`, `Parquet(String)`, `Xlsx(String)`, `Http(String)`,
      `ObjectStore(String)`) so the enum shape never changes across feature combinations, with `#[cfg(feature = "...")] impl From<...>` conversions gated
      cleanly.

5. **Sync/Async Safety & Macro Hygiene**
    - Blocking wrappers (`Job::run_blocking`, `PipelineParser::run_blocking`) use `block_on_send` (scoped thread fallback) so they never panic if called inside
      an existing `current_thread` Tokio runtime.
    - Constructors (e.g., `ParallelWriter::new`) must not call `tokio::spawn` synchronously — spawn background tasks lazily on first `write()` so they work
      cleanly with `Job::run_blocking`.
    - `record!` uses `$crate::serde_json::json!` (with `#[doc(hidden)] pub use serde_json;` in `src/lib.rs`) so downstream callers don't need `serde_json` in
      their own `Cargo.toml`.

---

## 6. Common Development Commands

### Building & Type-Checking

```shell
# Build default library (core only, zero optional deps)
cargo build

# Build with all optional connectors + CLI binary
cargo build --all-features

# Build just the CLI binary in release mode
cargo build --release --features cli --bin data-genie
```

### Testing

```shell
# Run core tests (default features)
cargo test

# Run ALL tests (core + xml + yaml + parquet + xlsx + http + s3 + CLI tests)
cargo test --all-features

# Run a single integration test by name
cargo test --all-features --test integration_tests test_parquet

# Run only unit tests inside src/
cargo test --lib --all-features
```

### Running Examples & CLI Locally

```shell
# Check all examples compile
cargo check --examples

# Run individual examples
cargo run --example csv_to_json
cargo run --example filter_transform
cargo run --example validation_dlq
cargo run --example group_totals

# Run the CLI directly
cargo run --features cli -- --help
cargo run --features cli -- preview examples/input/users.csv --limit 5
cargo run --features cli -- validate tests/declarative-pipeline.yaml
```

### Formatting, Linting & Docs

Formatting rules are configured in [rustfmt.toml](rustfmt.toml) (Rust edition 2024, `max_width = 160`, sorted imports/modules).

```shell
# Format all code
cargo fmt --all

# Check formatting (same check CI runs)
cargo fmt --all -- --check

# Lint default build and all-features build
cargo clippy --all-targets
cargo clippy --all-targets --all-features

# Build & open local rustdoc
cargo doc --no-deps --all-features --open
```

### Code Coverage & Benchmarks

Custom aliases are defined in [.cargo/config.toml](.cargo/config.toml):

```shell
# Terminal directory + per-file coverage summary
cargo cov

# Compact directory-only coverage summary
cargo cov-summary

# Generate forced light-mode HTML report and open in browser
cargo cov-html

# Export lcov.info for editors/CI
cargo cov-lcov

# Run throughput stress benchmark
cargo stress-test
```

---

## 7. Step-by-Step Contribution Recipes

### Recipe A: Add a New Filter or Transformer

1. Implement the function or struct in:
    - `src/filters/field_filters.rs` (filters), or
    - `src/transformers/field_transformers.rs` (transformers).
2. Re-export it in `src/{filters,transformers}/mod.rs` and `src/lib.rs`.
3. Add unit/integration tests in `tests/integration_tests.rs`.
4. Run `cargo test` and `cargo clippy --all-targets --all-features`.

### Recipe B: Add a New Core Reader or Writer

1. Create `src/readers/<name>_reader.rs` (or `src/writers/<name>_writer.rs`).
2. Accept `impl Into<SourceInput>` / `impl Into<SinkInput>` and wrap via `ensure_data_source` / `ensure_data_sink` so it automatically works with Files, Memory,
   Stdin/Stdout, S3, and HTTP.
3. For readers:
    - Stream records via `async_stream::try_stream!`.
    - Support UTF-8 BOM stripping and invalid-record DLQ routing (`with_dlq`, `set_dlq`, `set_ignore_errors`, `dlq_record_count`, `rejected_record_count`).
4. For writers:
    - Track `is_closed: bool`, enforce idempotent `close()`, error on `write()` after close, and implement `abort()`.
5. Re-export in `src/{readers,writers}/mod.rs` and `src/lib.rs`.
6. Add round-trip, edge-case, DLQ, and `abort()` tests in `tests/integration_tests.rs`.

### Recipe C: Add a New Optional Connector (Feature-Gated)

Whenever a format or connector requires heavy or specialized third-party crates:

1. Add the optional dependencies in [Cargo.toml](Cargo.toml) (`optional = true`) and create a feature flag (e.g., `avro = ["dep:apache-avro"]`), adding `"avro"`
   to `full`.
2. Add your reader/writer/transport module and gate its `pub mod` and `pub use` with `#[cfg(feature = "...")]` in `src/{core,readers,writers}/mod.rs` and
   `src/lib.rs`.
3. Add a `String` variant (if needed) and `#[cfg(feature = "...")] impl From<...>` in `src/core/error.rs`.
4. Add feature-gated tests (`#[cfg(feature = "...")]`) in `tests/integration_tests.rs`.
5. Verify **both** `cargo test` (without the feature) and `cargo test --all-features` pass with zero warnings.

### Recipe D: Add a Declarative YAML Pipeline Step

1. Update `src/core/pipeline_parser.rs`:
    - Add any new config fields to `ReaderConfig`, `WriterConfig`, or `TransformConfig`.
    - Validate all options in `PipelineConfig::validate()` (collect all issues into `issues` rather than bailing on the first one).
    - Wire execution in `create_reader()`, `apply_transforms()`, or `create_writer()`.
2. Add validation + end-to-end execution tests in `tests/integration_tests.rs` (under `#[cfg(feature = "yaml")]`).

---

## 8. Dependency Hygiene & Publishing

### Inspecting Dependency Footprint & Package Size

```shell
# Direct dependencies of the default build
cargo tree --no-default-features -e normal --depth 1

# Total transitive crate count (default build vs all features)
cargo tree --no-default-features -e normal --prefix none | sort -u | wc -l
cargo tree --all-features -e normal --prefix none | sort -u | wc -l

# Inspect exact files that will be packaged for crates.io
cargo package --list --allow-dirty
```

### Publishing a Release

1. Bump `version` in [Cargo.toml](Cargo.toml).
2. Run the full verification suite:
   ```shell
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features
   cargo test
   cargo test --all-features
   cargo check --examples
   cargo publish --dry-run
   ```
3. Commit, tag (`git tag -a v0.4.0 -m "Release v0.4.0"`), push (`git push origin production --tags`), and publish:
   ```shell
   cargo publish
   ```

---

## 9. Pull Request Checklist

Before opening a PR, make sure all of the following pass cleanly with zero warnings:

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features`
- [ ] `cargo test` (default minimal features)
- [ ] `cargo test --all-features` (all optional connectors + CLI tests)
- [ ] `cargo check --examples`
- [ ] `cargo doc --no-deps --all-features`
- [ ] New behavior is covered by unit/integration tests and reflected in `README.md` / `Dev.md` / `docs/` where relevant.
