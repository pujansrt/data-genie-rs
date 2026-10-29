# Standalone examples

These examples use Rust edition 2024. Run each command from the repository root.
Each example below is a complete program with its own small, embedded dataset.
No input files, credentials, optional features, or setup steps are needed. Results
are printed to the terminal; the examples do not write files.

| Command | Demonstrates | Expected result |
| --- | --- | --- |
| `cargo run --example csv_to_json` | Blocking CSV-to-JSON conversion | Alice and Bob as a JSON array |
| `cargo run --example async_pipeline` | Async execution and retrieving records from a writer | Two records, with numeric ages |
| `cargo run --example filter_transform` | Convert types, filter, rename, and select fields | Alice (31) and Cara (28); only name and age |
| `cargo run --example validation_dlq` | Field validation and a dead letter queue | Alice accepted; Bob and Cara rejected with `_errors` |
| `cargo run --example group_totals` | Group, count, sum, and average | West: 2 orders, 300 revenue, 150 average; East: 1 order, 50 revenue, 50 average |
| `cargo run --example multiple_outputs` | Write the same records as JSON and CSV | Alice and Bob in both formats |
| `cargo run --example infer_schema` | Infer Rust and SQL schemas from typed records | A User struct and users table definition |

Start with [csv_to_json.rs](csv_to_json.rs). To work with files, replace its
`MemorySource` with an input file path and its `MemorySink` with an output file
path. CSV fields are strings by default; [filter_transform.rs](filter_transform.rs)
shows explicit numeric conversion.

`MemoryReader`, `MemoryWriter`, and `MemorySink` hold their data in memory.
`GroupByReader` also buffers input records, so the aggregation example is intended
for datasets that fit in memory.

The existing `example` target is a larger performance demonstration that reads
`examples/input/heavy_data.csv` and writes under `examples/output/`. The
`stress-test` and `coverage-report` targets are development utilities rather than
introductory examples.
