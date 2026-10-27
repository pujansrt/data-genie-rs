# Multi-Sink Parallel Processing (Fan-out)

Broadcast every record from a single stream to multiple destinations simultaneously using `MultiWriter`.

## Implementation

```rust
use data_genie::{CSVReader, ConsoleWriter, Job, JsonWriter, MultiWriter, NDJsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("input.csv");

    let multi_writer = MultiWriter::new()
        .add_writer(JsonWriter::new("archive.json"))
        .add_writer(NDJsonWriter::new("events.ndjson"))
        .add_writer(ConsoleWriter::new());

    Job::run_blocking(reader, multi_writer)?;
    Ok(())
}
```

## Combining `MultiWriter` with `ParallelWriter`

If one sink is CPU-intensive or network-bound, wrap it in a `ParallelWriter` inside your `MultiWriter`:

```rust
use data_genie::{CSVReader, ConsoleWriter, Job, JsonWriter, MultiWriter, ParallelWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("input.csv");

    let parallel_sink = ParallelWriter::new(4, 250, || {
        Box::new(JsonWriter::new("heavy_archive.json"))
    });

    let multi = MultiWriter::new()
        .add_writer(ConsoleWriter::new())
        .add_writer(parallel_sink);

    Job::run_blocking(reader, multi)?;
    Ok(())
}
```
