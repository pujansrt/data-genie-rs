# Pipelines

Pipelines chain multiple operations together as records stream from a `DataReader` to a `DataWriter`. Data-Genie supports both **Programmatic Rust** pipelines and **Declarative YAML** pipelines.

## Programmatic Pipelines (Rust)

### Composing Pipeline Readers

Chain `FilteringReader`, `TransformingReader`, and `ValidatingReader` around any source reader:

```rust
use data_genie::{CSVReader, FilterExpression, FilteringReader, Job, JsonWriter, TransformingReader};
use serde_json::json;

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");

    let filtered = FilteringReader::new(reader)
        .add_rule(FilterExpression::new("age >= 18").create_record_filter());

    let pipeline = TransformingReader::new(filtered).add(|mut r| {
        let full = format!(
            "{} {}",
            r.get("fname").and_then(|v| v.as_str()).unwrap_or(""),
            r.get("lname").and_then(|v| v.as_str()).unwrap_or("")
        );
        r.insert("fullName".to_string(), json!(full));
        r
    });

    Job::run_blocking(pipeline, JsonWriter::new("output.json"))?;
    Ok(())
}
```

## Built-in Pipeline Readers

### `TransformingReader`
Chain reusable field transformers (`RenameField`, `SetCalculatedField`, `SelectFields`, `RemoveFields`, `TypeConvert`, `PIIMaskingTransformer`):

```rust
use data_genie::{CSVReader, RenameField, SetCalculatedField, TransformingReader};

let transformer = TransformingReader::new(CSVReader::new("orders.csv"))
    .add_transform(RenameField::new("old_name", "new_name").transform())
    .add_transform(SetCalculatedField::new("total", "price * qty").transform());
```

### `FilteringReader`
Discard records that do not match field rules or boolean expressions:

```rust
use data_genie::{CSVReader, FieldFilter, FilteringReader, GreaterThan};

let filter = FilteringReader::new(CSVReader::new("users.csv"))
    .add_rule(
        FieldFilter::new("age")
            .add_rule(GreaterThan::new(18.0))
            .create_record_filter(),
    );
```

## Fan-Out Pipelines (`MultiWriter`)

Broadcast a single stream to multiple destinations concurrently using `MultiWriter`:

```rust
use data_genie::{CSVReader, ConsoleWriter, Job, JsonWriter, MultiWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");
    let writer = MultiWriter::new()
        .add_writer(JsonWriter::new("archive.json"))
        .add_writer(ConsoleWriter::new());

    Job::run_blocking(reader, writer)?;
    Ok(())
}
```
