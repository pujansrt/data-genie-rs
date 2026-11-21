# Declarative Pipelines (YAML)

Declarative pipelines let you define ETL jobs in a YAML configuration file and execute them via the `data-genie` CLI or `PipelineParser::run` / `run_blocking`.

## Quick Start

```yaml
# pipeline.yaml
pipeline:
  read:
    type: csv
    path: input/example.csv
  write:
    type: json
    path: output/users.json
```

Run it via Cargo or the installed binary:
```bash
cargo run --bin data-genie -- run pipeline.yaml
# or if installed via `cargo install data-genie`:
data-genie run pipeline.yaml
```

Validate a pipeline without processing records:

```bash
data-genie validate pipeline.yaml
```

## Standard Input and Output

Use `-` as the path to stream bytes from standard input or write bytes to standard output. Keep informational logs on standard error so the output can be piped safely.

```yaml
pipeline:
  read:
    type: csv
    path: "-"
  write:
    type: ndjson
    path: "-"
```

```bash
cat input.csv | data-genie run pipeline.yaml > output.ndjson
```

Standard input works best with streaming formats such as CSV and NDJSON. Formats that need random access or parse the complete document can use more memory.

For XML input, set `read.options.recordPath` to the full element path (for example `items/item`) or deep-search path (`//item`) to emit. XML output accepts optional `write.options.rootTag`, `write.options.recordTag`, and `write.options.addDeclaration` values.

## Interactive Pipeline Builder

Use the visual builder below to configure your pipeline and copy the generated YAML:

<script setup>
import PipelineBuilder from '../.vitepress/components/PipelineBuilder.vue'
</script>

<PipelineBuilder />

## Full YAML Schema

```yaml
version: "1.0"

job:
  name: "Daily Sales Sync"
  showProgress: true

pipeline:
  read:
    type: csv          # csv | tsv | json | ndjson | xml
    path: input.csv
    options:
      delimiter: ","
      hasHeader: true

  transform:
    - type: filter
      expression: "amount > 100"
    - type: rename
      mapping:
        old_name: new_name
    - type: calculate
      field: tier
      expression: "amount >= 500 ? 'GOLD' : 'STANDARD'"
    - type: pii-masking
      masks:
        email: partial # redact | hash | partial | null
    - type: select
      fields: ["id", "new_name", "email", "tier"]

  write:
    type: json         # csv | tsv | json | ndjson | xml | console
    path: out.json
```

## Running YAML Pipelines from Rust

You can also execute a declarative YAML file directly from Rust code:

```rust
use data_genie::PipelineParser;

fn main() -> data_genie::Result<()> {
    let metrics = PipelineParser::run_blocking("pipeline.yaml")?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```

Call `PipelineParser::validate_file("pipeline.yaml")?` to validate a file without running it, or `PipelineParser::validate_yaml(yaml)?` for an in-memory YAML string. Validation reports multiple configuration issues in one pass.
