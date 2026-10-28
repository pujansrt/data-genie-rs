# Declarative Pipelines (YAML)

Declarative pipelines let you define ETL jobs in a YAML configuration file and execute them via the `data-genie` CLI or `PipelineParser::run_file`.

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
    let metrics = PipelineParser::run_file_blocking("pipeline.yaml")?;
    println!("Processed {} records", metrics.record_count);
    Ok(())
}
```
