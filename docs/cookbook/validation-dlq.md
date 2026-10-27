# Validation & Dead Letter Queue (DLQ)

Use `SchemaValidatingReader` or `ValidatingReader` to validate records and automatically divert invalid or malformed rows to a Dead Letter Queue (`DLQ`) without stopping the pipeline.

## Declarative Schema Validation (`SchemaValidatingReader`)

```rust
use data_genie::{
    CSVReader, FieldConstraint, FieldType, Job, JsonWriter, SchemaDefinition,
    SchemaValidatingReader,
};

fn main() -> data_genie::Result<()> {
    let schema = SchemaDefinition::new()
        .add_field("id", FieldConstraint::new(FieldType::Integer).required().coerce(true))
        .add_field("email", FieldConstraint::new(FieldType::Email).required())
        .add_field(
            "role",
            FieldConstraint::new(FieldType::String)
                .required()
                .enum_values(["admin", "user"]),
        );

    let reader = CSVReader::new("raw_users.csv");

    // Attaching .with_dlq() also propagates the DLQ upstream to CSVReader!
    let validated = SchemaValidatingReader::new(reader, schema)
        .with_dlq(JsonWriter::new("errors.json"));

    Job::run_blocking(validated, JsonWriter::new("clean_users.json"))?;
    Ok(())
}
```

## Custom Business Rule Validation (`ValidatingReader`)

```rust
use data_genie::{CSVReader, Job, JsonWriter, ValidatingReader};

fn main() -> data_genie::Result<()> {
    let validated = ValidatingReader::new(CSVReader::new("accounts.csv"))
        .add_rule("non_negative_savings", |record| {
            let acct_type = record.get("type").and_then(|v| v.as_str()).unwrap_or("");
            let balance = record.get("balance").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if acct_type == "SAVINGS" && balance < 0.0 {
                Err("Savings accounts cannot have a negative balance".to_string())
            } else {
                Ok(())
            }
        })
        .with_dlq(JsonWriter::new("business_rule_failures.json"));

    Job::run_blocking(validated, JsonWriter::new("valid_accounts.json"))?;
    Ok(())
}
```
