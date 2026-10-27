# PII Masking & Data Anonymization

Protect sensitive fields (emails, credit cards, SSNs) in-flight using `PIIMaskingTransformer`.

## Implementation

```rust
use data_genie::{
    CSVReader, Job, JsonWriter, MaskStrategy, PIIMaskingTransformer, TransformingReader,
};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users_raw.csv");

    let masker = PIIMaskingTransformer::new()
        .mask("email", MaskStrategy::Partial)          // p****@example.com
        .mask("ssn", MaskStrategy::Hash)               // SHA-256 hex digest
        .mask("internal_notes", MaskStrategy::Redact)  // [REDACTED]
        .mask("api_key", MaskStrategy::Null);          // null

    let pipeline = TransformingReader::new(reader).add_transform(masker.transform());
    let writer = JsonWriter::new("users_anonymized.json");

    Job::run_blocking(pipeline, writer)?;
    Ok(())
}
```

## Masking Strategies
- **`MaskStrategy::Partial`**: Keeps prefix/domain while masking sensitive characters (`p****@example.com`).
- **`MaskStrategy::Hash`**: Deterministic SHA-256 hash using the native Rust `sha2` crate.
- **`MaskStrategy::Redact`**: Replaces the value with `"[REDACTED]"`.
- **`MaskStrategy::Null`**: Replaces the value with `Value::Null`.
