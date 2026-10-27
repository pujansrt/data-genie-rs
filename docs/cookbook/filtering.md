# Filtering Data

Data-Genie provides rule-based filters, boolean expressions, and closure predicates for filtering streams.

## Method 1: Rule-Based Filtering (`FieldFilter`)

```rust
use data_genie::{
    CSVReader, FieldFilter, FilteringReader, IsNotNull, Job, JsonWriter, PatternMatch,
};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");

    let email_rule = FieldFilter::new("email")
        .add_rule(IsNotNull)
        .add_rule(PatternMatch::new(r"^[^@]+@[^@]+\.[^@]+$"))
        .create_record_filter();

    let filtered = FilteringReader::new(reader).add_rule(email_rule);
    Job::run_blocking(filtered, JsonWriter::new("valid_emails.json"))?;
    Ok(())
}
```

## Method 2: Logical Expressions (`FilterExpression`)

```rust
use data_genie::{CSVReader, FilterExpression, FilteringReader, Job, JsonWriter};

fn main() -> data_genie::Result<()> {
    let reader = CSVReader::new("users.csv");

    let expr = FilterExpression::new("age >= 18 && status == 'active' && balance > 0");
    let filtered = FilteringReader::new(reader).add_rule(expr.create_record_filter());

    Job::run_blocking(filtered, JsonWriter::new("active_adults.json"))?;
    Ok(())
}
```

## Method 3: Closure Predicate

```rust
use data_genie::{CSVReader, FilteringReader, Job, JsonWriter};

fn main() -> data_genie::Result<()> {
    let filtered = FilteringReader::new(CSVReader::new("scores.csv"))
        .add_fn(|record| record.get("score").and_then(|v| v.as_f64()).unwrap_or(0.0) > 50.0);

    Job::run_blocking(filtered, JsonWriter::new("high_scores.json"))?;
    Ok(())
}
```
