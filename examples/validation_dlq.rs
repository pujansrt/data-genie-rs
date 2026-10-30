use data_genie::{FieldValidator, Job, JsonWriter, MemoryReader, MemorySink, ValidatingReader, record};

fn main() -> data_genie::Result<()> {
    let reader = MemoryReader::new(vec![
        record!("name" => "Alice", "age" => 31),
        record!("name" => "Bob", "age" => -1),
        record!("name" => "Cara"),
    ]);
    let age_rule = FieldValidator::new("age")
        .add_rule(|value| value.as_u64().is_some(), "age must be a non-negative integer")
        .create_record_validation_rule();
    let accepted = MemorySink::new();
    let rejected = MemorySink::new();
    // Invalid records go to the dead letter queue with an _errors field.
    // ValidatingReader closes its DLQ when the stream finishes.
    let validated = ValidatingReader::new(reader).add_rule(age_rule).with_dlq(JsonWriter::new(rejected.clone()));

    Job::run_blocking(validated, JsonWriter::new(accepted.clone()))?;
    println!("Accepted: {}", accepted.get_text());
    println!("Rejected: {}", rejected.get_text());
    Ok(())
}
