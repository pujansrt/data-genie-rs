//! Run: cargo run --example infer_schema
use data_genie::{InferSchemaOptions, Job, MemoryReader, block_on, record};

fn main() -> data_genie::Result<()> {
    let reader = MemoryReader::new(vec![
        record!("name" => "Alice", "age" => 31, "active" => true),
        record!("name" => "Bob", "age" => 24, "active" => false),
    ]);
    let options = InferSchemaOptions {
        sample_size: Some(2),
        table_name: Some("users".into()),
        interface_name: Some("User".into()),
    };
    let schema = block_on(Job::infer_schema(reader, options))?;
    println!("Rust:\n{}", schema.rust);
    println!("SQL:\n{}", schema.sql);
    Ok(())
}
