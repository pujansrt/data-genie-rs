use data_genie::{Job, MemoryReader, MemoryWriter, block_on, record};

async fn run_pipeline() -> data_genie::Result<()> {
    let reader = MemoryReader::new(vec![record!("name" => "Alice", "age" => 31), record!("name" => "Bob", "age" => 24)]);
    let (metrics, writer) = Job::new(reader, MemoryWriter::new()).run_into_writer().await?;

    println!("Processed {} records", metrics.record_count);
    for record in writer.get_records() {
        println!("{}", serde_json::to_string(&record)?);
    }
    Ok(())
}

fn main() -> data_genie::Result<()> {
    block_on(run_pipeline())
}
