use data_genie::{CSVWriter, Job, JsonWriter, MemoryReader, MemorySink, MultiWriter, record};

fn main() -> data_genie::Result<()> {
    let reader = MemoryReader::new(vec![
        record!("name" => "Alice", "city" => "Dublin"),
        record!("name" => "Bob", "city" => "London"),
    ]);
    let json = MemorySink::new();
    let csv = MemorySink::new();
    // Each record is sent to both writers; Job closes both outputs.
    let writer = MultiWriter::new(vec![Box::new(JsonWriter::new(json.clone())), Box::new(CSVWriter::new(csv.clone()))]);

    Job::run_blocking(reader, writer)?;
    println!("JSON: {}", json.get_text());
    println!("CSV:\n{}", csv.get_text());
    Ok(())
}
