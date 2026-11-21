use data_genie::{CSVWriter, Job, JsonWriter, MemoryReader, MemorySink, MultiWriter, record};

fn main() -> data_genie::Result<()> {
    let record1 = record!("name" => "Alice", "city" => "Dublin");
    let record2 = record!("name" => "Bob", "city" => "London");

    let reader = MemoryReader::new([record1, record2]);
    let json = MemorySink::new();
    let csv = MemorySink::new();

    // Each record is sent to both writers; Job closes both outputs.
    let writer = MultiWriter::from_writer(JsonWriter::new(json.clone())).add(CSVWriter::new(csv.clone()));

    Job::run_blocking(reader, writer)?;
    println!("JSON: {}", json.get_text());
    println!("CSV:\n{}", csv.get_text());
    Ok(())
}
