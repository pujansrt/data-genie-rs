use data_genie::{CSVReader, Job, JsonWriter, MemorySink, MemorySource};

fn main() -> data_genie::Result<()> {
    // MemorySource treats this text as data; a plain string would be a file path.
    let input = MemorySource::new("name,city\nAlice,Dublin\nBob,London\n");
    let output = MemorySink::new();
    let metrics = Job::run_blocking(CSVReader::new(input), JsonWriter::new(output.clone()))?;

    // Job closes the writer before returning, completing the JSON array.
    println!("{}", output.get_text());
    println!("Converted {} records", metrics.record_count);
    Ok(())
}
