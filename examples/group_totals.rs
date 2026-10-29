use data_genie::{GroupByReader, Job, JsonWriter, MemoryReader, MemorySink, record};

fn main() -> data_genie::Result<()> {
    let sales = MemoryReader::new(vec![
        record!("region" => "West", "amount" => 100),
        record!("region" => "East", "amount" => 50),
        record!("region" => "West", "amount" => 200),
    ]);
    // GroupByReader buffers the input records; use it for datasets that fit in memory.
    let totals = GroupByReader::new(sales, "region")
        .count("orders")
        .sum("amount", "revenue")
        .avg("amount", "average_order");
    let output = MemorySink::new();
    Job::run_blocking(totals, JsonWriter::new(output.clone()))?;
    println!("{}", output.get_text());
    Ok(())
}
