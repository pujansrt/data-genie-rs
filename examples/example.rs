use data_genie::{block_on, CSVReader, Job, JobOptions, JsonWriter, SharedWriter};
use std::io::{self, Write};
use std::time::Instant;

/// Example 1: Synchronous (Blocking) Execution
/// No `#[tokio::main]` or `tokio` dependency required in your `Cargo.toml`.
fn run_blocking_example() -> data_genie::Result<()> {
    println!("=== 1. Running Synchronous (Blocking) Example ===");
    let dlq = SharedWriter::new(JsonWriter::new("examples/output/errors.json"));
    let reader = CSVReader::new("examples/input/heavy_data.csv").with_dlq(dlq.clone());
    let writer = JsonWriter::new("examples/output/output.json");

    let start_time = Instant::now();
    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    job.on_progress(|m| {
        print!(
            "\r⏳ Processed: {} | {:.0} rec/sec",
            m.record_count, m.records_per_second
        );
        let _ = io::stdout().flush();
    });

    // Runs on an internal runtime automatically — no `.await` needed
    let metrics = job.execute_blocking()?;
    dlq.close_dlq_blocking()?;

    let elapsed_mins = start_time.elapsed().as_secs_f64() / 60.0;
    println!(); // New line after progress bar
    println!("\n--- Performance Report (Blocking) ---");
    println!("Total Time: {:.2} minutes", elapsed_mins);
    println!("Avg Speed: {:.0} records/sec", metrics.records_per_second);
    println!("Final Count: {}", metrics.record_count);

    Ok(())
}

/// Example 2: Asynchronous Execution
/// Use `.execute().await` (or `Job::run(...).await`) when running inside an async context
/// such as `#[tokio::main]`, an async web server (Axum/Actix), or `data_genie::block_on`.
async fn run_async_example() -> data_genie::Result<()> {
    println!("\n=== 2. Running Asynchronous (.await) Example ===");
    let dlq = SharedWriter::new(JsonWriter::new("examples/output/errors.json"));
    let reader = CSVReader::new("examples/input/heavy_data.csv").with_dlq(dlq.clone());
    let writer = JsonWriter::new("examples/output/output.json");

    let start_time = Instant::now();
    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    job.on_progress(|m| {
        print!(
            "\r⏳ Processed: {} | {:.0} rec/sec",
            m.record_count, m.records_per_second
        );
        let _ = io::stdout().flush();
    });

    let metrics = job.execute().await?;
    dlq.close_dlq().await?;

    let elapsed_mins = start_time.elapsed().as_secs_f64() / 60.0;
    println!(); // New line after progress bar
    println!("\n--- Performance Report (Async) ---");
    println!("Total Time: {:.2} minutes", elapsed_mins);
    println!("Avg Speed: {:.0} records/sec", metrics.records_per_second);
    println!("Final Count: {}", metrics.record_count);

    Ok(())
}

fn main() -> data_genie::Result<()> {
    // 1. Run synchronously without async/await
    run_blocking_example()?;

    // 2. Run the async version (using `data_genie::block_on` here, or call `.await` inside `#[tokio::main]`)
    block_on(run_async_example())?;

    Ok(())
}
