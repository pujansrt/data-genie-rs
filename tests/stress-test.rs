use data_genie::{CSVReader, Job, JobOptions, JsonWriter, ValidatingReader};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const FILE_PATH: &str = "examples/input/heavy_data.csv";
const OUTPUT_PATH: &str = "examples/output/heavy_data.json";
const OUTPUT_DLQ_PATH: &str = "examples/output/error_data.json";
const TOTAL_RECORDS: usize = 1_000_000;

#[allow(dead_code)]
fn generate_heavy_data() -> data_genie::Result<()> {
    println!("🚀 Generating data ({} records)...", TOTAL_RECORDS);
    let file = File::create(FILE_PATH)?;
    let mut writer = BufWriter::with_capacity(256 * 1024, file);

    writer.write_all(b"id,timestamp,sensor_id,value,status\n")?;

    let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis();

    for i in 0..TOTAL_RECORDS {
        let val = ((i * 7919) % 1000000) as f64 / 10000.0;
        let status = if i % 10 == 0 { "ERROR" } else { "OK" };
        let row = format!("{},{},SNS_{},{:.4},{}\n", i, now_ms, i % 1000, val, status);
        writer.write_all(row.as_bytes())?;

        // Add a poison record halfway
        if i == TOTAL_RECORDS / 2 {
            writer.write_all(b"X,\n")?;
        }

        if i % 5_000_000 == 0 && i > 0 {
            println!("...Generated {} records", i);
        }
    }

    writer.flush()?;
    println!("✅ Generation complete.\n");
    Ok(())
}

fn start_conversion() -> data_genie::Result<()> {
    let metadata = fs::metadata(FILE_PATH)?;
    let file_size_in_bytes = metadata.len() as f64;
    let file_size_in_mb = file_size_in_bytes / (1024.0 * 1024.0);
    let file_size_in_gb = file_size_in_bytes / (1024.0 * 1024.0 * 1024.0);

    let size_display = if file_size_in_bytes > 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB", file_size_in_gb)
    } else {
        format!("{:.2} MB", file_size_in_mb)
    };

    println!("🔄 Starting {} Streaming Conversion (CSV -> JSON)...", size_display);
    let start_time = Instant::now();

    let reader = ValidatingReader::new(CSVReader::new(FILE_PATH)).with_dlq(JsonWriter::new(OUTPUT_DLQ_PATH));
    let writer = JsonWriter::new(OUTPUT_PATH);

    let mut job = Job::with_options(reader, writer, JobOptions::new().with_progress(false));

    job.on_progress(|m| {
        print!("\r⏳ Processed: {} | {:.0} rec/sec", m.record_count, m.records_per_second);
        let _ = io::stdout().flush();
    });

    let metrics = job.execute_blocking()?;

    let elapsed_mins = start_time.elapsed().as_secs_f64() / 60.0;
    println!(); // New line after progress bar
    println!("\n--- Performance Report ---");
    println!("Total Time: {:.2} minutes", elapsed_mins);
    println!("Avg Speed: {:.0} records/sec", metrics.records_per_second);
    println!("Final Count: {}", metrics.record_count);

    Ok(())
}

fn main() -> data_genie::Result<()> {
    if !Path::new(FILE_PATH).exists() {
        generate_heavy_data()?;
    }
    start_conversion()?;
    Ok(())
}
