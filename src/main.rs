use data_genie::{
    block_on, CSVReader, InferSchemaOptions, Job, JsonReader, NDJsonReader, PipelineParser,
    TSVReader,
};
use std::env;
use std::path::PathBuf;

fn print_help() {
    println!(
        r#"
data-genie - High-performance, streaming-first ETL Engine in Rust

Usage:
  data-genie <command> [arguments]

Commands:
  run <config.yaml>                  Execute a declarative ETL pipeline defined in YAML.
  preview <file> [--limit N]         Preview the first N records of a data file (default: 5).
  infer-schema <file> [--sample N]   Infer Rust, TypeScript, Zod, and SQL schemas.
  --help, -h                         Show this help message.

Example:
  data-genie run my-pipeline.yaml
"#
    );
}

fn main() {
    block_on(async_main());
}

async fn async_main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let Some(command) = args.first().map(String::as_str) else {
        print_help();
        return;
    };

    if command == "--help" || command == "-h" {
        print_help();
        return;
    }

    match command {
        "run" => {
            let Some(config_path) = args.get(1) else {
                eprintln!("Error: Please provide a path to a pipeline configuration file.");
                println!("Usage: data-genie run <pipeline.yaml>");
                std::process::exit(1);
            };
            if let Err(err) = PipelineParser::run(config_path).await {
                eprintln!("Job Failed: {}", err);
                std::process::exit(1);
            }
        }
        "preview" => {
            let Some(file_str) = args.get(1) else {
                eprintln!("Error: Please provide a file path to preview.");
                std::process::exit(1);
            };
            let file_path = PathBuf::from(file_str);
            let mut limit = 5usize;
            for i in 2..args.len() {
                if (args[i] == "--limit" || args[i] == "-l") && i + 1 < args.len() {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        limit = n;
                    }
                }
            }

            let ext = file_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("csv")
                .to_lowercase();

            let res = match ext.as_str() {
                "json" => Job::preview(JsonReader::new(&file_path), Some(limit), None).await,
                "ndjson" | "jsonl" => {
                    Job::preview(NDJsonReader::new(&file_path), Some(limit), None).await
                }
                "tsv" => Job::preview(TSVReader::new(&file_path), Some(limit), None).await,
                _ => Job::preview(CSVReader::new(&file_path), Some(limit), None).await,
            };

            if let Err(err) = res {
                eprintln!("Preview Failed: {}", err);
                std::process::exit(1);
            }
        }
        "infer-schema" => {
            let Some(file_str) = args.get(1) else {
                eprintln!("Error: Please provide a file path for schema inference.");
                std::process::exit(1);
            };
            let file_path = PathBuf::from(file_str);
            let mut sample_size = 1000usize;
            for i in 2..args.len() {
                if (args[i] == "--sample" || args[i] == "--sample-size" || args[i] == "-s")
                    && i + 1 < args.len()
                {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        sample_size = n;
                    }
                }
            }

            let ext = file_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("csv")
                .to_lowercase();

            let opts = InferSchemaOptions {
                sample_size: Some(sample_size),
                table_name: Some("inferred_table".to_string()),
                interface_name: Some("InferredRecord".to_string()),
            };

            let res = match ext.as_str() {
                "json" => Job::infer_schema(JsonReader::new(&file_path), opts).await,
                "ndjson" | "jsonl" => Job::infer_schema(NDJsonReader::new(&file_path), opts).await,
                "tsv" => Job::infer_schema(TSVReader::new(&file_path), opts).await,
                _ => Job::infer_schema(CSVReader::new(&file_path), opts).await,
            };

            match res {
                Ok(inferred) => {
                    println!("=== Rust Struct ===\n{}\n", inferred.rust);
                    println!("=== TypeScript Interface ===\n{}\n", inferred.typescript);
                    println!("=== Zod Schema ===\n{}\n", inferred.zod);
                    println!("=== SQL DDL ===\n{}", inferred.sql);
                }
                Err(err) => {
                    eprintln!("InferSchema Failed: {}", err);
                    std::process::exit(1);
                }
            }
        }
        other => {
            eprintln!("Unknown command: {}", other);
            print_help();
            std::process::exit(1);
        }
    }
}
