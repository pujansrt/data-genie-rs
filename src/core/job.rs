use chrono::{DateTime, Utc};
use futures::StreamExt;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Instant;

use crate::core::error::Result;
use crate::core::interfaces::{ConsoleLogger, DataReader, DataRecord, DataWriter, Logger};

/// Runs a future to completion on a multi-threaded Tokio runtime without requiring `#[tokio::main]`
/// or a direct `tokio` dependency in downstream crates.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime")
            .block_on(future)
    }
}

/// Execution metrics returned when a `Job` completes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMetrics {
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_ms: u64,
    pub record_count: usize,
    pub records_per_second: f64,
}

/// Options for configuring a `Job`.
#[derive(Clone, Default)]
pub struct JobOptions {
    pub logger: Option<Arc<dyn Logger>>,
    pub show_progress: bool,
}

impl JobOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_logger<L: Logger + 'static>(mut self, logger: L) -> Self {
        self.logger = Some(Arc::new(logger));
        self
    }

    pub fn with_progress(mut self, show_progress: bool) -> Self {
        self.show_progress = show_progress;
        self
    }
}

/// Inferred field metadata produced by `Job::infer_schema`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldSchemaInfo {
    pub field_type: String,
    pub nullable: bool,
}

/// Output of `Job::infer_schema`, providing Rust, TypeScript, Zod, and SQL DDL representations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredSchema {
    pub fields: IndexMap<String, FieldSchemaInfo>,
    pub rust: String,
    pub typescript: String,
    pub zod: String,
    pub sql: String,
}

#[derive(Debug, Clone, Default)]
pub struct InferSchemaOptions {
    pub sample_size: Option<usize>,
    pub table_name: Option<String>,
    pub interface_name: Option<String>,
}

pub type StartCallback = Box<dyn FnMut(DateTime<Utc>) + Send>;
pub type RecordCallback = Box<dyn FnMut(&DataRecord) + Send>;
pub type ProgressCallback = Box<dyn FnMut(&JobMetrics) + Send>;
pub type ErrorCallback = Box<dyn FnMut(&str, Option<&DataRecord>) + Send>;
pub type CompleteCallback = Box<dyn FnMut(&JobMetrics) + Send>;

/// `Job` orchestrates an ETL pipeline by streaming records from a `DataReader` into a `DataWriter`
/// with constant memory complexity, lifecycle hooks, and real-time throughput metrics.
pub struct Job<R: DataReader, W: DataWriter> {
    reader: R,
    writer: W,
    options: JobOptions,
    on_start: Vec<StartCallback>,
    on_record: Vec<RecordCallback>,
    on_progress: Vec<ProgressCallback>,
    on_error: Vec<ErrorCallback>,
    on_complete: Vec<CompleteCallback>,
}

impl<R: DataReader, W: DataWriter> Job<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self::with_options(reader, writer, JobOptions::default())
    }

    pub fn with_options(reader: R, writer: W, options: JobOptions) -> Self {
        Self {
            reader,
            writer,
            options,
            on_start: Vec::new(),
            on_record: Vec::new(),
            on_progress: Vec::new(),
            on_error: Vec::new(),
            on_complete: Vec::new(),
        }
    }

    /// Registers a listener for the `'start'` lifecycle event.
    pub fn on_start<F: FnMut(DateTime<Utc>) + Send + 'static>(&mut self, cb: F) -> &mut Self {
        self.on_start.push(Box::new(cb));
        self
    }

    /// Registers a listener for the `'record'` lifecycle event.
    pub fn on_record<F: FnMut(&DataRecord) + Send + 'static>(&mut self, cb: F) -> &mut Self {
        self.on_record.push(Box::new(cb));
        self
    }

    /// Registers a listener for the `'progress'` lifecycle event.
    pub fn on_progress<F: FnMut(&JobMetrics) + Send + 'static>(&mut self, cb: F) -> &mut Self {
        self.on_progress.push(Box::new(cb));
        self
    }

    /// Registers a listener for the `'error'` lifecycle event.
    pub fn on_error<F: FnMut(&str, Option<&DataRecord>) + Send + 'static>(
        &mut self,
        cb: F,
    ) -> &mut Self {
        self.on_error.push(Box::new(cb));
        self
    }

    /// Registers a listener for the `'complete'` lifecycle event.
    pub fn on_complete<F: FnMut(&JobMetrics) + Send + 'static>(&mut self, cb: F) -> &mut Self {
        self.on_complete.push(Box::new(cb));
        self
    }

    /// Executes the job, streaming all records from the reader to the writer.
    pub async fn execute(&mut self) -> Result<JobMetrics> {
        let default_logger: Arc<dyn Logger> = Arc::new(ConsoleLogger);
        let logger = self.options.logger.as_ref().unwrap_or(&default_logger);
        let show_progress = self.options.show_progress && std::io::stdout().is_terminal();

        let start_time = Utc::now();
        let start_instant = Instant::now();
        let mut last_update = Instant::now();
        let mut record_count: usize = 0;

        for cb in &mut self.on_start {
            cb(start_time);
        }

        let run_result: Result<()> = async {
            let mut stream = self.reader.read();
            while let Some(item) = stream.next().await {
                let record_opt = match item {
                    Ok(opt) => opt,
                    Err(err) => {
                        let msg = err.to_string();
                        for cb in &mut self.on_error {
                            cb(&msg, None);
                        }
                        return Err(err);
                    }
                };

                let Some(record) = record_opt else {
                    continue;
                };

                let has_record_hooks = !self.on_record.is_empty();
                let has_error_hooks = !self.on_error.is_empty();

                if has_record_hooks || has_error_hooks {
                    if let Err(err) = self.writer.write(record.clone()).await {
                        let msg = err.to_string();
                        for cb in &mut self.on_error {
                            cb(&msg, Some(&record));
                        }
                        return Err(err);
                    }
                    record_count += 1;
                    for cb in &mut self.on_record {
                        cb(&record);
                    }
                } else {
                    self.writer.write(record).await?;
                    record_count += 1;
                }

                if record_count % 1000 == 0
                    || (record_count & 255 == 0 && last_update.elapsed().as_millis() > 200)
                {
                    let now = Utc::now();
                    let duration_ms = start_instant.elapsed().as_millis() as u64;
                    let current_metrics =
                        Self::calculate_metrics(start_time, now, duration_ms, record_count);

                    for cb in &mut self.on_progress {
                        cb(&current_metrics);
                    }

                    if show_progress {
                        let rps = current_metrics.records_per_second;
                        let elapsed = current_metrics.duration_ms as f64 / 1000.0;
                        print!(
                            "\r⏳ Processing: {} records | {:.0} rec/sec | {:.1}s elapsed",
                            record_count, rps, elapsed
                        );
                        let _ = std::io::stdout().flush();
                    }
                    last_update = Instant::now();
                }
            }
            Ok(())
        }
        .await;

        if show_progress {
            print!("\r{}\r", " ".repeat(100));
            let _ = std::io::stdout().flush();
        }

        let close_res = self.writer.close().await;
        run_result?;
        close_res?;

        let end_time = Utc::now();
        let duration_ms = start_instant.elapsed().as_millis() as u64;
        let metrics = Self::calculate_metrics(start_time, end_time, duration_ms, record_count);

        logger.info("\n--- Job Completed ---");
        logger.info(&format!("Processed: {} records", record_count));
        logger.info(&format!(
            "Duration:  {:.2}s",
            metrics.duration_ms as f64 / 1000.0
        ));
        logger.info(&format!(
            "Throughput: {:.2} rec/sec",
            metrics.records_per_second
        ));

        for cb in &mut self.on_complete {
            cb(&metrics);
        }

        Ok(metrics)
    }

    /// Synchronous convenience wrapper around `execute()` that manages the async runtime internally.
    /// Allows running jobs from a normal `fn main()` without `#[tokio::main]`.
    pub fn execute_blocking(&mut self) -> Result<JobMetrics> {
        block_on(self.execute())
    }

    /// Consumes the `Job` and runs it to completion, returning both the `JobMetrics` and the writer.
    pub async fn run_into_writer(mut self) -> Result<(JobMetrics, W)> {
        let metrics = self.execute().await?;
        Ok((metrics, self.writer))
    }

    fn calculate_metrics(
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
        duration_ms: u64,
        record_count: usize,
    ) -> JobMetrics {
        let records_per_second = if duration_ms > 0 {
            (record_count as f64 / duration_ms as f64) * 1000.0
        } else {
            record_count as f64
        };
        JobMetrics {
            start_time,
            end_time,
            duration_ms,
            record_count,
            records_per_second,
        }
    }
}

/// Convenience static functions on `Job` (unit type specialization).
pub struct JobRunner;

impl Job<crate::readers::MemoryReader, crate::writers::MemoryWriter> {
    /// Static convenience method to run a job without manually constructing a `Job` instance.
    pub async fn run<R: DataReader, W: DataWriter>(reader: R, writer: W) -> Result<JobMetrics> {
        let mut job = Job::new(reader, writer);
        job.execute().await
    }

    /// Synchronous static convenience method to run a job from a standard `fn main()` without `#[tokio::main]`.
    pub fn run_blocking<R: DataReader, W: DataWriter>(reader: R, writer: W) -> Result<JobMetrics> {
        block_on(Self::run(reader, writer))
    }

    /// Static convenience method to run a job with custom `JobOptions`.
    pub async fn run_with_options<R: DataReader, W: DataWriter>(
        reader: R,
        writer: W,
        options: JobOptions,
    ) -> Result<JobMetrics> {
        let mut job = Job::with_options(reader, writer, options);
        job.execute().await
    }

    /// Previews the first `limit` records of a stream without writing them to a destination.
    pub async fn preview<R: DataReader>(
        mut reader: R,
        limit: Option<usize>,
        logger: Option<Arc<dyn Logger>>,
    ) -> Result<Vec<DataRecord>> {
        let default_logger: Arc<dyn Logger> = Arc::new(ConsoleLogger);
        let log = logger.as_ref().unwrap_or(&default_logger);
        let max_records = limit.unwrap_or(5);
        let mut preview_records = Vec::new();

        log.info(&format!("--- Previewing first {} records ---", max_records));

        let mut stream = reader.read();
        while let Some(item) = stream.next().await {
            if let Some(record) = item? {
                preview_records.push(record);
                if preview_records.len() >= max_records {
                    break;
                }
            }
        }

        if preview_records.is_empty() {
            log.warn("No records found to preview.");
        } else {
            for (idx, rec) in preview_records.iter().enumerate() {
                log.info(&format!(
                    "[{}] {}",
                    idx,
                    serde_json::to_string(rec).unwrap_or_default()
                ));
            }
            log.info(&format!(
                "--- Preview Complete ({} records shown) ---",
                preview_records.len()
            ));
        }

        Ok(preview_records)
    }

    /// Samples the first N records from a reader and infers Rust, TypeScript, Zod, and SQL schemas.
    pub async fn infer_schema<R: DataReader>(
        mut reader: R,
        options: InferSchemaOptions,
    ) -> Result<InferredSchema> {
        let sample_size = options.sample_size.unwrap_or(1000);
        let table_name = options
            .table_name
            .unwrap_or_else(|| "inferred_table".to_string());
        let interface_name = options
            .interface_name
            .unwrap_or_else(|| "InferredRecord".to_string());

        let mut field_stats: IndexMap<String, (HashSet<String>, bool)> = IndexMap::new();
        let mut count = 0;

        let mut stream = reader.read();
        while let Some(item) = stream.next().await {
            let Some(record) = item? else {
                continue;
            };

            for (key, value) in &record {
                let entry = field_stats
                    .entry(key.clone())
                    .or_insert_with(|| (HashSet::new(), false));

                match value {
                    Value::Null => {
                        entry.1 = true;
                    }
                    Value::Bool(_) => {
                        entry.0.insert("boolean".to_string());
                    }
                    Value::Number(_) => {
                        entry.0.insert("number".to_string());
                    }
                    Value::String(s) => {
                        if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                            entry.0.insert("date".to_string());
                        } else {
                            entry.0.insert("string".to_string());
                        }
                    }
                    Value::Array(_) | Value::Object(_) => {
                        entry.0.insert("object".to_string());
                    }
                }
            }

            count += 1;
            if count >= sample_size {
                break;
            }
        }

        let mut fields = IndexMap::new();
        for (key, (types, nullable)) in field_stats {
            let field_type = if types.contains("date") {
                "date"
            } else if types.contains("object") {
                "object"
            } else if types.contains("number") {
                "number"
            } else if types.contains("boolean") {
                "boolean"
            } else {
                "string"
            };
            fields.insert(
                key,
                FieldSchemaInfo {
                    field_type: field_type.to_string(),
                    nullable,
                },
            );
        }

        let rust = Self::generate_rust(&interface_name, &fields);
        let typescript = Self::generate_typescript(&interface_name, &fields);
        let zod = Self::generate_zod(&fields);
        let sql = Self::generate_sql(&table_name, &fields);

        Ok(InferredSchema {
            fields,
            rust,
            typescript,
            zod,
            sql,
        })
    }

    fn generate_rust(name: &str, fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let mut lines = vec![
            "#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]".to_string(),
            format!("pub struct {} {{", name),
        ];
        for (key, info) in fields {
            let base_type = match info.field_type.as_str() {
                "number" => "f64",
                "boolean" => "bool",
                "date" => "chrono::DateTime<chrono::Utc>",
                "object" => "serde_json::Value",
                _ => "String",
            };
            let rust_type = if info.nullable {
                format!("Option<{}>", base_type)
            } else {
                base_type.to_string()
            };
            lines.push(format!("    pub {}: {},", key, rust_type));
        }
        lines.push("}".to_string());
        lines.join("\n")
    }

    fn generate_typescript(name: &str, fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let mut lines = vec![format!("export interface {} {{", name)];
        for (key, info) in fields {
            let ts_type = match info.field_type.as_str() {
                "date" => "Date",
                "object" => "any",
                other => other,
            };
            let optional = if info.nullable { "?" } else { "" };
            let null_union = if info.nullable { " | null" } else { "" };
            lines.push(format!("  {}{}: {}{};", key, optional, ts_type, null_union));
        }
        lines.push("}".to_string());
        lines.join("\n")
    }

    fn generate_zod(fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let mut lines = vec![
            "import { z } from 'zod';".to_string(),
            "".to_string(),
            "export const InferredSchema = z.object({".to_string(),
        ];
        for (key, info) in fields {
            let zod_type = match info.field_type.as_str() {
                "number" => "z.number()",
                "boolean" => "z.boolean()",
                "date" => "z.date()",
                "object" => "z.any()",
                _ => "z.string()",
            };
            let mut line = format!("  {}: {}", key, zod_type);
            if info.nullable {
                line.push_str(".nullable().optional()");
            }
            line.push(',');
            lines.push(line);
        }
        lines.push("});".to_string());
        lines.join("\n")
    }

    fn generate_sql(table_name: &str, fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let mut lines = vec![format!("CREATE TABLE {} (", table_name)];
        let cols: Vec<String> = fields
            .iter()
            .map(|(key, info)| {
                let sql_type = match info.field_type.as_str() {
                    "number" => "DECIMAL(18, 2)",
                    "boolean" => "BOOLEAN",
                    "date" => "TIMESTAMP",
                    "object" => "JSONB",
                    _ => "VARCHAR(255)",
                };
                let not_null = if info.nullable { "" } else { " NOT NULL" };
                format!("  {} {}{}", key, sql_type, not_null)
            })
            .collect();
        lines.push(cols.join(",\n"));
        lines.push(");".to_string());
        lines.join("\n")
    }
}
