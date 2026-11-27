use chrono::{DateTime, Utc};
use futures_util::StreamExt;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::io::{IsTerminal, Write};
use std::sync::Arc;
use std::time::Instant;

use crate::core::error::Result;
use crate::core::traits::{ConsoleLogger, DataReader, DataRecord, DataWriter, Logger};

/// Runs a future to completion on a multi-threaded Tokio runtime without requiring `#[tokio::main]`
/// or a direct `tokio` dependency in downstream crates.
pub fn block_on<F: Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(future)),
            _ => panic!(
                "data_genie::block_on() cannot run an arbitrary non-Send future inside a current_thread Tokio runtime. \
                 Await the async method directly, use a multi-threaded runtime (e.g., #[tokio::test(flavor = \"multi_thread\")]), \
                 or use Job::run_blocking / Job::execute_blocking."
            ),
        }
    } else {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build Tokio runtime")
            .block_on(future)
    }
}

/// Runs a `Send` future to completion from any context, including inside a `current_thread`
/// Tokio runtime (such as default `#[tokio::test]`).
pub fn block_on_send<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        match handle.runtime_flavor() {
            tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(|| handle.block_on(future)),
            _ => std::thread::scope(|scope| {
                scope
                    .spawn(move || {
                        tokio::runtime::Builder::new_multi_thread()
                            .enable_all()
                            .build()
                            .expect("Failed to build Tokio runtime")
                            .block_on(future)
                    })
                    .join()
                    .expect("Background Tokio runtime thread panicked")
            }),
        }
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
    /// Records intentionally dropped by reader decorators, plus null records from the stream.
    #[serde(default)]
    pub skipped_record_count: usize,
    /// Records successfully written to a configured dead-letter queue during this run.
    #[serde(default)]
    pub dlq_record_count: usize,
    /// Records rejected and dropped by validation, or routed to a dead-letter queue.
    #[serde(default)]
    pub rejected_record_count: usize,
    /// Optional JSON-encoded size of records received at the job boundary.
    #[serde(default)]
    pub logical_record_bytes_read: u64,
    /// Optional JSON-encoded size of records sent to the writer.
    #[serde(default)]
    pub logical_record_bytes_written: u64,
}

/// Options for configuring a `Job`.
#[derive(Clone, Default)]
pub struct JobOptions {
    pub logger: Option<Arc<dyn Logger>>,
    pub show_progress: bool,
    pub track_record_bytes: bool,
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

    /// Enables JSON-size accounting for records at the job boundary.
    ///
    /// This is opt-in because computing encoded sizes requires an additional serialization pass.
    pub fn with_record_byte_metrics(mut self, enabled: bool) -> Self {
        self.track_record_bytes = enabled;
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
/// with streaming semantics, lifecycle hooks, and real-time throughput metrics.
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
    pub fn on_error<F: FnMut(&str, Option<&DataRecord>) + Send + 'static>(&mut self, cb: F) -> &mut Self {
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
        let mut null_record_count: usize = 0;
        let skipped_baseline = self.reader.skipped_record_count();
        let dlq_baseline = self.reader.dlq_record_count();
        let rejected_baseline = self.reader.rejected_record_count();
        let mut logical_record_bytes_read = 0u64;
        let mut logical_record_bytes_written = 0u64;

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
                    null_record_count += 1;
                    continue;
                };

                let record_bytes = if self.options.track_record_bytes {
                    Some(serde_json::to_vec(&record)?.len() as u64)
                } else {
                    None
                };
                if let Some(bytes) = record_bytes {
                    logical_record_bytes_read += bytes;
                }

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
                if let Some(bytes) = record_bytes {
                    logical_record_bytes_written += bytes;
                }

                if record_count % 1000 == 0 || (record_count & 255 == 0 && last_update.elapsed().as_millis() > 200) {
                    let now = Utc::now();
                    let duration_ms = start_instant.elapsed().as_millis() as u64;
                    let mut current_metrics = Self::calculate_metrics(start_time, now, duration_ms, record_count);
                    current_metrics.skipped_record_count = null_record_count + self.reader.skipped_record_count().saturating_sub(skipped_baseline);
                    current_metrics.dlq_record_count = self.reader.dlq_record_count().saturating_sub(dlq_baseline);
                    current_metrics.rejected_record_count = self.reader.rejected_record_count().saturating_sub(rejected_baseline);
                    current_metrics.logical_record_bytes_read = logical_record_bytes_read;
                    current_metrics.logical_record_bytes_written = logical_record_bytes_written;

                    for cb in &mut self.on_progress {
                        cb(&current_metrics);
                    }

                    if show_progress {
                        let rps = current_metrics.records_per_second;
                        let elapsed = current_metrics.duration_ms as f64 / 1000.0;
                        print!("\r⏳ Processing: {} records | {:.0} rec/sec | {:.1}s elapsed", record_count, rps, elapsed);
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

        match run_result {
            Ok(()) => {
                if let Err(err) = self.writer.close().await {
                    let msg = err.to_string();
                    for cb in &mut self.on_error {
                        cb(&msg, None);
                    }
                    return Err(err);
                }
            }
            Err(err) => {
                let _ = self.writer.abort().await;
                return Err(err);
            }
        }

        let end_time = Utc::now();
        let duration_ms = start_instant.elapsed().as_millis() as u64;
        let mut metrics = Self::calculate_metrics(start_time, end_time, duration_ms, record_count);
        metrics.skipped_record_count = null_record_count + self.reader.skipped_record_count().saturating_sub(skipped_baseline);
        metrics.dlq_record_count = self.reader.dlq_record_count().saturating_sub(dlq_baseline);
        metrics.rejected_record_count = self.reader.rejected_record_count().saturating_sub(rejected_baseline);
        metrics.logical_record_bytes_read = logical_record_bytes_read;
        metrics.logical_record_bytes_written = logical_record_bytes_written;

        logger.info("\n--- Job Completed ---");
        logger.info(&format!("Processed: {} records", record_count));
        logger.info(&format!("Duration:  {:.2}s", metrics.duration_ms as f64 / 1000.0));
        logger.info(&format!("Throughput: {:.2} rec/sec", metrics.records_per_second));

        for cb in &mut self.on_complete {
            cb(&metrics);
        }

        Ok(metrics)
    }

    /// Synchronous convenience wrapper around `execute()` that manages the async runtime internally.
    /// Allows running jobs from a normal `fn main()` without `#[tokio::main]`.
    pub fn execute_blocking(&mut self) -> Result<JobMetrics> {
        block_on_send(self.execute())
    }

    /// Consumes the `Job` and runs it to completion, returning both the `JobMetrics` and the writer.
    pub async fn run_into_writer(mut self) -> Result<(JobMetrics, W)> {
        let metrics = self.execute().await?;
        Ok((metrics, self.writer))
    }

    fn calculate_metrics(start_time: DateTime<Utc>, end_time: DateTime<Utc>, duration_ms: u64, record_count: usize) -> JobMetrics {
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
            skipped_record_count: 0,
            dlq_record_count: 0,
            rejected_record_count: 0,
            logical_record_bytes_read: 0,
            logical_record_bytes_written: 0,
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
        block_on_send(Self::run(reader, writer))
    }

    /// Static convenience method to run a job with custom `JobOptions`.
    pub async fn run_with_options<R: DataReader, W: DataWriter>(reader: R, writer: W, options: JobOptions) -> Result<JobMetrics> {
        let mut job = Job::with_options(reader, writer, options);
        job.execute().await
    }

    /// Previews the first `limit` records of a stream without writing them to a destination.
    pub async fn preview<R: DataReader>(mut reader: R, limit: Option<usize>, logger: Option<Arc<dyn Logger>>) -> Result<Vec<DataRecord>> {
        let default_logger: Arc<dyn Logger> = Arc::new(ConsoleLogger);
        let log = logger.as_ref().unwrap_or(&default_logger);
        let max_records = limit.unwrap_or(5);
        let mut preview_records = Vec::new();

        log.info(&format!("--- Previewing first {} records ---", max_records));

        if max_records > 0 {
            let mut stream = reader.read();
            while let Some(item) = stream.next().await {
                if let Some(record) = item? {
                    preview_records.push(record);
                    if preview_records.len() >= max_records {
                        break;
                    }
                }
            }
        }

        if preview_records.is_empty() {
            log.warn("No records found to preview.");
        } else {
            for (idx, rec) in preview_records.iter().enumerate() {
                log.info(&format!("[{}] {}", idx, serde_json::to_string(rec).unwrap_or_default()));
            }
            log.info(&format!("--- Preview Complete ({} records shown) ---", preview_records.len()));
        }

        Ok(preview_records)
    }

    /// Samples the first N records from a reader and infers Rust, TypeScript, Zod, and SQL schemas.
    pub async fn infer_schema<R: DataReader>(mut reader: R, options: InferSchemaOptions) -> Result<InferredSchema> {
        let sample_size = options.sample_size.unwrap_or(1000);
        let table_name = options.table_name.unwrap_or_else(|| "inferred_table".to_string());
        let interface_name = options.interface_name.unwrap_or_else(|| "InferredRecord".to_string());

        let mut field_stats: IndexMap<String, (HashSet<&'static str>, bool, usize)> = IndexMap::new();
        let mut count = 0usize;

        if sample_size > 0 {
            let mut stream = reader.read();
            while let Some(item) = stream.next().await {
                let Some(record) = item? else {
                    continue;
                };

                for (key, value) in &record {
                    let entry = field_stats.entry(key.clone()).or_insert_with(|| (HashSet::new(), false, 0));
                    entry.2 += 1;

                    match value {
                        Value::Null => {
                            entry.1 = true;
                        }
                        Value::Bool(_) => {
                            entry.0.insert("boolean");
                        }
                        Value::Number(_) => {
                            entry.0.insert("number");
                        }
                        Value::String(s) => {
                            if chrono::DateTime::parse_from_rfc3339(s).is_ok() {
                                entry.0.insert("date");
                            } else {
                                entry.0.insert("string");
                            }
                        }
                        Value::Array(_) | Value::Object(_) => {
                            entry.0.insert("object");
                        }
                    }
                }

                count += 1;
                if count >= sample_size {
                    break;
                }
            }
        }

        let mut fields = IndexMap::new();
        for (key, (types, explicit_null, seen_count)) in field_stats {
            let nullable = explicit_null || seen_count < count;
            let field_type = match types.len() {
                0 => "string",
                1 => *types.iter().next().unwrap(),
                _ => {
                    // Widen mixed types instead of narrowing to the most specific type.
                    if types.contains("object") { "object" } else { "string" }
                }
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

    fn sanitize_rust_ident(raw: &str, is_type: bool) -> String {
        const RUST_KEYWORDS: &[&str] = &[
            "as", "async", "await", "break", "const", "continue", "crate", "dyn", "else", "enum", "extern", "false", "fn", "for", "if", "impl", "in", "let",
            "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "self", "Self", "static", "struct", "super", "trait", "true", "type", "unsafe",
            "use", "where", "while", "abstract", "become", "box", "do", "final", "macro", "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
        ];

        let mut ident = String::with_capacity(raw.len());
        for ch in raw.chars() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ident.push(ch);
            } else {
                ident.push('_');
            }
        }
        if ident.is_empty() {
            ident = if is_type { "InferredRecord".to_string() } else { "field".to_string() };
        }
        if ident.chars().next().is_some_and(|c| c.is_ascii_digit()) {
            ident.insert(0, '_');
        }
        if RUST_KEYWORDS.contains(&ident.as_str()) {
            ident.push('_');
        }
        ident
    }

    fn is_simple_js_ident(s: &str) -> bool {
        let mut chars = s.chars();
        match chars.next() {
            Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => {}
            _ => return false,
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
    }

    fn format_js_key(key: &str) -> String {
        if Self::is_simple_js_ident(key) {
            key.to_string()
        } else {
            serde_json::to_string(key).unwrap_or_else(|_| format!("\"{}\"", key))
        }
    }

    fn format_sql_ident(name: &str) -> String {
        const SQL_KEYWORDS: &[&str] = &[
            "select",
            "from",
            "where",
            "order",
            "group",
            "table",
            "user",
            "index",
            "key",
            "value",
            "values",
            "insert",
            "update",
            "delete",
            "create",
            "drop",
            "alter",
            "default",
            "check",
            "primary",
            "foreign",
            "references",
            "null",
            "not",
            "and",
            "or",
            "in",
            "as",
            "by",
            "limit",
            "offset",
            "type",
            "status",
            "date",
            "time",
            "timestamp",
        ];
        let is_plain = !name.is_empty()
            && name.chars().next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && !SQL_KEYWORDS.contains(&name.to_ascii_lowercase().as_str());
        if is_plain {
            name.to_string()
        } else {
            format!("\"{}\"", name.replace('"', "\"\""))
        }
    }

    fn generate_rust(name: &str, fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let struct_name = Self::sanitize_rust_ident(name, true);
        let mut lines = vec![
            "#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]".to_string(),
            format!("pub struct {} {{", struct_name),
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
            let field_ident = Self::sanitize_rust_ident(key, false);
            if field_ident != *key {
                let escaped_orig = serde_json::to_string(key).unwrap_or_else(|_| format!("\"{}\"", key));
                lines.push(format!("    #[serde(rename = {})]", escaped_orig));
            }
            lines.push(format!("    pub {}: {},", field_ident, rust_type));
        }
        lines.push("}".to_string());
        lines.join("\n")
    }

    fn generate_typescript(name: &str, fields: &IndexMap<String, FieldSchemaInfo>) -> String {
        let iface_name = Self::sanitize_rust_ident(name, true);
        let mut lines = vec![format!("export interface {} {{", iface_name)];
        for (key, info) in fields {
            let ts_type = match info.field_type.as_str() {
                "date" => "Date",
                "object" => "any",
                other => other,
            };
            let optional = if info.nullable { "?" } else { "" };
            let null_union = if info.nullable { " | null" } else { "" };
            lines.push(format!("  {}{}: {}{};", Self::format_js_key(key), optional, ts_type, null_union));
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
            let mut line = format!("  {}: {}", Self::format_js_key(key), zod_type);
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
        let mut lines = vec![format!("CREATE TABLE {} (", Self::format_sql_ident(table_name))];
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
                format!("  {} {}{}", Self::format_sql_ident(key), sql_type, not_null)
            })
            .collect();
        lines.push(cols.join(",\n"));
        lines.push(");".to_string());
        lines.join("\n")
    }
}
