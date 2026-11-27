use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

use crate::core::error::{DataGenieError, Result};
use crate::core::expr::Expr;
use crate::core::job::{Job, JobMetrics, JobOptions};
use crate::core::traits::{DataReader, DataWriter, RecordStream, SharedWriter};
use crate::core::transport_utils::{SinkInput, SourceInput};
use crate::core::{StderrLogger, StdinSource, StdoutSink};
use crate::filters::{FilterExpression, FilteringReader};
use crate::readers::{CSVReader, CSVReaderOptions, JsonReader, NDJsonReader, TSVReader, TSVReaderOptions, XMLReader, XMLReaderOptions};
use crate::transformers::{
    BasicFieldTransformer, MaskStrategy, PIIMaskingTransformer, RemoveFields, RenameField, SelectFields, SetCalculatedField, TransformingReader,
};
use crate::writers::{CSVWriter, ConsoleWriter, JsonWriter, NDJsonWriter, TSVWriter, XMLWriter, XMLWriterOptions};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReaderConfig {
    #[serde(rename = "type")]
    pub reader_type: String,
    pub path: String,
    pub options: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriterConfig {
    #[serde(rename = "type")]
    pub writer_type: String,
    pub path: Option<String>,
    pub options: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformConfig {
    #[serde(rename = "type")]
    pub transform_type: String,
    pub mapping: Option<IndexMap<String, String>>,
    pub expression: Option<String>,
    pub field: Option<String>,
    pub fields: Option<Vec<String>>,
    pub to: Option<String>,
    pub masks: Option<IndexMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub name: Option<String>,
    #[serde(rename = "showProgress")]
    pub show_progress: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSection {
    pub read: ReaderConfig,
    pub transform: Option<Vec<TransformConfig>>,
    pub write: WriterConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub version: Option<String>,
    pub job: Option<JobConfig>,
    pub pipeline: Option<PipelineSection>,
}

impl PipelineConfig {
    /// Checks the pipeline structure and reports all configuration problems together.
    pub fn validate(&self) -> Result<()> {
        let mut issues = Vec::new();
        let Some(pipeline) = self.pipeline.as_ref() else {
            return Err(DataGenieError::Config("Missing required `pipeline` section".to_string()));
        };

        if pipeline.read.path.trim().is_empty() {
            issues.push("pipeline.read.path must not be empty".to_string());
        }
        match pipeline.read.reader_type.as_str() {
            "csv" | "tsv" | "json" | "ndjson" => {}
            "xml" => {
                let has_record_path = pipeline
                    .read
                    .options
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|options| options.get("recordPath").or_else(|| options.get("record_path")))
                    .and_then(Value::as_str)
                    .is_some_and(|path| !path.trim().is_empty());
                if !has_record_path {
                    issues.push("pipeline.read.options.recordPath is required for XML input".to_string());
                }
            }
            other => issues.push(format!("Unsupported reader type `{other}`")),
        }
        if let Some(options) = &pipeline.read.options {
            if let Value::Object(options) = options {
                if let Some(delimiter) = options.get("delimiter") {
                    match delimiter.as_str() {
                        Some(value) if value.len() == 1 && value.is_ascii() => {}
                        _ => issues.push("pipeline.read.options.delimiter must be one ASCII character".to_string()),
                    }
                }
                if let Some(has_header) = options.get("hasFieldNamesInFirstRow").or_else(|| options.get("hasHeader"))
                    && !has_header.is_boolean()
                {
                    issues.push("pipeline.read.options.hasFieldNamesInFirstRow must be a boolean".to_string());
                }
            } else {
                issues.push("pipeline.read.options must be a mapping".to_string());
            }
        }

        if let Some(transforms) = &pipeline.transform {
            for (index, transform) in transforms.iter().enumerate() {
                let prefix = format!("pipeline.transform[{index}]");
                match transform.transform_type.as_str() {
                    "filter" => match transform.expression.as_deref() {
                        Some(expression) if !expression.trim().is_empty() => {
                            if let Err(error) = Expr::parse(expression) {
                                issues.push(format!("{prefix}.expression is invalid: {error}"));
                            }
                        }
                        _ => issues.push(format!("{prefix} ({}) requires a non-empty `expression`", transform.transform_type)),
                    },
                    "calculate" => {
                        if !matches!(transform.field.as_deref(), Some(field) if !field.trim().is_empty()) {
                            issues.push(format!("{prefix}.field is required for `calculate`"));
                        }
                        match transform.expression.as_deref() {
                            Some(expression) if !expression.trim().is_empty() => {
                                if let Err(error) = Expr::parse(expression) {
                                    issues.push(format!("{prefix}.expression is invalid: {error}"));
                                }
                            }
                            _ => issues.push(format!("{prefix}.expression is required for `calculate`")),
                        }
                    }
                    "rename" => match &transform.mapping {
                        Some(mapping) if !mapping.is_empty() => {}
                        _ => issues.push(format!("{prefix} (rename) requires a non-empty `mapping`")),
                    },
                    "select" | "remove" => match &transform.fields {
                        Some(fields) if !fields.is_empty() => {}
                        _ => issues.push(format!("{prefix} ({}) requires a non-empty `fields` list", transform.transform_type)),
                    },
                    "type-convert" => {
                        if !matches!(transform.to.as_deref(), Some("int" | "float" | "date")) {
                            issues.push(format!("{prefix}.to must be one of `int`, `float`, or `date`"));
                        }
                        if !matches!(&transform.fields, Some(fields) if !fields.is_empty()) {
                            issues.push(format!("{prefix}.fields must be a non-empty list"));
                        }
                    }
                    "pii-masking" => match &transform.masks {
                        Some(masks) if !masks.is_empty() => {
                            for (field, strategy) in masks {
                                if !matches!(strategy.to_lowercase().as_str(), "redact" | "hash" | "partial" | "null") {
                                    issues.push(format!("{prefix}.masks.{field} has unsupported strategy `{strategy}`"));
                                }
                            }
                        }
                        _ => issues.push(format!("{prefix} (pii-masking) requires a non-empty `masks` mapping")),
                    },
                    other => issues.push(format!("{prefix} has unsupported transform type `{other}`")),
                }
            }
        }

        match pipeline.write.writer_type.as_str() {
            "console" => {}
            "csv" | "tsv" | "json" | "ndjson" | "xml" => match pipeline.write.path.as_deref() {
                Some(path) if !path.trim().is_empty() => {}
                _ => issues.push(format!("pipeline.write.path is required for `{}` output", pipeline.write.writer_type)),
            },
            other => issues.push(format!("Unsupported writer type `{other}`")),
        }
        if pipeline.write.writer_type == "xml"
            && let Some(options) = &pipeline.write.options
        {
            match options {
                Value::Object(options) => {
                    for key in ["rootTag", "recordTag"] {
                        if options
                            .get(key)
                            .is_some_and(|value| !value.is_string() || value.as_str().is_some_and(|s| s.trim().is_empty()))
                        {
                            issues.push(format!("pipeline.write.options.{key} must be a non-empty string"));
                        }
                    }
                    if options.get("addDeclaration").is_some_and(|value| !value.is_boolean()) {
                        issues.push("pipeline.write.options.addDeclaration must be a boolean".to_string());
                    }
                }
                _ => issues.push("pipeline.write.options must be a mapping".to_string()),
            }
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(DataGenieError::Config(format!("Invalid pipeline configuration:\n- {}", issues.join("\n- "))))
        }
    }
}

/// Boxed dynamic `DataReader` used when constructing pipelines dynamically from YAML.
pub struct BoxedReader(pub Box<dyn DataReader>);

impl DataReader for BoxedReader {
    fn read(&mut self) -> RecordStream {
        self.0.read()
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.0.set_dlq(writer);
    }

    fn skipped_record_count(&self) -> usize {
        self.0.skipped_record_count()
    }

    fn dlq_record_count(&self) -> usize {
        self.0.dlq_record_count()
    }

    fn rejected_record_count(&self) -> usize {
        self.0.rejected_record_count()
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.0.set_ignore_errors(ignore);
    }
}

/// Parses and executes declarative YAML ETL pipeline definitions.
pub struct PipelineParser;

impl PipelineParser {
    /// Validates a YAML pipeline definition without running it.
    pub fn validate_yaml(yaml_content: &str) -> Result<()> {
        let config: PipelineConfig = serde_yaml_ng::from_str(yaml_content)?;
        config.validate()
    }

    /// Reads and validates a YAML pipeline file without running it.
    pub fn validate_file<P: AsRef<Path>>(config_path: P) -> Result<()> {
        let path = config_path.as_ref();
        let content = std::fs::read_to_string(path)?;
        Self::validate_yaml(&content)
    }

    pub fn run_blocking<P: AsRef<Path>>(config_path: P) -> Result<JobMetrics> {
        let path = config_path.as_ref().to_path_buf();
        crate::core::job::block_on_send(Self::run(path))
    }

    pub async fn run<P: AsRef<Path>>(config_path: P) -> Result<JobMetrics> {
        let path = config_path.as_ref();
        if !path.exists() {
            return Err(DataGenieError::Config(format!("Config file not found: {}", path.display())));
        }

        let content = std::fs::read_to_string(path)?;
        Self::run_from_yaml(&content).await
    }

    pub async fn run_from_yaml(yaml_content: &str) -> Result<JobMetrics> {
        let config: PipelineConfig = serde_yaml_ng::from_str(yaml_content)?;
        config.validate()?;
        let Some(pipeline) = config.pipeline else {
            return Err(DataGenieError::Config("Invalid config: \"pipeline\" section is missing".to_string()));
        };

        let reader = Self::create_reader(&pipeline.read)?;
        let transformed_reader = Self::apply_transforms(reader, pipeline.transform.unwrap_or_default())?;
        let writer = Self::create_writer(&pipeline.write)?;

        let writes_to_stdout = pipeline.write.path.as_deref() == Some("-");
        let show_progress = config.job.as_ref().and_then(|j| j.show_progress).unwrap_or(true) && !writes_to_stdout;

        if let Some(name) = config.job.as_ref().and_then(|j| j.name.as_ref()) {
            if writes_to_stdout {
                eprintln!("Running Job: {}", name);
            } else {
                println!("Running Job: {}", name);
            }
        }

        let mut job_options = JobOptions::new().with_progress(show_progress);
        if writes_to_stdout {
            job_options = job_options.with_logger(StderrLogger);
        }
        let mut job = Job::with_options(transformed_reader, writer, job_options);
        job.execute().await
    }

    fn create_reader(config: &ReaderConfig) -> Result<BoxedReader> {
        let source = if config.path == "-" {
            SourceInput::new(StdinSource)
        } else {
            SourceInput::from(config.path.clone())
        };
        match config.reader_type.as_str() {
            "csv" => {
                let mut opts = CSVReaderOptions::default();
                if let Some(Value::Object(ref map)) = config.options {
                    if let Some(Value::String(delim)) = map.get("delimiter") {
                        opts.delimiter = delim.chars().next();
                    }
                    if let Some(Value::Bool(has_hdr)) = map.get("hasFieldNamesInFirstRow").or_else(|| map.get("hasHeader")) {
                        opts.has_field_names_in_first_row = Some(*has_hdr);
                    }
                }
                Ok(BoxedReader(Box::new(CSVReader::with_options(source, opts))))
            }
            "tsv" => {
                let has_field_names_in_first_row = config
                    .options
                    .as_ref()
                    .and_then(Value::as_object)
                    .and_then(|options| options.get("hasFieldNamesInFirstRow").or_else(|| options.get("hasHeader")))
                    .and_then(Value::as_bool);
                let options = TSVReaderOptions {
                    schema: None,
                    has_field_names_in_first_row,
                };
                Ok(BoxedReader(Box::new(TSVReader::with_options(source, options))))
            }
            "xml" => {
                let options = config.options.as_ref().and_then(Value::as_object);
                let record_path = options
                    .and_then(|options| options.get("recordPath").or_else(|| options.get("record_path")))
                    .and_then(Value::as_str)
                    .unwrap_or("record");
                Ok(BoxedReader(Box::new(XMLReader::new(source, XMLReaderOptions::new(record_path)))))
            }
            "json" => Ok(BoxedReader(Box::new(JsonReader::new(source)))),
            "ndjson" => Ok(BoxedReader(Box::new(NDJsonReader::new(source)))),
            other => Err(DataGenieError::Config(format!("Unsupported reader type: {}", other))),
        }
    }

    fn apply_transforms(mut current_reader: BoxedReader, transforms: Vec<TransformConfig>) -> Result<BoxedReader> {
        for transform in transforms {
            match transform.transform_type.as_str() {
                "filter" => {
                    let Some(expr) = transform.expression else {
                        return Err(DataGenieError::Config("Filter transform requires an \"expression\"".to_string()));
                    };
                    let filter_expr = FilterExpression::new(expr);
                    let filter_reader = FilteringReader::new(current_reader).add_rule(filter_expr.create_record_filter());
                    current_reader = BoxedReader(Box::new(filter_reader));
                }
                "calculate" => {
                    let (Some(field), Some(expression)) = (transform.field, transform.expression) else {
                        return Err(DataGenieError::Config("Calculate transform requires `field` and `expression`".to_string()));
                    };
                    let tr = TransformingReader::new(current_reader).add_transform(SetCalculatedField::new(field, expression).transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                "rename" => {
                    let Some(mapping) = transform.mapping else {
                        return Err(DataGenieError::Config("Rename transform requires a \"mapping\"".to_string()));
                    };
                    let mut tr = TransformingReader::new(current_reader);
                    for (old_name, new_name) in mapping {
                        tr.push_transform(RenameField::new(old_name, new_name).transform());
                    }
                    current_reader = BoxedReader(Box::new(tr));
                }
                "select" => {
                    let Some(fields) = transform.fields else {
                        return Err(DataGenieError::Config("Select transform requires \"fields\"".to_string()));
                    };
                    let tr = TransformingReader::new(current_reader).add_transform(SelectFields::new(fields).transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                "remove" => {
                    let Some(fields) = transform.fields else {
                        return Err(DataGenieError::Config("Remove transform requires \"fields\"".to_string()));
                    };
                    let tr = TransformingReader::new(current_reader).add_transform(RemoveFields::new(fields).transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                "type-convert" => {
                    let (Some(fields), Some(to)) = (transform.fields, transform.to) else {
                        return Err(DataGenieError::Config("Type-convert transform requires \"fields\" and \"to\"".to_string()));
                    };
                    let basic = BasicFieldTransformer::new(fields);
                    let fn_transform = match to.as_str() {
                        "int" => basic.string_to_int(),
                        "float" => basic.string_to_double(),
                        "date" => basic.date_parse(""),
                        other => return Err(DataGenieError::Config(format!("Unsupported type conversion: {}", other))),
                    };
                    let tr = TransformingReader::new(current_reader).add_transform(fn_transform);
                    current_reader = BoxedReader(Box::new(tr));
                }
                "pii-masking" => {
                    let Some(masks) = transform.masks else {
                        return Err(DataGenieError::Config("PII-masking transform requires \"masks\"".to_string()));
                    };
                    let mut pii = PIIMaskingTransformer::new();
                    for (field, strategy_str) in masks {
                        pii.add_mask(field, MaskStrategy::from_str_name(&strategy_str));
                    }
                    let tr = TransformingReader::new(current_reader).add_transform(pii.transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                other => return Err(DataGenieError::Config(format!("Unsupported transform type: {}", other))),
            }
        }

        Ok(current_reader)
    }

    fn create_writer(config: &WriterConfig) -> Result<Box<dyn DataWriter>> {
        match config.writer_type.as_str() {
            "console" => Ok(Box::new(ConsoleWriter::new())),
            "csv" => Ok(Box::new(CSVWriter::new(Self::create_sink(config)?))),
            "tsv" => Ok(Box::new(TSVWriter::new(Self::create_sink(config)?))),
            "json" => Ok(Box::new(JsonWriter::new(Self::create_sink(config)?))),
            "ndjson" => Ok(Box::new(NDJsonWriter::new(Self::create_sink(config)?))),
            "xml" => {
                let mut options = XMLWriterOptions::default();
                if let Some(Value::Object(map)) = &config.options {
                    if let Some(Value::String(root_tag)) = map.get("rootTag") {
                        options.root_tag = root_tag.clone();
                    }
                    if let Some(Value::String(record_tag)) = map.get("recordTag") {
                        options.record_tag = record_tag.clone();
                    }
                    if let Some(Value::Bool(add_declaration)) = map.get("addDeclaration") {
                        options.add_declaration = *add_declaration;
                    }
                }
                Ok(Box::new(XMLWriter::with_options(Self::create_sink(config)?, options)))
            }
            other => Err(DataGenieError::Config(format!("Unsupported writer type: {}", other))),
        }
    }

    fn create_sink(config: &WriterConfig) -> Result<SinkInput> {
        let path = config
            .path
            .as_deref()
            .ok_or_else(|| DataGenieError::Config(format!("{} writer requires a `path`", config.writer_type.to_uppercase())))?;
        if path == "-" {
            Ok(SinkInput::new(StdoutSink))
        } else {
            Ok(SinkInput::from(path))
        }
    }
}
