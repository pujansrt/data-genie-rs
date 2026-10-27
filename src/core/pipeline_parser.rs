use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::Path;

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{DataReader, DataWriter, RecordStream, SharedWriter};
use crate::core::job::{Job, JobMetrics, JobOptions};
use crate::filters::{FilterExpression, FilteringReader};
use crate::readers::{CSVReader, CSVReaderOptions, JsonReader, NDJsonReader};
use crate::transformers::{
    BasicFieldTransformer, MaskStrategy, PIIMaskingTransformer, RemoveFields, RenameField,
    SelectFields, TransformingReader,
};
use crate::writers::{CSVWriter, ConsoleWriter, JsonWriter, NDJsonWriter};

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

/// Boxed dynamic `DataReader` used when constructing pipelines dynamically from YAML.
pub struct BoxedReader(pub Box<dyn DataReader>);

impl DataReader for BoxedReader {
    fn read(&mut self) -> RecordStream {
        self.0.read()
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.0.set_dlq(writer);
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.0.set_ignore_errors(ignore);
    }
}

/// Parses and executes declarative YAML ETL pipeline definitions.
pub struct PipelineParser;

impl PipelineParser {
    pub fn run_blocking<P: AsRef<Path>>(config_path: P) -> Result<JobMetrics> {
        crate::core::job::block_on(Self::run(config_path))
    }

    pub async fn run<P: AsRef<Path>>(config_path: P) -> Result<JobMetrics> {
        let path = config_path.as_ref();
        if !path.exists() {
            return Err(DataGenieError::Config(format!(
                "Config file not found: {}",
                path.display()
            )));
        }

        let content = std::fs::read_to_string(path)?;
        Self::run_from_yaml(&content).await
    }

    pub async fn run_from_yaml(yaml_content: &str) -> Result<JobMetrics> {
        let config: PipelineConfig = serde_yaml::from_str(yaml_content)?;
        let Some(pipeline) = config.pipeline else {
            return Err(DataGenieError::Config(
                "Invalid config: \"pipeline\" section is missing".to_string(),
            ));
        };

        let reader = Self::create_reader(&pipeline.read)?;
        let transformed_reader =
            Self::apply_transforms(reader, pipeline.transform.unwrap_or_default())?;
        let writer = Self::create_writer(&pipeline.write)?;

        let show_progress = config
            .job
            .as_ref()
            .and_then(|j| j.show_progress)
            .unwrap_or(true);

        if let Some(name) = config.job.as_ref().and_then(|j| j.name.as_ref()) {
            println!("Running Job: {}", name);
        }

        let mut job = Job::with_options(
            transformed_reader,
            writer,
            JobOptions::new().with_progress(show_progress),
        );
        job.execute().await
    }

    fn create_reader(config: &ReaderConfig) -> Result<BoxedReader> {
        match config.reader_type.as_str() {
            "csv" => {
                let mut opts = CSVReaderOptions::default();
                if let Some(Value::Object(ref map)) = config.options {
                    if let Some(Value::String(delim)) = map.get("delimiter") {
                        opts.delimiter = delim.chars().next();
                    }
                    if let Some(Value::Bool(has_hdr)) = map.get("hasFieldNamesInFirstRow") {
                        opts.has_field_names_in_first_row = Some(*has_hdr);
                    }
                }
                Ok(BoxedReader(Box::new(CSVReader::with_options(
                    &config.path,
                    opts,
                ))))
            }
            "json" => Ok(BoxedReader(Box::new(JsonReader::new(&config.path)))),
            "ndjson" => Ok(BoxedReader(Box::new(NDJsonReader::new(&config.path)))),
            other => Err(DataGenieError::Config(format!(
                "Unsupported reader type: {}",
                other
            ))),
        }
    }

    fn apply_transforms(
        mut current_reader: BoxedReader,
        transforms: Vec<TransformConfig>,
    ) -> Result<BoxedReader> {
        for transform in transforms {
            match transform.transform_type.as_str() {
                "filter" => {
                    let Some(expr) = transform.expression else {
                        return Err(DataGenieError::Config(
                            "Filter transform requires an \"expression\"".to_string(),
                        ));
                    };
                    let filter_expr = FilterExpression::new(expr);
                    let filter_reader = FilteringReader::new(current_reader)
                        .add_rule(filter_expr.create_record_filter());
                    current_reader = BoxedReader(Box::new(filter_reader));
                }
                "rename" => {
                    let Some(mapping) = transform.mapping else {
                        return Err(DataGenieError::Config(
                            "Rename transform requires a \"mapping\"".to_string(),
                        ));
                    };
                    let mut tr = TransformingReader::new(current_reader);
                    for (old_name, new_name) in mapping {
                        tr.push_transform(RenameField::new(old_name, new_name).transform());
                    }
                    current_reader = BoxedReader(Box::new(tr));
                }
                "select" => {
                    let Some(fields) = transform.fields else {
                        return Err(DataGenieError::Config(
                            "Select transform requires \"fields\"".to_string(),
                        ));
                    };
                    let tr = TransformingReader::new(current_reader)
                        .add_transform(SelectFields::new(fields).transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                "remove" => {
                    let Some(fields) = transform.fields else {
                        return Err(DataGenieError::Config(
                            "Remove transform requires \"fields\"".to_string(),
                        ));
                    };
                    let tr = TransformingReader::new(current_reader)
                        .add_transform(RemoveFields::new(fields).transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                "type-convert" => {
                    let (Some(fields), Some(to)) = (transform.fields, transform.to) else {
                        return Err(DataGenieError::Config(
                            "Type-convert transform requires \"fields\" and \"to\"".to_string(),
                        ));
                    };
                    let basic = BasicFieldTransformer::new(fields);
                    let fn_transform = match to.as_str() {
                        "int" => basic.string_to_int(),
                        "float" => basic.string_to_double(),
                        "date" => basic.date_parse(""),
                        other => {
                            return Err(DataGenieError::Config(format!(
                                "Unsupported type conversion: {}",
                                other
                            )))
                        }
                    };
                    let tr = TransformingReader::new(current_reader).add_transform(fn_transform);
                    current_reader = BoxedReader(Box::new(tr));
                }
                "pii-masking" => {
                    let Some(masks) = transform.masks else {
                        return Err(DataGenieError::Config(
                            "PII-masking transform requires \"masks\"".to_string(),
                        ));
                    };
                    let mut pii = PIIMaskingTransformer::new();
                    for (field, strategy_str) in masks {
                        pii.add_mask(field, MaskStrategy::from_str_name(&strategy_str));
                    }
                    let tr = TransformingReader::new(current_reader).add_transform(pii.transform());
                    current_reader = BoxedReader(Box::new(tr));
                }
                other => {
                    return Err(DataGenieError::Config(format!(
                        "Unsupported transform type: {}",
                        other
                    )))
                }
            }
        }

        Ok(current_reader)
    }

    fn create_writer(config: &WriterConfig) -> Result<Box<dyn DataWriter>> {
        match config.writer_type.as_str() {
            "console" => Ok(Box::new(ConsoleWriter::new())),
            "csv" => {
                let Some(ref path) = config.path else {
                    return Err(DataGenieError::Config(
                        "CSV writer requires a \"path\"".to_string(),
                    ));
                };
                Ok(Box::new(CSVWriter::new(path)))
            }
            "json" => {
                let Some(ref path) = config.path else {
                    return Err(DataGenieError::Config(
                        "JSON writer requires a \"path\"".to_string(),
                    ));
                };
                Ok(Box::new(JsonWriter::new(path)))
            }
            "ndjson" => {
                let Some(ref path) = config.path else {
                    return Err(DataGenieError::Config(
                        "NDJSON writer requires a \"path\"".to_string(),
                    ));
                };
                Ok(Box::new(NDJsonWriter::new(path)))
            }
            other => Err(DataGenieError::Config(format!(
                "Unsupported writer type: {}",
                other
            ))),
        }
    }
}
