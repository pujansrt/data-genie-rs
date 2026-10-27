pub mod error;
pub mod expr;
pub mod file_transport;
#[cfg(feature = "http")]
pub mod http_transport;
pub mod interfaces;
pub mod job;
pub mod memory_transport;
pub mod pipeline_parser;
pub mod reader_ext;
#[cfg(feature = "s3")]
pub mod s3_transport;
pub mod transport_utils;

pub use error::{DataGenieError, Result};
pub use file_transport::{FileSink, FileSource};
#[cfg(feature = "http")]
pub use http_transport::{HttpSink, HttpSinkOptions, HttpSource, HttpSourceOptions};
pub use interfaces::{
    value_to_f64, value_to_string, ConsoleLogger, DataReader, DataRecord, DataSink, DataSource,
    DataWriter, Logger, RecordStream, SharedWriter, SqlConnection,
};
pub use job::{
    block_on, FieldSchemaInfo, InferSchemaOptions, InferredSchema, Job, JobMetrics, JobOptions,
};
pub use memory_transport::{MemorySink, MemorySource};
pub use pipeline_parser::{
    BoxedReader, JobConfig, PipelineConfig, PipelineParser, PipelineSection, ReaderConfig,
    TransformConfig, WriterConfig,
};
pub use reader_ext::DataReaderExt;
#[cfg(feature = "s3")]
pub use s3_transport::{S3Sink, S3Source};
pub use transport_utils::{ensure_data_sink, ensure_data_source, SinkInput, SourceInput};
