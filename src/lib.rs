pub mod core {
    pub use data_genie_core::core::*;

    #[cfg(feature = "http")]
    pub use data_genie_http::http_transport;
    #[cfg(feature = "http")]
    pub use data_genie_http::{HttpSink, HttpSinkOptions, HttpSource, HttpSourceOptions};

    #[cfg(feature = "s3")]
    pub use data_genie_s3::s3_transport;
    #[cfg(feature = "s3")]
    pub use data_genie_s3::{S3Sink, S3Source};

    #[cfg(feature = "yaml")]
    pub use data_genie_yaml::pipeline_parser;
    #[cfg(feature = "yaml")]
    pub use data_genie_yaml::{BoxedReader, JobConfig, PipelineConfig, PipelineParser, PipelineSection, ReaderConfig, TransformConfig, WriterConfig};
}

pub mod filters {
    pub use data_genie_core::filters::*;
}

pub mod readers {
    pub use data_genie_core::readers::*;

    #[cfg(feature = "xml")]
    pub use data_genie_xml::xml_reader;
    #[cfg(feature = "xml")]
    pub use data_genie_xml::{XMLReader, XMLReaderOptions};

    #[cfg(feature = "parquet")]
    pub use data_genie_parquet::ParquetReader;
    #[cfg(feature = "parquet")]
    pub use data_genie_parquet::parquet_reader;

    #[cfg(feature = "xlsx")]
    pub use data_genie_xlsx::xlsx_reader;
    #[cfg(feature = "xlsx")]
    pub use data_genie_xlsx::{XlsxReader, XlsxReaderOptions};

    #[cfg(feature = "http")]
    pub use data_genie_http::http_reader;
    #[cfg(feature = "http")]
    pub use data_genie_http::{HttpReader, HttpReaderOptions, NextPageExtractor, ResultsExtractor};
}

pub mod transformers {
    pub use data_genie_core::transformers::*;
}

pub mod writers {
    pub use data_genie_core::writers::*;

    #[cfg(feature = "xml")]
    pub use data_genie_xml::xml_writer;
    #[cfg(feature = "xml")]
    pub use data_genie_xml::{XMLWriter, XMLWriterOptions};

    #[cfg(feature = "parquet")]
    pub use data_genie_parquet::parquet_writer;
    #[cfg(feature = "parquet")]
    pub use data_genie_parquet::{ParquetColumnType, ParquetFieldSchema, ParquetSchema, ParquetWriter};

    #[cfg(feature = "xlsx")]
    pub use data_genie_xlsx::xlsx_writer;
    #[cfg(feature = "xlsx")]
    pub use data_genie_xlsx::{XlsxWriter, XlsxWriterOptions};
}

#[cfg(feature = "s3")]
pub use data_genie_s3 as s3;

#[doc(hidden)]
pub use data_genie_core::serde_json;

pub use data_genie_core::record;

// Core exports
pub use core::{
    BufferedByteReader, ByteReader, ByteWriter, ConsoleLogger, DEFAULT_IO_BUFFER_CAPACITY, DataGenieError, DataReader, DataRecord, DataSink, DataSource,
    DataWriter, FieldSchemaInfo, FileSink, FileSource, InferSchemaOptions, InferredSchema, Job, JobMetrics, JobOptions, Logger, MemorySink, MemorySource,
    RecordStream, Result, SharedWriter, SinkInput, SourceInput, SqlConnection, StderrLogger, StdinSource, StdoutSink, block_on, ensure_data_sink,
    ensure_data_source, value_to_f64, value_to_string,
};

#[cfg(feature = "yaml")]
pub use core::{BoxedReader, JobConfig, PipelineConfig, PipelineParser, PipelineSection, ReaderConfig, TransformConfig, WriterConfig};
#[cfg(feature = "http")]
pub use core::{HttpSink, HttpSinkOptions, HttpSource, HttpSourceOptions};
#[cfg(feature = "s3")]
pub use core::{S3Sink, S3Source};

// Filter exports
pub use filters::{
    FieldFilter, FieldFilterRule, FilterExpression, FilteringReader, RecordFilterRule, between, greater_than, is_empty, is_not_null, is_type, less_than,
    pattern_match, try_pattern_match, value_match,
};

// Reader exports
pub use readers::{
    CSVReader, CSVReaderOptions, FixedWidthReader, FixedWidthReaderOptions, JsonReader, JsonReaderOptions, MemoryReader, NDJsonReader, NDJsonReaderOptions,
    SQLReader, TSVReader, TSVReaderOptions,
};

#[cfg(feature = "parquet")]
pub use readers::ParquetReader;
#[cfg(feature = "http")]
pub use readers::{HttpReader, HttpReaderOptions, NextPageExtractor, ResultsExtractor};
#[cfg(feature = "xml")]
pub use readers::{XMLReader, XMLReaderOptions};
#[cfg(feature = "xlsx")]
pub use readers::{XlsxReader, XlsxReaderOptions};

// Transformer exports
pub use transformers::{
    BasicFieldTransformer, DuplicateKeyStore, FieldSchema, FieldValidator, GroupByReader, MapFields, MaskStrategy, MemoryKeyStore, PIIMaskingTransformer,
    RecordSchema, RecordTransformation, RecordValidationRule, RemoveDuplicatesReader, RemoveFields, RenameField, SchemaFieldType, SchemaValidatingReader,
    SchemaValidator, SelectFields, SetCalculatedField, SetField, TransformingReader, ValidatingReader, ValidationMessage,
};

// Writer exports
pub use writers::{
    BatchCallbackWriter, CSVWriter, CallbackWriter, ConsoleWriter, FixedWidthWriter, JsonWriter, MemoryWriter, MultiWriter, NDJsonWriter, ParallelWriter,
    ParallelWriterOptions, RetryOptions, RetryingWriter, SQLWriter, SqlDialect, TSVWriter,
};

#[cfg(feature = "parquet")]
pub use writers::{ParquetColumnType, ParquetFieldSchema, ParquetSchema, ParquetWriter};
#[cfg(feature = "xml")]
pub use writers::{XMLWriter, XMLWriterOptions};
#[cfg(feature = "xlsx")]
pub use writers::{XlsxWriter, XlsxWriterOptions};
