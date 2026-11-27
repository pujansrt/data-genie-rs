pub mod core;
pub mod filters;
pub mod readers;
pub mod transformers;
pub mod writers;

#[doc(hidden)]
pub use serde_json;

// Core exports
pub use core::{
    BufferedByteReader, ByteReader, ByteWriter, ConsoleLogger, DEFAULT_IO_BUFFER_CAPACITY, DataGenieError, DataReader, DataRecord, DataSink, DataSource,
    DataWriter, FieldSchemaInfo, FileSink, FileSource, InferSchemaOptions, InferredSchema, Job, JobMetrics, JobOptions, Logger, MemorySink, MemorySource,
    RecordStream, Result, SharedWriter, SinkInput, SourceInput, SqlConnection, StderrLogger, StdinSource, StdoutSink, block_on, ensure_data_sink,
    ensure_data_source, value_to_f64, value_to_string,
};

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
