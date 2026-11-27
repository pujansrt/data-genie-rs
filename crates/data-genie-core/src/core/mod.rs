pub mod error;
pub mod expr;
pub mod file_transport;
pub mod job;
pub mod memory_transport;
pub mod stdio_transport;
pub mod traits;
pub mod transport_utils;

pub use error::{DataGenieError, Result};
pub use file_transport::{FileSink, FileSource};
pub use job::{FieldSchemaInfo, InferSchemaOptions, InferredSchema, Job, JobMetrics, JobOptions, block_on};
pub use memory_transport::{MemorySink, MemorySource};
pub use stdio_transport::{StdinSource, StdoutSink};
pub use traits::{
    BufferedByteReader, ByteReader, ByteWriter, ConsoleLogger, DEFAULT_IO_BUFFER_CAPACITY, DataReader, DataRecord, DataSink, DataSource, DataWriter, Logger,
    RecordStream, SharedWriter, SqlConnection, StderrLogger, value_to_f64, value_to_string,
};
pub use transport_utils::{SinkInput, SourceInput, ensure_data_sink, ensure_data_source};
