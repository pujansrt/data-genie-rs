use async_trait::async_trait;

use crate::core::error::Result;
use crate::core::interfaces::{DataReader, DataRecord, DataWriter};
use crate::core::job::{Job, JobMetrics, JobOptions};
use crate::filters::FilteringReader;
use crate::transformers::{
    RecordTransformation, SchemaValidatingReader, SchemaValidator, TransformingReader,
};
use crate::writers::CallbackWriter;

/// Fluent extension methods available on all `DataReader` implementations
/// (analogous to `BaseReader` in the TypeScript edition).
#[async_trait]
pub trait DataReaderExt: DataReader + Sized + 'static {
    /// Chains a transformation function onto this reader.
    fn transform<F>(self, f: F) -> TransformingReader<Self>
    where
        F: Fn(DataRecord) -> DataRecord + Send + Sync + 'static,
    {
        TransformingReader::new(self).add(f)
    }

    /// Chains a `RecordTransformation` onto this reader.
    fn transform_with(self, transformation: RecordTransformation) -> TransformingReader<Self> {
        TransformingReader::new(self).add_transform(transformation)
    }

    /// Chains a filter predicate onto this reader.
    fn filter<F>(self, predicate: F) -> FilteringReader<Self>
    where
        F: Fn(&DataRecord) -> bool + Send + Sync + 'static,
    {
        FilteringReader::new(self).add(predicate)
    }

    /// Chains a schema validator onto this reader.
    fn validate<S: SchemaValidator + 'static>(self, schema: S) -> SchemaValidatingReader<Self> {
        SchemaValidatingReader::new(self, schema)
    }

    /// Runs a job writing all records from this reader into `writer`.
    async fn write_to<W: DataWriter>(self, writer: W) -> Result<JobMetrics> {
        Job::run(self, writer).await
    }

    /// Runs a job with custom `JobOptions` writing all records into `writer`.
    async fn write_to_with_options<W: DataWriter>(
        self,
        writer: W,
        options: JobOptions,
    ) -> Result<JobMetrics> {
        Job::run_with_options(self, writer, options).await
    }

    /// Runs a job invoking a callback for every record.
    async fn write_to_callback<F>(self, callback: F) -> Result<JobMetrics>
    where
        F: Fn(DataRecord) + Send + Sync + 'static,
    {
        Job::run(self, CallbackWriter::new(callback)).await
    }
}

impl<R: DataReader + Sized + 'static> DataReaderExt for R {}
