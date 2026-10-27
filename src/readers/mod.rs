pub mod csv_reader;
pub mod fixed_width_reader;
#[cfg(feature = "http")]
pub mod http_reader;
pub mod json_reader;
pub mod memory_reader;
pub mod nd_json_reader;
#[cfg(feature = "parquet")]
pub mod parquet_reader;
pub mod sql_reader;
pub mod tsv_reader;
#[cfg(feature = "xlsx")]
pub mod xlsx_reader;
pub mod xml_reader;

pub use csv_reader::{CSVReader, CSVReaderOptions};
pub use fixed_width_reader::{FixedWidthReader, FixedWidthReaderOptions};
#[cfg(feature = "http")]
pub use http_reader::{HttpReader, HttpReaderOptions, NextPageExtractor, ResultsExtractor};
pub use json_reader::{JsonReader, JsonReaderOptions};
pub use memory_reader::MemoryReader;
pub use nd_json_reader::{NDJsonReader, NDJsonReaderOptions};
#[cfg(feature = "parquet")]
pub use parquet_reader::ParquetReader;
pub use sql_reader::SQLReader;
pub use tsv_reader::{TSVReader, TSVReaderOptions};
#[cfg(feature = "xlsx")]
pub use xlsx_reader::{XlsxReader, XlsxReaderOptions};
pub use xml_reader::{XMLReader, XMLReaderOptions};
