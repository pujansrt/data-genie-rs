use std::fmt;

/// Unified error type for `data-genie` operations.
#[derive(Debug)]
pub enum DataGenieError {
    Io(std::io::Error),
    Csv(csv::Error),
    Json(serde_json::Error),
    Yaml(serde_yaml::Error),
    Xml(String),
    Parquet(String),
    Xlsx(String),
    Http(String),
    ObjectStore(String),
    Sql(String),
    Parse(String),
    Validation(String),
    Config(String),
    CircuitBreaker(String),
    MemoryLimit(String),
    Other(String),
}

impl fmt::Display for DataGenieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {}", e),
            Self::Csv(e) => write!(f, "CSV error: {}", e),
            Self::Json(e) => write!(f, "JSON error: {}", e),
            Self::Yaml(e) => write!(f, "YAML error: {}", e),
            Self::Xml(e) => write!(f, "XML error: {}", e),
            Self::Parquet(e) => write!(f, "Parquet error: {}", e),
            Self::Xlsx(e) => write!(f, "XLSX error: {}", e),
            Self::Http(e) => write!(f, "HTTP error: {}", e),
            Self::ObjectStore(e) => write!(f, "S3 / ObjectStore error: {}", e),
            Self::Sql(e) => write!(f, "SQL error: {}", e),
            Self::Parse(e) => write!(f, "Parse error: {}", e),
            Self::Validation(e) => write!(f, "Validation error: {}", e),
            Self::Config(e) => write!(f, "Configuration error: {}", e),
            Self::CircuitBreaker(e) => write!(f, "Circuit breaker error: {}", e),
            Self::MemoryLimit(e) => write!(f, "Memory limit exceeded: {}", e),
            Self::Other(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for DataGenieError {}

impl From<std::io::Error> for DataGenieError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<csv::Error> for DataGenieError {
    fn from(err: csv::Error) -> Self {
        Self::Csv(err)
    }
}

impl From<serde_json::Error> for DataGenieError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<serde_yaml::Error> for DataGenieError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::Yaml(err)
    }
}

impl From<quick_xml::Error> for DataGenieError {
    fn from(err: quick_xml::Error) -> Self {
        Self::Xml(err.to_string())
    }
}

#[cfg(feature = "parquet")]
impl From<parquet::errors::ParquetError> for DataGenieError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        Self::Parquet(err.to_string())
    }
}

#[cfg(feature = "parquet")]
impl From<arrow::error::ArrowError> for DataGenieError {
    fn from(err: arrow::error::ArrowError) -> Self {
        Self::Parquet(err.to_string())
    }
}

#[cfg(feature = "xlsx")]
impl From<calamine::Error> for DataGenieError {
    fn from(err: calamine::Error) -> Self {
        Self::Xlsx(err.to_string())
    }
}

#[cfg(feature = "xlsx")]
impl From<calamine::XlsxError> for DataGenieError {
    fn from(err: calamine::XlsxError) -> Self {
        Self::Xlsx(err.to_string())
    }
}

#[cfg(feature = "xlsx")]
impl From<rust_xlsxwriter::XlsxError> for DataGenieError {
    fn from(err: rust_xlsxwriter::XlsxError) -> Self {
        Self::Xlsx(err.to_string())
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for DataGenieError {
    fn from(err: reqwest::Error) -> Self {
        Self::Http(err.to_string())
    }
}

#[cfg(feature = "s3")]
impl From<object_store::Error> for DataGenieError {
    fn from(err: object_store::Error) -> Self {
        Self::ObjectStore(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, DataGenieError>;
