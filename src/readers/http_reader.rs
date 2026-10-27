use async_stream::try_stream;
use reqwest::{Client, Method};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{DataReader, DataRecord, RecordStream};
use crate::transformers::SchemaValidator;

pub type ResultsExtractor = Arc<dyn Fn(&Value) -> Result<Vec<DataRecord>> + Send + Sync>;
pub type NextPageExtractor = Arc<dyn Fn(&Value, &str) -> Option<String> + Send + Sync>;

#[derive(Clone)]
pub struct HttpReaderOptions {
    pub method: String,
    pub headers: HashMap<String, String>,
    pub body: Option<Value>,
    pub results_path: Option<ResultsExtractor>,
    pub next_page_url: Option<NextPageExtractor>,
    pub schema: Option<Arc<dyn SchemaValidator>>,
}

impl Default for HttpReaderOptions {
    fn default() -> Self {
        Self {
            method: "GET".to_string(),
            headers: HashMap::new(),
            body: None,
            results_path: None,
            next_page_url: None,
            schema: None,
        }
    }
}

/// REST API reader supporting automatic multi-page pagination, custom authentication headers,
/// and customizable response payload extraction.
pub struct HttpReader {
    url: String,
    options: HttpReaderOptions,
    client: Client,
}

impl HttpReader {
    pub fn new(url: impl Into<String>) -> Self {
        Self::with_options(url, HttpReaderOptions::default())
    }

    pub fn with_options(url: impl Into<String>, options: HttpReaderOptions) -> Self {
        Self {
            url: url.into(),
            options,
            client: Client::new(),
        }
    }

    fn default_extract_records(data: &Value) -> Result<Vec<DataRecord>> {
        let arr_opt = if let Some(arr) = data.as_array() {
            Some(arr)
        } else if let Some(arr) = data.get("data").and_then(|v| v.as_array()) {
            Some(arr)
        } else {
            data.get("items").and_then(|v| v.as_array())
        };

        let Some(arr) = arr_opt else {
            return Err(DataGenieError::Http(
                "HttpReader: Extracted records are not in an array format. Check resultsPath configuration.".to_string(),
            ));
        };

        let mut records = Vec::with_capacity(arr.len());
        for item in arr {
            if let Value::Object(map) = item {
                let mut rec = DataRecord::new();
                for (k, v) in map {
                    rec.insert(k.clone(), v.clone());
                }
                records.push(rec);
            } else {
                return Err(DataGenieError::Http(
                    "HttpReader: Record in array is not a JSON object.".to_string(),
                ));
            }
        }
        Ok(records)
    }
}

impl DataReader for HttpReader {
    fn read(&mut self) -> RecordStream {
        let initial_url = self.url.clone();
        let options = self.options.clone();
        let client = self.client.clone();

        Box::pin(try_stream! {
            let mut current_url: Option<String> = Some(initial_url);

            while let Some(url) = current_url {
                let method = Method::from_bytes(options.method.as_bytes()).map_err(|e| {
                    DataGenieError::Http(format!("Invalid HTTP method {}: {}", options.method, e))
                })?;

                let mut req = client
                    .request(method, &url)
                    .header("Content-Type", "application/json");

                for (k, v) in &options.headers {
                    req = req.header(k, v);
                }

                if let Some(ref body) = options.body {
                    req = req.json(body);
                }

                let response = req.send().await?;
                let status = response.status();
                if !status.is_success() {
                    let err_body = response.text().await.unwrap_or_else(|_| "No error body".to_string());
                    Err(DataGenieError::Http(format!(
                        "HttpReader: {} {} failed with {}. Body: {}",
                        options.method, url, status.as_u16(), err_body
                    )))?;
                    unreachable!()
                }

                let data: Value = response.json().await?;
                let records = if let Some(ref extractor) = options.results_path {
                    extractor(&data)?
                } else {
                    Self::default_extract_records(&data)?
                };

                for record in records {
                    if let Some(ref validator) = options.schema {
                        yield Some(validator.parse(record)?);
                    } else {
                        yield Some(record);
                    }
                }

                current_url = if let Some(ref next_fn) = options.next_page_url {
                    next_fn(&data, &url)
                } else {
                    None
                };
            }
        })
    }
}
