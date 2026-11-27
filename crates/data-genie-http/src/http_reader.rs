use async_stream::try_stream;
use reqwest::{Client, Method, Url};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::http_transport::default_http_client;
use data_genie_core::core::error::{DataGenieError, Result};
use data_genie_core::core::traits::{DataReader, DataRecord, RecordStream};
use data_genie_core::transformers::SchemaValidator;

const DEFAULT_MAX_PAGES: usize = 10_000;

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
    max_pages: usize,
}

impl HttpReader {
    pub fn new(url: impl Into<String>) -> Self {
        Self::with_options(url, HttpReaderOptions::default())
    }

    pub fn with_options(url: impl Into<String>, options: HttpReaderOptions) -> Self {
        Self {
            url: url.into(),
            options,
            client: default_http_client(),
            max_pages: DEFAULT_MAX_PAGES,
        }
    }

    /// Overrides the underlying `reqwest::Client` (for custom timeouts, proxies, or connection pooling).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    /// Sets the maximum number of paginated requests allowed before aborting with an error.
    pub fn with_max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages.max(1);
        self
    }

    fn is_sensitive_header(name: &str) -> bool {
        name.eq_ignore_ascii_case("authorization") || name.eq_ignore_ascii_case("cookie") || name.eq_ignore_ascii_case("proxy-authorization")
    }

    fn same_origin(a: &Url, b: &Url) -> bool {
        a.scheme() == b.scheme() && a.host_str() == b.host_str() && a.port_or_known_default() == b.port_or_known_default()
    }

    fn resolve_next_url(current_url: &str, next: &str) -> Result<String> {
        if let Ok(abs) = Url::parse(next) {
            return Ok(abs.to_string());
        }
        let base = Url::parse(current_url).map_err(|e| DataGenieError::Http(format!("HttpReader: invalid base URL '{}': {}", current_url, e)))?;
        let joined = base
            .join(next)
            .map_err(|e| DataGenieError::Http(format!("HttpReader: invalid next_page_url '{}': {}", next, e)))?;
        Ok(joined.to_string())
    }

    fn default_extract_records(data: Value) -> Result<Vec<DataRecord>> {
        let arr_opt = match data {
            Value::Array(arr) => Some(arr),
            Value::Object(mut obj) => {
                if let Some(Value::Array(arr)) = obj.remove("data") {
                    Some(arr)
                } else if let Some(Value::Array(arr)) = obj.remove("items") {
                    Some(arr)
                } else {
                    None
                }
            }
            _ => None,
        };

        let Some(arr) = arr_opt else {
            return Err(DataGenieError::Http(
                "HttpReader: Extracted records are not in an array format. Check results_path configuration.".to_string(),
            ));
        };

        let mut records = Vec::with_capacity(arr.len());
        for item in arr {
            if let Value::Object(map) = item {
                let mut rec = DataRecord::with_capacity(map.len());
                for (k, v) in map {
                    rec.insert(k, v);
                }
                records.push(rec);
            } else {
                return Err(DataGenieError::Http("HttpReader: Record in array is not a JSON object.".to_string()));
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
        let max_pages = self.max_pages;

        Box::pin(try_stream! {
            let initial_parsed = Url::parse(&initial_url).ok();
            let method_str = options.method.trim().to_ascii_uppercase();
            let method = Method::from_bytes(method_str.as_bytes()).map_err(|e| {
                DataGenieError::Http(format!("Invalid HTTP method {}: {}", options.method, e))
            })?;

            let mut current_url: Option<String> = Some(initial_url);
            let mut visited_urls: HashSet<String> = HashSet::new();
            let mut pages_fetched: usize = 0;

            while let Some(url) = current_url {
                if !visited_urls.insert(url.clone()) {
                    Err(DataGenieError::Http(format!(
                        "HttpReader: pagination loop detected at URL '{}'",
                        url
                    )))?;
                }
                pages_fetched += 1;
                if pages_fetched > max_pages {
                    Err(DataGenieError::Http(format!(
                        "HttpReader: exceeded max_pages limit ({})",
                        max_pages
                    )))?;
                }

                let is_same_origin = match (&initial_parsed, Url::parse(&url).ok()) {
                    (Some(orig), Some(curr)) => Self::same_origin(orig, &curr),
                    _ => true,
                };

                let mut req = client.request(method.clone(), &url);
                let has_content_type = options.headers.keys().any(|k| k.eq_ignore_ascii_case("content-type"));

                for (k, v) in &options.headers {
                    if !is_same_origin && Self::is_sensitive_header(k) {
                        continue;
                    }
                    req = req.header(k, v);
                }

                if let Some(ref body) = options.body {
                    if has_content_type {
                        let body_bytes = serde_json::to_vec(body)?;
                        req = req.body(body_bytes);
                    } else {
                        req = req.json(body);
                    }
                }

                let response = req.send().await.map_err(|e| DataGenieError::Http(e.to_string()))?;
                let status = response.status();
                if !status.is_success() {
                    let err_body = response.text().await.unwrap_or_else(|_| "No error body".to_string());
                    Err(DataGenieError::Http(format!(
                        "HttpReader: {} {} failed with {}. Body: {}",
                        method_str, url, status.as_u16(), err_body
                    )))?;
                    unreachable!()
                }

                let data: Value = response.json().await.map_err(|e| DataGenieError::Http(e.to_string()))?;
                let next_raw = options.next_page_url.as_ref().and_then(|next_fn| next_fn(&data, &url));

                let records = if let Some(ref extractor) = options.results_path {
                    extractor(&data)?
                } else {
                    Self::default_extract_records(data)?
                };

                for record in records {
                    if let Some(ref validator) = options.schema {
                        yield Some(validator.parse(record)?);
                    } else {
                        yield Some(record);
                    }
                }

                current_url = match next_raw {
                    Some(next) if !next.trim().is_empty() => Some(Self::resolve_next_url(&url, next.trim())?),
                    _ => None,
                };
            }
        })
    }
}
