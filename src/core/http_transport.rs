use async_trait::async_trait;
use futures_util::TryStreamExt;
use reqwest::{Client, Method, Response};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::AsyncBufRead;
use tokio_util::io::StreamReader;

use crate::core::error::{DataGenieError, Result};
use crate::core::memory_transport::SharedVecWriter;
use crate::core::traits::{BufferedByteReader, ByteReader, ByteWriter, DataSink, DataSource};

const DEFAULT_HTTP_CONNECT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 300;

pub(crate) fn default_http_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(DEFAULT_HTTP_CONNECT_TIMEOUT_SECS))
        .timeout(Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS))
        .build()
        .unwrap_or_else(|_| Client::new())
}

fn parse_method(method: Option<&str>, default: &str) -> Result<Method> {
    let method_str = method.unwrap_or(default).trim().to_ascii_uppercase();
    Method::from_bytes(method_str.as_bytes()).map_err(|e| DataGenieError::Http(format!("Invalid HTTP method {}: {}", method_str, e)))
}

#[derive(Debug, Clone, Default)]
pub struct HttpSourceOptions {
    pub method: Option<String>,
    pub headers: HashMap<String, String>,
    pub body: Option<Value>,
}

impl HttpSourceOptions {
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn with_body(mut self, body: Value) -> Self {
        self.body = Some(body);
        self
    }
}

/// HTTP [`DataSource`] that streams response bytes over HTTP/HTTPS.
#[derive(Debug, Clone)]
pub struct HttpSource {
    url: String,
    options: HttpSourceOptions,
    client: Client,
}

impl HttpSource {
    pub fn new<S: Into<String>>(url: S) -> Self {
        Self::with_options(url, HttpSourceOptions::default())
    }

    pub fn with_options<S: Into<String>>(url: S, options: HttpSourceOptions) -> Self {
        Self {
            url: url.into(),
            options,
            client: default_http_client(),
        }
    }

    /// Overrides the underlying `reqwest::Client` (e.g., to share a connection pool or custom timeouts).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }

    async fn send_request(&self) -> Result<Response> {
        let method = parse_method(self.options.method.as_deref(), "GET")?;

        let mut req = self.client.request(method, &self.url);
        for (k, v) in &self.options.headers {
            req = req.header(k, v);
        }
        if let Some(body) = &self.options.body {
            req = req.json(body);
        }

        let response = req.send().await.map_err(|e| DataGenieError::Http(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            return Err(DataGenieError::Http(format!("HttpSource: {} returned {}", self.url, status)));
        }

        Ok(response)
    }

    async fn open_stream_reader(&self) -> Result<impl AsyncBufRead + Send + Unpin + use<>> {
        let response = self.send_request().await?;
        let byte_stream = response.bytes_stream().map_err(std::io::Error::other);
        Ok(StreamReader::new(byte_stream))
    }
}

#[async_trait]
impl DataSource for HttpSource {
    async fn get_reader(&self) -> Result<ByteReader> {
        Ok(Box::pin(self.open_stream_reader().await?))
    }

    async fn get_buf_reader(&self) -> Result<BufferedByteReader> {
        Ok(Box::pin(self.open_stream_reader().await?))
    }

    async fn read_bytes(&self) -> Result<Vec<u8>> {
        let response = self.send_request().await?;
        Ok(response.bytes().await.map_err(|e| DataGenieError::Http(e.to_string()))?.to_vec())
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.url)
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpSinkOptions {
    pub method: Option<String>,
    pub headers: HashMap<String, String>,
}

impl HttpSinkOptions {
    pub fn with_method(mut self, method: impl Into<String>) -> Self {
        self.method = Some(method.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// HTTP [`DataSink`] that uploads written data to an HTTP endpoint upon [`finalize`](DataSink::finalize).
#[derive(Debug, Clone)]
pub struct HttpSink {
    url: String,
    options: HttpSinkOptions,
    buffer: Arc<Mutex<Vec<u8>>>,
    client: Client,
}

impl HttpSink {
    pub fn new<S: Into<String>>(url: S) -> Self {
        Self::with_options(url, HttpSinkOptions::default())
    }

    pub fn with_options<S: Into<String>>(url: S, options: HttpSinkOptions) -> Self {
        Self {
            url: url.into(),
            options,
            buffer: Arc::new(Mutex::new(Vec::new())),
            client: default_http_client(),
        }
    }

    /// Overrides the underlying `reqwest::Client` (e.g., to share a connection pool or custom timeouts).
    pub fn with_client(mut self, client: Client) -> Self {
        self.client = client;
        self
    }
}

#[async_trait]
impl DataSink for HttpSink {
    async fn get_writer(&self) -> Result<ByteWriter> {
        Ok(Box::pin(SharedVecWriter::new(Arc::clone(&self.buffer))))
    }

    async fn finalize(&mut self) -> Result<()> {
        let payload = {
            let guard = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };

        let method = parse_method(self.options.method.as_deref(), "POST")?;

        let mut req = self.client.request(method, &self.url);
        let has_content_type = self.options.headers.keys().any(|k| k.eq_ignore_ascii_case("content-type"));
        if !has_content_type {
            req = req.header("Content-Type", "application/octet-stream");
        }
        for (k, v) in &self.options.headers {
            req = req.header(k, v);
        }

        let response = req.body(payload).send().await.map_err(|e| DataGenieError::Http(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(DataGenieError::Http(format!("HttpSink: {} returned {}. {}", self.url, status, text)));
        }

        let mut guard = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        guard.clear();
        Ok(())
    }

    async fn abort(&mut self) -> Result<()> {
        let mut guard = self.buffer.lock().unwrap_or_else(|e| e.into_inner());
        guard.clear();
        Ok(())
    }

    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.url)
    }
}
