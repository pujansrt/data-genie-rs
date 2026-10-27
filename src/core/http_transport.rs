use async_trait::async_trait;
use futures::TryStreamExt;
use reqwest::{Client, Method};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::StreamReader;

use crate::core::error::{DataGenieError, Result};
use crate::core::interfaces::{DataSink, DataSource};
use crate::core::memory_transport::SharedVecWriter;

#[derive(Debug, Clone, Default)]
pub struct HttpSourceOptions {
    pub method: Option<String>,
    pub headers: HashMap<String, String>,
    pub body: Option<Value>,
}

/// HTTP `DataSource` that streams response bytes over HTTP/HTTPS.
#[derive(Debug, Clone)]
pub struct HttpSource {
    url: String,
    options: HttpSourceOptions,
    client: Client,
}

impl HttpSource {
    pub fn new<S: Into<String>>(url: S) -> Self {
        Self {
            url: url.into(),
            options: HttpSourceOptions::default(),
            client: Client::new(),
        }
    }

    pub fn with_options<S: Into<String>>(url: S, options: HttpSourceOptions) -> Self {
        Self {
            url: url.into(),
            options,
            client: Client::new(),
        }
    }
}

#[async_trait]
impl DataSource for HttpSource {
    async fn get_reader(&self) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>> {
        let method_str = self.options.method.as_deref().unwrap_or("GET");
        let method = Method::from_bytes(method_str.as_bytes()).map_err(|e| {
            DataGenieError::Http(format!("Invalid HTTP method {}: {}", method_str, e))
        })?;

        let mut req = self.client.request(method, &self.url);
        for (k, v) in &self.options.headers {
            req = req.header(k, v);
        }
        if let Some(body) = &self.options.body {
            req = req.json(body);
        }

        let response = req.send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(DataGenieError::Http(format!(
                "HttpSource: {} returned {}",
                self.url, status
            )));
        }

        let byte_stream = response.bytes_stream().map_err(std::io::Error::other);
        let reader = StreamReader::new(byte_stream);
        Ok(Box::pin(reader))
    }

    fn name(&self) -> &str {
        &self.url
    }
}

#[derive(Debug, Clone, Default)]
pub struct HttpSinkOptions {
    pub method: Option<String>,
    pub headers: HashMap<String, String>,
}

/// HTTP `DataSink` that streams or uploads written data to an HTTP endpoint upon `finalize()`.
#[derive(Debug, Clone)]
pub struct HttpSink {
    url: String,
    options: HttpSinkOptions,
    buffer: Arc<Mutex<Vec<u8>>>,
    client: Client,
}

impl HttpSink {
    pub fn new<S: Into<String>>(url: S) -> Self {
        Self {
            url: url.into(),
            options: HttpSinkOptions::default(),
            buffer: Arc::new(Mutex::new(Vec::new())),
            client: Client::new(),
        }
    }

    pub fn with_options<S: Into<String>>(url: S, options: HttpSinkOptions) -> Self {
        Self {
            url: url.into(),
            options,
            buffer: Arc::new(Mutex::new(Vec::new())),
            client: Client::new(),
        }
    }
}

#[async_trait]
impl DataSink for HttpSink {
    async fn get_writer(&self) -> Result<Pin<Box<dyn AsyncWrite + Send + Unpin>>> {
        Ok(Box::pin(SharedVecWriter {
            buffer: Arc::clone(&self.buffer),
        }))
    }

    async fn finalize(&mut self) -> Result<()> {
        let payload = {
            let guard = self.buffer.lock().unwrap();
            guard.clone()
        };

        let method_str = self.options.method.as_deref().unwrap_or("POST");
        let method = Method::from_bytes(method_str.as_bytes()).map_err(|e| {
            DataGenieError::Http(format!("Invalid HTTP method {}: {}", method_str, e))
        })?;

        let mut req = self
            .client
            .request(method, &self.url)
            .header("Content-Type", "application/octet-stream");

        for (k, v) in &self.options.headers {
            req = req.header(k, v);
        }

        let response = req.body(payload).send().await?;
        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            return Err(DataGenieError::Http(format!(
                "HttpSink: {} returned {}. {}",
                self.url, status, text
            )));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        &self.url
    }
}
