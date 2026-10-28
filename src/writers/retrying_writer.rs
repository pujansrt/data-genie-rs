use async_trait::async_trait;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use crate::core::error::{DataGenieError, Result};
use crate::core::traits::{DataRecord, DataWriter};

/// Configuration for `RetryingWriter` exponential backoff and circuit breaker.
#[derive(Debug, Clone)]
pub struct RetryOptions {
    pub max_retries: usize,
    pub initial_delay_ms: u64,
    pub backoff_factor: u64,
    pub circuit_breaker_threshold: usize,
    pub circuit_reset_timeout_ms: u64,
}

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay_ms: 1000,
            backoff_factor: 2,
            circuit_breaker_threshold: 5,
            circuit_reset_timeout_ms: 30000,
        }
    }
}

/// Fault-tolerant writer decorator providing exponential backoff retries and a Circuit Breaker.
pub struct RetryingWriter<W: DataWriter> {
    writer: W,
    options: RetryOptions,
    consecutive_failures: usize,
    circuit_open_until: Option<Instant>,
}

impl<W: DataWriter> RetryingWriter<W> {
    pub fn new(writer: W) -> Self {
        Self::with_options(writer, RetryOptions::default())
    }

    pub fn with_options(writer: W, options: RetryOptions) -> Self {
        Self {
            writer,
            options,
            consecutive_failures: 0,
            circuit_open_until: None,
        }
    }

    fn check_circuit(&mut self) -> Result<()> {
        if let Some(reset_at) = self.circuit_open_until {
            if Instant::now() > reset_at {
                self.circuit_open_until = None;
                self.consecutive_failures = 0;
            } else {
                return Err(DataGenieError::CircuitBreaker(
                    "Circuit Breaker is OPEN. Target service is likely down.".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn on_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn on_failure(&mut self) {
        self.consecutive_failures += 1;
        if self.consecutive_failures >= self.options.circuit_breaker_threshold {
            self.circuit_open_until = Some(Instant::now() + Duration::from_millis(self.options.circuit_reset_timeout_ms));
        }
    }
}

#[async_trait]
impl<W: DataWriter> DataWriter for RetryingWriter<W> {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.check_circuit()?;

        let mut attempt = 0;
        let mut delay = self.options.initial_delay_ms;

        loop {
            match self.writer.write(record.clone()).await {
                Ok(()) => {
                    self.on_success();
                    return Ok(());
                }
                Err(err) => {
                    attempt += 1;
                    if attempt > self.options.max_retries {
                        self.on_failure();
                        return Err(DataGenieError::Other(format!(
                            "Failed to write record after {} retries: {}",
                            self.options.max_retries, err
                        )));
                    }
                    sleep(Duration::from_millis(delay)).await;
                    delay = delay.saturating_mul(self.options.backoff_factor);
                }
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.writer.close().await
    }
}
