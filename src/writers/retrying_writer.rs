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

const MAX_BACKOFF_DELAY_MS: u64 = 60_000;

/// Fault-tolerant writer decorator providing exponential backoff retries and a Circuit Breaker.
///
/// Works best with idempotent per-record writers or retry-safe batched writers (`BatchCallbackWriter`,
/// non-transactional `SQLWriter`). Stateful streaming byte writers (e.g. `CSVWriter`, `JsonWriter`)
/// and transactional `SQLWriter`s cannot replay partial mid-stream writes safely.
pub struct RetryingWriter<W: DataWriter> {
    writer: W,
    options: RetryOptions,
    consecutive_failures: usize,
    circuit_open_until: Option<Instant>,
    half_open: bool,
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
            half_open: false,
        }
    }

    fn is_retryable(err: &DataGenieError) -> bool {
        !matches!(
            err,
            DataGenieError::Config(_) | DataGenieError::Validation(_) | DataGenieError::CircuitBreaker(_)
        )
    }

    fn check_circuit(&mut self) -> Result<()> {
        if let Some(reset_at) = self.circuit_open_until {
            if Instant::now() >= reset_at {
                // Transition to half-open: allow a single probe; if it fails, immediately re-open.
                self.circuit_open_until = None;
                self.half_open = true;
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
        self.half_open = false;
    }

    fn on_failure(&mut self) {
        let threshold = self.options.circuit_breaker_threshold.max(1);
        self.consecutive_failures += 1;
        if self.half_open || self.consecutive_failures >= threshold {
            self.half_open = false;
            self.circuit_open_until = Some(Instant::now() + Duration::from_millis(self.options.circuit_reset_timeout_ms));
        }
    }
}

#[async_trait]
impl<W: DataWriter> DataWriter for RetryingWriter<W> {
    async fn write(&mut self, record: DataRecord) -> Result<()> {
        self.check_circuit()?;

        let mut attempt = 0usize;
        let mut delay = self.options.initial_delay_ms.min(MAX_BACKOFF_DELAY_MS);
        let mut pending = Some(record);

        loop {
            let rec = if attempt >= self.options.max_retries {
                pending.take().unwrap()
            } else {
                pending.as_ref().unwrap().clone()
            };

            match self.writer.write(rec).await {
                Ok(()) => {
                    self.on_success();
                    return Ok(());
                }
                Err(err) => {
                    if !Self::is_retryable(&err) {
                        return Err(err);
                    }
                    attempt += 1;
                    if attempt > self.options.max_retries {
                        self.on_failure();
                        return Err(DataGenieError::Other(format!(
                            "Failed to write record after {} retries: {}",
                            self.options.max_retries, err
                        )));
                    }
                    sleep(Duration::from_millis(delay)).await;
                    delay = delay
                        .saturating_mul(self.options.backoff_factor.max(1))
                        .min(MAX_BACKOFF_DELAY_MS);
                }
            }
        }
    }

    async fn close(&mut self) -> Result<()> {
        self.check_circuit()?;

        let mut attempt = 0usize;
        let mut delay = self.options.initial_delay_ms.min(MAX_BACKOFF_DELAY_MS);

        loop {
            match self.writer.close().await {
                Ok(()) => {
                    self.on_success();
                    return Ok(());
                }
                Err(err) => {
                    if !Self::is_retryable(&err) {
                        return Err(err);
                    }
                    attempt += 1;
                    if attempt > self.options.max_retries {
                        self.on_failure();
                        return Err(DataGenieError::Other(format!(
                            "Failed to close writer after {} retries: {}",
                            self.options.max_retries, err
                        )));
                    }
                    sleep(Duration::from_millis(delay)).await;
                    delay = delay
                        .saturating_mul(self.options.backoff_factor.max(1))
                        .min(MAX_BACKOFF_DELAY_MS);
                }
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.writer.abort().await
    }
}
