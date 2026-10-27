# Cookbook

Welcome to the Data-Genie Rust Cookbook! Here you'll find practical recipes for solving real-world ETL challenges with `data-genie`.

## Featured Recipes

- **[CSV to PostgreSQL](/cookbook/csv-to-postgres)**: Stream large CSV files into PostgreSQL in batches.
- **[Idempotent SQL Sync (Upsert)](/cookbook/sql-upsert)**: Prevent duplicates with dialect-aware SQL upserts.
- **[CSV to S3 (JSON/Parquet)](/cookbook/csv-to-s3)**: Stream local files directly to AWS S3.
- **[S3 to JSON](/cookbook/s3-to-json)**: Stream objects from S3 to local files or another S3 bucket.
- **[CSV to Parquet](/cookbook/csv-to-parquet)**: Convert CSV files into Apache Parquet columnar format.
- **[Filtering Data](/cookbook/filtering)**: Filter streams with rules, regex patterns, and boolean expressions.
- **[Validation & Dead Letter Queue](/cookbook/validation-dlq)**: Divert malformed or invalid records to a DLQ without stopping the job.
- **[PII Masking & Anonymization](/cookbook/pii-masking)**: Redact, hash, or partially mask sensitive fields in-flight.
- **[Parallel Processing](/cookbook/parallel-processing)**: Scale throughput across Tokio worker tasks with `ParallelWriter`.
- **[Multi-Sink Fan-out](/cookbook/multi-sink)**: Broadcast a stream to multiple destinations simultaneously.
- **[Memory & Callbacks](/cookbook/memory-and-callbacks)**: Use in-memory buffers, async callbacks, and job lifecycle hooks.
- **[S3 to Message Queue](/cookbook/s3-to-queue)**: Stream S3 datasets into Kafka, RabbitMQ, or custom brokers.
- **[Real-time Web Dashboard](/cookbook/web-ui-dashboard)**: Stream live ETL progress over Server-Sent Events (SSE).

---

### Have a recipe to share?
Contributions are welcome! Feel free to [open a pull request](https://github.com/pujansrt/data-genie-rs/pulls).
