---
layout: home

hero:
  name: Data-Genie
  text: High-Performant ETL Engine in Rust
  tagline: A streaming-first ETL engine for Rust, designed for massive datasets with a constant O(1) memory footprint.
  actions:
    - theme: brand
      text: Get Started
      link: /guide/introduction
    - theme: alt
      text: View on GitHub
      link: https://github.com/pujansrt/data-genie-rs
    - theme: alt
      text: Crates.io
      link: https://crates.io/crates/data-genie

features:
  - title: Streaming-First (O(1) Memory)
    details: Process 10GB+ files with constant memory footprint using Tokio async streams (RecordStream) or synchronous blocking wrappers.
  - title: Multi-Format Support
    details: Native support for CSV, TSV, Fixed-Width, JSON, NDJSON, XML, Parquet, Excel (XLSX), and SQL dialects.
  - title: Transport Agnostic
    details: Seamlessly move data between Local Files, AWS S3, HTTP APIs, Databases, and in-memory buffers.
  - title: Built-in Resilience & DLQ
    details: Automatic Dead Letter Queue (DLQ) propagation, schema validation, PII masking, retries, and circuit breakers out of the box.
---

<style>
:root {
  --vp-home-hero-name-color: transparent;
  --vp-home-hero-name-background: -webkit-linear-gradient(120deg, #bd34fe 30%, #41d1ff);
}
</style>
