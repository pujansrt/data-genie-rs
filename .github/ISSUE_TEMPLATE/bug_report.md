---
name: 🐛 Bug Report
about: Create a report to help us improve Data-Genie
title: '[BUG] '
labels: bug
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps or minimal Rust snippet to reproduce the behavior:
1. Configure reader/writer (`CSVReader`, `JsonWriter`, etc.)
2. Run `job.execute_blocking()` or `job.execute().await`
3. See error or unexpected output

**Expected behavior**
A clear and concise description of what you expected to happen.

**Code Snippet / Backtrace**
```rust
// Paste minimal reproduction code or `RUST_BACKTRACE=1` output here
```

**Environment (please complete the following information):**
 - OS: [e.g. macOS, Ubuntu, Windows]
 - Rust version (`rustc --version`): [e.g. 1.85.0]
 - Data-Genie version: [e.g. 2.0.0]
 - Enabled Cargo features: [e.g. default, `parquet`, `xlsx`, `s3`, `http`, `full`]

**Additional context**
Add any other context about the problem here (e.g., dataset size, input/output format, DLQ configuration).
