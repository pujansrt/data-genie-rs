# Readers

Readers stream raw data from a file path or `DataSource` into a `RecordStream`.

## `CSVReader`
Streams Comma-Separated Values.
- **Constructor**: `CSVReader::new("path.csv")` or `CSVReader::from_source(source)`
- **Builder Methods**:
  - `.with_delimiter(b',')`
  - `.with_headers(true)`
  - `.with_trim(true)`
  - `.with_dlq(writer)`

## `TSVReader` & `FixedWidthReader`
- **`TSVReader::new("path.tsv")`**: Tab-separated values reader.
- **`FixedWidthReader::new("path.fw", fields)`**: Parses fixed-width columnar text files.

## `JsonReader` & `NDJsonReader`
- **`JsonReader::new("path.json")`**: Incremental streaming JSON array parser (`O(1)` memory).
- **`NDJsonReader::new("path.ndjson")`**: Line-by-line Newline-Delimited JSON reader.

## `XMLReader`
Streams XML records by tag name.
- **Constructor**: `XMLReader::new("path.xml", "record")`

## `ParquetReader` *(feature: `parquet`)*
Streams Apache Parquet columnar files using Arrow record batches.
- **Constructor**: `ParquetReader::new("path.parquet")`

## `XlsxReader` *(feature: `xlsx`)*
Reads Excel `.xlsx` workbooks.
- **Constructor**: `XlsxReader::new("path.xlsx")`
- **Builder Methods**: `.with_sheet("Sheet1")`

## `SQLReader`
Streams rows from a SQL database using paginated queries (`LIMIT` / `OFFSET`) via the `SqlClient` trait.
- **Constructor**: `SQLReader::new(client, "SELECT * FROM users")`
- **Builder Methods**:
  - `.with_chunk_size(1000)`
  - `.with_order_by("id ASC")`

## `HttpReader` *(feature: `http`)*
Streams JSON/NDJSON responses or paginated REST APIs.
- **Constructor**: `HttpReader::new("https://api.example.com/users")`
