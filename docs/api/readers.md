# Readers

Readers stream raw data from a file path or `DataSource` into a `RecordStream`.

`StdinSource` reads bytes from process standard input. It can be passed directly to any format reader, for example `CSVReader::new(StdinSource)`. In YAML pipelines, set `read.path: "-"`.

`MemoryReader::new(records)` accepts a `Vec`, array, or other iterator of `DataRecord`s. Its read stream shares the collection and clones one record as it is yielded, avoiding a full collection clone per read.

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
- **`JsonReader::new("path.json")`**: Reads and parses a complete JSON document in memory.
- **`NDJsonReader::new("path.ndjson")`**: Streams Newline-Delimited JSON one line at a time; use it for large JSON record sets.

## `XMLReader`
Reads the XML document into memory, then emits matching records by tag name.
- **Constructor**: `XMLReader::new("path.xml", "record")`

## `ParquetReader` *(feature: `parquet`)*
Streams Apache Parquet columnar rows from local files; non-file `DataSource` inputs are read into memory before parsing.
- **Constructor**: `ParquetReader::new("path.parquet")`

## `XlsxReader` *(feature: `xlsx`)*
Reads the complete Excel `.xlsx` workbook into memory, then yields worksheet rows as records.
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
