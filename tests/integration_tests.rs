use async_trait::async_trait;
use data_genie::{
    BasicFieldTransformer, BatchCallbackWriter, CSVReader, CSVWriter, CallbackWriter, ConsoleWriter, DataGenieError, DataReader, DataRecord, DataWriter,
    FieldFilter, FieldSchema, FieldValidator, FilterExpression, FilteringReader, FixedWidthReader, FixedWidthWriter, GroupByReader, InferSchemaOptions, Job,
    JsonReader, JsonWriter, MapFields, MaskStrategy, MemoryReader, MemorySink, MemorySource, MemoryWriter, MultiWriter, NDJsonReader, NDJsonWriter,
    PIIMaskingTransformer, ParallelWriter, ParallelWriterOptions, PipelineParser, RecordSchema, RemoveDuplicatesReader, RemoveFields, RenameField, Result,
    RetryOptions, RetryingWriter, SQLReader, SQLWriter, SchemaValidatingReader, SchemaValidator, SelectFields, SetCalculatedField, SetField, SharedWriter,
    SqlConnection, SqlDialect, TSVReader, TSVReaderOptions, TSVWriter, TransformingReader, ValidatingReader, XMLReader, XMLReaderOptions, XMLWriter,
    XMLWriterOptions, between, block_on, greater_than, is_empty, is_not_null, is_type, less_than, pattern_match, record, value_match,
};
use indexmap::IndexMap;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[test]
fn test_csv_read_write_and_dlq() {
    block_on(async {
        let csv_content = "id,name\n1,foo\nPOISON\n2,bar";
        let dlq = MemoryWriter::new();
        let output = MemoryWriter::new();

        let reader = CSVReader::new(MemorySource::new(csv_content)).with_dlq(dlq.clone());
        Job::run(reader, output.clone()).await.unwrap();

        let out_records = output.get_records();
        let dlq_records = dlq.get_records();
        assert_eq!(out_records.len(), 2);
        assert_eq!(out_records[0].get("name"), Some(&json!("foo")));
        assert_eq!(out_records[1].get("name"), Some(&json!("bar")));

        assert_eq!(dlq_records.len(), 1);
        assert_eq!(dlq_records[0].get("_type"), Some(&json!("parse_error")));
        assert!(dlq_records[0].get("_error").unwrap().as_str().unwrap().contains("Inconsistent column count"));

        let sink = MemorySink::new();
        let writer = CSVWriter::new(&sink);
        Job::run(MemoryReader::new(out_records), writer).await.unwrap();
        let written_csv = sink.get_text();
        assert_eq!(written_csv, "id,name\n1,foo\n2,bar\n");
    });
}

#[test]
fn test_dlq_propagation_from_validating_reader_to_csv_reader() {
    let csv_content = "id,name\n1,foo\nPOISON\n2,bar";
    let dlq = MemoryWriter::new();
    let output = MemoryWriter::new();

    let reader = ValidatingReader::new(CSVReader::new(MemorySource::new(csv_content))).with_dlq(dlq.clone());

    Job::run_blocking(reader, output.clone()).unwrap();

    assert_eq!(output.get_records().len(), 2);
    assert_eq!(dlq.get_records().len(), 1);
    assert_eq!(dlq.get_records()[0].get("_type"), Some(&json!("parse_error")));
}

#[test]
fn test_json_and_ndjson_dlq_and_streaming() {
    block_on(async {
        let bad_json = r#"[{"id": 1}, {"id": 2}, malformed]"#;
        let dlq = MemoryWriter::new();
        let out = MemoryWriter::new();

        let reader = JsonReader::new(MemorySource::new(bad_json)).with_dlq(dlq.clone());
        Job::run(reader, out.clone()).await.unwrap();
        assert_eq!(out.get_records().len(), 0);
        assert_eq!(dlq.get_records().len(), 1);
        assert_eq!(dlq.get_records()[0].get("_type"), Some(&json!("parse_error")));

        let json_content = r#"[{"id": 1, "name": "foo"}, {"id": 2}]"#;
        let dlq2 = MemoryWriter::new();
        let out2 = MemoryWriter::new();

        let validator = FieldValidator::new("name")
            .add_rule(|v| !v.is_null(), "Name is missing")
            .create_record_validation_rule();

        let reader2 = ValidatingReader::new(JsonReader::new(MemorySource::new(json_content)))
            .add_rule(validator)
            .with_dlq(dlq2.clone());

        Job::run(reader2, out2.clone()).await.unwrap();
        assert_eq!(out2.get_records().len(), 1);
        assert_eq!(dlq2.get_records().len(), 1);
        assert_eq!(dlq2.get_records()[0].get("id"), Some(&json!(2)));
        assert_eq!(dlq2.get_records()[0].get("_errors"), Some(&json!(["Name is missing"])));

        let ndjson_content = "{\"id\": 1}\n{\"id\": 2, malformed}\n{\"id\": 3}";
        let dlq3 = MemoryWriter::new();
        let out3 = MemoryWriter::new();
        let reader3 = NDJsonReader::new(MemorySource::new(ndjson_content)).with_dlq(dlq3.clone());
        Job::run(reader3, out3.clone()).await.unwrap();
        assert_eq!(out3.get_records().len(), 2);
        assert_eq!(dlq3.get_records().len(), 1);
        assert_eq!(dlq3.get_records()[0].get("_type"), Some(&json!("parse_error")));

        let json_sink = MemorySink::new();
        Job::run(MemoryReader::new(out3.get_records()), JsonWriter::new(&json_sink)).await.unwrap();
        let parsed_back: Vec<DataRecord> = serde_json::from_str(&json_sink.get_text()).unwrap();
        assert_eq!(parsed_back.len(), 2);

        let ndjson_sink = MemorySink::new();
        Job::run(MemoryReader::new(out3.get_records()), NDJsonWriter::new(&ndjson_sink)).await.unwrap();
        assert_eq!(ndjson_sink.get_text().lines().count(), 2);
    });
}

#[test]
fn test_comprehensive_iot_etl_pipeline() {
    let csv_data = [
        "timestamp,device_id,reading,status,battery_level",
        "2026-04-30T10:00:00Z,SN-1001,65.5,OK,85",
        "2026-04-30T10:01:00Z,SN-1002,22.1,ERROR,12",
        "2026-04-30T10:02:00Z,INVALID-ID,0,OK,100",
        "2026-04-30T10:03:00Z,SN-1003,null,OK,50",
        "2026-04-30T10:04:00Z,SN-1001,70.2,OK,5",
        "2026-04-30T10:05:00Z,SN-1004,15.0,MAINT,90",
    ]
    .join("\n");

    let reader = CSVReader::new(MemorySource::new(csv_data));

    let mut pattern_filter = FieldFilter::new("device_id");
    pattern_filter.add_shared_rule(pattern_match(r"^SN-\d+$"));
    let filtered_reader = FilteringReader::new(reader).add_rule(pattern_filter.create_record_filter());

    let expr_filter = FilterExpression::new("(status == 'OK' || status == 'ERROR') && reading != 'null'");
    let expr_filtered_reader = FilteringReader::new(filtered_reader).add_rule(expr_filter.create_record_filter());

    let transformed_reader = TransformingReader::new(expr_filtered_reader)
        .add_transform(RenameField::new("device_id", "sensor_id").transform())
        .add_transform(SetCalculatedField::new("alert_level", "battery_level < 20 || status == 'ERROR' ? 'HIGH' : 'NORMAL'").transform())
        .add_transform(SelectFields::new(["timestamp", "sensor_id", "reading", "alert_level"]).transform());

    let output = MemoryWriter::new();
    Job::run_blocking(transformed_reader, output.clone()).unwrap();
    let results = output.get_records();
    assert_eq!(results.len(), 3);

    let sn1002 = results.iter().find(|r| r.get("sensor_id") == Some(&json!("SN-1002"))).unwrap();
    assert_eq!(sn1002.get("alert_level"), Some(&json!("HIGH")));

    let low_battery = results
        .iter()
        .find(|r| r.get("sensor_id") == Some(&json!("SN-1001")) && r.get("reading") == Some(&json!("70.2")))
        .unwrap();
    assert_eq!(low_battery.get("alert_level"), Some(&json!("HIGH")));
}

#[test]
fn test_tsv_and_fixed_width_handlers() {
    let tsv_input = "id\tname\trole\n1\tAlice\tAdmin\n2\tBob\tUser\n";
    let tsv_reader = TSVReader::new(MemorySource::new(tsv_input));
    let tsv_sink = MemorySink::new();
    let tsv_writer = TSVWriter::new(&tsv_sink);
    Job::run_blocking(tsv_reader, tsv_writer).unwrap();
    assert_eq!(tsv_sink.get_text(), tsv_input);

    let fw_sink = MemorySink::new();
    let fw_writer = FixedWidthWriter::new(&fw_sink)
        .with_field_widths([5, 10, 8])
        .set_field_names(["id", "name", "status"])
        .set_field_names_in_first_row(true);

    let records = vec![
        record! { "id" => "101", "name" => "Alice", "status" => "ACTIVE" },
        record! { "id" => "102", "name" => "Bob", "status" => "PENDING" },
    ];
    Job::run_blocking(MemoryReader::new(records), fw_writer).unwrap();

    let fw_text = fw_sink.get_text();
    let fw_reader = FixedWidthReader::new(MemorySource::new(fw_text))
        .set_field_widths([5, 10, 8])
        .set_field_names_in_first_row(true);

    let fw_out = MemoryWriter::new();
    Job::run_blocking(fw_reader, fw_out.clone()).unwrap();
    let parsed_fw = fw_out.get_records();
    assert_eq!(parsed_fw.len(), 2);
    assert_eq!(parsed_fw[0].get("name"), Some(&json!("Alice")));
    assert_eq!(parsed_fw[1].get("status"), Some(&json!("PENDING")));
}

#[test]
fn test_xml_reader_and_writer() {
    let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
<library>
  <ns:book id="101" category="programming">
    <title>The Rust Programming Language</title>
    <author>Steve Klabnik</author>
  </ns:book>
  <ns:book id="102" category="systems">
    <title>Designing Data-Intensive Applications</title>
    <author>Martin Kleppmann</author>
  </ns:book>
</library>"#;

    let mut mapping = IndexMap::new();
    mapping.insert("book_id".to_string(), "@id".to_string());
    mapping.insert("category".to_string(), "@category".to_string());
    mapping.insert("title".to_string(), "title".to_string());
    mapping.insert("author".to_string(), "author".to_string());

    let opts = XMLReaderOptions::new("//book").with_strip_namespaces(true).with_field_mapping(mapping);

    let reader = XMLReader::new(MemorySource::new(xml_data), opts);
    let out = MemoryWriter::new();
    Job::run_blocking(reader, out.clone()).unwrap();

    let records = out.get_records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get("book_id"), Some(&json!("101")));
    assert_eq!(records[0].get("title"), Some(&json!("The Rust Programming Language")));

    let xml_sink = MemorySink::new();
    let xml_writer = XMLWriter::with_options(
        &xml_sink,
        XMLWriterOptions {
            root_tag: "books".to_string(),
            record_tag: "book".to_string(),
            add_declaration: true,
        },
    );
    Job::run_blocking(MemoryReader::new(records), xml_writer).unwrap();
    let written_xml = xml_sink.get_text();
    assert!(written_xml.contains("<books>"));
    assert!(written_xml.contains("<title>The Rust Programming Language</title>"));
}

#[cfg(all(feature = "parquet", feature = "xlsx"))]
#[test]
fn test_parquet_and_xlsx_roundtrip() {
    use data_genie::{ParquetFieldSchema, ParquetReader, ParquetSchema, ParquetWriter, XlsxReader, XlsxWriter};

    let dir = std::env::temp_dir().join(format!("data_genie_test_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let parquet_path = dir.join("users.parquet");
    let xlsx_path = dir.join("users.xlsx");

    let records = vec![
        record! { "id" => 1, "name" => "Alice", "score" => 95.5, "active" => true },
        record! { "id" => 2, "name" => "Bob", "score" => 82.0, "active" => false },
    ];

    let schema = ParquetSchema::new()
        .add_field("id", ParquetFieldSchema::int64())
        .add_field("name", ParquetFieldSchema::utf8())
        .add_field("score", ParquetFieldSchema::double())
        .add_field("active", ParquetFieldSchema::boolean());

    let pq_writer = ParquetWriter::new(&parquet_path).set_schema(schema);
    Job::run_blocking(MemoryReader::new(records.clone()), pq_writer).unwrap();

    let pq_out = MemoryWriter::new();
    Job::run_blocking(ParquetReader::new(&parquet_path), pq_out.clone()).unwrap();
    let pq_records = pq_out.get_records();
    assert_eq!(pq_records.len(), 2);
    assert_eq!(pq_records[0].get("name"), Some(&json!("Alice")));
    assert_eq!(pq_records[1].get("active"), Some(&json!(false)));

    let xlsx_writer = XlsxWriter::new(&xlsx_path);
    Job::run_blocking(MemoryReader::new(records), xlsx_writer).unwrap();

    let xlsx_out = MemoryWriter::new();
    Job::run_blocking(XlsxReader::new(&xlsx_path), xlsx_out.clone()).unwrap();
    let xlsx_records = xlsx_out.get_records();
    assert_eq!(xlsx_records.len(), 2);
    assert_eq!(xlsx_records[0].get("name"), Some(&json!("Alice")));
    assert_eq!(xlsx_records[1].get("id"), Some(&json!(2)));

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(all(feature = "s3", feature = "http"))]
#[test]
fn test_s3_transport_and_http_reader() {
    block_on(async {
        use data_genie::{HttpReader, HttpReaderOptions, HttpSink, HttpSource, S3Sink, S3Source};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let store = S3Source::in_memory();
        let s3_sink = S3Sink::new(Arc::clone(&store), "my-etl-bucket", "exports/users.ndjson");
        let records = vec![record! { "id" => 1, "user" => "pujan" }, record! { "id" => 2, "user" => "genie" }];
        Job::run(MemoryReader::new(records), NDJsonWriter::new(s3_sink)).await.unwrap();

        let s3_source = S3Source::new(Arc::clone(&store), "my-etl-bucket", "exports/users.ndjson");
        let s3_out = MemoryWriter::new();
        Job::run(NDJsonReader::new(s3_source), s3_out.clone()).await.unwrap();
        assert_eq!(s3_out.get_records().len(), 2);
        assert_eq!(s3_out.get_records()[0].get("user"), Some(&json!("pujan")));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server_base = base_url.clone();

        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let base = server_base.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let body = if req.starts_with("GET /api/page1") {
                        serde_json::to_string(&json!({
                            "data": [{"id": 1, "name": "Page1User"}],
                            "next": format!("{}/api/page2", base)
                        }))
                        .unwrap()
                    } else if req.starts_with("GET /api/page2") {
                        serde_json::to_string(&json!({
                            "data": [{"id": 2, "name": "Page2User"}],
                            "next": null
                        }))
                        .unwrap()
                    } else if req.starts_with("GET /download.csv") {
                        "a,b\n10,20\n".to_string()
                    } else {
                        "{}".to_string()
                    };
                    let resp = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let mut http_opts = HttpReaderOptions::default();
        http_opts.next_page_url = Some(Arc::new(|res: &Value, _curr: &str| res.get("next").and_then(|v| v.as_str()).map(String::from)));

        let http_reader = HttpReader::with_options(format!("{}/api/page1", base_url), http_opts);
        let http_out = MemoryWriter::new();
        Job::run(http_reader, http_out.clone()).await.unwrap();
        assert_eq!(http_out.get_records().len(), 2);
        assert_eq!(http_out.get_records()[1].get("name"), Some(&json!("Page2User")));

        let http_sink = HttpSink::new(format!("{}/upload", base_url));
        Job::run(MemoryReader::new(vec![record! { "ok" => true }]), JsonWriter::new(http_sink))
            .await
            .unwrap();

        let http_src = HttpSource::new(format!("{}/download.csv", base_url));
        let csv_from_http = MemoryWriter::new();
        Job::run(CSVReader::new(http_src), csv_from_http.clone()).await.unwrap();
        assert_eq!(csv_from_http.get_records().len(), 1);
    });
}

struct MockSqlConn {
    queries: Mutex<Vec<(String, Vec<Value>)>>,
    rows_to_return: Vec<DataRecord>,
}

impl MockSqlConn {
    fn new(rows_to_return: Vec<DataRecord>) -> Self {
        Self {
            queries: Mutex::new(Vec::new()),
            rows_to_return,
        }
    }

    fn recorded_queries(&self) -> Vec<(String, Vec<Value>)> {
        self.queries.lock().unwrap().clone()
    }
}

#[async_trait]
impl SqlConnection for MockSqlConn {
    async fn query(&self, sql: &str, params: &[Value]) -> Result<Vec<DataRecord>> {
        self.queries.lock().unwrap().push((sql.to_string(), params.to_vec()));
        if sql.contains("OFFSET 2") {
            return Ok(Vec::new());
        }
        Ok(self.rows_to_return.clone())
    }
}

#[test]
fn test_sql_reader_and_writer_dialects() {
    let sample_rows = vec![record! { "id" => 1, "email" => "a@test.com" }, record! { "id" => 2, "email" => "b@test.com" }];
    let conn = Arc::new(MockSqlConn::new(sample_rows.clone()));

    let sql_reader = SQLReader::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "SELECT * FROM users", vec![])
        .set_chunk_size(2)
        .set_order_by("id")
        .set_use_transaction(true);

    let sql_writer = SQLWriter::new(Arc::clone(&conn) as Arc<dyn SqlConnection>, "users_target")
        .set_dialect(SqlDialect::Postgres)
        .set_upsert("id")
        .with_batch_size(2)
        .set_use_transaction(true);

    Job::run_blocking(sql_reader, sql_writer).unwrap();

    let queries = conn.recorded_queries();
    assert!(queries.iter().any(|(q, _)| q.contains("ORDER BY \"id\" LIMIT 2 OFFSET 0")));
    assert!(queries.iter().any(|(q, _)| q.contains(
        "INSERT INTO \"users_target\" (\"id\", \"email\") VALUES ($1, $2), ($3, $4) ON CONFLICT (\"id\") DO UPDATE SET \"email\" = EXCLUDED.\"email\""
    )));
}

#[test]
fn test_transformers_filters_groupby_dedup_pii_and_schema() {
    let sales = vec![
        record! { "dept" => "Eng", "amount" => 100 },
        record! { "dept" => "Eng", "amount" => 200 },
        record! { "dept" => "Sales", "amount" => 150 },
    ];
    let group_reader = GroupByReader::new(MemoryReader::new(sales), "dept")
        .count("count")
        .sum("amount", "total")
        .avg("amount", "average")
        .min("amount", "min_val")
        .max("amount", "max_val");

    let grp_out = MemoryWriter::new();
    Job::run_blocking(group_reader, grp_out.clone()).unwrap();
    let grp_records = grp_out.get_records();
    assert_eq!(grp_records.len(), 2);
    assert_eq!(grp_records[0].get("total"), Some(&json!(300)));
    assert_eq!(grp_records[0].get("average"), Some(&json!(150)));

    let dup_records = vec![
        record! { "id" => 1, "name" => "A" },
        record! { "id" => 1, "name" => "A-dup" },
        record! { "id" => 2, "name" => "B" },
    ];
    let dedup_reader = RemoveDuplicatesReader::new(MemoryReader::new(dup_records), ["id"]);
    let dedup_out = MemoryWriter::new();
    Job::run_blocking(dedup_reader, dedup_out.clone()).unwrap();
    assert_eq!(dedup_out.get_records().len(), 2);

    let raw_users = vec![record! {
        "first" => "Pujan",
        "last" => "Srivastava",
        "email" => "pujan@example.com",
        "card" => "4111222233334444",
        "secret" => "top-secret",
        "age_str" => "29",
        "temp" => "drop_me"
    }];

    let pii = PIIMaskingTransformer::new()
        .mask("email", MaskStrategy::Partial)
        .mask("card", MaskStrategy::Partial)
        .mask("secret", MaskStrategy::Redact);

    let full_name_map = MapFields::new("full_name", ["first", "last"], |args| {
        json!(format!("{} {}", args[0].as_str().unwrap_or(""), args[1].as_str().unwrap_or("")))
    });

    let transformed = TransformingReader::new(MemoryReader::new(raw_users))
        .add_transform(pii.transform())
        .add_transform(full_name_map.transform())
        .add_transform(BasicFieldTransformer::new(["age_str"]).string_to_int())
        .add_transform(SetField::new("verified", true).transform())
        .add_transform(RemoveFields::new(["temp"]).transform());

    let tr_out = MemoryWriter::new();
    Job::run_blocking(transformed, tr_out.clone()).unwrap();
    let r = &tr_out.get_records()[0];
    assert_eq!(r.get("email"), Some(&json!("p****@example.com")));
    assert_eq!(r.get("card"), Some(&json!("****4444")));
    assert_eq!(r.get("secret"), Some(&json!("[REDACTED]")));
    assert_eq!(r.get("full_name"), Some(&json!("Pujan Srivastava")));
    assert_eq!(r.get("age_str"), Some(&json!(29)));
    assert_eq!(r.get("verified"), Some(&json!(true)));
    assert!(!r.contains_key("temp"));

    assert!(is_not_null()(&json!(1)));
    assert!(is_type("number")(&json!(42)));
    assert!(value_match(["A", "B"])(&json!("A")));
    assert!(greater_than(10.0)(&json!(15)));
    assert!(less_than(10.0)(&json!(5)));
    assert!(between(1.0, 5.0)(&json!(3)));
    assert!(is_empty()(&json!("")));

    let schema = RecordSchema::new()
        .field("id", FieldSchema::integer().coerce())
        .field("score", FieldSchema::number().coerce().min(0.0).max(100.0));

    let schema_dlq = MemoryWriter::new();
    let schema_out = MemoryWriter::new();
    let schema_input = vec![record! { "id" => "10", "score" => "88.5" }, record! { "id" => "bad", "score" => "200.0" }];
    let schema_reader = SchemaValidatingReader::new(MemoryReader::new(schema_input), schema).with_dlq(schema_dlq.clone());
    Job::run_blocking(schema_reader, schema_out.clone()).unwrap();
    assert_eq!(schema_out.get_records().len(), 1);
    assert_eq!(schema_out.get_records()[0].get("id"), Some(&json!(10)));
    assert_eq!(schema_dlq.get_records().len(), 1);
}

#[test]
fn test_composite_writers_and_infer_schema_and_declarative_yaml() {
    block_on(async {
        let mem1 = MemoryWriter::new();
        let mem2 = MemoryWriter::new();
        let multi = MultiWriter::new(vec![Box::new(mem1.clone()), Box::new(mem2.clone())]);

        let records = vec![
            record! { "id" => 1, "name" => "One", "active" => true },
            record! { "id" => 2, "name" => "Two", "active" => false },
            record! { "id" => 3, "name" => "Three", "active" => true },
        ];
        Job::run(MemoryReader::new(records.clone()), multi).await.unwrap();
        assert_eq!(mem1.len(), 3);
        assert_eq!(mem2.len(), 3);

        let batches = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches);
        let batch_writer = BatchCallbackWriter::new(2, move |b| {
            batches_clone.lock().unwrap().push(b.len());
        });
        Job::run(MemoryReader::new(records.clone()), batch_writer).await.unwrap();
        assert_eq!(*batches.lock().unwrap(), vec![2, 1]);

        let parallel_count = Arc::new(Mutex::new(0usize));
        let pc_clone = Arc::clone(&parallel_count);
        let parallel_writer = ParallelWriter::from_handler(ParallelWriterOptions { concurrency: 2, batch_size: 2 }, move |chunk| {
            *pc_clone.lock().unwrap() += chunk.len();
            Ok(())
        });
        Job::run(MemoryReader::new(records.clone()), parallel_writer).await.unwrap();
        assert_eq!(*parallel_count.lock().unwrap(), 3);

        struct FlakyWriter {
            attempts: usize,
            inner: MemoryWriter,
        }
        #[async_trait]
        impl DataWriter for FlakyWriter {
            async fn write(&mut self, record: DataRecord) -> Result<()> {
                self.attempts += 1;
                if self.attempts == 1 {
                    return Err(DataGenieError::Other("transient network glitch".to_string()));
                }
                self.inner.write(record).await
            }
            async fn close(&mut self) -> Result<()> {
                self.inner.close().await
            }
        }

        let flaky_mem = MemoryWriter::new();
        let retrying = RetryingWriter::with_options(
            FlakyWriter {
                attempts: 0,
                inner: flaky_mem.clone(),
            },
            RetryOptions {
                max_retries: 2,
                initial_delay_ms: 5,
                backoff_factor: 1,
                circuit_breaker_threshold: 5,
                circuit_reset_timeout_ms: 1000,
            },
        );
        Job::run(MemoryReader::new(records.clone()), retrying).await.unwrap();
        assert_eq!(flaky_mem.len(), 3);

        let cb_count = Arc::new(Mutex::new(0usize));
        let cb_clone = Arc::clone(&cb_count);
        let filtered = FilteringReader::new(MemoryReader::new(records.clone())).add(|r| r.get("active") == Some(&json!(true)));
        Job::run(
            filtered,
            CallbackWriter::new(move |_| {
                *cb_clone.lock().unwrap() += 1;
            }),
        )
        .await
        .unwrap();
        assert_eq!(*cb_count.lock().unwrap(), 2);

        let inferred = Job::infer_schema(
            MemoryReader::new(records),
            InferSchemaOptions {
                sample_size: Some(10),
                table_name: Some("users".to_string()),
                interface_name: Some("UserRecord".to_string()),
            },
        )
        .await
        .unwrap();
        assert!(inferred.rust.contains("pub struct UserRecord"));
        assert!(inferred.typescript.contains("export interface UserRecord"));
        assert!(inferred.zod.contains("z.object"));
        assert!(inferred.sql.contains("CREATE TABLE users"));

        let dir = std::env::temp_dir().join(format!("data_genie_yaml_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let out_json = dir.join("declarative-output.json");
        let yaml = format!(
            r#"
version: '1.0'
job:
  name: "Declarative Test Job"
  showProgress: false
pipeline:
  read:
    type: csv
    path: examples/input/users.csv
  transform:
    - type: type-convert
      fields: [age]
      to: int
    - type: rename
      mapping:
        fname: firstName
        lname: lastName
    - type: filter
      expression: "age > 30"
    - type: select
      fields: [firstName, lastName, email, age]
  write:
    type: json
    path: {}
"#,
            out_json.display()
        );

        let metrics = PipelineParser::run_from_yaml(&yaml).await.unwrap();
        assert!(metrics.record_count > 0);
        assert!(out_json.exists());
        let _ = std::fs::remove_dir_all(&dir);
    });
}

#[test]
fn test_console_and_retrying_writer_circuit_breaker() {
    block_on(async {
        // 1. ConsoleWriter (new, default, write, close, and via Job)
        let mut cw = ConsoleWriter::new();
        cw.write(record! { "id" => 1, "msg" => "hello" }).await.unwrap();
        cw.close().await.unwrap();

        let cw_default = ConsoleWriter::default();
        let metrics = Job::run(MemoryReader::new(vec![record! { "k" => "v" }]), cw_default).await.unwrap();
        assert_eq!(metrics.record_count, 1);

        // 2. RetryingWriter::new with default options succeeding immediately
        let mem = MemoryWriter::new();
        let mut rw_default = RetryingWriter::new(mem.clone());
        rw_default.write(record! { "ok" => true }).await.unwrap();
        rw_default.close().await.unwrap();
        assert_eq!(mem.get_records().len(), 1);

        // 3. RetryingWriter exhausting retries, opening Circuit Breaker, then resetting after timeout
        struct AlwaysFailUntilResetWriter {
            should_fail: Arc<Mutex<bool>>,
            written: Arc<Mutex<usize>>,
        }

        #[async_trait]
        impl DataWriter for AlwaysFailUntilResetWriter {
            async fn write(&mut self, _record: DataRecord) -> Result<()> {
                if *self.should_fail.lock().unwrap() {
                    return Err(DataGenieError::Other("downstream outage".to_string()));
                }
                *self.written.lock().unwrap() += 1;
                Ok(())
            }

            async fn close(&mut self) -> Result<()> {
                Ok(())
            }
        }

        let should_fail = Arc::new(Mutex::new(true));
        let written = Arc::new(Mutex::new(0usize));

        let mut rw = RetryingWriter::with_options(
            AlwaysFailUntilResetWriter {
                should_fail: Arc::clone(&should_fail),
                written: Arc::clone(&written),
            },
            RetryOptions {
                max_retries: 1,
                initial_delay_ms: 1,
                backoff_factor: 1,
                circuit_breaker_threshold: 2,
                circuit_reset_timeout_ms: 25,
            },
        );

        // First failure (consecutive_failures = 1)
        let err1 = rw.write(record! { "id" => 1 }).await.unwrap_err();
        assert!(err1.to_string().contains("Failed to write record after 1 retries"));

        // Second failure reaches threshold=2 and opens the circuit breaker
        let err2 = rw.write(record! { "id" => 2 }).await.unwrap_err();
        assert!(err2.to_string().contains("Failed to write record after 1 retries"));

        // Immediate third call hits OPEN circuit breaker fast-fail
        let err3 = rw.write(record! { "id" => 3 }).await.unwrap_err();
        assert!(matches!(err3, DataGenieError::CircuitBreaker(_)));

        // Wait for circuit_reset_timeout_ms to elapse, then heal downstream
        tokio::time::sleep(std::time::Duration::from_millis(35)).await;
        *should_fail.lock().unwrap() = false;

        // Circuit resets and write succeeds
        rw.write(record! { "id" => 4 }).await.unwrap();
        rw.close().await.unwrap();
        assert_eq!(*written.lock().unwrap(), 1);
    });
}

#[test]
fn test_tsv_reader_options_schema_and_dlq() {
    block_on(async {
        // 1. TSVReader::with_options + headerless mode (has_field_names_in_first_row = false)
        let raw_no_header = "10\tAlice\n20\tBob\n";
        let reader_no_header = TSVReader::with_options(
            MemorySource::new(raw_no_header),
            TSVReaderOptions {
                schema: None,
                has_field_names_in_first_row: Some(false),
            },
        );
        let out_no_header = MemoryWriter::new();
        Job::run(reader_no_header, out_no_header.clone()).await.unwrap();
        let rows = out_no_header.get_records();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get("0"), Some(&json!("10")));
        assert_eq!(rows[0].get("1"), Some(&json!("Alice")));

        // 2. TSVReader with set_field_names_in_first_row, with_schema, set_dlq, and set_ignore_errors
        let schema = RecordSchema::new()
            .field("id", FieldSchema::integer().coerce())
            .field("name", FieldSchema::string().min_length(2));

        let raw_tsv = "id\tname\n1\tValidName\nnot_an_int\tX\n2\tOk\n";
        let dlq = MemoryWriter::new();
        let mut reader = TSVReader::new(MemorySource::new(raw_tsv))
            .set_field_names_in_first_row(true)
            .with_schema(schema);
        reader.set_dlq(SharedWriter::new(dlq.clone()));
        reader.set_ignore_errors(true);

        let out_valid = MemoryWriter::new();
        Job::run(reader, out_valid.clone()).await.unwrap();
        dlq.clone().close().await.unwrap();

        let valid_rows = out_valid.get_records();
        assert_eq!(valid_rows.len(), 2);
        assert_eq!(valid_rows[0].get("id"), Some(&json!(1)));
        assert_eq!(dlq.get_records().len(), 1);
    });
}

#[test]
fn test_schema_validating_reader_comprehensive() {
    block_on(async {
        let schema = RecordSchema::new()
            .strict()
            .field("id", FieldSchema::integer().coerce().min(1.0).max(1000.0))
            .field("score", FieldSchema::number().coerce().min(0.0).max(100.0))
            .field("active", FieldSchema::boolean().coerce())
            .field(
                "username",
                FieldSchema::string()
                    .coerce()
                    .min_length(3)
                    .refine(|v| v.as_str().map(|s| !s.contains(' ')).unwrap_or(false), "cannot contain spaces"),
            )
            .field("nickname", FieldSchema::string().optional().nullable())
            .field("meta", FieldSchema::any());

        // Test direct RecordSchema::parse edge cases
        // Valid record with coercions and extra field stripped by .strict()
        let ok_rec = schema
            .parse(record! {
                "id" => "42",
                "score" => "95.5",
                "active" => "yes",
                "username" => 12345, // coerced to "12345"
                "nickname" => Value::Null,
                "meta" => json!({"k": 1}),
                "extra_unwanted" => "removed_by_strict"
            })
            .unwrap();
        assert_eq!(ok_rec.get("id"), Some(&json!(42)));
        assert_eq!(ok_rec.get("score"), Some(&json!(95.5)));
        assert_eq!(ok_rec.get("active"), Some(&json!(true)));
        assert_eq!(ok_rec.get("username"), Some(&json!("12345")));
        assert_eq!(ok_rec.get("nickname"), Some(&Value::Null));
        assert!(!ok_rec.contains_key("extra_unwanted"));

        // Boolean false coercions ("false", "0", "no") and native Bool
        for f_val in [json!(false), json!("false"), json!("0"), json!("no")] {
            let r = schema
                .parse(record! {
                    "id" => 1,
                    "score" => 10.0,
                    "active" => f_val,
                    "username" => "alice"
                })
                .unwrap();
            assert_eq!(r.get("active"), Some(&json!(false)));
        }

        // Failure cases covering every branch in FieldSchema::validate_value
        // 1. Missing required field
        assert!(schema.parse(record! { "score" => 10, "active" => true, "username" => "abc" }).is_err());
        // 2. Null on non-nullable required field
        assert!(
            schema
                .parse(record! { "id" => Value::Null, "score" => 10, "active" => true, "username" => "abc" })
                .is_err()
        );
        // 3. Invalid integer coercion & non-coerced type mismatch
        assert!(
            schema
                .parse(record! { "id" => "abc", "score" => 10, "active" => true, "username" => "abc" })
                .is_err()
        );
        assert!(FieldSchema::integer().validate_or_err("id", json!("12")).is_err());
        assert!(FieldSchema::number().validate_or_err("score", json!("12.3")).is_err());
        assert!(FieldSchema::boolean().validate_or_err("active", json!("true")).is_err());
        assert!(FieldSchema::string().validate_or_err("name", json!(123)).is_err());
        // 4. Invalid number & boolean coercions
        assert!(
            schema
                .parse(record! { "id" => 1, "score" => "not_num", "active" => true, "username" => "abc" })
                .is_err()
        );
        assert!(
            schema
                .parse(record! { "id" => 1, "score" => 10, "active" => "maybe", "username" => "abc" })
                .is_err()
        );
        // 5. Below min / above max
        assert!(
            schema
                .parse(record! { "id" => 0, "score" => 10, "active" => true, "username" => "abc" })
                .is_err()
        );
        assert!(
            schema
                .parse(record! { "id" => 1, "score" => 150.0, "active" => true, "username" => "abc" })
                .is_err()
        );
        // 6. Below min_length & custom refine failure
        assert!(
            schema
                .parse(record! { "id" => 1, "score" => 10, "active" => true, "username" => "ab" })
                .is_err()
        );
        assert!(
            schema
                .parse(record! { "id" => 1, "score" => 10, "active" => true, "username" => "bad name" })
                .is_err()
        );

        // Test SchemaValidatingReader::from_arc without DLQ (skips invalid records) and with set_dlq / set_ignore_errors
        let input_records = vec![
            record! { "id" => "10", "score" => "80", "active" => "true", "username" => "valid_user" },
            record! { "id" => "bad", "score" => "80", "active" => "true", "username" => "invalid_user" },
        ];
        let arc_schema: Arc<dyn SchemaValidator> = Arc::new(schema.clone());
        let reader_no_dlq = SchemaValidatingReader::from_arc(MemoryReader::new(input_records.clone()), Arc::clone(&arc_schema));
        let out_no_dlq = MemoryWriter::new();
        Job::run(reader_no_dlq, out_no_dlq.clone()).await.unwrap();
        assert_eq!(out_no_dlq.get_records().len(), 1);

        // With set_dlq and set_ignore_errors + closure SchemaValidator
        let dlq = MemoryWriter::new();
        let mut closure_validator = SchemaValidatingReader::new(MemoryReader::new(input_records), |rec: DataRecord| {
            if rec.get("id") == Some(&json!("bad")) {
                Err(DataGenieError::Validation("bad id".to_string()))
            } else {
                Ok(rec)
            }
        });
        closure_validator.set_dlq(SharedWriter::new(dlq.clone()));
        closure_validator.set_ignore_errors(true);

        let out_with_dlq = MemoryWriter::new();
        Job::run(closure_validator, out_with_dlq.clone()).await.unwrap();
        assert_eq!(out_with_dlq.get_records().len(), 1);
        assert_eq!(dlq.get_records().len(), 1);
        assert!(dlq.get_records()[0].contains_key("_schema_error"));
    });
}

trait FieldSchemaTestExt {
    fn validate_or_err(self, name: &str, v: Value) -> Result<DataRecord>;
}

impl FieldSchemaTestExt for FieldSchema {
    fn validate_or_err(self, name: &str, v: Value) -> Result<DataRecord> {
        let mut r = DataRecord::new();
        r.insert(name.to_string(), v);
        RecordSchema::new().field(name, self).parse(r)
    }
}

struct CustomStaticSource {
    label: &'static str,
    bytes: &'static [u8],
}

#[async_trait]
impl data_genie::DataSource for CustomStaticSource {
    async fn get_reader(&self) -> Result<data_genie::ByteReader> {
        Ok(Box::pin(std::io::Cursor::new(self.bytes)))
    }

    fn name(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed(self.label)
    }
}

struct CustomVecSink {
    output: Arc<Mutex<Vec<u8>>>,
}

#[async_trait]
impl data_genie::DataSink for CustomVecSink {
    async fn get_writer(&self) -> Result<data_genie::ByteWriter> {
        struct VecBufWriter(Arc<Mutex<Vec<u8>>>);
        impl tokio::io::AsyncWrite for VecBufWriter {
            fn poll_write(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>, buf: &[u8]) -> std::task::Poll<std::io::Result<usize>> {
                self.0.lock().unwrap().extend_from_slice(buf);
                std::task::Poll::Ready(Ok(buf.len()))
            }

            fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }

            fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }
        Ok(Box::pin(VecBufWriter(Arc::clone(&self.output))))
    }

    fn name(&self) -> std::borrow::Cow<'_, str> {
        std::borrow::Cow::Borrowed("custom-vec-sink")
    }
}

#[test]
fn test_custom_source_and_sink_ergonomics() {
    block_on(async {
        use data_genie::{DataSource, FileSink, FileSource};

        // 1. Custom DataSource passed directly by value into CSVReader, and custom DataSink passed directly into NDJsonWriter
        let custom_src = CustomStaticSource {
            label: "in-memory-custom",
            bytes: b"name,city\nAlice,Dublin\nBob,London\n",
        };
        assert_eq!(custom_src.name(), "in-memory-custom");
        assert_eq!(custom_src.read_bytes().await.unwrap(), b"name,city\nAlice,Dublin\nBob,London\n");

        let sink_buf = Arc::new(Mutex::new(Vec::new()));
        let custom_sink = CustomVecSink { output: Arc::clone(&sink_buf) };
        Job::run(CSVReader::new(custom_src), NDJsonWriter::new(custom_sink)).await.unwrap();

        let ndjson_text = String::from_utf8(sink_buf.lock().unwrap().clone()).unwrap();
        assert!(ndjson_text.contains("\"Alice\""));
        assert!(ndjson_text.contains("\"London\""));

        // 2. Custom DataSource wrapped in Arc<T> passed directly to NDJsonReader
        let shared_custom = Arc::new(CustomStaticSource {
            label: "shared-custom",
            bytes: b"{\"id\":1}\n{\"id\":2}\n",
        });
        let mem_out = MemoryWriter::new();
        Job::run(NDJsonReader::new(Arc::clone(&shared_custom)), mem_out.clone()).await.unwrap();
        assert_eq!(mem_out.get_records().len(), 2);

        // 3. FileSource / FileSink by reference, name(), and read_bytes()
        let temp_path = std::env::temp_dir().join("data_genie_custom_source_test.csv");
        let file_sink = FileSink::new(&temp_path);
        let file_src = FileSource::new(&temp_path);
        let mem_src = MemorySource::new("k,v\nx,1\ny,2\n");

        Job::run(CSVReader::new(&mem_src), CSVWriter::new(&file_sink)).await.unwrap();
        assert_eq!(file_src.name(), temp_path.to_string_lossy());
        assert!(String::from_utf8(file_src.read_bytes().await.unwrap()).unwrap().contains("x,1"));

        let roundtrip_out = MemoryWriter::new();
        Job::run(CSVReader::new(&file_src), roundtrip_out.clone()).await.unwrap();
        assert_eq!(roundtrip_out.get_records().len(), 2);
        let _ = std::fs::remove_file(&temp_path);
    });
}

#[test]
fn test_stdio_transport_and_cli_piping() {
    use data_genie::{DataSink, DataSource, Logger, SinkInput, SourceInput, StderrLogger, StdinSource, StdoutSink};
    use std::io::Write;
    use std::process::{Command, Stdio};

    // Unit-level API checks for StdinSource, StdoutSink, and StderrLogger
    assert_eq!(StdinSource.name(), "stdin");
    assert_eq!(StdoutSink.name(), "stdout");
    let _src: SourceInput = StdinSource.into();
    let _sink: SinkInput = StdoutSink.into();
    let logger = StderrLogger;
    logger.info("stderr info test");
    logger.warn("stderr warn test");
    logger.error("stderr error test");
    logger.debug("stderr debug test");

    // End-to-end CLI test: stdin ("-") -> stdout ("-") keeps stdout clean and routes logs to stderr
    let dir = std::env::temp_dir().join(format!("data_genie_stdio_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let yaml_path = dir.join("stdio.yaml");
    std::fs::write(
        &yaml_path,
        r#"
job:
  name: "Stdio Pipe Test"
  showProgress: true
pipeline:
  read:
    type: csv
    path: "-"
  transform:
    - type: type-convert
      fields: [amount]
      to: float
    - type: calculate
      field: tier
      expression: "amount >= 500 ? 'GOLD' : 'STANDARD'"
    - type: filter
      expression: "amount > 100"
  write:
    type: ndjson
    path: "-"
"#,
    )
    .unwrap();

    let mut child = Command::new(env!("CARGO_BIN_EXE_data-genie"))
        .args(["run", yaml_path.to_str().unwrap()])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    child.stdin.take().unwrap().write_all(b"id,amount\n1,50\n2,250\n3,600\n").unwrap();

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(output.status.success(), "CLI failed: {}", stderr);

    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 2, "Expected pure NDJSON on stdout, got: {:?}", stdout);
    let rec0: DataRecord = serde_json::from_str(lines[0]).unwrap();
    let rec1: DataRecord = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(rec0.get("id"), Some(&json!("2")));
    assert_eq!(rec0.get("tier"), Some(&json!("STANDARD")));
    assert_eq!(rec1.get("id"), Some(&json!("3")));
    assert_eq!(rec1.get("tier"), Some(&json!("GOLD")));

    assert!(stderr.contains("Running Job: Stdio Pipe Test"));
    assert!(stderr.contains("Job Completed"));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_pipeline_validation_and_tsv_xml_calculate_steps() {
    use data_genie::{PipelineConfig, PipelineSection, ReaderConfig, TransformConfig, WriterConfig};
    use std::process::Command;

    // 1. Aggregated validation via PipelineConfig::validate & PipelineParser::validate_yaml
    let missing_section = PipelineConfig {
        version: None,
        job: None,
        pipeline: None,
    };
    assert!(
        missing_section
            .validate()
            .unwrap_err()
            .to_string()
            .contains("Missing required `pipeline` section")
    );

    let invalid_yaml = r#"
pipeline:
  read:
    type: xml
    path: "   "
    options:
      delimiter: ",,"
      hasHeader: "yes"
  transform:
    - type: filter
      expression: "amount >"
    - type: calculate
    - type: rename
    - type: select
      fields: []
    - type: type-convert
      to: boolean
      fields: []
    - type: pii-masking
      masks:
        email: bogus
    - type: unknown-step
  write:
    type: xml
    options:
      rootTag: "   "
      recordTag: ""
      addDeclaration: "nope"
"#;
    let err = PipelineParser::validate_yaml(invalid_yaml).unwrap_err().to_string();
    for expected in [
        "pipeline.read.path must not be empty",
        "pipeline.read.options.recordPath is required for XML input",
        "pipeline.read.options.delimiter must be one ASCII character",
        "pipeline.read.options.hasFieldNamesInFirstRow must be a boolean",
        "pipeline.transform[0].expression is invalid",
        "pipeline.transform[1].field is required for `calculate`",
        "pipeline.transform[1].expression is required for `calculate`",
        "pipeline.transform[2] (rename) requires a non-empty `mapping`",
        "pipeline.transform[3] (select) requires a non-empty `fields` list",
        "pipeline.transform[4].to must be one of `int`, `float`, or `date`",
        "pipeline.transform[4].fields must be a non-empty list",
        "pipeline.transform[5].masks.email has unsupported strategy `bogus`",
        "pipeline.transform[6] has unsupported transform type `unknown-step`",
        "pipeline.write.path is required for `xml` output",
        "pipeline.write.options.rootTag must be a non-empty string",
        "pipeline.write.options.recordTag must be a non-empty string",
        "pipeline.write.options.addDeclaration must be a boolean",
    ] {
        assert!(err.contains(expected), "Expected validation error to contain {:?}, got:\n{}", expected, err);
    }

    // Also test programmatic PipelineConfig::validate with a valid config
    let valid_cfg = PipelineConfig {
        version: Some("1.0".to_string()),
        job: None,
        pipeline: Some(PipelineSection {
            read: ReaderConfig {
                reader_type: "tsv".to_string(),
                path: "input.tsv".to_string(),
                options: None,
            },
            transform: Some(vec![TransformConfig {
                transform_type: "calculate".to_string(),
                mapping: None,
                expression: Some("qty * 2".to_string()),
                field: Some("double_qty".to_string()),
                fields: None,
                to: None,
                masks: None,
            }]),
            write: WriterConfig {
                writer_type: "console".to_string(),
                path: None,
                options: None,
            },
        }),
    };
    valid_cfg.validate().unwrap();

    // 2. CLI `data-genie validate` & end-to-end declarative TSV -> calculate -> XML -> TSV execution
    let dir = std::env::temp_dir().join(format!("data_genie_val_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let input_tsv = dir.join("input.tsv");
    let mid_xml = dir.join("mid.xml");
    let out_tsv = dir.join("out.tsv");
    let tsv_to_xml_yaml = dir.join("tsv_to_xml.yaml");
    let xml_to_tsv_yaml = dir.join("xml_to_tsv.yaml");
    let bad_yaml_path = dir.join("bad.yaml");

    std::fs::write(&input_tsv, "name\tval\nalpha\t10\nbeta\t25\n").unwrap();
    std::fs::write(
        &tsv_to_xml_yaml,
        format!(
            r#"
pipeline:
  read:
    type: tsv
    path: "{}"
  transform:
    - type: calculate
      field: doubled
      expression: "val * 2"
  write:
    type: xml
    path: "{}"
    options:
      rootTag: items
      recordTag: item
      addDeclaration: true
"#,
            input_tsv.display(),
            mid_xml.display()
        ),
    )
    .unwrap();
    std::fs::write(
        &xml_to_tsv_yaml,
        format!(
            r#"
pipeline:
  read:
    type: xml
    path: "{}"
    options:
      recordPath: "items/item"
  write:
    type: tsv
    path: "{}"
"#,
            mid_xml.display(),
            out_tsv.display()
        ),
    )
    .unwrap();
    std::fs::write(&bad_yaml_path, invalid_yaml).unwrap();

    // validate_file & CLI validate (pass + fail)
    PipelineParser::validate_file(&tsv_to_xml_yaml).unwrap();
    PipelineParser::validate_file(&xml_to_tsv_yaml).unwrap();
    assert!(PipelineParser::validate_file(&bad_yaml_path).is_err());

    let ok_out = Command::new(env!("CARGO_BIN_EXE_data-genie"))
        .args(["validate", tsv_to_xml_yaml.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(ok_out.status.success());
    assert!(String::from_utf8_lossy(&ok_out.stdout).contains("Pipeline configuration is valid."));

    let bad_out = Command::new(env!("CARGO_BIN_EXE_data-genie"))
        .args(["validate", bad_yaml_path.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!bad_out.status.success());
    assert!(String::from_utf8_lossy(&bad_out.stderr).contains("Invalid pipeline configuration"));

    // Run both declarative pipelines
    let m1 = PipelineParser::run_blocking(&tsv_to_xml_yaml).unwrap();
    assert_eq!(m1.record_count, 2);
    let xml_text = std::fs::read_to_string(&mid_xml).unwrap();
    assert!(xml_text.contains("<items>"));
    assert!(xml_text.contains("<doubled>20</doubled>"));
    assert!(xml_text.contains("<doubled>50</doubled>"));

    let m2 = PipelineParser::run_blocking(&xml_to_tsv_yaml).unwrap();
    assert_eq!(m2.record_count, 2);
    let roundtrip = std::fs::read_to_string(&out_tsv).unwrap();
    assert_eq!(roundtrip, "name\tval\tdoubled\nalpha\t10\t20\nbeta\t25\t50\n");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_job_metrics_counts_and_record_bytes() {
    use data_genie::{JobMetrics, JobOptions, RecordStream};

    block_on(async {
        // 1. Skipped counts from FilteringReader + RemoveDuplicatesReader + null records through decorators
        struct NullYieldingReader {
            items: Vec<Option<DataRecord>>,
        }
        impl DataReader for NullYieldingReader {
            fn read(&mut self) -> RecordStream {
                let items = self.items.clone();
                Box::pin(async_stream::try_stream! {
                    for item in items {
                        yield item;
                    }
                })
            }
        }

        let r1 = record! { "id" => "1", "keep" => true };
        let r2 = record! { "id" => "1", "keep" => true }; // duplicate
        let r3 = record! { "id" => "2", "keep" => false }; // filtered out
        let r4 = record! { "id" => "3", "keep" => true };

        let null_reader = NullYieldingReader {
            items: vec![None, Some(r1.clone()), Some(r2), Some(r3), None, Some(r4.clone())],
        };
        let transformed = TransformingReader::new(null_reader);
        let filtered = FilteringReader::new(transformed).add(|r| r.get("keep") == Some(&json!(true)));
        let deduped = RemoveDuplicatesReader::new(filtered, ["id"]);

        let out = MemoryWriter::new();
        let default_metrics = Job::run(deduped, out.clone()).await.unwrap();
        assert_eq!(default_metrics.record_count, 2);
        assert_eq!(default_metrics.skipped_record_count, 4); // 2 nulls + 1 filtered + 1 duplicate
        assert_eq!(default_metrics.dlq_record_count, 0);
        assert_eq!(default_metrics.rejected_record_count, 0);
        assert_eq!(default_metrics.logical_record_bytes_read, 0);
        assert_eq!(default_metrics.logical_record_bytes_written, 0);

        // 2. Opt-in logical JSON record byte metrics
        let expected_bytes = (serde_json::to_vec(&r1).unwrap().len() + serde_json::to_vec(&r4).unwrap().len()) as u64;
        let byte_metrics = Job::run_with_options(
            MemoryReader::new([r1, r4]),
            MemoryWriter::new(),
            JobOptions::new().with_record_byte_metrics(true),
        )
        .await
        .unwrap();
        assert_eq!(byte_metrics.record_count, 2);
        assert_eq!(byte_metrics.logical_record_bytes_read, expected_bytes);
        assert_eq!(byte_metrics.logical_record_bytes_written, expected_bytes);

        // 3. Shared DLQ across CSVReader + ValidatingReader (no double-counting)
        let csv_data = "id,age\n1,30\nMALFORMED_ROW\n2,-5\n3,40\n";
        let dlq = MemoryWriter::new();
        let validator = FieldValidator::new("age")
            .add_rule(|v| v.as_str().and_then(|s| s.parse::<i64>().ok()).is_some_and(|n| n >= 0), "age >= 0")
            .create_record_validation_rule();
        let reader = ValidatingReader::new(CSVReader::new(MemorySource::new(csv_data)))
            .add_rule(validator)
            .with_dlq(dlq.clone());
        let dlq_metrics = Job::run(reader, MemoryWriter::new()).await.unwrap();
        assert_eq!(dlq_metrics.record_count, 2);
        assert_eq!(dlq_metrics.dlq_record_count, 2);
        assert_eq!(dlq_metrics.rejected_record_count, 2);
        assert_eq!(dlq.get_records().len(), 2);

        // 4. SchemaValidatingReader without DLQ counts rejected_record_count while dlq_record_count stays 0
        let schema = RecordSchema::new().field("age", FieldSchema::integer().coerce().min(0.0));
        let schema_reader = SchemaValidatingReader::new(
            MemoryReader::new([record! { "age" => "20" }, record! { "age" => "-1" }, record! { "age" => "bad" }]),
            schema,
        );
        let schema_metrics = Job::run(schema_reader, MemoryWriter::new()).await.unwrap();
        assert_eq!(schema_metrics.record_count, 1);
        assert_eq!(schema_metrics.dlq_record_count, 0);
        assert_eq!(schema_metrics.rejected_record_count, 2);

        // 5. Reader with ignore_errors(true) and no DLQ counts rejected_record_count
        let csv_ignore = CSVReader::new(MemorySource::new("id,name\n1,Alice\nBROKEN\n2,Bob\n")).with_ignore_errors(true);
        let csv_ignore_metrics = Job::run(csv_ignore, MemoryWriter::new()).await.unwrap();
        assert_eq!(csv_ignore_metrics.record_count, 2);
        assert_eq!(csv_ignore_metrics.dlq_record_count, 0);
        assert_eq!(csv_ignore_metrics.rejected_record_count, 1);

        let ndjson_ignore = NDJsonReader::new(MemorySource::new("{\"id\":1}\n{bad}\n{\"id\":2}\n")).with_ignore_errors(true);
        let ndjson_ignore_metrics = Job::run(ndjson_ignore, MemoryWriter::new()).await.unwrap();
        assert_eq!(ndjson_ignore_metrics.record_count, 2);
        assert_eq!(ndjson_ignore_metrics.dlq_record_count, 0);
        assert_eq!(ndjson_ignore_metrics.rejected_record_count, 1);

        // 6. Serde backwards compatibility when new JobMetrics fields are absent
        let legacy_json = r#"{
            "start_time": "2026-01-01T00:00:00Z",
            "end_time": "2026-01-01T00:00:01Z",
            "duration_ms": 1000,
            "record_count": 10,
            "records_per_second": 10.0
        }"#;
        let parsed: JobMetrics = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(parsed.record_count, 10);
        assert_eq!(parsed.skipped_record_count, 0);
        assert_eq!(parsed.dlq_record_count, 0);
        assert_eq!(parsed.rejected_record_count, 0);
        assert_eq!(parsed.logical_record_bytes_read, 0);
        assert_eq!(parsed.logical_record_bytes_written, 0);
    });
}

#[test]
fn test_memory_reader_shared_collection_and_multi_writer_builder() {
    use futures::StreamExt;

    block_on(async {
        // Array input, empty Vec type inference, and repeated reads sharing the stored collection
        let mut reader = MemoryReader::new([
            record! { "name" => "Alice", "city" => "Dublin" },
            record! { "name" => "Bob", "city" => "London" },
        ]);

        let first_pass: Vec<DataRecord> = reader.read().map(|item| item.unwrap().unwrap()).collect().await;
        let second_pass: Vec<DataRecord> = reader.read().map(|item| item.unwrap().unwrap()).collect().await;
        assert_eq!(first_pass.len(), 2);
        assert_eq!(first_pass, second_pass);

        let empty_out = MemoryWriter::new();
        let empty_metrics = Job::run(MemoryReader::new(vec![]), empty_out.clone()).await.unwrap();
        assert_eq!(empty_metrics.record_count, 0);

        // MultiWriter::from_writer(...).add(...) ergonomic builder
        let json_sink = MemorySink::new();
        let csv_sink = MemorySink::new();
        let multi = MultiWriter::from_writer(JsonWriter::new(json_sink.clone())).add(CSVWriter::new(csv_sink.clone()));
        let metrics = Job::run(reader, multi).await.unwrap();
        assert_eq!(metrics.record_count, 2);
        assert!(json_sink.get_text().contains("\"Alice\""));
        assert_eq!(csv_sink.get_text(), "name,city\nAlice,Dublin\nBob,London\n");
    });
}
