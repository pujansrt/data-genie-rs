use async_trait::async_trait;
use data_genie::{
    between, block_on, greater_than, is_empty, is_not_null, is_type, less_than, pattern_match,
    record, value_match, BasicFieldTransformer, BatchCallbackWriter, CSVReader, CSVWriter,
    CallbackWriter, DataGenieError, DataReaderExt, DataRecord, DataWriter, FieldFilter,
    FieldSchema, FieldValidator, FilterExpression, FilteringReader, FixedWidthReader,
    FixedWidthWriter, GroupByReader, InferSchemaOptions, Job, JsonReader, JsonWriter, MapFields,
    MaskStrategy, MemoryReader, MemorySink, MemorySource, MemoryWriter, MultiWriter, NDJsonReader,
    NDJsonWriter, PIIMaskingTransformer, ParallelWriter, ParallelWriterOptions, PipelineParser,
    RecordSchema, RemoveDuplicatesReader, RemoveFields, RenameField, Result, RetryOptions,
    RetryingWriter, SQLReader, SQLWriter, SchemaValidatingReader, SelectFields, SetCalculatedField,
    SetField, SqlConnection, SqlDialect, TSVReader, TSVWriter, TransformingReader,
    ValidatingReader, XMLReader, XMLReaderOptions, XMLWriter, XMLWriterOptions,
};
use indexmap::IndexMap;
use serde_json::{json, Value};
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
        assert!(dlq_records[0]
            .get("_error")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("Inconsistent column count"));

        let sink = MemorySink::new();
        let writer = CSVWriter::new(&sink);
        Job::run(MemoryReader::new(out_records), writer)
            .await
            .unwrap();
        let written_csv = sink.get_text();
        assert_eq!(written_csv, "id,name\n1,foo\n2,bar\n");
    });
}

#[test]
fn test_dlq_propagation_from_validating_reader_to_csv_reader() {
    let csv_content = "id,name\n1,foo\nPOISON\n2,bar";
    let dlq = MemoryWriter::new();
    let output = MemoryWriter::new();

    let reader =
        ValidatingReader::new(CSVReader::new(MemorySource::new(csv_content))).with_dlq(dlq.clone());

    Job::run_blocking(reader, output.clone()).unwrap();

    assert_eq!(output.get_records().len(), 2);
    assert_eq!(dlq.get_records().len(), 1);
    assert_eq!(
        dlq.get_records()[0].get("_type"),
        Some(&json!("parse_error"))
    );
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
        assert_eq!(
            dlq.get_records()[0].get("_type"),
            Some(&json!("parse_error"))
        );

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
        assert_eq!(
            dlq2.get_records()[0].get("_errors"),
            Some(&json!(["Name is missing"]))
        );

        let ndjson_content = "{\"id\": 1}\n{\"id\": 2, malformed}\n{\"id\": 3}";
        let dlq3 = MemoryWriter::new();
        let out3 = MemoryWriter::new();
        let reader3 = NDJsonReader::new(MemorySource::new(ndjson_content)).with_dlq(dlq3.clone());
        Job::run(reader3, out3.clone()).await.unwrap();
        assert_eq!(out3.get_records().len(), 2);
        assert_eq!(dlq3.get_records().len(), 1);
        assert_eq!(
            dlq3.get_records()[0].get("_type"),
            Some(&json!("parse_error"))
        );

        let json_sink = MemorySink::new();
        Job::run(
            MemoryReader::new(out3.get_records()),
            JsonWriter::new(&json_sink),
        )
        .await
        .unwrap();
        let parsed_back: Vec<DataRecord> = serde_json::from_str(&json_sink.get_text()).unwrap();
        assert_eq!(parsed_back.len(), 2);

        let ndjson_sink = MemorySink::new();
        Job::run(
            MemoryReader::new(out3.get_records()),
            NDJsonWriter::new(&ndjson_sink),
        )
        .await
        .unwrap();
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
    let filtered_reader =
        FilteringReader::new(reader).add_rule(pattern_filter.create_record_filter());

    let expr_filter =
        FilterExpression::new("(status == 'OK' || status == 'ERROR') && reading != 'null'");
    let expr_filtered_reader =
        FilteringReader::new(filtered_reader).add_rule(expr_filter.create_record_filter());

    let transformed_reader = TransformingReader::new(expr_filtered_reader)
        .add_transform(RenameField::new("device_id", "sensor_id").transform())
        .add_transform(
            SetCalculatedField::new(
                "alert_level",
                "battery_level < 20 || status == 'ERROR' ? 'HIGH' : 'NORMAL'",
            )
            .transform(),
        )
        .add_transform(
            SelectFields::new(["timestamp", "sensor_id", "reading", "alert_level"]).transform(),
        );

    let output = MemoryWriter::new();
    Job::run_blocking(transformed_reader, output.clone()).unwrap();
    let results = output.get_records();
    assert_eq!(results.len(), 3);

    let sn1002 = results
        .iter()
        .find(|r| r.get("sensor_id") == Some(&json!("SN-1002")))
        .unwrap();
    assert_eq!(sn1002.get("alert_level"), Some(&json!("HIGH")));

    let low_battery = results
        .iter()
        .find(|r| {
            r.get("sensor_id") == Some(&json!("SN-1001"))
                && r.get("reading") == Some(&json!("70.2"))
        })
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

    let opts = XMLReaderOptions::new("//book")
        .with_strip_namespaces(true)
        .with_field_mapping(mapping);

    let reader = XMLReader::new(MemorySource::new(xml_data), opts);
    let out = MemoryWriter::new();
    Job::run_blocking(reader, out.clone()).unwrap();

    let records = out.get_records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].get("book_id"), Some(&json!("101")));
    assert_eq!(
        records[0].get("title"),
        Some(&json!("The Rust Programming Language"))
    );

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
    use data_genie::{
        ParquetFieldSchema, ParquetReader, ParquetSchema, ParquetWriter, XlsxReader, XlsxWriter,
    };

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
        let records = vec![
            record! { "id" => 1, "user" => "pujan" },
            record! { "id" => 2, "user" => "genie" },
        ];
        Job::run(MemoryReader::new(records), NDJsonWriter::new(s3_sink))
            .await
            .unwrap();

        let s3_source = S3Source::new(Arc::clone(&store), "my-etl-bucket", "exports/users.ndjson");
        let s3_out = MemoryWriter::new();
        Job::run(NDJsonReader::new(s3_source), s3_out.clone())
            .await
            .unwrap();
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
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let mut http_opts = HttpReaderOptions::default();
        http_opts.next_page_url = Some(Arc::new(|res: &Value, _curr: &str| {
            res.get("next").and_then(|v| v.as_str()).map(String::from)
        }));

        let http_reader = HttpReader::with_options(format!("{}/api/page1", base_url), http_opts);
        let http_out = MemoryWriter::new();
        Job::run(http_reader, http_out.clone()).await.unwrap();
        assert_eq!(http_out.get_records().len(), 2);
        assert_eq!(
            http_out.get_records()[1].get("name"),
            Some(&json!("Page2User"))
        );

        let http_sink = HttpSink::new(format!("{}/upload", base_url));
        Job::run(
            MemoryReader::new(vec![record! { "ok" => true }]),
            JsonWriter::new(http_sink),
        )
        .await
        .unwrap();

        let http_src = HttpSource::new(format!("{}/download.csv", base_url));
        let csv_from_http = MemoryWriter::new();
        Job::run(CSVReader::new(http_src), csv_from_http.clone())
            .await
            .unwrap();
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
        self.queries
            .lock()
            .unwrap()
            .push((sql.to_string(), params.to_vec()));
        if sql.contains("OFFSET 2") {
            return Ok(Vec::new());
        }
        Ok(self.rows_to_return.clone())
    }
}

#[test]
fn test_sql_reader_and_writer_dialects() {
    let sample_rows = vec![
        record! { "id" => 1, "email" => "a@test.com" },
        record! { "id" => 2, "email" => "b@test.com" },
    ];
    let conn = Arc::new(MockSqlConn::new(sample_rows.clone()));

    let sql_reader = SQLReader::new(
        Arc::clone(&conn) as Arc<dyn SqlConnection>,
        "SELECT * FROM users",
        vec![],
    )
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
    assert!(queries
        .iter()
        .any(|(q, _)| q.contains("ORDER BY \"id\" LIMIT 2 OFFSET 0")));
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
        json!(format!(
            "{} {}",
            args[0].as_str().unwrap_or(""),
            args[1].as_str().unwrap_or("")
        ))
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
    let schema_input = vec![
        record! { "id" => "10", "score" => "88.5" },
        record! { "id" => "bad", "score" => "200.0" },
    ];
    let schema_reader = SchemaValidatingReader::new(MemoryReader::new(schema_input), schema)
        .with_dlq(schema_dlq.clone());
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
        Job::run(MemoryReader::new(records.clone()), multi)
            .await
            .unwrap();
        assert_eq!(mem1.len(), 3);
        assert_eq!(mem2.len(), 3);

        let batches = Arc::new(Mutex::new(Vec::new()));
        let batches_clone = Arc::clone(&batches);
        let batch_writer = BatchCallbackWriter::new(2, move |b| {
            batches_clone.lock().unwrap().push(b.len());
        });
        Job::run(MemoryReader::new(records.clone()), batch_writer)
            .await
            .unwrap();
        assert_eq!(*batches.lock().unwrap(), vec![2, 1]);

        let parallel_count = Arc::new(Mutex::new(0usize));
        let pc_clone = Arc::clone(&parallel_count);
        let parallel_writer = ParallelWriter::from_handler(
            ParallelWriterOptions {
                concurrency: 2,
                batch_size: 2,
            },
            move |chunk| {
                *pc_clone.lock().unwrap() += chunk.len();
                Ok(())
            },
        );
        Job::run(MemoryReader::new(records.clone()), parallel_writer)
            .await
            .unwrap();
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
                    return Err(DataGenieError::Other(
                        "transient network glitch".to_string(),
                    ));
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
        Job::run(MemoryReader::new(records.clone()), retrying)
            .await
            .unwrap();
        assert_eq!(flaky_mem.len(), 3);

        let cb_count = Arc::new(Mutex::new(0usize));
        let cb_clone = Arc::clone(&cb_count);
        MemoryReader::new(records.clone())
            .filter(|r| r.get("active") == Some(&json!(true)))
            .write_to(CallbackWriter::new(move |_| {
                *cb_clone.lock().unwrap() += 1;
            }))
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
