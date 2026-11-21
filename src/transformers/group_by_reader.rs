use async_stream::try_stream;
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter, value_to_f64, value_to_string};

#[derive(Debug, Clone)]
enum AggregationType {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}

#[derive(Debug, Clone)]
struct AggregationOp {
    op_type: AggregationType,
    output_field: String,
}

/// Groups records by a specified field and computes aggregations (`count`, `sum`, `min`, `max`, `avg`).
pub struct GroupByReader<R: DataReader> {
    reader: R,
    group_by_field: String,
    aggregations: Vec<AggregationOp>,
    skipped_records: Arc<AtomicUsize>,
}

impl<R: DataReader> GroupByReader<R> {
    pub fn new(reader: R, group_by_field: impl Into<String>) -> Self {
        Self {
            reader,
            group_by_field: group_by_field.into(),
            aggregations: Vec::new(),
            skipped_records: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn count(mut self, output_field: impl Into<String>) -> Self {
        self.aggregations.push(AggregationOp {
            op_type: AggregationType::Count,
            output_field: output_field.into(),
        });
        self
    }

    pub fn sum(mut self, source_field: impl Into<String>, output_field: impl Into<String>) -> Self {
        self.aggregations.push(AggregationOp {
            op_type: AggregationType::Sum(source_field.into()),
            output_field: output_field.into(),
        });
        self
    }

    pub fn min(mut self, source_field: impl Into<String>, output_field: impl Into<String>) -> Self {
        self.aggregations.push(AggregationOp {
            op_type: AggregationType::Min(source_field.into()),
            output_field: output_field.into(),
        });
        self
    }

    pub fn max(mut self, source_field: impl Into<String>, output_field: impl Into<String>) -> Self {
        self.aggregations.push(AggregationOp {
            op_type: AggregationType::Max(source_field.into()),
            output_field: output_field.into(),
        });
        self
    }

    pub fn avg(mut self, source_field: impl Into<String>, output_field: impl Into<String>) -> Self {
        self.aggregations.push(AggregationOp {
            op_type: AggregationType::Avg(source_field.into()),
            output_field: output_field.into(),
        });
        self
    }
}

impl<R: DataReader + 'static> DataReader for GroupByReader<R> {
    fn read(&mut self) -> RecordStream {
        let mut upstream = self.reader.read();
        let group_by_field = self.group_by_field.clone();
        let aggregations = self.aggregations.clone();
        let skipped_records = Arc::clone(&self.skipped_records);

        Box::pin(try_stream! {
            let mut grouped: IndexMap<String, (Value, Vec<DataRecord>)> = IndexMap::new();

            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let key_val = record.get(&group_by_field).cloned().unwrap_or(Value::Null);
                let key_str = value_to_string(&key_val);
                grouped
                    .entry(key_str)
                    .or_insert_with(|| (key_val, Vec::new()))
                    .1
                    .push(record);
            }

            for (_key_str, (key_val, records)) in grouped {
                let mut agg_record = DataRecord::new();
                agg_record.insert(group_by_field.clone(), key_val);

                for agg in &aggregations {
                    match &agg.op_type {
                        AggregationType::Count => {
                            agg_record.insert(agg.output_field.clone(), json!(records.len()));
                        }
                        AggregationType::Sum(src) => {
                            let sum: f64 = records
                                .iter()
                                .map(|r| r.get(src).and_then(value_to_f64).unwrap_or(0.0))
                                .sum();
                            agg_record.insert(agg.output_field.clone(), format_num(sum));
                        }
                        AggregationType::Max(src) => {
                            let max = records
                                .iter()
                                .map(|r| r.get(src).and_then(value_to_f64).unwrap_or(0.0))
                                .fold(f64::NEG_INFINITY, f64::max);
                            let val = if max.is_finite() { max } else { 0.0 };
                            agg_record.insert(agg.output_field.clone(), format_num(val));
                        }
                        AggregationType::Min(src) => {
                            let min = records
                                .iter()
                                .map(|r| r.get(src).and_then(value_to_f64).unwrap_or(0.0))
                                .fold(f64::INFINITY, f64::min);
                            let val = if min.is_finite() { min } else { 0.0 };
                            agg_record.insert(agg.output_field.clone(), format_num(val));
                        }
                        AggregationType::Avg(src) => {
                            let sum: f64 = records
                                .iter()
                                .map(|r| r.get(src).and_then(value_to_f64).unwrap_or(0.0))
                                .sum();
                            let avg = if !records.is_empty() {
                                sum / (records.len() as f64)
                            } else {
                                0.0
                            };
                            agg_record.insert(agg.output_field.clone(), format_num(avg));
                        }
                    }
                }

                yield Some(agg_record);
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.reader.set_dlq(writer);
    }

    fn skipped_record_count(&self) -> usize {
        self.reader.skipped_record_count() + self.skipped_records.load(Ordering::Relaxed)
    }

    fn dlq_record_count(&self) -> usize {
        self.reader.dlq_record_count()
    }

    fn rejected_record_count(&self) -> usize {
        self.reader.rejected_record_count()
    }

    fn set_ignore_errors(&mut self, ignore: bool) {
        self.reader.set_ignore_errors(ignore);
    }
}

fn format_num(n: f64) -> Value {
    if n.fract() == 0.0 && n >= (i64::MIN as f64) && n <= (i64::MAX as f64) {
        json!(n as i64)
    } else {
        json!(n)
    }
}
