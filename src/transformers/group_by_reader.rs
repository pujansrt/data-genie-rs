use async_stream::try_stream;
use futures::StreamExt;
use indexmap::IndexMap;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::core::traits::{DataReader, DataRecord, RecordStream, SharedWriter, value_to_f64};

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

#[derive(Debug, Clone)]
enum AggAccumulator {
    Count(usize),
    Sum(f64),
    Min(Option<f64>),
    Max(Option<f64>),
    Avg { sum: f64, count: usize },
}

impl AggAccumulator {
    fn from_op(op: &AggregationType) -> Self {
        match op {
            AggregationType::Count => Self::Count(0),
            AggregationType::Sum(_) => Self::Sum(0.0),
            AggregationType::Min(_) => Self::Min(None),
            AggregationType::Max(_) => Self::Max(None),
            AggregationType::Avg(_) => Self::Avg { sum: 0.0, count: 0 },
        }
    }

    fn update(&mut self, op: &AggregationType, record: &DataRecord) {
        match (self, op) {
            (Self::Count(c), AggregationType::Count) => *c += 1,
            (Self::Sum(sum), AggregationType::Sum(src)) => {
                if let Some(v) = record.get(src).and_then(value_to_f64) {
                    *sum += v;
                }
            }
            (Self::Min(min), AggregationType::Min(src)) => {
                if let Some(v) = record.get(src).and_then(value_to_f64) {
                    *min = Some(match *min {
                        Some(curr) => curr.min(v),
                        None => v,
                    });
                }
            }
            (Self::Max(max), AggregationType::Max(src)) => {
                if let Some(v) = record.get(src).and_then(value_to_f64) {
                    *max = Some(match *max {
                        Some(curr) => curr.max(v),
                        None => v,
                    });
                }
            }
            (Self::Avg { sum, count }, AggregationType::Avg(src)) => {
                if let Some(v) = record.get(src).and_then(value_to_f64) {
                    *sum += v;
                    *count += 1;
                }
            }
            _ => {}
        }
    }

    fn finish(self) -> Value {
        match self {
            Self::Count(c) => json!(c),
            Self::Sum(sum) => format_num(sum),
            Self::Min(Some(min)) => format_num(min),
            Self::Min(None) => Value::Null,
            Self::Max(Some(max)) => format_num(max),
            Self::Max(None) => Value::Null,
            Self::Avg { sum, count } if count > 0 => format_num(sum / (count as f64)),
            Self::Avg { .. } => Value::Null,
        }
    }
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
            let mut grouped: IndexMap<String, (Value, Vec<AggAccumulator>)> = IndexMap::new();

            while let Some(item) = upstream.next().await {
                let Some(record) = item? else {
                    skipped_records.fetch_add(1, Ordering::Relaxed);
                    continue;
                };
                let key_val = record.get(&group_by_field).cloned().unwrap_or(Value::Null);
                let key_str = serde_json::to_string(&key_val).unwrap_or_default();
                let entry = grouped.entry(key_str).or_insert_with(|| {
                    let accs = aggregations
                        .iter()
                        .map(|agg| AggAccumulator::from_op(&agg.op_type))
                        .collect();
                    (key_val, accs)
                });
                for (acc, agg) in entry.1.iter_mut().zip(aggregations.iter()) {
                    acc.update(&agg.op_type, &record);
                }
            }

            for (_key_str, (key_val, accumulators)) in grouped {
                let mut agg_record = DataRecord::with_capacity(1 + aggregations.len());
                agg_record.insert(group_by_field.clone(), key_val);

                for (agg, acc) in aggregations.iter().zip(accumulators.into_iter()) {
                    agg_record.insert(agg.output_field.clone(), acc.finish());
                }

                yield Some(agg_record);
            }
        })
    }

    fn set_dlq(&mut self, writer: SharedWriter) {
        self.reader.set_dlq(writer);
    }

    fn propagate_dlq(&mut self, writer: SharedWriter) {
        self.reader.propagate_dlq(writer);
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
    if !n.is_finite() {
        return Value::Null;
    }
    if n.fract() == 0.0 && n >= (i64::MIN as f64) && n < (i64::MAX as f64) {
        json!(n as i64)
    } else {
        json!(n)
    }
}
