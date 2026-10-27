# Transformers & Filters

Transformers and filters wrap any `DataReader` to modify, validate, or aggregate records in-flight.

## `TransformingReader`
Chains field-level or record-level transformations:
- **`RenameField::new("old", "new").transform()`**
- **`SetCalculatedField::new("total", "price * qty").transform()`**
- **`SelectFields::new(["id", "name"]).transform()`**
- **`RemoveFields::new(["password"]).transform()`**
- **`TypeConvert::new(["age"], TargetType::Int).transform()`**
- **`PIIMaskingTransformer::new().mask("email", MaskStrategy::Partial).transform()`**

## `FilteringReader`
Filters records using `FilterExpression` or `FieldFilter` rules:
- **`FilterExpression::new("age >= 18 && status == 'active'").create_record_filter()`**
- **`FieldFilter::new("email").add_rule(IsNotNull).add_rule(PatternMatch::new(".*@example\\.com")).create_record_filter()`**

## `SchemaValidatingReader` & `ValidatingReader`
Validate records and automatically route invalid rows to a Dead Letter Queue (`.with_dlq(writer)`):
- **`SchemaValidatingReader::new(reader, schema).with_dlq(dlq)`**
- **`ValidatingReader::new(reader).add_rule("positive_balance", |r| ...).with_dlq(dlq)`**

## `GroupByReader` & `RemoveDuplicatesReader`
- **`GroupByReader::new(reader, ["department"], vec![AggregateSpec::new("salary", AggregateOp::Avg, "avg_salary")])`**
- **`RemoveDuplicatesReader::new(reader, ["email"])`**
