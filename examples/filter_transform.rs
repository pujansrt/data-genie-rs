use data_genie::{
    BasicFieldTransformer, CSVReader, FilterExpression, FilteringReader, Job, JsonWriter, MemorySink, MemorySource, RenameField, SelectFields,
    TransformingReader,
};

fn main() -> data_genie::Result<()> {
    let input = MemorySource::new("fname,age,city\nAlice,31,Dublin\nBob,17,London\nCara,28,Paris\n");
    // CSV values start as strings. Convert age before applying the numeric rule.
    let typed = TransformingReader::new(CSVReader::new(input)).add_transform(BasicFieldTransformer::new(["age"]).string_to_int());
    let adults = FilteringReader::new(typed).add_rule(FilterExpression::new("age >= 18").create_record_filter());
    let selected = TransformingReader::new(adults)
        .add_transform(RenameField::new("fname", "name").transform())
        .add_transform(SelectFields::new(["name", "age"]).transform());

    let output = MemorySink::new();
    Job::run_blocking(selected, JsonWriter::new(output.clone()))?;
    println!("{}", output.get_text());
    Ok(())
}
