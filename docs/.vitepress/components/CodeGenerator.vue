<script setup>
import { ref, computed } from 'vue'

const readerType = ref('CSVReader')
const readerSource = ref('file') // file, s3, http
const readerPath = ref('input.csv')
const readerBucket = ref('my-source-bucket')
const readerKey = ref('data/input.csv')
const readerUrl = ref('https://api.example.com/data')

// SQL Reader Specifics
const sqlReaderTable = ref('users')

// Writer Strategy
const writerMode = ref('single') // single, multi, parallel

const writers = ref([
  {
    type: 'JsonWriter',
    sink: 'file',
    path: 'output.json',
    bucket: 'my-dest-bucket',
    key: 'data/output.json',
    sqlTable: 'users',
    sqlDialect: 'Postgres'
  }
])

// DLQ Configuration
const useDLQ = ref(false)
const dlqWriter = ref({
  type: 'JsonWriter',
  sink: 'file',
  path: 'errors.json',
  bucket: 'my-dlq-bucket',
  key: 'errors/failed_records.json'
})

const addWriter = () => {
  writers.value.push({
    type: 'CSVWriter',
    sink: 'file',
    path: `output_${writers.value.length + 1}.csv`,
    bucket: 'my-dest-bucket',
    key: `data/output_${writers.value.length + 1}.csv`,
    sqlTable: 'users',
    sqlDialect: 'Postgres'
  })
}
const removeWriter = (index) => {
  if (writers.value.length > 1) writers.value.splice(index, 1)
}

const useValidation = ref(false)
const showEvents = ref(false)
const asyncMode = ref(false)

const transforms = ref([])

const addTransform = (type) => {
  if (type === 'rename') transforms.value.push({ type: 'rename', old: 'fname', new: 'firstName' })
  if (type === 'filter') transforms.value.push({ type: 'filter', expr: 'age > 18' })
  if (type === 'calculate') transforms.value.push({ type: 'calculate', field: 'total', expr: 'price * qty' })
  if (type === 'mask') transforms.value.push({ type: 'mask', field: 'email', strategy: 'Redact' })
  if (type === 'remove') transforms.value.push({ type: 'remove', fields: 'password, internal_id' })
}

const removeTransform = (index) => transforms.value.splice(index, 1)

const isFileSink = (type) => !['MemoryWriter', 'ConsoleWriter', 'CallbackWriter', 'SQLWriter'].includes(type)

const generatedCode = computed(() => {
  const imports = new Set(['Job'])
  imports.add(readerType.value)

  const activeWriters = writerMode.value === 'multi' ? writers.value : [writers.value[0]]

  if (writerMode.value === 'multi' && activeWriters.length > 1) imports.add('MultiWriter')
  if (writerMode.value === 'parallel') imports.add('ParallelWriter')

  activeWriters.forEach(w => {
    imports.add(w.type)
    if (w.type === 'SQLWriter') imports.add('SqlDialect')
  })

  if (useDLQ.value) {
    imports.add(dlqWriter.value.type)
    if (!useValidation.value) imports.add('ValidatingReader')
  }

  const usesS3 =
    readerSource.value === 's3' ||
    activeWriters.some(w => w.sink === 's3' && isFileSink(w.type)) ||
    (useDLQ.value && dlqWriter.value.sink === 's3')

  if (usesS3) {
    imports.add('S3Config')
    if (readerSource.value === 's3') imports.add('S3Source')
    if (activeWriters.some(w => w.sink === 's3' && isFileSink(w.type)) || (useDLQ.value && dlqWriter.value.sink === 's3')) {
      imports.add('S3Sink')
    }
  }

  if (useValidation.value) {
    imports.add('SchemaValidatingReader')
    imports.add('SchemaDefinition')
    imports.add('FieldConstraint')
    imports.add('FieldType')
  }

  const hasStructural = transforms.value.some(t => ['rename', 'calculate', 'mask', 'remove'].includes(t.type))
  if (hasStructural) imports.add('TransformingReader')
  if (transforms.value.some(t => t.type === 'rename')) imports.add('RenameField')
  if (transforms.value.some(t => t.type === 'calculate')) imports.add('SetCalculatedField')
  if (transforms.value.some(t => t.type === 'filter')) {
    imports.add('FilteringReader')
    imports.add('FilterExpression')
  }
  if (transforms.value.some(t => t.type === 'mask')) {
    imports.add('PIIMaskingTransformer')
    imports.add('MaskStrategy')
  }
  if (transforms.value.some(t => t.type === 'remove')) imports.add('RemoveFields')
  if (showEvents.value) imports.add('JobOptions')

  let code = `use data_genie::{${Array.from(imports).sort().join(', ')}};\n\n`

  if (asyncMode.value) {
    code += `#[tokio::main]\nasync fn main() -> data_genie::Result<()> {\n`
  } else {
    code += `fn main() -> data_genie::Result<()> {\n`
  }

  if (usesS3) {
    code += `    let s3_config = S3Config::new("us-east-1");\n`
  }

  // Reader Setup
  if (readerSource.value === 's3' && !['MemoryReader', 'SQLReader', 'HttpReader'].includes(readerType.value)) {
    code += `    let source = S3Source::new("${readerBucket.value}", "${readerKey.value}", s3_config.clone())?;\n`
    code += `    let reader = ${readerType.value}::from_source(Box::new(source));\n\n`
  } else if (readerType.value === 'HttpReader') {
    code += `    let reader = HttpReader::new("${readerUrl.value}");\n\n`
  } else if (readerType.value === 'MemoryReader') {
    code += `    let reader = MemoryReader::new(vec![]);\n\n`
  } else if (readerType.value === 'SQLReader') {
    code += `    let reader = SQLReader::new(db_client.clone(), "SELECT * FROM ${sqlReaderTable.value}");\n\n`
  } else if (readerType.value === 'XMLReader') {
    code += `    let reader = XMLReader::new("${readerPath.value}", "record");\n\n`
  } else {
    code += `    let reader = ${readerType.value}::new("${readerPath.value}");\n\n`
  }

  let currentVar = 'reader'

  transforms.value.forEach((t) => {
    if (t.type === 'filter') {
      code += `    let filtered = FilteringReader::new(${currentVar})\n`
      code += `        .add_rule(FilterExpression::new("${t.expr}").create_record_filter());\n`
      currentVar = 'filtered'
    } else if (t.type === 'rename') {
      code += `    let transformed = TransformingReader::new(${currentVar})\n`
      code += `        .add_transform(RenameField::new("${t.old}", "${t.new}").transform());\n`
      currentVar = 'transformed'
    } else if (t.type === 'calculate') {
      code += `    let transformed = TransformingReader::new(${currentVar})\n`
      code += `        .add_transform(SetCalculatedField::new("${t.field}", "${t.expr}").transform());\n`
      currentVar = 'transformed'
    } else if (t.type === 'mask') {
      code += `    let transformed = TransformingReader::new(${currentVar})\n`
      code += `        .add_transform(PIIMaskingTransformer::new().mask("${t.field}", MaskStrategy::${t.strategy}).transform());\n`
      currentVar = 'transformed'
    } else if (t.type === 'remove') {
      const fields = t.fields.split(',').map(f => `"${f.trim()}"`).join(', ')
      code += `    let transformed = TransformingReader::new(${currentVar})\n`
      code += `        .add_transform(RemoveFields::new([${fields}]).transform());\n`
      currentVar = 'transformed'
    }
  })

  if (useValidation.value) {
    code += `    let schema = SchemaDefinition::new()\n`
    code += `        .add_field("id", FieldConstraint::new(FieldType::Integer).required())\n`
    code += `        .add_field("email", FieldConstraint::new(FieldType::Email).required());\n`
    if (useDLQ.value) {
      const dlqExpr = dlqWriter.value.sink === 's3'
        ? `${dlqWriter.value.type}::from_sink(Box::new(S3Sink::new("${dlqWriter.value.bucket}", "${dlqWriter.value.key}", s3_config.clone())?))`
        : `${dlqWriter.value.type}::new("${dlqWriter.value.path}")`
      code += `    let validated = SchemaValidatingReader::new(${currentVar}, schema)\n`
      code += `        .with_dlq(${dlqExpr});\n`
    } else {
      code += `    let validated = SchemaValidatingReader::new(${currentVar}, schema);\n`
    }
    currentVar = 'validated'
  } else if (useDLQ.value) {
    const dlqExpr = dlqWriter.value.sink === 's3'
      ? `${dlqWriter.value.type}::from_sink(Box::new(S3Sink::new("${dlqWriter.value.bucket}", "${dlqWriter.value.key}", s3_config.clone())?))`
      : `${dlqWriter.value.type}::new("${dlqWriter.value.path}")`
    code += `    let validated = ValidatingReader::new(${currentVar})\n`
    code += `        .with_dlq(${dlqExpr});\n`
    currentVar = 'validated'
  }

  const buildWriterExpr = (w) => {
    if (w.sink === 's3' && isFileSink(w.type)) {
      return `${w.type}::from_sink(Box::new(S3Sink::new("${w.bucket}", "${w.key}", s3_config.clone())?))`
    }
    if (w.type === 'MemoryWriter' || w.type === 'ConsoleWriter') {
      return `${w.type}::new()`
    }
    if (w.type === 'CallbackWriter') {
      return `CallbackWriter::new(|record| async move { let _ = record; Ok(()) })`
    }
    if (w.type === 'SQLWriter') {
      return `SQLWriter::new(db_client.clone(), "${w.sqlTable}").with_dialect(SqlDialect::${w.sqlDialect})`
    }
    if (w.type === 'XMLWriter') {
      return `XMLWriter::new("${w.path}", "records", "record")`
    }
    return `${w.type}::new("${w.path}")`
  }

  if (writerMode.value === 'multi' && activeWriters.length > 1) {
    code += `\n    let writer = MultiWriter::new()\n`
    activeWriters.forEach(w => {
      code += `        .add_writer(${buildWriterExpr(w)})\n`
    })
    code = code.trimEnd() + `;\n\n`
  } else if (writerMode.value === 'parallel') {
    code += `\n    let writer = ParallelWriter::new(4, 250, || Box::new(${buildWriterExpr(activeWriters[0])}));\n\n`
  } else {
    code += `\n    let writer = ${buildWriterExpr(activeWriters[0])};\n\n`
  }

  if (showEvents.value) {
    code += `    let mut job = Job::with_options(${currentVar}, writer, JobOptions::new());\n`
    code += `    job.on_progress(|m| println!("Processed {} records ({:.0} rec/s)", m.record_count, m.records_per_second));\n`
    if (asyncMode.value) {
      code += `    let metrics = job.execute().await?;\n`
    } else {
      code += `    let metrics = job.execute_blocking()?;\n`
    }
  } else {
    if (asyncMode.value) {
      code += `    let metrics = Job::run(${currentVar}, writer).await?;\n`
    } else {
      code += `    let metrics = Job::run_blocking(${currentVar}, writer)?;\n`
    }
  }

  code += `    println!("Processed {} records", metrics.record_count);\n`
  code += `    Ok(())\n}`

  return code
})

const highlightedCode = computed(() => {
  let code = generatedCode.value
  code = code.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  const tokens = []
  let i = 0
  const addToken = (str, cls) => {
    const id = `__TOKEN_${i++}__`
    tokens.push({ id, html: `<span class="${cls}">${str}</span>` })
    return id
  }
  code = code.replace(/(".*?")/g, (m) => addToken(m, 'token-string'))
  code = code.replace(/\/\/.*/g, (m) => addToken(m, 'token-comment'))
  code = code.replace(/\b(use|fn|let|mut|async|await|move|Ok|Box)\b/g, (m) => addToken(m, 'token-keyword'))
  code = code.replace(/\b(Job|JobOptions|CSVReader|JsonReader|NDJsonReader|ParquetReader|XMLReader|XlsxReader|MemoryReader|SQLReader|HttpReader|JsonWriter|CSVWriter|NDJsonWriter|ParquetWriter|XMLWriter|SQLWriter|ConsoleWriter|MemoryWriter|CallbackWriter|ParallelWriter|MultiWriter|TransformingReader|FilteringReader|ValidatingReader|SchemaValidatingReader|SchemaDefinition|FieldConstraint|FieldType|RenameField|SetCalculatedField|FilterExpression|PIIMaskingTransformer|MaskStrategy|RemoveFields|S3Source|S3Sink|S3Config|SqlDialect)\b/g, (m) => addToken(m, 'token-class'))
  code = code.replace(/\b(\d+)\b/g, (m) => addToken(m, 'token-number'))
  tokens.forEach(t => { code = code.replace(t.id, t.html) })
  return code
})

const copyToClipboard = () => {
  navigator.clipboard.writeText(generatedCode.value)
  alert('Rust code copied to clipboard!')
}
</script>

<template>
  <div class="code-builder">
    <div class="controls">
      <div class="control-grid">
        <!-- Reader Card -->
        <section class="card">
          <div class="label">📥 Source Reader</div>
          
          <div class="sub-label">Format</div>
          <select v-model="readerType">
            <optgroup label="Standard">
              <option value="CSVReader">CSV Reader</option>
              <option value="JsonReader">JSON Reader</option>
              <option value="NDJsonReader">NDJSON Reader</option>
            </optgroup>
            <optgroup label="Advanced">
              <option value="ParquetReader">Parquet Reader</option>
              <option value="XMLReader">XML Reader</option>
              <option value="XlsxReader">Excel Reader</option>
            </optgroup>
            <optgroup label="Specialized">
              <option value="MemoryReader">Memory Reader</option>
              <option value="SQLReader">SQL Reader</option>
              <option value="HttpReader">HTTP Reader</option>
            </optgroup>
          </select>

          <div v-if="readerType !== 'MemoryReader' && readerType !== 'SQLReader' && readerType !== 'HttpReader'">
            <div class="sub-label">Transport</div>
            <div class="segmented-control">
              <label :class="{ active: readerSource === 'file' }">
                <input type="radio" v-model="readerSource" value="file" /> File
              </label>
              <label :class="{ active: readerSource === 's3' }">
                <input type="radio" v-model="readerSource" value="s3" /> S3
              </label>
            </div>
          </div>
          
          <div v-if="readerSource === 'file' && !['MemoryReader', 'SQLReader', 'HttpReader'].includes(readerType)" class="input-stack">
            <div class="sub-label">File Path</div>
            <input v-model="readerPath" />
          </div>
          
          <div v-if="readerSource === 's3' && !['MemoryReader', 'SQLReader', 'HttpReader'].includes(readerType)" class="input-stack">
            <div class="sub-label">Bucket</div>
            <input v-model="readerBucket" />
            <div class="sub-label">Key</div>
            <input v-model="readerKey" />
          </div>

          <div v-if="readerType === 'HttpReader'" class="input-stack">
            <div class="sub-label">URL</div>
            <input v-model="readerUrl" />
          </div>

          <div v-if="readerType === 'SQLReader'" class="input-stack">
            <div class="sub-label">Table Name</div>
            <input v-model="sqlReaderTable" />
          </div>
        </section>

        <!-- Writer Card -->
        <section class="card">
          <div class="label">📤 Destination Writer</div>
          
          <div class="sub-label">Strategy</div>
          <div class="segmented-control">
            <label :class="{ active: writerMode === 'single' }">
              <input type="radio" v-model="writerMode" value="single" /> Direct
            </label>
            <label :class="{ active: writerMode === 'multi' }">
              <input type="radio" v-model="writerMode" value="multi" /> Multi
            </label>
            <label :class="{ active: writerMode === 'parallel' }">
              <input type="radio" v-model="writerMode" value="parallel" /> Parallel
            </label>
          </div>

          <div v-if="writerMode === 'multi'" class="label-row mb-1">
            <span class="sub-label">Configured Sinks</span>
            <button class="small-add-btn" @click="addWriter">+ Add</button>
          </div>

          <div class="writers-container">
            <div v-for="(w, idx) in (writerMode === 'multi' ? writers : [writers[0]])" :key="idx" 
                 :class="['writer-item card-nested', { 'mb-0': writerMode !== 'multi' }]">
              
              <div class="writer-header" v-if="writerMode === 'multi'">
                <span>Sink #{{ idx + 1 }}</span>
                <button v-if="writers.length > 1" @click="removeWriter(idx)" class="del-btn">×</button>
              </div>

              <div class="sub-label">Format</div>
              <select v-model="w.type">
                <option value="JsonWriter">JSON Writer</option>
                <option value="CSVWriter">CSV Writer</option>
                <option value="NDJsonWriter">NDJSON Writer</option>
                <option value="ParquetWriter">Parquet Writer</option>
                <option value="XMLWriter">XML Writer</option>
                <option value="SQLWriter">SQL Writer</option>
                <option value="ConsoleWriter">Console Writer</option>
                <option value="MemoryWriter">Memory Writer</option>
                <option value="CallbackWriter">Callback Writer</option>
              </select>
              
              <div v-if="isFileSink(w.type)">
                <div class="sub-label">Transport</div>
                <div class="segmented-control small">
                  <label :class="{ active: w.sink === 'file' }">
                    <input type="radio" v-model="w.sink" value="file" /> File
                  </label>
                  <label :class="{ active: w.sink === 's3' }">
                    <input type="radio" v-model="w.sink" value="s3" /> S3
                  </label>
                </div>
                <div v-if="w.sink === 'file'" class="input-stack">
                  <input v-model="w.path" placeholder="Path" />
                </div>
                <div v-if="w.sink === 's3'" class="input-stack">
                  <input v-model="w.bucket" placeholder="Bucket" />
                  <input v-model="w.key" placeholder="Key" />
                </div>
              </div>

              <div v-if="w.type === 'SQLWriter'" class="input-stack">
                <div class="sub-label">Dialect / Table</div>
                <div class="inner-row">
                  <select v-model="w.sqlDialect" style="flex: 1">
                    <option value="Postgres">Postgres</option>
                    <option value="MySql">MySQL</option>
                    <option value="Sqlite">SQLite</option>
                    <option value="Oracle">Oracle</option>
                  </select>
                  <input v-model="w.sqlTable" placeholder="Table" style="flex: 1" />
                </div>
              </div>
            </div>
          </div>
        </section>
      </div>

      <!-- Transforms Card -->
      <section class="card">
        <div class="label">⚡ Transformations & Validation</div>
        
        <div class="toggle-row">
          <label class="checkbox-label"><input type="checkbox" v-model="useValidation" /> Enable Schema Validation</label>
        </div>

        <div class="btn-row">
          <button class="add-btn" @click="addTransform('filter')">+ Filter</button>
          <button class="add-btn" @click="addTransform('rename')">+ Rename</button>
          <button class="add-btn" @click="addTransform('calculate')">+ Calc Field</button>
          <button class="add-btn" @click="addTransform('mask')">+ PII Mask</button>
          <button class="add-btn" @click="addTransform('remove')">+ Remove</button>
        </div>

        <div class="transform-stack">
          <div v-for="(t, i) in transforms" :key="i" class="transform-box">
            <div class="transform-header">
              <span class="pill-type">{{ t.type }}</span>
              <button class="del-btn" @click="removeTransform(i)">×</button>
            </div>
            
            <div class="transform-body">
              <div v-if="t.type === 'filter'" class="input-group">
                <label>Expression</label>
                <input v-model="t.expr" placeholder="e.g. age > 18" />
              </div>

              <div v-if="t.type === 'rename'" class="input-row">
                <div class="input-group">
                  <label>From</label>
                  <input v-model="t.old" placeholder="old_name" />
                </div>
                <div class="input-group">
                  <label>To</label>
                  <input v-model="t.new" placeholder="new_name" />
                </div>
              </div>

              <div v-if="t.type === 'calculate'" class="input-row">
                <div class="input-group">
                  <label>Field Name</label>
                  <input v-model="t.field" placeholder="total" />
                </div>
                <div class="input-group">
                  <label>Calculation</label>
                  <input v-model="t.expr" placeholder="price * 1.2" />
                </div>
              </div>

              <div v-if="t.type === 'mask'" class="input-row">
                <div class="input-group">
                  <label>Field</label>
                  <input v-model="t.field" placeholder="email" />
                </div>
                <div class="input-group">
                  <label>Strategy</label>
                  <select v-model="t.strategy">
                    <option value="Redact">Redact</option>
                    <option value="Hash">Hash</option>
                    <option value="Partial">Partial</option>
                  </select>
                </div>
              </div>

              <div v-if="t.type === 'remove'" class="input-group">
                <label>Fields (comma separated)</label>
                <input v-model="t.fields" placeholder="password, internal_id" />
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- Advanced Features Card -->
      <section class="card">
        <div class="label">🛠️ Advanced Resilience</div>
        
        <div class="toggle-row">
          <label class="checkbox-label">
            <input type="checkbox" v-model="useDLQ" /> Enable Dead Letter Queue (DLQ)
          </label>
        </div>

        <div v-if="useDLQ" class="card-nested mt-2">
          <div class="sub-label">DLQ Destination</div>
          <div class="segmented-control small">
            <label :class="{ active: dlqWriter.sink === 'file' }">
              <input type="radio" v-model="dlqWriter.sink" value="file" /> File
            </label>
            <label :class="{ active: dlqWriter.sink === 's3' }">
              <input type="radio" v-model="dlqWriter.sink" value="s3" /> S3
            </label>
          </div>

          <div v-if="dlqWriter.sink === 'file'" class="input-stack mb-2">
            <input v-model="dlqWriter.path" placeholder="Path (e.g. errors.json)" />
          </div>
          <div v-if="dlqWriter.sink === 's3'" class="input-stack mb-2">
            <input v-model="dlqWriter.bucket" placeholder="Bucket" />
            <input v-model="dlqWriter.key" placeholder="Key" class="mt-1" />
          </div>

          <div class="sub-label">DLQ Format</div>
          <select v-model="dlqWriter.type">
            <option value="JsonWriter">JSON Writer</option>
            <option value="CSVWriter">CSV Writer</option>
            <option value="NDJsonWriter">NDJSON Writer</option>
          </select>
        </div>
      </section>

      <!-- Options Card -->
      <section class="card">
        <div class="label">⚙️ Execution Mode & Options</div>
        <div class="toggle-col">
          <label class="checkbox-label"><input type="checkbox" v-model="asyncMode" /> Async Mode (#[tokio::main])</label>
          <label class="checkbox-label"><input type="checkbox" v-model="showEvents" /> Show Progress Callback</label>
        </div>
      </section>
    </div>

    <!-- Preview Area -->
    <div class="preview">
      <div class="preview-header">
        <span>Rust Code</span>
        <button class="copy-btn" @click="copyToClipboard">Copy Code</button>
      </div>
      <pre><code class="highlighted-code" v-html="highlightedCode"></code></pre>
    </div>
  </div>
</template>

<style scoped>
.code-builder {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
  margin: 1.5rem 0;
  font-family: var(--vp-font-family-base);
}

.controls {
  display: flex;
  flex-direction: column;
  gap: 1.25rem;
}

.control-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
  gap: 1.25rem;
}

.card {
  background: var(--vp-c-bg-soft);
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  padding: 1rem;
}

.card-nested {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  padding: 0.75rem;
  margin-bottom: 0.75rem;
}

.mb-0 { margin-bottom: 0; }

.label {
  font-weight: 700;
  font-size: 0.9rem;
  margin-bottom: 0.75rem;
  color: var(--vp-c-text-1);
}

.label-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.writer-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.75rem;
  font-weight: 700;
  color: var(--vp-c-brand-1);
  margin-bottom: 0.5rem;
}

.sub-label {
  font-size: 0.7rem;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--vp-c-text-2);
  margin: 0.5rem 0 0.25rem;
}

.segmented-control {
  display: flex;
  background: var(--vp-c-bg);
  padding: 2px;
  border-radius: 6px;
  border: 1px solid var(--vp-c-divider);
  margin-bottom: 0.5rem;
}

.segmented-control label {
  flex: 1;
  text-align: center;
  font-size: 0.75rem;
  padding: 4px;
  cursor: pointer;
  border-radius: 4px;
  color: var(--vp-c-text-2);
  display: flex;
  align-items: center;
  justify-content: center;
}

.segmented-control input { display: none; }
.segmented-control label.active {
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  font-weight: bold;
}

.segmented-control.small label { padding: 2px; font-size: 0.7rem; }

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.85rem;
  cursor: pointer;
}

.input-stack { display: flex; flex-direction: column; gap: 0.25rem; }

select, input:not([type="radio"]):not([type="checkbox"]), .code-textarea {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
  border-radius: 4px;
  padding: 4px 8px;
  font-size: 0.85rem;
  color: var(--vp-c-text-1);
  width: 100%;
}

.code-textarea {
  font-family: var(--vp-font-family-mono);
  resize: vertical;
}

.input-group { display: flex; flex-direction: column; gap: 0.2rem; flex: 1; }
.input-group label { font-size: 0.65rem; color: var(--vp-c-text-3); }

.inner-row, .input-row { display: flex; gap: 0.5rem; }

.transform-stack { display: flex; flex-direction: column; gap: 1rem; margin-top: 1rem; }
.transform-box {
  background: var(--vp-c-bg);
  border: 1px solid var(--vp-c-divider);
  border-radius: 6px;
  padding: 0.75rem;
}

.transform-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
}

.pill-type {
  font-size: 0.6rem;
  font-weight: 800;
  text-transform: uppercase;
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  padding: 2px 6px;
  border-radius: 10px;
}

.btn-row { display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.5rem; }
.add-btn, .small-add-btn {
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
  border: 1px solid var(--vp-c-brand-soft);
  padding: 2px 8px;
  border-radius: 4px;
  font-size: 0.75rem;
  font-weight: 600;
  cursor: pointer;
}

.del-btn { background: none; border: none; color: var(--vp-c-text-3); cursor: pointer; font-size: 1.1rem; }
.del-btn:hover { color: var(--vp-c-danger-1); }

.preview { background: #1e1e1e; border-radius: 8px; overflow: hidden; margin-top: 1rem; }
.preview-header {
  padding: 0.5rem 1rem;
  background: #2d2d2d;
  display: flex;
  justify-content: space-between;
  color: #aaa;
  font-size: 0.8rem;
}

.mb-1 { margin-bottom: 0.25rem; }
.mb-2 { margin-top: 0.5rem; }
.mt-1 { margin-top: 0.25rem; }
.mt-2 { margin-top: 0.5rem; }

pre { margin: 0; padding: 1rem; color: #d4d4d4; font-size: 0.85rem; overflow-x: auto; }
code { font-family: var(--vp-font-family-mono); }

/* Syntax Highlighting Tokens */
:deep(.token-keyword) { color: #c678dd !important; font-weight: bold; }
:deep(.token-class) { color: #e5c07b !important; }
:deep(.token-string) { color: #98c379 !important; }
:deep(.token-comment) { color: #5c6370 !important; font-style: italic; }
:deep(.token-number) { color: #d19a66 !important; }
</style>
