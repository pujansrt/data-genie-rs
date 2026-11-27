use data_genie::DataRecord;
use serde_json::json;
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn test_cli_stdio_piping() {
    let dir = std::env::temp_dir().join(format!("data_genie_cli_stdio_{}", std::process::id()));
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
fn test_cli_validate_command() {
    let dir = std::env::temp_dir().join(format!("data_genie_cli_validate_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let valid_yaml = dir.join("valid.yaml");
    let invalid_yaml = dir.join("invalid.yaml");

    std::fs::write(
        &valid_yaml,
        r#"
pipeline:
  read:
    type: tsv
    path: "input.tsv"
  write:
    type: xml
    path: "output.xml"
    options:
      rootTag: items
      recordTag: item
      addDeclaration: true
"#,
    )
    .unwrap();

    std::fs::write(
        &invalid_yaml,
        r#"
pipeline:
  read:
    type: xml
    path: "input.xml"
  write:
    type: csv
"#,
    )
    .unwrap();

    let ok_out = Command::new(env!("CARGO_BIN_EXE_data-genie"))
        .args(["validate", valid_yaml.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(ok_out.status.success());
    assert!(String::from_utf8_lossy(&ok_out.stdout).contains("Pipeline configuration is valid."));

    let bad_out = Command::new(env!("CARGO_BIN_EXE_data-genie"))
        .args(["validate", invalid_yaml.to_str().unwrap()])
        .output()
        .unwrap();
    assert!(!bad_out.status.success());
    assert!(String::from_utf8_lossy(&bad_out.stderr).contains("Invalid pipeline configuration"));

    let _ = std::fs::remove_dir_all(&dir);
}
