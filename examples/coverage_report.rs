use serde::Deserialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Deserialize)]
struct LlvmCovExport {
    data: Vec<LlvmCovData>,
}

#[derive(Deserialize)]
struct LlvmCovData {
    files: Vec<LlvmCovFile>,
}

#[derive(Deserialize)]
struct LlvmCovFile {
    filename: String,
    summary: LlvmCovSummary,
}

#[derive(Deserialize, Default, Clone, Copy)]
struct MetricCount {
    count: usize,
    covered: usize,
}

impl MetricCount {
    fn add(&mut self, other: MetricCount) {
        self.count += other.count;
        self.covered += other.covered;
    }

    fn missed(self) -> usize {
        self.count.saturating_sub(self.covered)
    }

    fn percent(self) -> f64 {
        if self.count == 0 {
            100.0
        } else {
            (self.covered as f64 / self.count as f64) * 100.0
        }
    }
}

#[derive(Deserialize, Default, Clone, Copy)]
struct LlvmCovSummary {
    lines: MetricCount,
    functions: MetricCount,
    regions: MetricCount,
}

impl LlvmCovSummary {
    fn add(&mut self, other: &LlvmCovSummary) {
        self.lines.add(other.lines);
        self.functions.add(other.functions);
        self.regions.add(other.regions);
    }
}

struct FileEntry {
    file_name: String,
    rel_path: String,
    html_href: String,
    summary: LlvmCovSummary,
}

#[derive(Default)]
struct DirEntry {
    files: Vec<FileEntry>,
    summary: LlvmCovSummary,
}

fn clean_stale_cov_artifacts() {
    // Clean only crate-local test binaries & profraw files so dependency crates stay cached,
    // preventing Cargo 1.100 stale-binary coverage dilution without triggering full rebuilds.
    let target_dir = Path::new("target/llvm-cov-target");
    let build_pkg_dir = target_dir.join("debug/build/data-genie");
    if build_pkg_dir.exists() {
        let _ = fs::remove_dir_all(&build_pkg_dir);
    }
    if let Ok(entries) = fs::read_dir(target_dir) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.extension().is_some_and(|e| e == "profraw" || e == "profdata") {
                let _ = fs::remove_file(p);
            }
        }
    }
}

fn run_llvm_cov(generate_html: bool) {
    clean_stale_cov_artifacts();

    let mut args = vec!["llvm-cov", "--all-features", "--lib", "--test", "integration_tests"];
    if generate_html {
        args.push("--html");
    } else {
        args.push("--no-report");
    }

    let status = Command::new("cargo")
        .args(&args)
        .status()
        .expect("Failed to run `cargo llvm-cov`. Is cargo-llvm-cov installed?");
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
}

fn load_coverage_json() -> LlvmCovExport {
    let output = Command::new("cargo")
        .args(["llvm-cov", "report", "--json"])
        .stderr(Stdio::inherit())
        .output()
        .expect("Failed to run `cargo llvm-cov report --json`");
    if !output.status.success() {
        std::process::exit(output.status.code().unwrap_or(1));
    }
    serde_json::from_slice(&output.stdout).expect("Failed to parse llvm-cov JSON report")
}

fn group_by_directory(export: LlvmCovExport, manifest_dir: &Path) -> (BTreeMap<String, DirEntry>, LlvmCovSummary) {
    let src_root = manifest_dir.join("src");
    let mut dirs: BTreeMap<String, DirEntry> = BTreeMap::new();
    let mut total = LlvmCovSummary::default();

    for data in export.data {
        for file in data.files {
            let file_path = Path::new(&file.filename);
            let Ok(rel) = file_path.strip_prefix(&src_root) else {
                continue;
            };
            let rel_str = rel.to_string_lossy().replace('\\', "/");
            let (dir_name, file_name) = match rel_str.rsplit_once('/') {
                Some((dir, name)) => (dir.to_string(), name.to_string()),
                None => ("(root)".to_string(), rel_str.clone()),
            };

            let html_href = format!("coverage/{}.html", file.filename.trim_start_matches('/'));
            let entry = dirs.entry(dir_name).or_default();
            entry.summary.add(&file.summary);
            total.add(&file.summary);
            entry.files.push(FileEntry {
                file_name,
                rel_path: format!("src/{}", rel_str),
                html_href,
                summary: file.summary,
            });
        }
    }

    for dir in dirs.values_mut() {
        dir.files.sort_by(|a, b| a.file_name.cmp(&b.file_name));
    }

    (dirs, total)
}

fn print_terminal_summary(dirs: &BTreeMap<String, DirEntry>, total: &LlvmCovSummary, show_files: bool) {
    println!();
    println!(
        "{:<34} {:>16} {:>16} {:>16}",
        "Directory / Package", "Line Coverage", "Function Cov", "Region Cov"
    );
    println!("{}", "-".repeat(86));

    for (dir_name, dir) in dirs {
        let label = if dir_name == "(root)" {
            "src/ (root)".to_string()
        } else {
            format!("src/{}/", dir_name)
        };
        println!(
            "{:<34} {:>6.2}% ({:>4}/{:<4}) {:>5.2}% ({:>3}/{:<3}) {:>5.2}% ({:>4}/{:<4})",
            label,
            dir.summary.lines.percent(),
            dir.summary.lines.covered,
            dir.summary.lines.count,
            dir.summary.functions.percent(),
            dir.summary.functions.covered,
            dir.summary.functions.count,
            dir.summary.regions.percent(),
            dir.summary.regions.covered,
            dir.summary.regions.count,
        );

        if show_files {
            for f in &dir.files {
                println!(
                    "  {:<32} {:>6.2}% ({:>4}/{:<4}) {:>5.2}% ({:>3}/{:<3}) {:>5.2}% ({:>4}/{:<4})",
                    f.file_name,
                    f.summary.lines.percent(),
                    f.summary.lines.covered,
                    f.summary.lines.count,
                    f.summary.functions.percent(),
                    f.summary.functions.covered,
                    f.summary.functions.count,
                    f.summary.regions.percent(),
                    f.summary.regions.covered,
                    f.summary.regions.count,
                );
            }
        }
    }

    println!("{}", "-".repeat(86));
    println!(
        "{:<34} {:>6.2}% ({:>4}/{:<4}) {:>5.2}% ({:>3}/{:<3}) {:>5.2}% ({:>4}/{:<4})",
        "TOTAL",
        total.lines.percent(),
        total.lines.covered,
        total.lines.count,
        total.functions.percent(),
        total.functions.covered,
        total.functions.count,
        total.regions.percent(),
        total.regions.covered,
        total.regions.count,
    );
    println!();
}

fn write_light_mode_css(html_dir: &Path) {
    let css_path = html_dir.join("style.css");
    let css = r#"/* Forced Light Mode stylesheet for llvm-cov HTML pages */
:root {
  color-scheme: light;
}
html {
  scroll-behavior: smooth;
  background-color: #ffffff;
  color: #1f2937;
}
body {
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background-color: #ffffff;
  color: #1f2937;
  margin: 20px 28px;
}
pre {
  margin-top: 0px !important;
  margin-bottom: 0px !important;
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
  font-size: 13px;
}
.red {
  background-color: #fee2e2;
}
.cyan {
  background-color: #e0f2fe;
}
.source-name-title {
  padding: 8px 12px;
  border-bottom: 1px solid #d1d5db;
  background-color: #f8fafc;
  color: #111827;
  line-height: 32px;
  font-weight: 600;
}
.centered {
  display: table;
  margin-left: 0;
  margin-right: auto;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  background-color: #ffffff;
}
.expansion-view {
  margin: 5px 5px 5px 0;
  border: 1px solid #cbd5e1;
  border-radius: 4px;
  background-color: #f8fafc;
}
table {
  border-collapse: collapse;
}
.light-row {
  border-top: 1px solid #e5e7eb;
  border-bottom: 1px solid #e5e7eb;
}
.light-row-bold {
  border-top: 2px solid #cbd5e1;
  border-bottom: 1px solid #cbd5e1;
  font-weight: 700;
  background-color: #f8fafc;
}
.column-entry, .column-entry-bold {
  text-align: left;
}
.column-entry-bold {
  font-weight: 600;
}
.column-entry-yellow {
  text-align: left;
  background-color: #fef3c7;
  color: #92400e;
}
.column-entry-red {
  text-align: left;
  background-color: #fee2e2;
  color: #991b1b;
}
.column-entry-gray {
  text-align: left;
  background-color: #f3f4f6;
  color: #6b7280;
}
.column-entry-green {
  text-align: left;
  background-color: #dcfce7;
  color: #166534;
}
.line-number {
  text-align: right;
  color: #6b7280;
  background-color: #f9fafb;
  user-select: none;
}
.covered-line {
  text-align: right;
  color: #15803d;
  background-color: #f0fdf4;
}
.uncovered-line {
  text-align: right;
  color: #b91c1c;
  background-color: #fef2f2;
}
.uncovered-line.selected {
  color: #991b1b;
  font-weight: bold;
}
.region.red.selected, .branch.red.selected {
  background-color: #fca5a5;
  font-weight: bold;
}
.tooltip {
  position: relative;
  display: inline;
  background-color: #dbeafe;
  text-decoration: none;
}
.tooltip span.tooltip-content {
  position: absolute;
  width: 100px;
  margin-left: -50px;
  color: #ffffff;
  background: #1f2937;
  height: 28px;
  line-height: 28px;
  text-align: center;
  visibility: hidden;
  border-radius: 6px;
}
:hover.tooltip span.tooltip-content {
  visibility: visible;
  opacity: 0.92;
  bottom: 28px;
  left: 50%;
  z-index: 999;
}
th, td {
  vertical-align: top;
  padding: 3px 10px;
  border-right: 1px solid #e5e7eb;
  border-left: 1px solid #e5e7eb;
  text-align: left;
}
td pre {
  display: inline-block;
  text-decoration: inherit;
}
td:first-child { border-left: none; }
td:last-child { border-right: none; }
tr:hover { background-color: #f3f4f6; }
tr:last-child { border-bottom: none; }
tr:has(> td > a:target), tr:has(> td.uncovered-line.selected) {
  background-color: #fef9c3;
}
a {
  color: #1d4ed8;
  text-decoration: none;
}
a:hover {
  text-decoration: underline;
}
.control {
  position: fixed;
  top: 0.75em;
  right: 0.75em;
  padding: 0.6em 0.9em;
  background: #ffffff;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08);
  color: #1f2937;
}
"#;
    fs::write(css_path, css).expect("Failed to write light-mode style.css");
}

fn bar_html(m: MetricCount) -> String {
    let pct = m.percent();
    let cls = if pct >= 80.0 {
        "cov-high"
    } else if pct >= 50.0 {
        "cov-mid"
    } else {
        "cov-low"
    };
    format!(
        "<div class=\"bar-wrap\"><div class=\"bar\"><div class=\"bar-fill {}\" style=\"width:{:.2}%\"></div></div><span class=\"pct\">{:.2}%</span><span class=\"ratio\">({}/{})</span></div>",
        cls, pct, pct, m.covered, m.count
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

fn write_directory_index_html(html_dir: &Path, dirs: &BTreeMap<String, DirEntry>, total: &LlvmCovSummary) {
    let mut rows = String::new();
    for (idx, (dir_name, dir)) in dirs.iter().enumerate() {
        let pkg_label = if dir_name == "(root)" {
            "src (root)".to_string()
        } else {
            format!("src/{}", dir_name)
        };
        let group_id = format!("dir-{}", idx);
        rows.push_str(&format!(
            r#"<tr class="pkg-row" onclick="toggleDir('{gid}', this)" data-expanded="false">
  <td class="pkg-name"><span class="caret">&#9654;</span> {label} <span class="file-count">({n} files)</span></td>
  <td>{lines}</td>
  <td class="num">{missed}</td>
  <td>{funcs}</td>
  <td>{regions}</td>
</tr>
"#,
            gid = group_id,
            label = html_escape(&pkg_label),
            n = dir.files.len(),
            lines = bar_html(dir.summary.lines),
            missed = dir.summary.lines.missed(),
            funcs = bar_html(dir.summary.functions),
            regions = bar_html(dir.summary.regions),
        ));

        for f in &dir.files {
            rows.push_str(&format!(
                r#"<tr class="file-row {gid}" style="display:none;">
  <td class="file-name"><a href="{href}">{name}</a></td>
  <td>{lines}</td>
  <td class="num">{missed}</td>
  <td>{funcs}</td>
  <td>{regions}</td>
</tr>
"#,
                gid = group_id,
                href = html_escape(&f.html_href),
                name = html_escape(&f.rel_path),
                lines = bar_html(f.summary.lines),
                missed = f.summary.lines.missed(),
                funcs = bar_html(f.summary.functions),
                regions = bar_html(f.summary.regions),
            ));
        }
    }

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>data-genie-rs — Coverage Report</title>
  <style>
    :root {{ color-scheme: light; }}
    * {{ box-sizing: border-box; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #ffffff;
      color: #1f2937;
      margin: 0;
      padding: 28px 36px;
    }}
    .header {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      border-bottom: 2px solid #e5e7eb;
      padding-bottom: 12px;
      margin-bottom: 20px;
    }}
    h1 {{ margin: 0; font-size: 22px; color: #111827; }}
    .actions button {{
      background: #f8fafc;
      border: 1px solid #cbd5e1;
      border-radius: 6px;
      padding: 6px 12px;
      font-size: 13px;
      color: #1f2937;
      cursor: pointer;
      margin-left: 6px;
    }}
    .actions button:hover {{ background: #e2e8f0; }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(3, minmax(180px, 260px));
      gap: 14px;
      margin-bottom: 22px;
    }}
    .card {{
      border: 1px solid #e5e7eb;
      border-radius: 8px;
      padding: 14px 16px;
      background: #f9fafb;
    }}
    .card-title {{ font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; color: #6b7280; }}
    .card-val {{ font-size: 26px; font-weight: 700; margin-top: 4px; color: #111827; }}
    .card-sub {{ font-size: 12px; color: #4b5563; margin-top: 2px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      border: 1px solid #d1d5db;
      font-size: 14px;
    }}
    thead th {{
      background: #f3f4f6;
      color: #111827;
      font-weight: 600;
      text-align: left;
      padding: 10px 12px;
      border-bottom: 2px solid #d1d5db;
    }}
    td {{
      padding: 8px 12px;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: middle;
    }}
    .pkg-row {{
      background: #f8fafc;
      cursor: pointer;
      font-weight: 600;
    }}
    .pkg-row:hover {{ background: #eef2ff; }}
    .caret {{ display: inline-block; width: 16px; font-size: 11px; color: #6b7280; }}
    .file-count {{ font-weight: 400; color: #6b7280; font-size: 12px; }}
    .file-row {{ background: #ffffff; }}
    .file-row:hover {{ background: #f9fafb; }}
    .file-name {{ padding-left: 34px; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }}
    .file-name a {{ color: #1d4ed8; text-decoration: none; }}
    .file-name a:hover {{ text-decoration: underline; }}
    .num {{ text-align: right; font-variant-numeric: tabular-nums; color: #374151; }}
    tfoot tr {{
      background: #f3f4f6;
      font-weight: 700;
      border-top: 2px solid #9ca3af;
    }}
    .bar-wrap {{ display: flex; align-items: center; gap: 8px; }}
    .bar {{
      width: 110px;
      height: 11px;
      background: #fee2e2;
      border: 1px solid #fca5a5;
      border-radius: 3px;
      overflow: hidden;
      flex-shrink: 0;
    }}
    .bar-fill {{ height: 100%; }}
    .cov-high {{ background: #16a34a; }}
    .cov-mid {{ background: #d97706; }}
    .cov-low {{ background: #dc2626; }}
    .pct {{ font-variant-numeric: tabular-nums; min-width: 56px; text-align: right; font-weight: 600; }}
    .ratio {{ color: #6b7280; font-size: 12px; font-variant-numeric: tabular-nums; }}
    .hint {{ margin-top: 12px; font-size: 12px; color: #6b7280; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>data-genie-rs — Directory Coverage Report</h1>
    <div class="actions">
      <button onclick="setAll(true)">Expand All</button>
      <button onclick="setAll(false)">Collapse All</button>
    </div>
  </div>
  <div class="cards">
    <div class="card">
      <div class="card-title">Line Coverage</div>
      <div class="card-val">{line_pct:.2}%</div>
      <div class="card-sub">{line_cov} / {line_tot} lines ({line_miss} missed)</div>
    </div>
    <div class="card">
      <div class="card-title">Function Coverage</div>
      <div class="card-val">{fn_pct:.2}%</div>
      <div class="card-sub">{fn_cov} / {fn_tot} functions</div>
    </div>
    <div class="card">
      <div class="card-title">Region Coverage</div>
      <div class="card-val">{reg_pct:.2}%</div>
      <div class="card-sub">{reg_cov} / {reg_tot} regions</div>
    </div>
  </div>
  <table>
    <thead>
      <tr>
        <th>Directory / File</th>
        <th>Lines</th>
        <th style="text-align:right">Missed Lines</th>
        <th>Functions</th>
        <th>Regions</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
    <tfoot>
      <tr>
        <td>Total</td>
        <td>{tot_lines}</td>
        <td class="num">{line_miss}</td>
        <td>{tot_funcs}</td>
        <td>{tot_regions}</td>
      </tr>
    </tfoot>
  </table>
  <p class="hint">Click any directory row to expand its source files. Click a file to view line-by-line llvm-cov highlighting.</p>
  <script>
    function toggleDir(gid, row) {{
      var expanded = row.getAttribute('data-expanded') === 'true';
      var next = !expanded;
      row.setAttribute('data-expanded', String(next));
      row.querySelector('.caret').innerHTML = next ? '&#9660;' : '&#9654;';
      document.querySelectorAll('tr.' + gid).forEach(function(r) {{
        r.style.display = next ? '' : 'none';
      }});
    }}
    function setAll(expand) {{
      document.querySelectorAll('tr.pkg-row').forEach(function(row) {{
        var expanded = row.getAttribute('data-expanded') === 'true';
        if (expanded !== expand) {{
          row.click();
        }}
      }});
    }}
  </script>
</body>
</html>
"#,
        line_pct = total.lines.percent(),
        line_cov = total.lines.covered,
        line_tot = total.lines.count,
        line_miss = total.lines.missed(),
        fn_pct = total.functions.percent(),
        fn_cov = total.functions.covered,
        fn_tot = total.functions.count,
        reg_pct = total.regions.percent(),
        reg_cov = total.regions.covered,
        reg_tot = total.regions.count,
        rows = rows,
        tot_lines = bar_html(total.lines),
        tot_funcs = bar_html(total.functions),
        tot_regions = bar_html(total.regions),
    );

    fs::write(html_dir.join("index.html"), html).expect("Failed to write directory index.html");
}

fn open_in_browser(path: &Path) {
    #[cfg(target_os = "macos")]
    let _ = Command::new("open").arg(path).status();
    #[cfg(target_os = "linux")]
    let _ = Command::new("xdg-open").arg(path).status();
    #[cfg(target_os = "windows")]
    let _ = Command::new("cmd").args(["/C", "start", ""]).arg(path).status();
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let open_browser = args.iter().any(|a| a == "--open");
    let generate_html = open_browser || args.iter().any(|a| a == "--html");
    let summary_only = args.iter().any(|a| a == "--summary-only");
    let no_run = args.iter().any(|a| a == "--no-run");

    if !no_run {
        run_llvm_cov(generate_html);
    }

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let export = load_coverage_json();
    let (dirs, total) = group_by_directory(export, &manifest_dir);

    print_terminal_summary(&dirs, &total, !summary_only);

    if generate_html {
        let html_dir = manifest_dir.join("target/llvm-cov/html");
        write_light_mode_css(&html_dir);
        write_directory_index_html(&html_dir, &dirs, &total);
        let index_path = html_dir.join("index.html");
        println!("Light-mode directory coverage report: {}", index_path.display());
        if open_browser {
            open_in_browser(&index_path);
        }
    }
}
