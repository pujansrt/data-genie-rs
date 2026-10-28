use serde::Serialize;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

#[derive(Serialize, Default, Clone)]
struct FileCoverage {
    package: String,
    file_name: String,
    rel_path: String,
    lines_found: usize,
    lines_hit: usize,
    functions_found: usize,
    functions_hit: usize,
    line_hits: BTreeMap<usize, u64>,
    source_lines: Vec<String>,
}

#[derive(Serialize, Default)]
struct PackageSummary {
    name: String,
    files: Vec<FileCoverage>,
    lines_found: usize,
    lines_hit: usize,
    functions_found: usize,
    functions_hit: usize,
}

fn main() {
    let args: Vec<String> = env::args().skip(1).collect();
    let skip_run = args.iter().any(|a| a == "--no-run");
    let open_browser = args.iter().any(|a| a == "--open");

    let lcov_path = PathBuf::from("target/lcov.info");

    if !skip_run || !lcov_path.exists() {
        println!("Running cargo llvm-cov --all-features...");
        let status = Command::new("cargo")
            .args(["llvm-cov", "--all-features", "--lcov", "--output-path", "target/lcov.info"])
            .status()
            .expect("Failed to execute cargo llvm-cov");
        if !status.success() {
            std::process::exit(status.code().unwrap_or(1));
        }
    }

    let lcov_content = fs::read_to_string(&lcov_path).expect("Unable to read target/lcov.info");
    let packages = parse_lcov(&lcov_content);

    let out_dir = PathBuf::from("target/coverage");
    fs::create_dir_all(&out_dir).expect("Failed to create target/coverage");
    let html_path = out_dir.join("index.html");

    let html = render_html(&packages);
    fs::write(&html_path, html).expect("Failed to write target/coverage/index.html");

    let abs_path = fs::canonicalize(&html_path).unwrap_or(html_path.clone());
    println!("\n✅ JaCoCo-style Coverage Report generated: {}", abs_path.display());

    if open_browser {
        let _ = Command::new("open").arg(&abs_path).status();
    }
}

fn parse_lcov(content: &str) -> Vec<PackageSummary> {
    let mut files_map: BTreeMap<String, FileCoverage> = BTreeMap::new();
    let mut current_sf: Option<String> = None;
    let mut fn_hits: BTreeMap<String, u64> = BTreeMap::new();

    for line in content.lines() {
        if let Some(sf) = line.strip_prefix("SF:") {
            let rel = normalize_rel_path(sf);
            current_sf = Some(rel.clone());
            fn_hits.clear();
            files_map.entry(rel.clone()).or_insert_with(|| {
                let (pkg, fname) = split_package_and_file(&rel);
                let src = fs::read_to_string(sf).or_else(|_| fs::read_to_string(&rel)).unwrap_or_default();
                let source_lines = src.lines().map(String::from).collect();
                FileCoverage {
                    package: pkg,
                    file_name: fname,
                    rel_path: rel,
                    source_lines,
                    ..Default::default()
                }
            });
        } else if let Some(fnda) = line.strip_prefix("FNDA:") {
            if let Some((count_str, fn_name)) = fnda.split_once(',') {
                if let Ok(c) = count_str.parse::<u64>() {
                    let entry = fn_hits.entry(fn_name.to_string()).or_insert(0);
                    *entry = (*entry).max(c);
                }
            }
        } else if let Some(da) = line.strip_prefix("DA:") {
            if let Some(ref rel) = current_sf {
                if let Some((line_str, count_str)) = da.split_once(',') {
                    let count_clean = count_str.split(',').next().unwrap_or("0");
                    if let (Ok(line_no), Ok(count)) = (line_str.parse::<usize>(), count_clean.parse::<u64>()) {
                        if let Some(fc) = files_map.get_mut(rel) {
                            let prev = fc.line_hits.entry(line_no).or_insert(0);
                            *prev = (*prev).max(count);
                        }
                    }
                }
            }
        } else if let Some(fnf) = line.strip_prefix("FNF:") {
            if let Some(ref rel) = current_sf {
                if let Ok(n) = fnf.parse::<usize>() {
                    if let Some(fc) = files_map.get_mut(rel) {
                        fc.functions_found = fc.functions_found.max(n);
                    }
                }
            }
        } else if let Some(fnh) = line.strip_prefix("FNH:") {
            if let Some(ref rel) = current_sf {
                if let Ok(n) = fnh.parse::<usize>() {
                    if let Some(fc) = files_map.get_mut(rel) {
                        fc.functions_hit = fc.functions_hit.max(n);
                    }
                }
            }
        } else if line == "end_of_record" {
            if let Some(ref rel) = current_sf {
                if let Some(fc) = files_map.get_mut(rel) {
                    fc.lines_found = fc.line_hits.len();
                    fc.lines_hit = fc.line_hits.values().filter(|&&c| c > 0).count();
                    if !fn_hits.is_empty() && fc.functions_found == 0 {
                        fc.functions_found = fn_hits.len();
                        fc.functions_hit = fn_hits.values().filter(|&&c| c > 0).count();
                    }
                }
            }
            current_sf = None;
            fn_hits.clear();
        }
    }

    let mut pkg_map: BTreeMap<String, PackageSummary> = BTreeMap::new();
    for (_, fc) in files_map {
        let entry = pkg_map.entry(fc.package.clone()).or_insert_with(|| PackageSummary {
            name: fc.package.clone(),
            ..Default::default()
        });
        entry.lines_found += fc.lines_found;
        entry.lines_hit += fc.lines_hit;
        entry.functions_found += fc.functions_found;
        entry.functions_hit += fc.functions_hit;
        entry.files.push(fc);
    }

    pkg_map.into_values().collect()
}

fn normalize_rel_path(sf: &str) -> String {
    if let Some(idx) = sf.find("/src/") {
        sf[idx + 1..].to_string()
    } else {
        Path::new(sf).file_name().and_then(|s| s.to_str()).unwrap_or(sf).to_string()
    }
}

fn split_package_and_file(rel: &str) -> (String, String) {
    let trimmed = rel.strip_prefix("src/").unwrap_or(rel);
    if let Some((pkg, file)) = trimmed.rsplit_once('/') {
        (format!("data_genie::{}", pkg.replace('/', "::")), file.to_string())
    } else {
        ("data_genie (root)".to_string(), trimmed.to_string())
    }
}

fn render_html(packages: &[PackageSummary]) -> String {
    let json_data = serde_json::to_string(packages).expect("Failed to serialize coverage JSON");
    format!(
        r##"<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>data-genie — Code Coverage Report</title>
  <style>
    :root, [data-theme="light"] {{
      --bg: #f8fafc;
      --surface: #ffffff;
      --surface-alt: #f1f5f9;
      --border: #e2e8f0;
      --text: #0f172a;
      --text-muted: #64748b;
      --primary: #2563eb;
      --green: #16a34a;
      --red: #dc2626;
      --cov-hit-bg: #dcfce7;
      --cov-hit-border: #22c55e;
      --cov-miss-bg: #fee2e2;
      --cov-miss-border: #ef4444;
      --code-bg: #ffffff;
    }}
    [data-theme="dark"] {{
      --bg: #0f172a;
      --surface: #1e293b;
      --surface-alt: #334155;
      --border: #334155;
      --text: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #60a5fa;
      --green: #22c55e;
      --red: #ef4444;
      --cov-hit-bg: rgba(34, 197, 94, 0.18);
      --cov-hit-border: #22c55e;
      --cov-miss-bg: rgba(239, 68, 68, 0.22);
      --cov-miss-border: #ef4444;
      --code-bg: #0b1120;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
    }}
    header {{
      background: var(--surface);
      border-bottom: 1px solid var(--border);
      padding: 0.85rem 1.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
      position: sticky;
      top: 0;
      z-index: 20;
    }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 0.75rem;
      font-weight: 700;
      font-size: 1.1rem;
    }}
    .badge {{
      font-size: 0.75rem;
      padding: 0.15rem 0.5rem;
      border-radius: 999px;
      background: var(--surface-alt);
      color: var(--text-muted);
      font-weight: 600;
    }}
    .controls {{
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }}
    input.search {{
      padding: 0.4rem 0.75rem;
      border-radius: 6px;
      border: 1px solid var(--border);
      background: var(--bg);
      color: var(--text);
      font-size: 0.85rem;
      width: 220px;
    }}
    button.theme-btn {{
      padding: 0.4rem 0.8rem;
      border-radius: 6px;
      border: 1px solid var(--border);
      background: var(--surface-alt);
      color: var(--text);
      cursor: pointer;
      font-size: 0.85rem;
      font-weight: 600;
    }}
    main {{
      max-width: 1280px;
      margin: 1.25rem auto;
      padding: 0 1.5rem 3rem;
    }}
    .breadcrumbs {{
      display: flex;
      align-items: center;
      gap: 0.4rem;
      font-size: 0.9rem;
      margin-bottom: 1rem;
      padding: 0.6rem 1rem;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
    }}
    .breadcrumbs a {{
      color: var(--primary);
      text-decoration: none;
      font-weight: 600;
      cursor: pointer;
    }}
    .breadcrumbs a:hover {{ text-decoration: underline; }}
    .summary-cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 1rem;
      margin-bottom: 1.25rem;
    }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 1rem 1.25rem;
    }}
    .card-label {{
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--text-muted);
      font-weight: 600;
    }}
    .card-value {{
      font-size: 1.65rem;
      font-weight: 700;
      margin-top: 0.2rem;
    }}
    .card-sub {{
      font-size: 0.8rem;
      color: var(--text-muted);
    }}
    table.cov-table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--surface);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow: hidden;
      font-size: 0.88rem;
    }}
    .cov-table th, .cov-table td {{
      padding: 0.65rem 0.9rem;
      border-bottom: 1px solid var(--border);
      text-align: left;
    }}
    .cov-table th {{
      background: var(--surface-alt);
      font-weight: 600;
      color: var(--text-muted);
      cursor: pointer;
      user-select: none;
      font-size: 0.78rem;
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }}
    .cov-table tr:hover td {{
      background: var(--surface-alt);
    }}
    .cov-table tfoot td {{
      background: var(--surface-alt);
      font-weight: 700;
      border-top: 2px solid var(--border);
    }}
    .num {{ text-align: right !important; font-variant-numeric: tabular-nums; }}
    .link {{
      color: var(--primary);
      font-weight: 600;
      cursor: pointer;
      text-decoration: none;
      display: inline-flex;
      align-items: center;
      gap: 0.4rem;
    }}
    .link:hover {{ text-decoration: underline; }}
    .bar-wrap {{
      width: 140px;
      height: 12px;
      background: var(--red);
      border-radius: 3px;
      overflow: hidden;
      display: inline-block;
      vertical-align: middle;
    }}
    .bar-fill {{
      height: 100%;
      background: var(--green);
    }}
    .source-box {{
      background: var(--code-bg);
      border: 1px solid var(--border);
      border-radius: 8px;
      overflow-x: auto;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
      font-size: 0.82rem;
    }}
    .src-row {{
      display: flex;
      min-width: max-content;
    }}
    .src-row.hit {{
      background: var(--cov-hit-bg);
      border-left: 3px solid var(--cov-hit-border);
    }}
    .src-row.miss {{
      background: var(--cov-miss-bg);
      border-left: 3px solid var(--cov-miss-border);
    }}
    .src-row.neutral {{
      border-left: 3px solid transparent;
    }}
    .src-ln {{
      width: 52px;
      text-align: right;
      padding: 0 0.6rem;
      color: var(--text-muted);
      user-select: none;
      border-right: 1px solid var(--border);
      flex-shrink: 0;
    }}
    .src-cnt {{
      width: 56px;
      text-align: right;
      padding: 0 0.5rem;
      font-weight: 600;
      color: var(--text-muted);
      user-select: none;
      border-right: 1px solid var(--border);
      flex-shrink: 0;
    }}
    .src-code {{
      padding: 0 0.85rem;
      white-space: pre;
    }}
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <span>🧞‍♂️ data-genie Coverage Report</span>
      <span class="badge">JaCoCo Package View</span>
    </div>
    <div class="controls">
      <input type="text" id="searchInput" class="search" placeholder="Filter package or file..." />
      <button class="theme-btn" id="themeBtn" onclick="toggleTheme()">🌙 Dark Mode</button>
    </div>
  </header>

  <main>
    <nav class="breadcrumbs" id="breadcrumbs"></nav>
    <section class="summary-cards" id="summaryCards"></section>
    <section id="viewContainer"></section>
  </main>

  <script>
    const PACKAGES = {json_data};
    let state = {{ view: 'packages', pkgName: null, fileName: null, filter: '', sortCol: 'name', sortAsc: true }};

    function pct(hit, found) {{
      return found === 0 ? 100 : (hit / found) * 100;
    }}
    function fmtPct(hit, found) {{
      return pct(hit, found).toFixed(2) + '%';
    }}
    function renderBar(hit, found) {{
      const p = pct(hit, found).toFixed(1);
      return `<div class="bar-wrap" title="${{p}}% covered"><div class="bar-fill" style="width:${{p}}%"></div></div>`;
    }}
    function escHtml(s) {{
      return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }}

    function toggleTheme() {{
      const root = document.documentElement;
      const next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
      root.setAttribute('data-theme', next);
      localStorage.setItem('dg_cov_theme', next);
      document.getElementById('themeBtn').textContent = next === 'dark' ? '☀️ Light Mode' : '🌙 Dark Mode';
    }}

    (function initTheme() {{
      const saved = localStorage.getItem('dg_cov_theme') || 'light';
      document.documentElement.setAttribute('data-theme', saved);
      document.getElementById('themeBtn').textContent = saved === 'dark' ? '☀️ Light Mode' : '🌙 Dark Mode';
    }})();

    document.getElementById('searchInput').addEventListener('input', (e) => {{
      state.filter = e.target.value.toLowerCase();
      render();
    }});

    function navPackages() {{
      state.view = 'packages';
      state.pkgName = null;
      state.fileName = null;
      render();
    }}
    function navPackage(name) {{
      state.view = 'package';
      state.pkgName = name;
      state.fileName = null;
      render();
    }}
    function navFile(pkgName, fileName) {{
      state.view = 'file';
      state.pkgName = pkgName;
      state.fileName = fileName;
      render();
    }}
    function setSort(col) {{
      if (state.sortCol === col) state.sortAsc = !state.sortAsc;
      else {{ state.sortCol = col; state.sortAsc = col === 'name'; }}
      render();
    }}

    function sortItems(items, nameKey) {{
      const dir = state.sortAsc ? 1 : -1;
      return [...items].sort((a, b) => {{
        if (state.sortCol === 'name') return a[nameKey].localeCompare(b[nameKey]) * dir;
        if (state.sortCol === 'linePct') return (pct(a.lines_hit, a.lines_found) - pct(b.lines_hit, b.lines_found)) * dir;
        if (state.sortCol === 'missedLines') return ((a.lines_found - a.lines_hit) - (b.lines_found - b.lines_hit)) * dir;
        if (state.sortCol === 'fnPct') return (pct(a.functions_hit, a.functions_found) - pct(b.functions_hit, b.functions_found)) * dir;
        return 0;
      }});
    }}

    function renderSummaryCards(linesHit, linesFound, fnHit, fnFound, itemCount, itemLabel) {{
      const missedLines = linesFound - linesHit;
      const missedFns = fnFound - fnHit;
      document.getElementById('summaryCards').innerHTML = `
        <div class="card">
          <div class="card-label">Line Coverage</div>
          <div class="card-value">${{fmtPct(linesHit, linesFound)}}</div>
          <div class="card-sub">${{linesHit}} / ${{linesFound}} lines (${{missedLines}} missed)</div>
        </div>
        <div class="card">
          <div class="card-label">Function Coverage</div>
          <div class="card-value">${{fmtPct(fnHit, fnFound)}}</div>
          <div class="card-sub">${{fnHit}} / ${{fnFound}} functions (${{missedFns}} missed)</div>
        </div>
        <div class="card">
          <div class="card-label">${{itemLabel}}</div>
          <div class="card-value">${{itemCount}}</div>
          <div class="card-sub">Click any row to drill down</div>
        </div>
      `;
    }}

    function render() {{
      const bc = document.getElementById('breadcrumbs');
      const container = document.getElementById('viewContainer');

      if (state.view === 'packages') {{
        bc.innerHTML = `<span>📦 All Packages (data-genie)</span>`;
        const filtered = PACKAGES.filter(p =>
          p.name.toLowerCase().includes(state.filter) ||
          p.files.some(f => f.file_name.toLowerCase().includes(state.filter))
        );
        const sorted = sortItems(filtered, 'name');

        let totLH = 0, totLF = 0, totFH = 0, totFF = 0;
        sorted.forEach(p => {{ totLH += p.lines_hit; totLF += p.lines_found; totFH += p.functions_hit; totFF += p.functions_found; }});
        renderSummaryCards(totLH, totLF, totFH, totFF, sorted.length, 'Packages');

        let rows = sorted.map(p => `
          <tr>
            <td><a class="link" onclick="navPackage('${{p.name}}')">📦 ${{escHtml(p.name)}}</a></td>
            <td>${{renderBar(p.lines_hit, p.lines_found)}}</td>
            <td class="num">${{fmtPct(p.lines_hit, p.lines_found)}}</td>
            <td class="num">${{p.lines_found - p.lines_hit}}</td>
            <td class="num">${{p.lines_found}}</td>
            <td class="num">${{fmtPct(p.functions_hit, p.functions_found)}}</td>
            <td class="num">${{p.functions_found - p.functions_hit}}</td>
            <td class="num">${{p.functions_found}}</td>
            <td class="num">${{p.files.length}}</td>
          </tr>
        `).join('');

        container.innerHTML = `
          <table class="cov-table">
            <thead>
              <tr>
                <th onclick="setSort('name')">Package</th>
                <th onclick="setSort('linePct')">Coverage</th>
                <th class="num" onclick="setSort('linePct')">Lines %</th>
                <th class="num" onclick="setSort('missedLines')">Missed Lines</th>
                <th class="num">Total Lines</th>
                <th class="num" onclick="setSort('fnPct')">Functions %</th>
                <th class="num">Missed Fns</th>
                <th class="num">Total Fns</th>
                <th class="num">Files</th>
              </tr>
            </thead>
            <tbody>${{rows}}</tbody>
            <tfoot>
              <tr>
                <td>Total</td>
                <td>${{renderBar(totLH, totLF)}}</td>
                <td class="num">${{fmtPct(totLH, totLF)}}</td>
                <td class="num">${{totLF - totLH}}</td>
                <td class="num">${{totLF}}</td>
                <td class="num">${{fmtPct(totFH, totFF)}}</td>
                <td class="num">${{totFF - totFH}}</td>
                <td class="num">${{totFF}}</td>
                <td class="num">${{sorted.reduce((a, p) => a + p.files.length, 0)}}</td>
              </tr>
            </tfoot>
          </table>
        `;
      }} else if (state.view === 'package') {{
        const pkg = PACKAGES.find(p => p.name === state.pkgName);
        if (!pkg) return navPackages();
        bc.innerHTML = `<a onclick="navPackages()">📦 All Packages</a> <span>›</span> <span>📂 ${{escHtml(pkg.name)}}</span>`;

        const filtered = pkg.files.filter(f => f.file_name.toLowerCase().includes(state.filter));
        const sorted = sortItems(filtered, 'file_name');
        renderSummaryCards(pkg.lines_hit, pkg.lines_found, pkg.functions_hit, pkg.functions_found, sorted.length, 'Files in Package');

        let rows = sorted.map(f => `
          <tr>
            <td><a class="link" onclick="navFile('${{pkg.name}}', '${{f.file_name}}')">🦀 ${{escHtml(f.file_name)}}</a></td>
            <td>${{renderBar(f.lines_hit, f.lines_found)}}</td>
            <td class="num">${{fmtPct(f.lines_hit, f.lines_found)}}</td>
            <td class="num">${{f.lines_found - f.lines_hit}}</td>
            <td class="num">${{f.lines_found}}</td>
            <td class="num">${{fmtPct(f.functions_hit, f.functions_found)}}</td>
            <td class="num">${{f.functions_found - f.functions_hit}}</td>
            <td class="num">${{f.functions_found}}</td>
          </tr>
        `).join('');

        container.innerHTML = `
          <table class="cov-table">
            <thead>
              <tr>
                <th onclick="setSort('name')">Source File</th>
                <th onclick="setSort('linePct')">Coverage</th>
                <th class="num" onclick="setSort('linePct')">Lines %</th>
                <th class="num" onclick="setSort('missedLines')">Missed Lines</th>
                <th class="num">Total Lines</th>
                <th class="num" onclick="setSort('fnPct')">Functions %</th>
                <th class="num">Missed Fns</th>
                <th class="num">Total Fns</th>
              </tr>
            </thead>
            <tbody>${{rows}}</tbody>
            <tfoot>
              <tr>
                <td>Package Total</td>
                <td>${{renderBar(pkg.lines_hit, pkg.lines_found)}}</td>
                <td class="num">${{fmtPct(pkg.lines_hit, pkg.lines_found)}}</td>
                <td class="num">${{pkg.lines_found - pkg.lines_hit}}</td>
                <td class="num">${{pkg.lines_found}}</td>
                <td class="num">${{fmtPct(pkg.functions_hit, pkg.functions_found)}}</td>
                <td class="num">${{pkg.functions_found - pkg.functions_hit}}</td>
                <td class="num">${{pkg.functions_found}}</td>
              </tr>
            </tfoot>
          </table>
        `;
      }} else if (state.view === 'file') {{
        const pkg = PACKAGES.find(p => p.name === state.pkgName);
        const file = pkg && pkg.files.find(f => f.file_name === state.fileName);
        if (!file) return navPackages();

        bc.innerHTML = `
          <a onclick="navPackages()">📦 All Packages</a> <span>›</span>
          <a onclick="navPackage('${{pkg.name}}')">📂 ${{escHtml(pkg.name)}}</a> <span>›</span>
          <span>🦀 ${{escHtml(file.file_name)}}</span>
        `;
        renderSummaryCards(file.lines_hit, file.lines_found, file.functions_hit, file.functions_found, file.source_lines.length, 'Total Source Lines');

        const codeRows = file.source_lines.map((lineText, idx) => {{
          const ln = idx + 1;
          const hit = file.line_hits[ln];
          const cls = hit === undefined ? 'neutral' : (hit > 0 ? 'hit' : 'miss');
          const cntText = hit === undefined ? '' : (hit + 'x');
          return `<div class="src-row ${{cls}}"><span class="src-ln">${{ln}}</span><span class="src-cnt">${{cntText}}</span><span class="src-code">${{escHtml(lineText) || ' '}}</span></div>`;
        }}).join('');

        container.innerHTML = `<div class="source-box">${{codeRows}}</div>`;
      }}
    }}

    render();
  </script>
</body>
</html>
"##
    )
}
