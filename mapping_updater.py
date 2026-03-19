#!/usr/bin/env python3
"""
JSON Connector Mapping Updater
Upload a connector JSON + key-value CSV/Excel → download updated JSON
Runs on http://localhost:8081
"""

import json
import io
import re
import copy
from http.server import HTTPServer, BaseHTTPRequestHandler


def parse_multipart(body: bytes, content_type: str) -> dict:
    """Parse multipart/form-data body (no external deps).
    Returns {field_name: (filename_or_None, bytes)}."""
    boundary_match = re.search(r'boundary=(?:"([^"]+)"|([^;\s]+))', content_type)
    if not boundary_match:
        raise ValueError("Missing boundary in Content-Type header")

    boundary = boundary_match.group(1) or boundary_match.group(2)
    boundary_bytes = ("--" + boundary).encode("utf-8")

    fields = {}
    parts = body.split(boundary_bytes)

    for part in parts:
        part = part.strip(b"\r\n")
        if not part or part == b"--":
            continue
        if part.endswith(b"--"):
            part = part[:-2].rstrip(b"\r\n")

        headers_blob, sep, data = part.partition(b"\r\n\r\n")
        if not sep:
            continue

        headers_text = headers_blob.decode("utf-8", errors="replace")
        data = data.rstrip(b"\r\n")

        name = filename = None
        for line in headers_text.split("\r\n"):
            if line.lower().startswith("content-disposition:"):
                name_match = re.search(r'name="([^"]+)"', line)
                file_match = re.search(r'filename="([^"]*)"', line)
                if name_match:
                    name = name_match.group(1)
                if file_match:
                    filename = file_match.group(1)

        if name:
            fields[name] = (filename, data)

    return fields

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Connector JSON Mapping Updater</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&family=Inter:wght@400;500;600&display=swap');

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --bg: #0c0c0f;
    --surface: #13131a;
    --border: #22222e;
    --border-hover: #3a3a50;
    --accent: #f5c842;
    --accent-dim: rgba(245,200,66,0.12);
    --green: #3ecf8e;
    --green-dim: rgba(62,207,142,0.1);
    --red: #e05c5c;
    --text: #e2e2e8;
    --muted: #5a5a72;
    --mono: 'JetBrains Mono', monospace;
    --sans: 'Inter', sans-serif;
  }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    min-height: 100vh;
    padding: 48px 24px;
  }

  .container { max-width: 820px; margin: 0 auto; }

  header { margin-bottom: 48px; }
  .badge {
    display: inline-flex; align-items: center; gap: 7px;
    background: var(--accent-dim); border: 1px solid rgba(245,200,66,0.25);
    color: var(--accent); font-family: var(--mono); font-size: 10px;
    letter-spacing: 2px; text-transform: uppercase;
    padding: 5px 12px; border-radius: 20px; margin-bottom: 16px;
  }
  .badge::before { content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--accent); animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }

  h1 { font-size: 32px; font-weight: 700; color: #fff; line-height: 1.2; margin-bottom: 8px; }
  .subtitle { color: var(--muted); font-size: 14px; line-height: 1.6; }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 28px;
    margin-bottom: 20px;
    transition: border-color 0.2s;
  }
  .card:hover { border-color: var(--border-hover); }

  .card-header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 20px;
  }
  .card-icon {
    width: 32px; height: 32px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 15px;
  }
  .icon-json { background: rgba(245,200,66,0.15); }
  .icon-csv  { background: rgba(62,207,142,0.15); }

  .card-title { font-size: 13px; font-weight: 600; color: #fff; }
  .card-desc  { font-size: 11px; color: var(--muted); margin-top: 2px; }

  .drop-zone {
    border: 1.5px dashed var(--border);
    border-radius: 8px;
    padding: 36px 24px;
    text-align: center;
    cursor: pointer;
    transition: all 0.2s;
    position: relative;
  }
  .drop-zone:hover, .drop-zone.dragover {
    border-color: var(--accent);
    background: var(--accent-dim);
  }
  .drop-zone input[type=file] {
    position: absolute; inset: 0; opacity: 0; cursor: pointer; width: 100%; height: 100%;
  }
  .drop-icon { font-size: 28px; margin-bottom: 10px; }
  .drop-label { font-size: 13px; color: var(--muted); }
  .drop-label span { color: var(--accent); font-weight: 600; }
  .drop-ext { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 6px; }

  .file-selected {
    display: none;
    align-items: center; gap: 10px;
    background: var(--green-dim); border: 1px solid rgba(62,207,142,0.25);
    border-radius: 8px; padding: 12px 16px; margin-top: 12px;
    font-family: var(--mono); font-size: 12px; color: var(--green);
  }
  .file-selected.show { display: flex; }

  .detected-table {
    display: none; align-items: center; gap: 8px;
    background: rgba(90,90,114,0.12); border: 1px solid var(--border-hover);
    border-radius: 8px; padding: 10px 16px; margin-top: 12px;
    font-family: var(--mono); font-size: 12px;
  }
  .detected-table.show { display: flex; }
  .detected-label { color: var(--muted); }
  .detected-value { color: var(--accent); font-weight: 600; margin-left: 4px; }

  .replace-section {
    display: none; margin-top: 16px; padding-top: 16px;
    border-top: 1px solid var(--border);
  }
  .replace-section.show { display: block; }
  .checkbox-row { display: flex; align-items: center; gap: 10px; cursor: pointer; user-select: none; }
  .checkbox-row input[type=checkbox] { width: 15px; height: 15px; accent-color: var(--accent); cursor: pointer; flex-shrink: 0; }
  .checkbox-label { font-size: 13px; color: var(--text); }
  .new-table-input { display: none; margin-top: 14px; }
  .new-table-input.show { display: block; }
  .text-input {
    width: 100%; background: var(--bg); border: 1.5px solid var(--border);
    border-radius: 8px; padding: 11px 14px; font-family: var(--mono);
    font-size: 13px; color: var(--text); outline: none; transition: border-color 0.2s;
  }
  .text-input:focus { border-color: var(--accent); }
  .text-input::placeholder { color: var(--muted); }

  .btn-submit {
    width: 100%;
    background: var(--accent);
    color: #000;
    border: none;
    border-radius: 8px;
    padding: 15px;
    font-family: var(--sans);
    font-size: 15px;
    font-weight: 700;
    cursor: pointer;
    letter-spacing: 0.5px;
    transition: all 0.2s;
    margin-top: 8px;
  }
  .btn-submit:hover { background: #ffd84d; transform: translateY(-1px); box-shadow: 0 8px 24px rgba(245,200,66,0.25); }
  .btn-submit:active { transform: translateY(0); }
  .btn-submit:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }

  .alert {
    padding: 14px 18px; border-radius: 8px;
    font-size: 13px; margin-bottom: 20px; display: flex; gap: 10px; align-items: flex-start;
  }
  .alert-error { background: rgba(224,92,92,0.1); border: 1px solid rgba(224,92,92,0.3); color: #e05c5c; }
  .alert-success { background: var(--green-dim); border: 1px solid rgba(62,207,142,0.3); color: var(--green); }

  .mapping-preview {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 16px;
    margin-top: 16px;
    font-family: var(--mono);
    font-size: 11px;
    color: var(--muted);
    max-height: 200px;
    overflow-y: auto;
  }
  .mapping-preview .kv { display: flex; gap: 12px; padding: 3px 0; border-bottom: 1px solid #1a1a22; }
  .mapping-preview .kv:last-child { border: none; }
  .mapping-preview .k { color: var(--accent); min-width: 160px; }
  .mapping-preview .v { color: var(--green); }
  .mapping-preview .arrow { color: #333; }

  .divider { border: none; border-top: 1px solid var(--border); margin: 28px 0; }

  .how-it-works {
    background: rgba(255,255,255,0.02);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 20px 24px;
    margin-top: 28px;
  }
  .how-title { font-size: 11px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; margin-bottom: 14px; }
  .steps { display: flex; gap: 0; }
  .step { flex: 1; padding: 0 16px 0 0; border-right: 1px solid var(--border); margin-right: 16px; }
  .step:last-child { border: none; margin: 0; padding: 0; }
  .step-num { font-family: var(--mono); font-size: 20px; font-weight: 700; color: var(--border-hover); margin-bottom: 4px; }
  .step-text { font-size: 12px; color: var(--muted); line-height: 1.5; }
  .step-text strong { color: var(--text); }

  .paste-area {
    width: 100%; min-height: 160px; resize: vertical;
    background: var(--bg); border: 1.5px solid var(--border);
    border-radius: 8px; padding: 14px 16px;
    font-family: var(--mono); font-size: 12px; color: var(--text);
    outline: none; transition: border-color 0.2s; line-height: 1.7;
  }
  .paste-area:focus { border-color: var(--accent); }
  .paste-area::placeholder { color: var(--muted); }
  .pair-count {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-top: 8px; text-align: right;
  }
  .pair-count.has-data { color: var(--green); }
</style>
</head>
<body>
<div class="container">

  <header>
    <div class="badge">connector tool</div>
    <h1>Connector JSON Mapping Updater</h1>
    <p class="subtitle">Upload a connector JSON, then paste your key→value columns copied from Excel.<br>The tool replaces the mapping inside config — nothing else changes.</p>
  </header>

  {alert_html}

  <form method="POST" action="/update" enctype="multipart/form-data" id="mainForm">

    <div class="card">
      <div class="card-header">
        <div class="card-icon icon-json">📄</div>
        <div>
          <div class="card-title">Connector JSON</div>
          <div class="card-desc">Your connector configuration file</div>
        </div>
      </div>
      <div class="drop-zone" id="jsonZone">
        <input type="file" name="json_file" id="jsonFile" accept=".json" required>
        <div class="drop-icon">{ }</div>
        <div class="drop-label">Drop JSON file or <span>browse</span></div>
        <div class="drop-ext">.json</div>
      </div>
      <div class="file-selected" id="jsonSelected">✓ <span id="jsonName"></span></div>
      <div class="detected-table" id="detectedTableWrap">
        <span class="detected-label">Detected table →</span>
        <span class="detected-value" id="detectedTableVal"></span>
      </div>
      <div class="replace-section" id="replaceTableSection">
        <label class="checkbox-row">
          <input type="checkbox" id="replaceTableCheck" name="replace_table" value="1">
          <span class="checkbox-label">Replace table name in output</span>
        </label>
        <input type="hidden" name="detected_table" id="detectedTableInput" value="">
        <div class="new-table-input" id="newTableWrap">
          <input type="text" name="new_table" id="newTableInput" class="text-input"
                 placeholder="New table name…" autocomplete="off">
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-icon icon-csv">⇄</div>
        <div>
          <div class="card-title">Key → Value Mapping</div>
          <div class="card-desc">Select the two columns in Excel (key + value), copy, and paste below</div>
        </div>
      </div>
      <textarea
        name="mapping_text"
        id="mappingText"
        class="paste-area"
        placeholder="Paste your Excel cells here…&#10;&#10;plant&#9;{{.plant}}&#10;material&#9;{{.material}}&#10;movetype&#9;{{.movetype}}"
        spellcheck="false"
      ></textarea>
      <div class="pair-count" id="pairCount"></div>
      <div class="mapping-preview" id="mappingPreview" style="display:none"></div>
    </div>

    <button type="submit" class="btn-submit" id="submitBtn" disabled>
      ⚡ Update Mapping &amp; Download JSON
    </button>
  </form>

  <div class="how-it-works">
    <div class="how-title">How it works</div>
    <div class="steps">
      <div class="step">
        <div class="step-num">01</div>
        <div class="step-text"><strong>Upload</strong> your connector JSON</div>
      </div>
      <div class="step">
        <div class="step-num">02</div>
        <div class="step-text"><strong>Copy</strong> two columns from Excel and <strong>paste</strong> into the mapping area</div>
      </div>
      <div class="step">
        <div class="step-num">03</div>
        <div class="step-text"><strong>Download</strong> updated JSON — only <code>mapping</code> is changed</div>
      </div>
    </div>
  </div>

</div>

<script>
  const jsonFile   = document.getElementById('jsonFile');
  const jsonZone   = document.getElementById('jsonZone');
  const mappingText = document.getElementById('mappingText');
  const submitBtn  = document.getElementById('submitBtn');

  function checkReady() {
    submitBtn.disabled = !(jsonFile.files.length && mappingText.value.trim().length > 0);
  }

  // ── JSON upload: show filename + detect table name ──────────────────────
  jsonFile.addEventListener('change', () => {
    if (jsonFile.files.length) {
      document.getElementById('jsonSelected').classList.add('show');
      document.getElementById('jsonName').textContent = jsonFile.files[0].name;
      // Hide drop zone text, show as "loaded"
      jsonZone.querySelector('.drop-icon').textContent = '✓';
      jsonZone.querySelector('.drop-label').innerHTML = 'File loaded: <span>' + jsonFile.files[0].name + '</span>';
      jsonZone.querySelector('.drop-ext').textContent = '';
      const reader = new FileReader();
      reader.onload = (e) => {
        const wrap = document.getElementById('detectedTableWrap');
        const sec  = document.getElementById('replaceTableSection');
        try {
          const parsed = JSON.parse(e.target.result);
          const tbl = findTableInJson(parsed);
          if (tbl) {
            document.getElementById('detectedTableVal').textContent = tbl;
            document.getElementById('detectedTableInput').value = tbl;
            wrap.classList.add('show');
            sec.classList.add('show');
          } else {
            wrap.classList.remove('show');
            sec.classList.remove('show');
          }
        } catch(err) {
          console.error('JSON parse error:', err);
          wrap.classList.remove('show');
          sec.classList.remove('show');
        }
      };
      reader.onerror = () => console.error('FileReader error');
      reader.readAsText(jsonFile.files[0]);
    }
    checkReady();
  });

  // ── Paste area: live preview ─────────────────────────────────────────────
  mappingText.addEventListener('input', () => {
    updatePreview();
    checkReady();
  });

  function parsePasted(text) {
    const pairs = [];
    for (const line of text.split(/\\r?\\n/)) {
      const delim = line.includes('\\t') ? '\\t' : ',';
      const idx = line.indexOf(delim);
      if (idx === -1) continue;
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      if (!k || ['key','keys','value','values'].includes(k.toLowerCase())) continue;
      pairs.push([k, v]);
    }
    return pairs;
  }

  function escHtml(s) {
    return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }

  function updatePreview() {
    const preview  = document.getElementById('mappingPreview');
    const counter  = document.getElementById('pairCount');
    const pairs    = parsePasted(mappingText.value);
    if (!pairs.length) {
      preview.style.display = 'none';
      counter.textContent = '';
      counter.className = 'pair-count';
      return;
    }
    counter.textContent = pairs.length + ' pair' + (pairs.length !== 1 ? 's' : '') + ' detected';
    counter.className = 'pair-count has-data';
    preview.style.display = 'block';
    preview.innerHTML = pairs.map(([k, v]) =>
      `<div class="kv"><span class="k">${escHtml(k)}</span>` +
      `<span class="arrow">→</span><span class="v">${escHtml(v)}</span></div>`
    ).join('');
  }

  // ── Drag and drop (JSON zone only) ───────────────────────────────────────
  jsonZone.addEventListener('dragover', e => { e.preventDefault(); jsonZone.classList.add('dragover'); });
  jsonZone.addEventListener('dragleave', () => jsonZone.classList.remove('dragover'));
  jsonZone.addEventListener('drop', e => {
    e.preventDefault();
    jsonZone.classList.remove('dragover');
    const file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (!file) return;
    const dt = new DataTransfer();
    dt.items.add(file);
    jsonFile.files = dt.files;
    handleJsonFile(file);
  });

  // ── Table replace toggle ─────────────────────────────────────────────────
  document.getElementById('replaceTableCheck').addEventListener('change', function() {
    document.getElementById('newTableWrap').classList.toggle('show', this.checked);
    if (!this.checked) document.getElementById('newTableInput').value = '';
  });

  // ── Walk parsed JSON to find first "table" inside a config string ─────────
  function findTableInJson(obj) {
    if (Array.isArray(obj)) {
      for (const item of obj) { const r = findTableInJson(item); if (r) return r; }
    } else if (obj && typeof obj === 'object') {
      for (const [k, v] of Object.entries(obj)) {
        if (typeof v === 'string') {
          // Try to parse as embedded JSON string (e.g. the "config" field)
          try {
            const inner = JSON.parse(v);
            if (inner && typeof inner === 'object' && inner.table)
              return String(inner.table);
          } catch(_) {}
        } else if (v && typeof v === 'object') {
          // Recurse into nested objects/arrays
          const r = findTableInJson(v); if (r) return r;
        }
      }
    }
    return null;
  }
</script>
</body>
</html>
"""


def parse_mapping_text(text: str) -> dict:
    """Parse tab- or comma-separated key→value text pasted from Excel/CSV."""
    skip = {"key", "keys", "value", "values"}
    mapping = {}
    for line in text.splitlines():
        delim = "\t" if "\t" in line else ","
        idx = line.find(delim)
        if idx == -1:
            continue
        k = line[:idx].strip()
        v = line[idx + 1:].strip()
        if not k or k.lower() in skip:
            continue
        mapping[k] = v
    return mapping


def replace_mapping_in_json(obj, new_mapping: dict):
    """Recursively find and replace (or inject) every 'mapping' key in the
    structure, including inside JSON-encoded 'config' strings."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "mapping" and isinstance(v, dict):
                # Direct mapping object — replace in-place
                obj[k] = new_mapping
            elif k == "config" and isinstance(v, str):
                # config is a JSON-encoded string — parse, upsert, re-encode
                try:
                    cfg = json.loads(v)
                    if isinstance(cfg, dict):
                        # Replace or inject the mapping key
                        cfg["mapping"] = new_mapping
                        obj[k] = json.dumps(cfg, ensure_ascii=False)
                except (json.JSONDecodeError, TypeError):
                    pass
            else:
                replace_mapping_in_json(v, new_mapping)
    elif isinstance(obj, list):
        for item in obj:
            replace_mapping_in_json(item, new_mapping)
    return obj


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        try:
            print(f"  {self.address_string()} \u2192 {format % args}")
        except Exception:
            print(f"  {self.address_string()} \u2192 {format}")

    def send_html(self, html: str, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            self.send_html(HTML.replace("{alert_html}", ""))
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        if self.path != "/update":
            self.send_response(404)
            self.end_headers()
            return

        content_type = self.headers.get("Content-Type", "")
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            # Parse multipart form data (pure stdlib, works on Python 3.13+)
            fields = parse_multipart(body, content_type)

            if "json_file" not in fields:
                raise ValueError("No JSON file received. Please upload a connector JSON.")

            # Read uploaded JSON
            _, json_data = fields["json_file"]

            # Parse JSON
            try:
                connector = json.loads(json_data.decode("utf-8-sig", errors="replace"))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON file: {e}")

            # Read pasted mapping text
            _, mapping_text_b = fields.get("mapping_text", (None, b""))
            mapping_text = mapping_text_b.decode("utf-8", errors="replace")

            # Parse mapping
            new_mapping = parse_mapping_text(mapping_text)
            if not new_mapping:
                raise ValueError("No key-value pairs found. Copy exactly two columns from Excel (key column + value column) and paste into the mapping area.")

            # Apply mapping
            updated = replace_mapping_in_json(copy.deepcopy(connector), new_mapping)
            output_str = json.dumps(updated, indent=2, ensure_ascii=False)

            # Optional: replace table name everywhere in the serialized output
            _, replace_flag = fields.get("replace_table", (None, b""))
            if replace_flag.decode("utf-8", errors="replace").strip() == "1":
                _, old_tbl_b = fields.get("detected_table", (None, b""))
                _, new_tbl_b = fields.get("new_table",      (None, b""))
                old_tbl = old_tbl_b.decode("utf-8", errors="replace").strip()
                new_tbl = new_tbl_b.decode("utf-8", errors="replace").strip()
                if old_tbl and new_tbl and old_tbl != new_tbl:
                    output_str = output_str.replace(old_tbl, new_tbl)

            output_bytes = output_str.encode("utf-8")

            # Send file download
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Disposition", 'attachment; filename="updated_connector.json"')
            self.send_header("Content-Length", str(len(output_bytes)))
            self.end_headers()
            self.wfile.write(output_bytes)

        except Exception as e:
            alert = f'<div class="alert alert-error">⚠ {str(e)}</div>'
            self.send_html(HTML.replace("{alert_html}", alert))


def main():
    port = 8081
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"""
╔══════════════════════════════════════════════╗
║       Connector JSON Mapping Updater — Ready           ║
╠══════════════════════════════════════════════╣
║  Open:  http://localhost:{port}                 ║
║  Stop:  Ctrl+C                               ║
╚══════════════════════════════════════════════╝

  Upload JSON → Paste mapping from Excel → Download updated JSON
""")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()