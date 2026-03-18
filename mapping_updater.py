#!/usr/bin/env python3
"""
JSON Connector Mapping Updater
Upload a connector JSON + key-value CSV/Excel → download updated JSON
Runs on http://localhost:8081
"""

import json
import csv
import io
import copy
from http.server import HTTPServer, BaseHTTPRequestHandler

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False


def parse_multipart(body: bytes, content_type: str) -> dict:
    """Parse multipart/form-data body (no external deps).
    Returns {field_name: (filename_or_None, bytes)}."""
    boundary = None
    for part in content_type.split(";"):
        part = part.strip()
        if part.lower().startswith("boundary="):
            boundary = part[9:].strip('"')
            break
    if not boundary:
        raise ValueError("Missing boundary in Content-Type header")

    sep = ("--" + boundary).encode()
    fields = {}
    for chunk in body.split(sep)[1:]:          # skip preamble
        if chunk.lstrip(b"\r\n").startswith(b"--"):  # final --boundary--
            break
        if chunk.startswith(b"\r\n"):
            chunk = chunk[2:]
        if chunk.endswith(b"\r\n"):
            chunk = chunk[:-2]
        hdr_end = chunk.find(b"\r\n\r\n")
        if hdr_end == -1:
            continue
        raw_headers = chunk[:hdr_end].decode("utf-8", errors="replace")
        data = chunk[hdr_end + 4:]

        name = filename = None
        for line in raw_headers.splitlines():
            if line.lower().startswith("content-disposition"):
                for item in line.split(";"):
                    item = item.strip()
                    if item.startswith("name="):
                        name = item[5:].strip('"')
                    elif item.startswith("filename="):
                        filename = item[9:].strip('"')
        if name:
            fields[name] = (filename, data)
    return fields

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>JSON Mapping Updater</title>
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
</style>
</head>
<body>
<div class="container">

  <header>
    <div class="badge">connector tool</div>
    <h1>JSON Mapping Updater</h1>
    <p class="subtitle">Upload a connector JSON and a key→value file.<br>The tool replaces the mapping inside config — nothing else changes.</p>
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
    </div>

    <div class="card">
      <div class="card-header">
        <div class="card-icon icon-csv">⇄</div>
        <div>
          <div class="card-title">Key → Value Mapping File</div>
          <div class="card-desc">Two-column file: first column = key, second column = value (e.g. {{.plant}})</div>
        </div>
      </div>
      <div class="drop-zone" id="csvZone">
        <input type="file" name="mapping_file" id="mappingFile" accept=".csv,.xlsx,.xls,.tsv,.txt" required>
        <div class="drop-icon">⇄</div>
        <div class="drop-label">Drop mapping file or <span>browse</span></div>
        <div class="drop-ext">.csv · .xlsx · .tsv · .txt</div>
      </div>
      <div class="file-selected" id="csvSelected">✓ <span id="csvName"></span></div>
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
        <div class="step-text"><strong>Upload</strong> your connector JSON (any structure)</div>
      </div>
      <div class="step">
        <div class="step-num">02</div>
        <div class="step-text"><strong>Upload</strong> CSV/Excel with key-value columns</div>
      </div>
      <div class="step">
        <div class="step-num">03</div>
        <div class="step-text"><strong>Download</strong> updated JSON — only <code>mapping</code> is changed</div>
      </div>
    </div>
  </div>

</div>

<script>
  const jsonFile = document.getElementById('jsonFile');
  const mappingFile = document.getElementById('mappingFile');
  const submitBtn = document.getElementById('submitBtn');

  function checkReady() {
    submitBtn.disabled = !(jsonFile.files.length && mappingFile.files.length);
  }

  jsonFile.addEventListener('change', () => {
    if (jsonFile.files.length) {
      document.getElementById('jsonSelected').classList.add('show');
      document.getElementById('jsonName').textContent = jsonFile.files[0].name;
    }
    checkReady();
  });

  mappingFile.addEventListener('change', () => {
    if (mappingFile.files.length) {
      document.getElementById('csvSelected').classList.add('show');
      document.getElementById('csvName').textContent = mappingFile.files[0].name;
    }
    checkReady();
  });

  // Drag and drop visual
  ['jsonZone','csvZone'].forEach(id => {
    const zone = document.getElementById(id);
    zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('dragover'));
    zone.addEventListener('drop', () => zone.classList.remove('dragover'));
  });
</script>
</body>
</html>
"""


def parse_mapping_csv(data: bytes, filename: str) -> dict:
    """Parse CSV, TSV, TXT, or XLSX into a key→value dict."""
    ext = filename.rsplit(".", 1)[-1].lower()

    if ext in ("xlsx", "xls"):
        if not HAS_OPENPYXL:
            raise ValueError("openpyxl not installed. Run: pip install openpyxl")
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb.active
        mapping = {}
        for row in ws.iter_rows(values_only=True):
            if row[0] is not None and len(row) >= 2 and row[1] is not None:
                k = str(row[0]).strip()
                v = str(row[1]).strip()
                if k and k.lower() not in ("key", "keys"):
                    mapping[k] = v
        return mapping

    # CSV / TSV / TXT
    text = data.decode("utf-8-sig", errors="replace")
    # Auto-detect delimiter
    delimiter = "\t" if "\t" in text.split("\n")[0] else ","
    reader = csv.reader(io.StringIO(text), delimiter=delimiter)
    mapping = {}
    for i, row in enumerate(reader):
        if len(row) < 2:
            continue
        k = row[0].strip()
        v = row[1].strip()
        if not k or k.lower() in ("key", "keys"):
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
        print(f"  {self.address_string()} → {args[0]}")

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
            if "mapping_file" not in fields:
                raise ValueError("No mapping file received. Please upload a CSV or Excel file.")

            # Read uploaded files
            _, json_data = fields["json_file"]
            mapping_filename, mapping_data = fields["mapping_file"]
            mapping_filename = mapping_filename or "mapping.csv"

            # Parse JSON
            try:
                connector = json.loads(json_data.decode("utf-8-sig", errors="replace"))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON file: {e}")

            # Parse mapping
            new_mapping = parse_mapping_csv(mapping_data, mapping_filename)
            if not new_mapping:
                raise ValueError("No key-value pairs found in mapping file. Make sure it has two columns: key and value.")

            # Apply mapping
            updated = replace_mapping_in_json(copy.deepcopy(connector), new_mapping)
            output_bytes = json.dumps(updated, indent=2, ensure_ascii=False).encode("utf-8")

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
║       JSON Mapping Updater — Ready           ║
╠══════════════════════════════════════════════╣
║  Open:  http://localhost:{port}                 ║
║  Stop:  Ctrl+C                               ║
╚══════════════════════════════════════════════╝

  Supports:
    JSON  → any connector structure
    CSV   → comma-separated (key,value)
    TSV   → tab-separated (key  value)
    XLSX  → Excel (key in col A, value in col B)
""")

    if not HAS_OPENPYXL:
        print("  ⚠  openpyxl not found — Excel (.xlsx) upload disabled.")
        print("     Install with:  pip install openpyxl\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()