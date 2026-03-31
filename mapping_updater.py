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
import random
import ssl
import base64
import urllib.request
import urllib.error
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
<title>Litmus Deployment Assist</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'><text y='.9em' font-size='90'>⬡</text></svg>">
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
    --sidebar-w: 200px;
  }

  html, body { height: 100%; overflow: hidden; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--sans);
    display: flex;
    flex-direction: row;
  }

  /* ── Sidebar ──────────────────────────────────────────────────────────── */
  .sidebar {
    width: var(--sidebar-w);
    flex-shrink: 0;
    background: #0a0a0d;
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    height: 100vh;
    overflow: hidden;
  }

  .sidebar-logo {
    padding: 20px 18px 14px;
    border-bottom: 1px solid var(--border);
  }
  .sidebar-logo-line1 {
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--accent);
  }
  .sidebar-logo-line2 {
    font-size: 10px;
    color: var(--muted);
    margin-top: 3px;
  }

  .sidebar-nav { flex: 1; padding: 12px 8px; overflow-y: auto; }

  .nav-section-label {
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--muted);
    padding: 10px 10px 6px;
  }

  .nav-item {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 9px 10px;
    border-radius: 8px;
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
    margin-bottom: 2px;
    border: 1.5px solid transparent;
  }
  .nav-item:hover { background: rgba(255,255,255,0.04); }
  .nav-item.active {
    background: var(--accent-dim);
    border-color: rgba(245,200,66,0.25);
  }
  .nav-item-icon {
    font-size: 15px;
    flex-shrink: 0;
    margin-top: 1px;
  }
  .nav-item-text {}
  .nav-item-title {
    font-size: 12px;
    font-weight: 600;
    color: var(--text);
    line-height: 1.3;
  }
  .nav-item.active .nav-item-title { color: var(--accent); }
  .nav-item-sub {
    font-size: 10px;
    color: var(--muted);
    margin-top: 2px;
    line-height: 1.4;
  }

  .sidebar-footer {
    padding: 12px 18px;
    border-top: 1px solid var(--border);
    font-family: var(--mono);
    font-size: 9px;
    color: var(--muted);
    letter-spacing: 1px;
  }

  /* ── Main area ───────────────────────────────────────────────────────── */
  .main-area {
    flex: 1;
    display: flex;
    flex-direction: column;
    height: 100vh;
    min-width: 0;
    overflow: hidden;
  }

  /* ── Top panel (fixed, shared LE connection) ──────────────────────────── */
  .top-panel {
    border-bottom: 1px solid var(--border);
    background: #0f0f14;
    padding: 14px 24px;
    flex-shrink: 0;
  }

  .top-header-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 10px;
  }
  .top-app-title {
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 2px;
    text-transform: uppercase;
    color: var(--muted);
  }

  .mode-bar {
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .mode-label {
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.5px;
    transition: color 0.2s;
  }
  .mode-label.active  { color: var(--accent); }
  .mode-label.inactive { color: var(--muted); }
  .toggle-track {
    width: 44px; height: 24px; border-radius: 12px;
    background: var(--border-hover); cursor: pointer;
    position: relative; transition: background 0.25s;
    border: 1px solid var(--border);
  }
  .toggle-track.le-active { background: var(--accent-dim); border-color: rgba(245,200,66,0.35); }
  .toggle-knob {
    width: 18px; height: 18px; border-radius: 50%;
    background: #fff; position: absolute; top: 2px; left: 3px;
    transition: transform 0.25s; box-shadow: 0 1px 4px rgba(0,0,0,0.3);
  }
  .toggle-track.le-active .toggle-knob { transform: translateX(20px); }

  .le-row {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
  }
  .le-row-hidden { display: none !important; }

  .le-version-badge {
    display: inline-flex; align-items: center; gap: 5px;
    background: var(--accent-dim); color: var(--accent);
    font-family: var(--mono); font-size: 11px; font-weight: 600;
    padding: 3px 10px; border-radius: 20px;
  }
  .le-version-badge .le-ver-label { color: var(--muted); font-weight: 400; }

  .le-status {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-left: 4px;
  }
  .le-status.error   { color: var(--red); }
  .le-status.success { color: var(--green); }

  /* DT status (reuse le-status pattern) */
  .dt-status {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    padding: 8px 14px 0;
  }
  .dt-status.error   { color: var(--red); }
  .dt-status.success { color: var(--green); }

  /* DT Preview tabs & table */
  .dt-preview-tabs {
    display: flex; gap: 0; border-bottom: 1px solid var(--border);
    margin: 14px 0 0; padding: 0 14px;
  }
  .dt-tab-btn {
    background: none; border: none; border-bottom: 2px solid transparent;
    color: var(--muted); font-family: var(--mono); font-size: 11px;
    padding: 8px 14px; cursor: pointer; transition: color 0.15s, border-color 0.15s;
  }
  .dt-tab-btn:hover { color: var(--text); }
  .dt-tab-btn.active { color: var(--green); border-bottom-color: var(--green); }
  .dt-tab-content { display: none; padding: 14px; max-height: 420px; overflow-y: auto; }
  .dt-tab-content.active { display: block; }
  .dt-preview-status {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    padding: 8px 14px 0;
  }
  .dt-preview-status.error { color: var(--red); }
  .dt-attr-table {
    width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 12px;
  }
  .dt-attr-table th {
    text-align: left; color: var(--muted); font-weight: 600; font-size: 10px;
    text-transform: uppercase; letter-spacing: 0.5px;
    padding: 6px 10px; border-bottom: 1px solid var(--border);
  }
  .dt-attr-table td {
    padding: 7px 10px; border-bottom: 1px solid rgba(34,34,46,0.5);
    color: var(--text); word-break: break-all;
  }
  .dt-attr-table tr:hover td { background: rgba(255,255,255,0.02); }
  .dt-attr-table td:first-child { color: var(--accent); font-weight: 600; }
  .dt-empty {
    display: none; text-align: center; color: var(--muted);
    font-size: 12px; padding: 24px 14px;
  }
  .dt-tree {
    font-family: var(--mono); font-size: 12px; padding: 4px 0;
  }
  .dt-tree-node { padding-left: 18px; }
  .dt-tree-label {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 3px 6px; border-radius: 4px; cursor: default;
  }
  .dt-tree-label:hover { background: rgba(255,255,255,0.03); }
  .dt-tree-folder { color: var(--accent); font-weight: 600; }
  .dt-tree-attr-static { color: var(--muted); }
  .dt-tree-attr-dynamic { color: var(--green); }
  .dt-tree-icon { font-size: 11px; width: 14px; text-align: center; }

  /* Raw JSON modal */
  .dt-raw-btn {
    background: rgba(255,255,255,0.06); border: 1px solid var(--border);
    color: var(--muted); font-family: var(--mono); font-size: 10px;
    padding: 4px 10px; border-radius: 4px; cursor: pointer; transition: all 0.15s;
  }
  .dt-raw-btn:hover { color: var(--text); background: rgba(255,255,255,0.1); }
  .dt-raw-overlay {
    display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.6);
    z-index: 1000; justify-content: center; align-items: center;
  }
  .dt-raw-overlay.open { display: flex; }
  .dt-raw-modal {
    background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
    width: 700px; max-width: 90vw; max-height: 75vh; display: flex; flex-direction: column;
  }
  .dt-raw-modal-header {
    display: flex; justify-content: space-between; align-items: center;
    padding: 12px 16px; border-bottom: 1px solid var(--border);
  }
  .dt-raw-modal-header h3 { margin: 0; font-size: 13px; color: var(--text); }
  .dt-raw-close {
    background: none; border: none; color: var(--muted); font-size: 18px;
    cursor: pointer; padding: 0 4px; line-height: 1;
  }
  .dt-raw-close:hover { color: var(--red); }
  .dt-raw-tabs {
    display: flex; gap: 0; border-bottom: 1px solid var(--border); padding: 0 16px;
  }
  .dt-raw-tab {
    background: none; border: none; border-bottom: 2px solid transparent;
    color: var(--muted); font-family: var(--mono); font-size: 10px;
    padding: 7px 12px; cursor: pointer; transition: color 0.15s;
  }
  .dt-raw-tab:hover { color: var(--text); }
  .dt-raw-tab.active { color: var(--green); border-bottom-color: var(--green); }
  .dt-raw-body {
    flex: 1; overflow-y: auto; padding: 14px 16px;
  }
  .dt-raw-body pre {
    margin: 0; font-family: var(--mono); font-size: 11px; color: var(--text);
    white-space: pre-wrap; word-break: break-all; line-height: 1.5;
  }

  /* DT Clone tabs */
  .dtc-tab-bar {
    display: flex; align-items: center; gap: 0;
    border-bottom: 1px solid var(--border); margin-bottom: 16px; overflow-x: auto;
  }
  .dtc-tab-btn {
    background: none; border: none; border-bottom: 2px solid transparent;
    color: var(--muted); font-family: var(--sans); font-size: 12px; font-weight: 500;
    padding: 9px 15px; cursor: pointer; white-space: nowrap;
    transition: color 0.2s, border-color 0.2s;
    display: flex; align-items: center; gap: 6px;
  }
  .dtc-tab-btn:hover { color: var(--text); }
  .dtc-tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }
  .dtc-tab-btn .dtc-close {
    display: inline-flex; align-items: center; justify-content: center;
    width: 15px; height: 15px; border-radius: 50%; font-size: 10px;
    background: transparent; color: var(--muted); cursor: pointer;
    border: none; line-height: 1; transition: background 0.15s, color 0.15s;
  }
  .dtc-tab-btn .dtc-close:hover { background: var(--red); color: #fff; }
  .dtc-tab-add {
    background: none; border: 1px dashed var(--border);
    color: var(--muted); font-size: 15px; width: 26px; height: 26px;
    border-radius: 6px; cursor: pointer; display: flex;
    align-items: center; justify-content: center; margin-left: 6px;
    transition: border-color 0.2s, color 0.2s; flex-shrink: 0;
  }
  .dtc-tab-add:hover { border-color: var(--accent); color: var(--accent); }
  .dtc-pane { display: none; }
  .dtc-pane.active { display: block; }
  .dtc-name-row {
    display: flex; align-items: center; gap: 10px; margin-bottom: 14px;
  }
  .dtc-name-row label {
    color: var(--muted); font-size: 12px; font-weight: 600; white-space: nowrap;
  }
  .dtc-name-input {
    flex: 1; background: var(--bg); border: 1px solid var(--border);
    color: var(--text); font-family: var(--mono); font-size: 12px;
    padding: 7px 12px; border-radius: 6px; outline: none;
  }
  .dtc-name-input:focus { border-color: var(--accent); }
  .dtc-paste-area {
    width: 100%; min-height: 120px; background: var(--bg); border: 1px solid var(--border);
    color: var(--text); font-family: var(--mono); font-size: 12px;
    padding: 12px; border-radius: 6px; resize: vertical; outline: none;
    box-sizing: border-box;
  }
  .dtc-paste-area:focus { border-color: var(--accent); }
  .dtc-row-count {
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    margin-top: 6px;
  }
  .dtc-preview-table {
    width: 100%; border-collapse: collapse; font-family: var(--mono); font-size: 11px;
    margin-top: 10px;
  }
  .dtc-preview-table th {
    text-align: left; color: var(--muted); font-weight: 600; font-size: 10px;
    text-transform: uppercase; letter-spacing: 0.5px;
    padding: 5px 8px; border-bottom: 1px solid var(--border);
  }
  .dtc-preview-table td {
    padding: 5px 8px; border-bottom: 1px solid rgba(34,34,46,0.5); color: var(--text);
  }
  .dtc-preview-table td:first-child { color: var(--accent); font-weight: 600; }
  .btn-push-clone {
    display: inline-flex; align-items: center; gap: 8px;
    background: var(--green); color: #111; border: none; border-radius: 8px;
    font-family: var(--sans); font-size: 13px; font-weight: 600;
    padding: 10px 22px; cursor: pointer; margin-top: 18px;
    transition: opacity 0.15s;
  }
  .btn-push-clone:hover { opacity: 0.85; }
  .btn-push-clone:disabled { opacity: 0.4; cursor: not-allowed; }
  .dtc-push-results {
    margin-top: 12px; display: none; flex-direction: column; gap: 6px;
  }
  .dtc-push-result-item {
    font-family: var(--mono); font-size: 11px; padding: 8px 12px;
    border-radius: 6px;
  }
  .dtc-push-result-item.ok { background: var(--green-dim); color: var(--green); }
  .dtc-push-result-item.fail { background: rgba(224,92,92,0.1); color: var(--red); }
  .dtc-push-result-item.info { background: rgba(255,200,60,0.08); color: #e0c84a; }

  /* Ping row inside top panel */
  .ping-separator { width: 1px; height: 26px; background: var(--border); margin: 0 4px; }
  .ping-result-inline {
    font-family: var(--mono); font-size: 11px; padding: 4px 10px;
    border-radius: 6px; display: none;
  }
  .ping-result-inline.ok   { background: var(--green-dim); color: var(--green); display: inline-block; }
  .ping-result-inline.fail { background: rgba(224,92,92,0.1); color: var(--red); display: inline-block; }
  .ping-result-inline.pending { background: var(--surface); color: var(--muted); display: inline-block; }

  /* ── Bottom panel (scrollable, per-app content) ──────────────────────── */
  .bottom-panel {
    flex: 1;
    overflow-y: auto;
    padding: 28px 28px;
    min-height: 0;
  }

  .app-pane { display: none; }
  .app-pane.active { display: block; }

  /* ── Common card / input styles ──────────────────────────────────────── */
  .badge {
    display: inline-flex; align-items: center; gap: 7px;
    background: var(--accent-dim); border: 1px solid rgba(245,200,66,0.25);
    color: var(--accent); font-family: var(--mono); font-size: 10px;
    letter-spacing: 2px; text-transform: uppercase;
    padding: 5px 12px; border-radius: 20px; margin-bottom: 14px;
  }
  .badge::before { content: ''; width: 6px; height: 6px; border-radius: 50%; background: var(--accent); animation: pulse 2s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }

  .pane-header { margin-bottom: 24px; }
  .pane-title { font-size: 24px; font-weight: 700; color: #fff; line-height: 1.2; margin-bottom: 6px; }
  .pane-subtitle { color: var(--muted); font-size: 13px; line-height: 1.6; }

  .card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 16px;
    transition: border-color 0.2s;
  }
  .card:hover { border-color: var(--border-hover); }

  .card-header {
    display: flex; align-items: center; gap: 10px;
    margin-bottom: 18px;
  }
  .card-icon {
    width: 30px; height: 30px; border-radius: 8px;
    display: flex; align-items: center; justify-content: center;
    font-size: 14px;
  }
  .icon-json { background: rgba(245,200,66,0.15); }
  .icon-csv  { background: rgba(62,207,142,0.15); }

  .card-title { font-size: 13px; font-weight: 600; color: #fff; }
  .card-desc  { font-size: 11px; color: var(--muted); margin-top: 2px; }

  .drop-zone {
    border: 1.5px dashed var(--border);
    border-radius: 8px;
    padding: 32px 20px;
    text-align: center;
    cursor: pointer;
    transition: all 0.2s;
    position: relative;
  }
  .drop-zone:hover, .drop-zone.dragover { border-color: var(--accent); background: var(--accent-dim); }
  .drop-zone input[type=file] { position: absolute; inset: 0; opacity: 0; cursor: pointer; width: 100%; height: 100%; }
  .drop-icon  { font-size: 24px; margin-bottom: 8px; }
  .drop-label { font-size: 13px; color: var(--muted); }
  .drop-label span { color: var(--accent); font-weight: 600; }
  .drop-ext   { font-family: var(--mono); font-size: 10px; color: var(--muted); margin-top: 5px; }

  .file-selected {
    display: none; align-items: center; gap: 10px;
    background: var(--green-dim); border: 1px solid rgba(62,207,142,0.25);
    border-radius: 8px; padding: 11px 14px; margin-top: 10px;
    font-family: var(--mono); font-size: 12px; color: var(--green);
  }
  .file-selected.show { display: flex; }

  .detected-table {
    display: none; align-items: center; gap: 8px;
    background: rgba(90,90,114,0.12); border: 1px solid var(--border-hover);
    border-radius: 8px; padding: 9px 14px; margin-top: 10px;
    font-family: var(--mono); font-size: 12px;
  }
  .detected-table.show { display: flex; }
  .detected-label { color: var(--muted); }
  .detected-value { color: var(--accent); font-weight: 600; margin-left: 4px; }

  .replace-section { display: none; margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--border); }
  .replace-section.show { display: block; }
  .checkbox-row { display: flex; align-items: center; gap: 10px; cursor: pointer; user-select: none; }
  .checkbox-row input[type=checkbox] { width: 15px; height: 15px; accent-color: var(--accent); cursor: pointer; flex-shrink: 0; }
  .checkbox-label { font-size: 13px; color: var(--text); }
  .new-table-input { display: none; margin-top: 12px; }
  .new-table-input.show { display: block; }

  .text-input {
    background: var(--bg); border: 1.5px solid var(--border);
    border-radius: 8px; padding: 9px 12px; font-family: var(--mono);
    font-size: 12px; color: var(--text); outline: none; transition: border-color 0.2s;
  }
  .text-input:focus { border-color: var(--accent); }
  .text-input::placeholder { color: var(--muted); }

  .btn-submit {
    width: 100%; background: var(--accent); color: #000; border: none;
    border-radius: 8px; padding: 14px; font-family: var(--sans);
    font-size: 14px; font-weight: 700; cursor: pointer; letter-spacing: 0.5px;
    transition: all 0.2s; margin-top: 8px;
  }
  .btn-submit:hover { background: #ffd84d; transform: translateY(-1px); box-shadow: 0 8px 24px rgba(245,200,66,0.25); }
  .btn-submit:active { transform: translateY(0); }
  .btn-submit:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }

  .alert { padding: 12px 16px; border-radius: 8px; font-size: 13px; margin-bottom: 16px; display: flex; gap: 10px; align-items: flex-start; }
  .alert-error   { background: rgba(224,92,92,0.1); border: 1px solid rgba(224,92,92,0.3); color: #e05c5c; }
  .alert-success { background: var(--green-dim); border: 1px solid rgba(62,207,142,0.3); color: var(--green); }

  .mapping-preview {
    background: var(--bg); border: 1px solid var(--border);
    border-radius: 8px; padding: 14px; margin-top: 14px;
    font-family: var(--mono); font-size: 11px; color: var(--muted);
    max-height: 180px; overflow-y: auto;
  }
  .mapping-preview .kv { display: flex; gap: 12px; padding: 3px 0; border-bottom: 1px solid #1a1a22; }
  .mapping-preview .kv:last-child { border: none; }
  .mapping-preview .k { color: var(--accent); min-width: 150px; }
  .mapping-preview .v { color: var(--green); }
  .mapping-preview .arrow { color: #333; }

  /* ── Tab UI ──────────────────────────────────────────────────────────── */
  .tab-bar { display: flex; align-items: center; gap: 0; border-bottom: 1px solid var(--border); margin-bottom: 16px; overflow-x: auto; }
  .tab-btn {
    background: none; border: none; border-bottom: 2px solid transparent;
    color: var(--muted); font-family: var(--sans); font-size: 12px; font-weight: 500;
    padding: 9px 15px; cursor: pointer; white-space: nowrap;
    transition: color 0.2s, border-color 0.2s;
    display: flex; align-items: center; gap: 6px;
  }
  .tab-btn:hover { color: var(--text); }
  .tab-btn.active { color: var(--accent); border-bottom-color: var(--accent); }
  .tab-btn .tab-close {
    display: inline-flex; align-items: center; justify-content: center;
    width: 15px; height: 15px; border-radius: 50%; font-size: 10px;
    background: transparent; color: var(--muted); cursor: pointer;
    border: none; line-height: 1; transition: background 0.15s, color 0.15s;
  }
  .tab-btn .tab-close:hover { background: var(--red); color: #fff; }
  .tab-add {
    background: none; border: 1px dashed var(--border);
    color: var(--muted); font-size: 15px; width: 26px; height: 26px;
    border-radius: 6px; cursor: pointer; display: flex;
    align-items: center; justify-content: center; margin-left: 6px;
    transition: border-color 0.2s, color 0.2s; flex-shrink: 0;
  }
  .tab-add:hover { border-color: var(--accent); color: var(--accent); }
  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  /* ── Instance list (LE mode) ─────────────────────────────────────────── */
  .instance-list { margin-top: 14px; display: flex; flex-direction: column; gap: 5px; max-height: 240px; overflow-y: auto; }
  .instance-item {
    background: var(--bg); border: 1.5px solid var(--border);
    border-radius: 8px; padding: 9px 14px; cursor: pointer;
    font-family: var(--mono); font-size: 12px; color: var(--text);
    transition: border-color 0.15s, background 0.15s;
    display: flex; align-items: center; gap: 10px;
  }
  .instance-item:hover { border-color: var(--border-hover); background: var(--surface); }
  .instance-item.selected { border-color: var(--accent); background: var(--accent-dim); }
  .instance-item .inst-icon { color: var(--muted); font-size: 13px; flex-shrink: 0; }
  .instance-item.selected .inst-icon { color: var(--accent); }
  .instance-item .inst-table { color: var(--accent); font-weight: 600; }
  .instance-item .inst-provider { color: var(--muted); font-size: 10px; margin-left: auto; }

  /* ── Push button & results ───────────────────────────────────────────── */
  .btn-push {
    width: 100%; background: var(--green); color: #000; border: none;
    border-radius: 8px; padding: 14px; font-family: var(--sans);
    font-size: 14px; font-weight: 700; cursor: pointer; letter-spacing: 0.5px;
    transition: all 0.2s; margin-top: 8px; display: none;
  }
  .btn-push:hover { background: #4de6a0; transform: translateY(-1px); box-shadow: 0 8px 24px rgba(62,207,142,0.25); }
  .btn-push:active { transform: translateY(0); }
  .btn-push:disabled { opacity: 0.4; cursor: not-allowed; transform: none; }
  .push-results { margin-top: 14px; display: flex; flex-direction: column; gap: 6px; }
  .push-result-item {
    font-family: var(--mono); font-size: 12px; padding: 8px 12px;
    border-radius: 6px; display: flex; align-items: center; gap: 8px;
  }
  .push-result-item.ok   { background: var(--green-dim); color: var(--green); border: 1px solid rgba(62,207,142,0.25); }
  .push-result-item.fail { background: rgba(224,92,92,0.1); color: var(--red);   border: 1px solid rgba(224,92,92,0.25); }

  .paste-area {
    width: 100%; min-height: 150px; resize: vertical;
    background: var(--bg); border: 1.5px solid var(--border);
    border-radius: 8px; padding: 12px 14px;
    font-family: var(--mono); font-size: 12px; color: var(--text);
    outline: none; transition: border-color 0.2s; line-height: 1.7;
  }
  .paste-area:focus { border-color: var(--accent); }
  .paste-area::placeholder { color: var(--muted); }
  .pair-count { font-family: var(--mono); font-size: 11px; color: var(--muted); margin-top: 6px; text-align: right; }
  .pair-count.has-data { color: var(--green); }

  .how-it-works {
    background: rgba(255,255,255,0.02); border: 1px solid var(--border);
    border-radius: 10px; padding: 18px 22px; margin-top: 24px;
  }
  .how-title { font-size: 11px; color: var(--muted); letter-spacing: 2px; text-transform: uppercase; margin-bottom: 12px; }
  .steps { display: flex; gap: 0; }
  .step { flex: 1; padding: 0 14px 0 0; border-right: 1px solid var(--border); margin-right: 14px; }
  .step:last-child { border: none; margin: 0; padding: 0; }
  .step-num { font-family: var(--mono); font-size: 18px; font-weight: 700; color: var(--border-hover); margin-bottom: 4px; }
  .step-text { font-size: 12px; color: var(--muted); line-height: 1.5; }
  .step-text strong { color: var(--text); }

  /* ── connect button (top panel) ─────────────────────────────────────── */
  .btn-connect {
    background: var(--accent); color: #000; border: none;
    border-radius: 8px; padding: 9px 16px;
    font-family: var(--sans); font-size: 12px; font-weight: 700;
    cursor: pointer; white-space: nowrap; transition: all 0.2s;
  }
  .btn-connect:hover { background: #ffd84d; }
  .btn-connect:disabled { opacity: 0.4; cursor: not-allowed; }

  .btn-ping {
    padding: 7px 14px; border-radius: 8px; border: 1.5px solid var(--border);
    background: var(--surface); color: var(--text);
    font-family: var(--sans); font-size: 11px; font-weight: 600;
    cursor: pointer; white-space: nowrap; transition: all 0.2s;
  }
  .btn-ping:hover { border-color: var(--accent); color: var(--accent); }
  .btn-ping:disabled { opacity: 0.4; cursor: not-allowed; }

  /* ── Digital Twin placeholder ────────────────────────────────────────── */
  .placeholder-shell {
    display: flex; flex-direction: column; align-items: center;
    justify-content: center; min-height: 340px; text-align: center;
    border: 1.5px dashed var(--border); border-radius: 14px;
    padding: 48px 32px; gap: 16px;
    background: linear-gradient(135deg, rgba(245,200,66,0.03) 0%, rgba(62,207,142,0.03) 100%);
  }
  .placeholder-icon { font-size: 48px; opacity: 0.55; margin-bottom: 4px; }
  .placeholder-title { font-size: 20px; font-weight: 700; color: var(--text); }
  .placeholder-sub { font-size: 13px; color: var(--muted); line-height: 1.7; max-width: 420px; }
  .coming-soon-badge {
    display: inline-flex; align-items: center; gap: 6px;
    background: rgba(62,207,142,0.08); border: 1px solid rgba(62,207,142,0.2);
    color: var(--green); font-family: var(--mono); font-size: 10px;
    letter-spacing: 2px; text-transform: uppercase;
    padding: 5px 14px; border-radius: 20px; margin-top: 4px;
  }

</style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════════════════════════
     SIDEBAR
════════════════════════════════════════════════════════════════════════ -->
<aside class="sidebar">
  <div class="sidebar-logo">
    <div class="sidebar-logo-line1">Litmus</div>
    <div class="sidebar-logo-line2">Deployment Assist</div>
  </div>

  <nav class="sidebar-nav">
    <div class="nav-section-label">Applications</div>

    <div class="nav-item active" data-app="connector" onclick="selectApp('connector', this)">
      <div class="nav-item-icon">⇄</div>
      <div class="nav-item-text">
        <div class="nav-item-title">Integration Connector Assist</div>
        <div class="nav-item-sub">JSON mapping updater</div>
      </div>
    </div>

    <div class="nav-item" data-app="dtwin" onclick="selectApp('dtwin', this)">
      <div class="nav-item-icon">⬡</div>
      <div class="nav-item-text">
        <div class="nav-item-title">Digital Twin Model Assist</div>
        <div class="nav-item-sub">Model automation</div>
      </div>
    </div>
  </nav>

  <div class="sidebar-footer">v4.0.1 &nbsp;·&nbsp; localhost:8081</div>
</aside>

<!-- ═══════════════════════════════════════════════════════════════════════
     MAIN AREA
════════════════════════════════════════════════════════════════════════ -->
<div class="main-area">

  <!-- ── Top panel: shared Litmus Edge connection ─────────────────────── -->
  <div class="top-panel">
    <div class="top-header-row">
      <span class="top-app-title" id="topAppTitle">Integration Connector Assist</span>
      <div class="mode-bar">
        <span class="mode-label active"  id="labelManual">Manual</span>
        <div class="toggle-track" id="modeToggle" title="Switch mode">
          <div class="toggle-knob"></div>
        </div>
        <span class="mode-label inactive" id="labelLE">Litmus Edge</span>
      </div>
    </div>

    <!-- LE connection row — hidden in Manual mode -->
    <div class="le-row le-row-hidden" id="leRow">
      <input type="text" id="leIpInput"    class="text-input" placeholder="e.g. 192.168.1.100" autocomplete="off" style="width:160px;">
      <input type="text" id="leTokenInput" class="text-input" placeholder="API token"           autocomplete="off" style="width:200px;">
      <button type="button" class="btn-connect" id="leConnectBtn">Connect</button>
      <span class="le-version-badge" id="leVersionBadge" style="display:none">
        <span class="le-ver-label">LE</span>
        <span id="leVersionValue"></span>
      </span>
      <span class="le-status" id="leStatus"></span>

      <div class="ping-separator"></div>
      <input type="text" id="pingIpInput" class="text-input" placeholder="IP to ping" autocomplete="off" style="width:140px;">
      <button type="button" class="btn-ping" id="pingBtn">Ping</button>
      <span class="ping-result-inline" id="pingResult"></span>
    </div>
  </div>

  <!-- ── Bottom panel: per-app dynamic content ────────────────────────── -->
  <div class="bottom-panel">

    <!-- ════════════════════════════════════════════════════════════════
         APP 1 · Integration Connector Assist
    ═════════════════════════════════════════════════════════════════ -->
    <div class="app-pane active" id="pane-connector">

      {alert_html}

      <div class="pane-header">
        <div class="badge">connector tool</div>
        <div class="pane-title">⇄ Connector JSON Mapping Updater</div>
        <p class="pane-subtitle">Upload a connector JSON, then paste your key→value columns copied from Excel.<br>The tool replaces the mapping inside config — nothing else changes.</p>
      </div>

      <form method="POST" action="/update" enctype="multipart/form-data" id="mainForm">

        <!-- Manual: JSON upload card -->
        <div class="card" id="manualCard">
          <div class="card-header">
            <div class="card-icon icon-json">📄</div>
            <div>
              <div class="card-title">Connector JSON</div>
              <div class="card-desc">Your connector configuration file (shared across all tabs)</div>
            </div>
          </div>
          <div class="drop-zone" id="jsonZone">
            <input type="file" name="json_file" id="jsonFile" accept=".json">
            <div class="drop-icon">{ }</div>
            <div class="drop-label">Drop JSON file or <span>browse</span></div>
            <div class="drop-ext">.json</div>
          </div>
          <div class="file-selected" id="jsonSelected">✓ <span id="jsonName"></span></div>
          <div class="detected-table" id="detectedTableWrap">
            <span class="detected-label">Detected table →</span>
            <span class="detected-value" id="detectedTableVal"></span>
          </div>
        </div>

        <!-- LE mode: instance list (shown inside bottom panel when LE active) -->
        <div class="card" id="leInstanceCard" style="display:none">
          <div class="card-header">
            <div class="card-icon icon-json" style="background:rgba(245,200,66,0.15)">🔗</div>
            <div>
              <div class="card-title">Select Connector Instance</div>
              <div class="card-desc">Pick an instance from your connected Litmus Edge device</div>
            </div>
          </div>
          <div class="instance-list" id="instanceList"></div>
        </div>

        <input type="hidden" name="le_instance_json" id="leInstanceJson" value="">

        <!-- Tabbed mapping cards -->
        <div class="card" id="tabCard">
          <div class="card-header">
            <div class="card-icon icon-csv">⇄</div>
            <div>
              <div class="card-title">Mapping Tabs</div>
              <div class="card-desc">Each tab produces a separate output file. Add tabs with <strong>+</strong>.</div>
            </div>
          </div>

          <div class="tab-bar" id="tabBar">
            <button type="button" class="tab-btn active" data-tab="0">Tab 1</button>
            <button type="button" class="tab-add" id="addTabBtn" title="Add tab">+</button>
          </div>

          <div id="tabPanes">
            <div class="tab-pane active" data-tab="0">
              <div class="replace-section">
                <label class="checkbox-row">
                  <input type="checkbox" class="replaceCheck" name="replace_table_0" value="1">
                  <span class="checkbox-label">Replace table name in output</span>
                </label>
                <div class="new-table-input">
                  <input type="text" name="new_table_0" class="text-input newTableInput"
                         placeholder="New table name…" autocomplete="off" style="width:100%">
                </div>
              </div>
              <div style="margin-top:14px">
                <textarea
                  name="mapping_text_0"
                  class="paste-area mappingArea"
                  placeholder="Paste your Excel cells here…&#10;&#10;plant&#9;{{.plant}}&#10;material&#9;{{.material}}&#10;movetype&#9;{{.movetype}}"
                  spellcheck="false"
                ></textarea>
                <div class="pair-count"></div>
                <div class="mapping-preview" style="display:none"></div>
              </div>
            </div>
          </div>
        </div>

        <input type="hidden" name="detected_table"  id="detectedTableInput" value="">
        <input type="hidden" name="tab_count"        id="tabCountInput"      value="1">
        <input type="hidden" name="le_ip"            id="leIpHidden"         value="">
        <input type="hidden" name="le_token"         id="leTokenHidden"      value="">

        <button type="submit" class="btn-submit" id="submitBtn" disabled>
          Update Mapping &amp; Download
        </button>
        <button type="button" class="btn-push" id="pushBtn" disabled>
          Push to Litmus Edge
        </button>
        <div class="push-results" id="pushResults" style="display:none"></div>
      </form>

      <div class="how-it-works">
        <div class="how-title">How it works</div>
        <div class="steps" id="howSteps">
          <div class="step">
            <div class="step-num">01</div>
            <div class="step-text"><strong>Upload</strong> your connector JSON</div>
          </div>
          <div class="step">
            <div class="step-num">02</div>
            <div class="step-text"><strong>Add tabs</strong> for each connector. <strong>Paste</strong> key→value columns and set table names</div>
          </div>
          <div class="step">
            <div class="step-num">03</div>
            <div class="step-text"><strong>Download</strong> one JSON with all tabs as separate instances</div>
          </div>
        </div>
      </div>
    </div>
    <!-- /pane-connector -->


    <!-- ════════════════════════════════════════════════════════════════
         APP 2 · Digital Twin Model Assist  (placeholder)
    ═════════════════════════════════════════════════════════════════ -->
    <div class="app-pane" id="pane-dtwin">
      <div class="pane-header">
        <div class="badge" style="background:rgba(62,207,142,0.08);border-color:rgba(62,207,142,0.2);color:var(--green)">digital twin</div>
        <div class="pane-title">⬡ Digital Twin Model Assist</div>
        <p class="pane-subtitle">Automate the deployment and configuration of Digital Twin models on Litmus Edge.</p>
      </div>

      <!-- DT: no-connection prompt (shown when LE not connected) -->
      <div class="card" id="dtNoConnection">
        <div class="card-header">
          <div class="card-icon icon-json" style="background:rgba(62,207,142,0.12)">⬡</div>
          <div>
            <div class="card-title">Connect to Litmus Edge</div>
            <div class="card-desc">Switch to <strong>Litmus Edge</strong> mode and connect using the top panel to list Digital Twin models.</div>
          </div>
        </div>
      </div>

      <!-- DT: model list (shown after LE connected) -->
      <div class="card" id="dtModelCard" style="display:none">
        <div class="card-header">
          <div class="card-icon icon-json" style="background:rgba(62,207,142,0.12)">⬡</div>
          <div>
            <div class="card-title">Select Template Model</div>
            <div class="card-desc">Pick a Digital Twin model from your connected Litmus Edge device</div>
          </div>
        </div>
        <div class="dt-status" id="dtStatus" style="display:none"></div>
        <div class="instance-list" id="dtModelList"></div>
      </div>

      <!-- DT: Model Preview (shown after a model is selected) -->
      <div class="card" id="dtPreviewCard" style="display:none">
        <div class="card-header">
          <div class="card-icon icon-json" style="background:rgba(62,207,142,0.12)">&#x1f50d;</div>
          <div style="flex:1">
            <div class="card-title">Model Preview</div>
            <div class="card-desc" id="dtPreviewDesc">Inspect the selected template model configuration</div>
          </div>
          <button type="button" class="dt-raw-btn" id="dtRawBtn" title="View raw API responses">{ } Raw JSON</button>
        </div>
        <div class="dt-preview-tabs">
          <button type="button" class="dt-tab-btn active" data-dtab="static">Static Attributes</button>
          <button type="button" class="dt-tab-btn" data-dtab="dynamic">Dynamic Attributes</button>
          <button type="button" class="dt-tab-btn" data-dtab="hierarchy">Hierarchy / Schema</button>
        </div>
        <div class="dt-preview-status" id="dtPreviewStatus"></div>
        <div class="dt-tab-content active" id="dtTabStatic">
          <table class="dt-attr-table" id="dtStaticTable">
            <thead><tr><th>Key</th><th>Value</th></tr></thead>
            <tbody></tbody>
          </table>
          <div class="dt-empty" id="dtStaticEmpty">No static attributes found</div>
        </div>
        <div class="dt-tab-content" id="dtTabDynamic">
          <table class="dt-attr-table" id="dtDynamicTable">
            <thead><tr><th>Name</th><th>Unit</th><th>Data Type</th><th>Skip Key</th><th>Topic Expression</th><th>Schema ID</th></tr></thead>
            <tbody></tbody>
          </table>
          <div class="dt-empty" id="dtDynamicEmpty">No dynamic attributes found</div>
        </div>
        <div class="dt-tab-content" id="dtTabHierarchy">
          <div class="dt-tree" id="dtHierarchyTree"></div>
          <div class="dt-empty" id="dtHierarchyEmpty">No hierarchy data found</div>
        </div>
      </div>

      <!-- DT: Cloning Tabs (shown after a model is selected) -->
      <div class="card" id="dtCloneCard" style="display:none">
        <div class="card-header">
          <div class="card-icon icon-json" style="background:rgba(255,200,60,0.12)">&#x2699;</div>
          <div style="flex:1">
            <div class="card-title">Cloning Tabs</div>
            <div class="card-desc">Configure clone models &mdash; each tab creates a new clone from the template</div>
          </div>
        </div>

        <div class="dtc-tab-bar" id="dtcTabBar">
          <button type="button" class="dtc-tab-btn active" data-dtctab="0">Clone 1</button>
          <button type="button" class="dtc-tab-add" id="dtcAddBtn" title="Add clone tab">+</button>
        </div>

        <div id="dtcTabPanes">
          <div class="dtc-pane active" data-dtctab="0">
            <div class="dtc-name-row">
              <label>Clone Name:</label>
              <input type="text" class="dtc-name-input" placeholder="Enter clone model name (required)..." autocomplete="off">
            </div>
            <textarea class="dtc-paste-area" placeholder="Paste your Excel cells here...&#10;&#10;Name&#9;Unit&#9;DataType&#9;SkipKey&#9;TopicExpression&#9;SchemaID&#9;Hierarchy&#10;CYCLE_COMPLETE&#9;&#9;JSON&#9;&#9;devicehub.alias.{device}.CYCLE_COMPLETE&#9;data_format&#9;root/data" spellcheck="false"></textarea>
            <div class="dtc-row-count"></div>
            <div class="dtc-preview-wrap" style="max-height:200px;overflow-y:auto">
              <table class="dtc-preview-table" style="display:none">
                <thead><tr><th>Name</th><th>Unit</th><th>DataType</th><th>Skip Key</th><th>Topic Expr</th><th>Schema ID</th><th>Hierarchy</th></tr></thead>
                <tbody></tbody>
              </table>
            </div>
          </div>
        </div>

        <button type="button" class="btn-push-clone" id="dtcPushBtn" disabled>
          Push Clones to Litmus Edge
        </button>
        <div class="dtc-push-results" id="dtcPushResults"></div>
      </div>

    </div>
    <!-- /pane-dtwin -->

    <!-- Raw JSON Modal -->
    <div class="dt-raw-overlay" id="dtRawOverlay">
      <div class="dt-raw-modal">
        <div class="dt-raw-modal-header">
          <h3>Raw API Responses</h3>
          <button type="button" class="dt-raw-close" id="dtRawClose">&times;</button>
        </div>
        <div class="dt-raw-tabs" id="dtRawTabs">
          <button type="button" class="dt-raw-tab active" data-raw="static">Static Attributes</button>
          <button type="button" class="dt-raw-tab" data-raw="dynamic">Dynamic Attributes</button>
          <button type="button" class="dt-raw-tab" data-raw="hierarchy">Hierarchy</button>
          <button type="button" class="dt-raw-tab" data-raw="transformations">Transformations</button>
        </div>
        <div class="dt-raw-body">
          <pre id="dtRawContent"></pre>
        </div>
      </div>
    </div>

  </div>
  <!-- /bottom-panel -->
</div>
<!-- /main-area -->

<script>
  // ══════════════════════════════════════════════════════════════════════════
  //  SIDEBAR NAVIGATION
  // ══════════════════════════════════════════════════════════════════════════
  const APP_TITLES = {
    connector: 'Integration Connector Assist',
    dtwin:     'Digital Twin Model Assist',
  };

  function selectApp(appId, navEl) {
    // Update sidebar active state
    document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
    navEl.classList.add('active');
    // Swap pane
    document.querySelectorAll('.app-pane').forEach(p => p.classList.remove('active'));
    const pane = document.getElementById('pane-' + appId);
    if (pane) pane.classList.add('active');
    // Update top bar title
    document.getElementById('topAppTitle').textContent = APP_TITLES[appId] || '';
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  MODE TOGGLE  (Manual ↔ Litmus Edge)
  // ══════════════════════════════════════════════════════════════════════════
  const submitBtn = document.getElementById('submitBtn');
  const pushBtn   = document.getElementById('pushBtn');
  const jsonFile  = document.getElementById('jsonFile');
  let currentMode = 'manual';
  let leInstances = [];
  let selectedInstanceIdx = -1;
  let detectedTable = '';
  let tabCounter = 1;

  const manualSteps =
    '<div class="step"><div class="step-num">01</div><div class="step-text"><strong>Upload</strong> your connector JSON</div></div>' +
    '<div class="step"><div class="step-num">02</div><div class="step-text"><strong>Add tabs</strong> for each connector. <strong>Paste</strong> key\u2192value columns and set table names</div></div>' +
    '<div class="step"><div class="step-num">03</div><div class="step-text"><strong>Download</strong> one JSON with all tabs as separate instances</div></div>';
  const leSteps =
    '<div class="step"><div class="step-num">01</div><div class="step-text"><strong>Connect</strong> to your Litmus Edge device using IP &amp; API token</div></div>' +
    '<div class="step"><div class="step-num">02</div><div class="step-text"><strong>Select</strong> a connector instance, then <strong>add tabs</strong> with new mappings &amp; table names</div></div>' +
    '<div class="step"><div class="step-num">03</div><div class="step-text"><strong>Push</strong> updated instances directly back to Litmus Edge</div></div>';

  document.getElementById('modeToggle').addEventListener('click', function() {
    const howSteps = document.getElementById('howSteps');
    if (currentMode === 'manual') {
      currentMode = 'le';
      this.classList.add('le-active');
      document.getElementById('labelManual').className = 'mode-label inactive';
      document.getElementById('labelLE').className     = 'mode-label active';
      // Show LE row in top panel
      document.getElementById('leRow').classList.remove('le-row-hidden');
      // In bottom panel: hide manual card, show instance selector
      document.getElementById('manualCard').style.display      = 'none';
      document.getElementById('leInstanceCard').style.display  = 'block';
      submitBtn.style.display = 'none';
      pushBtn.style.display   = 'block';
      if (howSteps) howSteps.innerHTML = leSteps;
    } else {
      currentMode = 'manual';
      this.classList.remove('le-active');
      document.getElementById('labelManual').className = 'mode-label active';
      document.getElementById('labelLE').className     = 'mode-label inactive';
      document.getElementById('leRow').classList.add('le-row-hidden');
      document.getElementById('manualCard').style.display      = 'block';
      document.getElementById('leInstanceCard').style.display  = 'none';
      submitBtn.style.display = 'block';
      pushBtn.style.display   = 'none';
      document.getElementById('pushResults').style.display = 'none';
      document.getElementById('leVersionBadge').style.display = 'none';
      if (howSteps) howSteps.innerHTML = manualSteps;
      // Reset DT pane state
      resetDtPane();
    }
    checkReady();
  });

  // ══════════════════════════════════════════════════════════════════════════
  //  PING (top panel)
  // ══════════════════════════════════════════════════════════════════════════
  document.getElementById('pingBtn').addEventListener('click', function() {
    const ip = document.getElementById('pingIpInput').value.trim()
             || document.getElementById('leIpInput').value.trim();
    if (!ip) return;
    document.getElementById('pingIpInput').value = ip;
    const res = document.getElementById('pingResult');
    const btn = this;
    btn.disabled = true; btn.textContent = 'Pinging\u2026';
    res.className = 'ping-result-inline pending';
    res.textContent = 'Pinging\u2026';
    fetch('/api/ping?ip=' + encodeURIComponent(ip))
      .then(r => r.json())
      .then(data => {
        if (data.reachable) {
          res.className = 'ping-result-inline ok';
          res.textContent = '\u2713 ' + ip + ' reachable';
        } else {
          res.className = 'ping-result-inline fail';
          res.textContent = '\u2717 ' + ip + ' unreachable';
        }
      })
      .catch(err => {
        res.className = 'ping-result-inline fail';
        res.textContent = '\u2717 ' + err.message;
      })
      .finally(() => { btn.disabled = false; btn.textContent = 'Ping'; });
  });

  // ══════════════════════════════════════════════════════════════════════════
  //  LITMUS EDGE — Connect & fetch instances
  // ══════════════════════════════════════════════════════════════════════════
  document.getElementById('leConnectBtn').addEventListener('click', function() {
    const ip    = document.getElementById('leIpInput').value.trim();
    const token = document.getElementById('leTokenInput').value.trim();
    const status = document.getElementById('leStatus');
    if (!ip)    { status.textContent = 'Please enter an IP address'; status.className = 'le-status error'; return; }
    if (!token) { status.textContent = 'Please enter an API token';  status.className = 'le-status error'; return; }
    const btn = this;
    btn.disabled = true; btn.textContent = 'Connecting\u2026';
    status.textContent = 'Fetching instances\u2026'; status.className = 'le-status';

    fetch('/api/instances?ip=' + encodeURIComponent(ip) + '&token=' + encodeURIComponent(token))
      .then(r => {
        if (!r.ok) return r.text().then(t => { throw new Error(t || 'Connection failed'); });
        return r.json();
      })
      .then(data => {
        leInstances = data;
        selectedInstanceIdx = -1;
        document.getElementById('leInstanceJson').value = '';
        status.textContent = data.length + ' instance' + (data.length !== 1 ? 's' : '') + ' found';
        status.className = 'le-status success';
        renderInstanceList(data);
        // Fetch LE firmware version
        fetch('/api/deviceinfo?ip=' + encodeURIComponent(ip) + '&token=' + encodeURIComponent(token))
          .then(r => r.ok ? r.json() : null)
          .then(info => {
            if (info && info.firmwareVersion) {
              document.getElementById('leVersionValue').textContent = 'v' + info.firmwareVersion;
              document.getElementById('leVersionBadge').style.display = 'inline-flex';
            }
          })
          .catch(() => {});
        // Also fetch DT models for Digital Twin pane
        fetchDtModels(ip, token);
      })
      .catch(err => {
        status.textContent = 'Error: ' + err.message; status.className = 'le-status error';
        leInstances = [];
      })
      .finally(() => { btn.disabled = false; btn.textContent = 'Connect'; });
  });

  function renderInstanceList(instances) {
    const list = document.getElementById('instanceList');
    list.innerHTML = '';
    instances.forEach((inst, i) => {
      const div = document.createElement('div');
      div.className = 'instance-item';
      div.setAttribute('data-idx', i);
      const tableName = inst._tableName || 'unknown';
      const provider  = inst.providerId || '';
      div.innerHTML = '<span class="inst-icon">\u25cb</span>' +
        '<span class="inst-table">' + escHtml(tableName) + '</span>' +
        '<span class="inst-provider">' + escHtml(provider) + '</span>';
      div.addEventListener('click', () => selectInstance(i));
      list.appendChild(div);
    });
  }

  function selectInstance(idx) {
    selectedInstanceIdx = idx;
    document.querySelectorAll('.instance-item').forEach((el, i) => {
      el.classList.toggle('selected', i === idx);
      el.querySelector('.inst-icon').textContent = (i === idx) ? '\u25cf' : '\u25cb';
    });
    const inst = leInstances[idx];
    document.getElementById('leInstanceJson').value = JSON.stringify(inst._original);
    const tbl = inst._tableName || '';
    if (tbl) {
      detectedTable = tbl;
      document.getElementById('detectedTableInput').value = tbl;
      document.querySelectorAll('.replace-section').forEach(s => s.classList.add('show'));
    }
    checkReady();
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  PUSH TO LITMUS EDGE
  // ══════════════════════════════════════════════════════════════════════════
  pushBtn.addEventListener('click', function() {
    document.getElementById('leIpHidden').value    = document.getElementById('leIpInput').value.trim();
    document.getElementById('leTokenHidden').value = document.getElementById('leTokenInput').value.trim();
    const formData = new FormData(document.getElementById('mainForm'));
    pushBtn.disabled = true; pushBtn.textContent = 'Pushing\u2026';
    const resultsDiv = document.getElementById('pushResults');
    resultsDiv.style.display = 'none'; resultsDiv.innerHTML = '';
    fetch('/api/push', { method: 'POST', body: formData })
      .then(r => r.json())
      .then(data => {
        resultsDiv.style.display = 'flex';
        if (data.results && data.results.length) {
          data.results.forEach(r => {
            const div = document.createElement('div');
            div.className = 'push-result-item ' + (r.ok ? 'ok' : 'fail');
            div.textContent = (r.ok ? '\u2713 ' : '\u2717 ') + r.name + ': ' + r.message;
            resultsDiv.appendChild(div);
          });
        } else if (data.error) {
          const div = document.createElement('div');
          div.className = 'push-result-item fail';
          div.textContent = '\u2717 ' + data.error;
          resultsDiv.appendChild(div);
        }
      })
      .catch(err => {
        resultsDiv.style.display = 'flex';
        const div = document.createElement('div');
        div.className = 'push-result-item fail';
        div.textContent = '\u2717 Request failed: ' + err.message;
        resultsDiv.appendChild(div);
      })
      .finally(() => { pushBtn.disabled = false; pushBtn.textContent = 'Push to Litmus Edge'; });
  });

  // ══════════════════════════════════════════════════════════════════════════
  //  FORM READINESS CHECK
  // ══════════════════════════════════════════════════════════════════════════
  function checkReady() {
    let hasSource = (currentMode === 'manual')
      ? jsonFile.files.length > 0
      : selectedInstanceIdx >= 0;
    if (!hasSource) { submitBtn.disabled = true; pushBtn.disabled = true; return; }
    const areas = document.querySelectorAll('.mappingArea');
    let anyMapping = false;
    areas.forEach(a => { if (a.value.trim()) anyMapping = true; });
    if (currentMode === 'manual') {
      submitBtn.disabled = !anyMapping;
      pushBtn.disabled   = true;
    } else {
      submitBtn.disabled = true;
      pushBtn.disabled   = !anyMapping;
    }
    document.getElementById('tabCountInput').value   = document.querySelectorAll('.tab-pane').length;
    document.getElementById('leIpHidden').value      = document.getElementById('leIpInput').value.trim();
    document.getElementById('leTokenHidden').value   = document.getElementById('leTokenInput').value.trim();
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  JSON UPLOAD
  // ══════════════════════════════════════════════════════════════════════════
  function handleJsonUpload() {
    if (!jsonFile.files.length) return;
    document.getElementById('jsonSelected').classList.add('show');
    document.getElementById('jsonName').textContent = jsonFile.files[0].name;
    const zone = document.getElementById('jsonZone');
    zone.querySelector('.drop-icon').textContent = '\u2713';
    zone.querySelector('.drop-label').innerHTML = 'File loaded: <span>' + jsonFile.files[0].name + '</span>';
    zone.querySelector('.drop-ext').textContent  = '';
    const reader = new FileReader();
    reader.onload = (e) => {
      const wrap = document.getElementById('detectedTableWrap');
      try {
        const parsed = JSON.parse(e.target.result);
        const tbl = findTableInJson(parsed);
        if (tbl) {
          detectedTable = tbl;
          document.getElementById('detectedTableVal').textContent   = tbl;
          document.getElementById('detectedTableInput').value       = tbl;
          wrap.classList.add('show');
          document.querySelectorAll('.replace-section').forEach(s => s.classList.add('show'));
        } else {
          detectedTable = '';
          wrap.classList.remove('show');
          document.querySelectorAll('.replace-section').forEach(s => s.classList.remove('show'));
        }
      } catch(err) { detectedTable = ''; }
    };
    reader.readAsText(jsonFile.files[0]);
    checkReady();
  }
  jsonFile.addEventListener('change', handleJsonUpload);

  const jsonZone = document.getElementById('jsonZone');
  jsonZone.addEventListener('dragover',  e => { e.preventDefault(); jsonZone.classList.add('dragover'); });
  jsonZone.addEventListener('dragleave', () => jsonZone.classList.remove('dragover'));
  jsonZone.addEventListener('drop', e => {
    e.preventDefault(); jsonZone.classList.remove('dragover');
    const file = e.dataTransfer && e.dataTransfer.files && e.dataTransfer.files[0];
    if (!file) return;
    const dt = new DataTransfer(); dt.items.add(file); jsonFile.files = dt.files;
    handleJsonUpload();
  });

  // ══════════════════════════════════════════════════════════════════════════
  //  PASTE HELPERS
  // ══════════════════════════════════════════════════════════════════════════
  function parsePasted(text) {
    const pairs = [];
    for (const line of text.split(/\\r?\\n/)) {
      const delim = line.includes('\\t') ? '\\t' : ',';
      const idx   = line.indexOf(delim);
      if (idx === -1) continue;
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      if (!k || ['key','keys','value','values'].includes(k.toLowerCase())) continue;
      pairs.push([k, v]);
    }
    return pairs;
  }
  function escHtml(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

  function updatePreviewFor(pane) {
    const area    = pane.querySelector('.mappingArea');
    const preview = pane.querySelector('.mapping-preview');
    const counter = pane.querySelector('.pair-count');
    const pairs   = parsePasted(area.value);
    if (!pairs.length) {
      preview.style.display = 'none';
      counter.textContent   = '';
      counter.className     = 'pair-count';
      return;
    }
    counter.textContent = pairs.length + ' pair' + (pairs.length !== 1 ? 's' : '') + ' detected';
    counter.className   = 'pair-count has-data';
    preview.style.display = 'block';
    preview.innerHTML = pairs.map(([k, v]) =>
      '<div class="kv"><span class="k">' + escHtml(k) + '</span>' +
      '<span class="arrow">\u2192</span><span class="v">' + escHtml(v) + '</span></div>'
    ).join('');
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  TAB MANAGEMENT
  // ══════════════════════════════════════════════════════════════════════════
  function activateTab(idx) {
    document.querySelectorAll('.tab-btn[data-tab]').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
    const btn  = document.querySelector('.tab-btn[data-tab="' + idx + '"]');
    const pane = document.querySelector('.tab-pane[data-tab="' + idx + '"]');
    if (btn)  btn.classList.add('active');
    if (pane) pane.classList.add('active');
  }

  function renumberTabLabels() {
    const btns  = document.querySelectorAll('.tab-btn[data-tab]');
    const panes = document.querySelectorAll('.tab-pane');
    btns.forEach((b, i) => { b.childNodes[0].textContent = 'Tab ' + (i + 1) + ' '; });
    panes.forEach((p, i) => {
      p.querySelector('.replaceCheck').name   = 'replace_table_' + i;
      p.querySelector('.newTableInput').name  = 'new_table_' + i;
      p.querySelector('.mappingArea').name    = 'mapping_text_' + i;
    });
    document.getElementById('tabCountInput').value = panes.length;
  }

  function addTab() {
    const idx       = tabCounter++;
    const tabBar    = document.getElementById('tabBar');
    const panes     = document.getElementById('tabPanes');
    const paneCount = document.querySelectorAll('.tab-pane').length;

    const btn = document.createElement('button');
    btn.type = 'button'; btn.className = 'tab-btn';
    btn.setAttribute('data-tab', idx);
    btn.innerHTML = 'Tab ' + (paneCount + 1) + ' <button type="button" class="tab-close" title="Close tab">\u00d7</button>';
    btn.addEventListener('click', (e) => { if (e.target.classList.contains('tab-close')) return; activateTab(idx); });
    btn.querySelector('.tab-close').addEventListener('click', () => removeTab(idx));
    tabBar.insertBefore(btn, document.getElementById('addTabBtn'));

    const pane = document.createElement('div');
    pane.className = 'tab-pane'; pane.setAttribute('data-tab', idx);
    const showReplace = detectedTable ? ' show' : '';
    pane.innerHTML =
      '<div class="replace-section' + showReplace + '">' +
        '<label class="checkbox-row"><input type="checkbox" class="replaceCheck" name="replace_table_' + paneCount + '" value="1">' +
        '<span class="checkbox-label">Replace table name in output</span></label>' +
        '<div class="new-table-input"><input type="text" name="new_table_' + paneCount + '" class="text-input newTableInput" placeholder="New table name\u2026" autocomplete="off" style="width:100%"></div>' +
      '</div>' +
      '<div style="margin-top:14px">' +
        '<textarea name="mapping_text_' + paneCount + '" class="paste-area mappingArea" placeholder="Paste your Excel cells here\u2026" spellcheck="false"></textarea>' +
        '<div class="pair-count"></div><div class="mapping-preview" style="display:none"></div>' +
      '</div>';
    pane.querySelector('.replaceCheck').addEventListener('change', function() {
      pane.querySelector('.new-table-input').classList.toggle('show', this.checked);
      if (!this.checked) pane.querySelector('.newTableInput').value = '';
    });
    pane.querySelector('.mappingArea').addEventListener('input', () => { updatePreviewFor(pane); checkReady(); });
    panes.appendChild(pane);
    renumberTabLabels();
    activateTab(idx);
    checkReady();
  }

  function removeTab(idx) {
    if (document.querySelectorAll('.tab-pane').length <= 1) return;
    const pane = document.querySelector('.tab-pane[data-tab="' + idx + '"]');
    const btn  = document.querySelector('.tab-btn[data-tab="' + idx + '"]');
    const wasActive = btn && btn.classList.contains('active');
    if (pane) pane.remove();
    if (btn)  btn.remove();
    renumberTabLabels();
    if (wasActive) {
      const first = document.querySelector('.tab-btn[data-tab]');
      if (first) activateTab(first.getAttribute('data-tab'));
    }
    checkReady();
  }

  // Wire up initial tab 0
  (function() {
    const pane0 = document.querySelector('.tab-pane[data-tab="0"]');
    pane0.querySelector('.replaceCheck').addEventListener('change', function() {
      pane0.querySelector('.new-table-input').classList.toggle('show', this.checked);
      if (!this.checked) pane0.querySelector('.newTableInput').value = '';
    });
    pane0.querySelector('.mappingArea').addEventListener('input', () => { updatePreviewFor(pane0); checkReady(); });
    document.querySelector('.tab-btn[data-tab="0"]').addEventListener('click', () => activateTab(0));
  })();
  document.getElementById('addTabBtn').addEventListener('click', addTab);

  // ══════════════════════════════════════════════════════════════════════════
  //  JSON HELPERS
  // ══════════════════════════════════════════════════════════════════════════
  // ══════════════════════════════════════════════════════════════════════════
  //  DIGITAL TWIN — Fetch & render model list
  // ══════════════════════════════════════════════════════════════════════════
  let dtModels = [];
  let selectedDtModelIdx = -1;

  function fetchDtModels(ip, token) {
    const card   = document.getElementById('dtModelCard');
    const noConn = document.getElementById('dtNoConnection');
    const status = document.getElementById('dtStatus');
    const list   = document.getElementById('dtModelList');
    // Show card, hide no-connection prompt
    noConn.style.display = 'none';
    card.style.display   = 'block';
    status.style.display = 'block';
    status.textContent   = 'Fetching models\u2026';
    status.className     = 'dt-status';
    list.innerHTML       = '';

    fetch('/api/dt/models?ip=' + encodeURIComponent(ip) + '&token=' + encodeURIComponent(token))
      .then(r => {
        if (!r.ok) return r.text().then(t => { throw new Error(t || 'Failed to fetch models'); });
        return r.json();
      })
      .then(models => {
        dtModels = models;
        selectedDtModelIdx = -1;
        if (!models.length) {
          status.textContent = 'No Digital Twin models found on this device';
          status.className   = 'dt-status';
          return;
        }
        status.textContent = models.length + ' model' + (models.length !== 1 ? 's' : '') + ' found';
        status.className   = 'dt-status success';
        renderDtModelList(models);
      })
      .catch(err => {
        status.textContent = 'Error: ' + err.message;
        status.className   = 'dt-status error';
        dtModels = [];
      });
  }

  function renderDtModelList(models) {
    const list = document.getElementById('dtModelList');
    list.innerHTML = '';
    models.forEach((m, i) => {
      const div = document.createElement('div');
      div.className = 'instance-item';
      div.setAttribute('data-idx', i);
      div.innerHTML = '<span class="inst-icon">\u25cb</span>' +
        '<span class="inst-table">' + escHtml(m.Name || 'Unnamed') + '</span>' +
        '<span class="inst-provider">' + escHtml(m.ID || '') + '</span>';
      div.addEventListener('click', () => selectDtModel(i));
      list.appendChild(div);
    });
  }

  function selectDtModel(idx) {
    selectedDtModelIdx = idx;
    document.querySelectorAll('#dtModelList .instance-item').forEach((el, i) => {
      el.classList.toggle('selected', i === idx);
      el.querySelector('.inst-icon').textContent = (i === idx) ? '\u25cf' : '\u25cb';
    });
    // Show preview and fetch model details
    const model = dtModels[idx];
    if (model) {
      document.getElementById('dtPreviewCard').style.display = 'block';
      document.getElementById('dtCloneCard').style.display = 'block';
      document.getElementById('dtPreviewDesc').textContent = 'Inspecting: ' + (model.Name || 'Unnamed');
      fetchDtPreview(model.ID);
    }
  }

  // ── DT Preview tabs ──────────────────────────────────────────────────
  document.querySelectorAll('.dt-tab-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll('.dt-tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.dt-tab-content').forEach(c => c.classList.remove('active'));
      this.classList.add('active');
      const target = document.getElementById('dtTab' + this.getAttribute('data-dtab').charAt(0).toUpperCase() + this.getAttribute('data-dtab').slice(1));
      if (target) target.classList.add('active');
    });
  });

  // ── DT Preview fetch ─────────────────────────────────────────────────
  function dtApiParams() {
    const ip = document.getElementById('leIpInput').value.trim();
    const token = document.getElementById('leTokenInput').value.trim();
    return 'ip=' + encodeURIComponent(ip) + '&token=' + encodeURIComponent(token);
  }

  // ── Raw JSON modal ──
  document.getElementById('dtRawBtn').addEventListener('click', function() {
    if (!window._dtRawData) return;
    showRawTab('static');
    document.getElementById('dtRawOverlay').classList.add('open');
  });
  document.getElementById('dtRawClose').addEventListener('click', function() {
    document.getElementById('dtRawOverlay').classList.remove('open');
  });
  document.getElementById('dtRawOverlay').addEventListener('click', function(e) {
    if (e.target === this) this.classList.remove('open');
  });
  document.querySelectorAll('.dt-raw-tab').forEach(btn => {
    btn.addEventListener('click', function() {
      document.querySelectorAll('.dt-raw-tab').forEach(b => b.classList.remove('active'));
      this.classList.add('active');
      showRawTab(this.dataset.raw);
    });
  });
  function showRawTab(key) {
    var data = window._dtRawData ? window._dtRawData[key] : null;
    document.getElementById('dtRawContent').textContent = data ? JSON.stringify(data, null, 2) : 'No data';
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  DT CLONING TABS
  // ══════════════════════════════════════════════════════════════════════════
  var dtcTabCounter = 1;

  function dtcActivateTab(idx) {
    document.querySelectorAll('.dtc-tab-btn[data-dtctab]').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.dtc-pane').forEach(p => p.classList.remove('active'));
    var btn  = document.querySelector('.dtc-tab-btn[data-dtctab="' + idx + '"]');
    var pane = document.querySelector('.dtc-pane[data-dtctab="' + idx + '"]');
    if (btn)  btn.classList.add('active');
    if (pane) pane.classList.add('active');
  }

  function dtcRenumberLabels() {
    document.querySelectorAll('.dtc-tab-btn[data-dtctab]').forEach(function(b, i) {
      b.childNodes[0].textContent = 'Clone ' + (i + 1) + ' ';
    });
  }

  function dtcParsePasted(text) {
    var rows = [];
    text.split(/\\r?\\n/).forEach(function(line) {
      if (!line.trim()) return;
      var delim = line.indexOf('\\t') !== -1 ? '\\t' : ',';
      var cols = line.split(delim).map(function(c) { return c.trim(); });
      if (cols.length < 1) return;
      var nameL = (cols[0] || '').toLowerCase();
      if (nameL === 'name' || nameL === 'header') return;
      rows.push({
        Name: cols[0] || '',
        Unit: cols[1] || '',
        DataType: cols[2] || 'JSON',
        SkipKey: cols[3] || '',
        TopicExpression: cols[4] || '',
        SchemaID: cols[5] || '',
        Hierarchy: cols[6] || ''
      });
    });
    return rows;
  }

  function dtcUpdatePreview(pane) {
    var area = pane.querySelector('.dtc-paste-area');
    var tbl  = pane.querySelector('.dtc-preview-table');
    var cnt  = pane.querySelector('.dtc-row-count');
    var tbody = tbl.querySelector('tbody');
    tbody.innerHTML = '';
    var rows = dtcParsePasted(area.value);
    if (rows.length === 0) {
      tbl.style.display = 'none';
      cnt.textContent = '';
    } else {
      tbl.style.display = 'table';
      cnt.textContent = rows.length + ' attribute(s) parsed';
      rows.forEach(function(r) {
        var tr = document.createElement('tr');
        tr.innerHTML =
          '<td>' + escHtml(r.Name) + '</td>' +
          '<td>' + escHtml(r.Unit || '\\u2014') + '</td>' +
          '<td>' + escHtml(r.DataType) + '</td>' +
          '<td>' + escHtml(r.SkipKey || '\\u2014') + '</td>' +
          '<td>' + escHtml(r.TopicExpression || '\\u2014') + '</td>' +
          '<td>' + escHtml(r.SchemaID || '\\u2014') + '</td>' +
          '<td>' + escHtml(r.Hierarchy || '\\u2014') + '</td>';
        tbody.appendChild(tr);
      });
    }
    dtcCheckReady();
  }

  function dtcCheckReady() {
    var anyReady = false;
    document.querySelectorAll('.dtc-pane').forEach(function(pane) {
      var name = pane.querySelector('.dtc-name-input').value.trim();
      var rows = dtcParsePasted(pane.querySelector('.dtc-paste-area').value);
      if (name && rows.length > 0) anyReady = true;
    });
    document.getElementById('dtcPushBtn').disabled = !anyReady;
  }

  function dtcAddTab() {
    var idx = dtcTabCounter++;
    var bar = document.getElementById('dtcTabBar');
    var panes = document.getElementById('dtcTabPanes');
    var count = document.querySelectorAll('.dtc-pane').length;

    var btn = document.createElement('button');
    btn.type = 'button'; btn.className = 'dtc-tab-btn';
    btn.setAttribute('data-dtctab', idx);
    btn.innerHTML = 'Clone ' + (count + 1) + ' <button type="button" class="dtc-close" title="Close tab">\\u00d7</button>';
    btn.addEventListener('click', function(e) { if (e.target.classList.contains('dtc-close')) return; dtcActivateTab(idx); });
    btn.querySelector('.dtc-close').addEventListener('click', function() { dtcRemoveTab(idx); });
    bar.insertBefore(btn, document.getElementById('dtcAddBtn'));

    var pane = document.createElement('div');
    pane.className = 'dtc-pane'; pane.setAttribute('data-dtctab', idx);
    pane.innerHTML =
      '<div class="dtc-name-row">' +
        '<label>Clone Name:</label>' +
        '<input type="text" class="dtc-name-input" placeholder="Enter clone model name (required)..." autocomplete="off">' +
      '</div>' +
      '<textarea class="dtc-paste-area" placeholder="Paste your Excel cells here...\\nName\\tUnit\\tDataType\\tSkipKey\\tTopicExpression\\tSchemaID\\tHierarchy" spellcheck="false"></textarea>' +
      '<div class="dtc-row-count"></div>' +
      '<div class="dtc-preview-wrap" style="max-height:200px;overflow-y:auto">' +
        '<table class="dtc-preview-table" style="display:none">' +
          '<thead><tr><th>Name</th><th>Unit</th><th>DataType</th><th>Skip Key</th><th>Topic Expr</th><th>Schema ID</th><th>Hierarchy</th></tr></thead>' +
          '<tbody></tbody>' +
        '</table>' +
      '</div>';
    pane.querySelector('.dtc-paste-area').addEventListener('input', function() { dtcUpdatePreview(pane); });
    pane.querySelector('.dtc-name-input').addEventListener('input', function() { dtcCheckReady(); });
    panes.appendChild(pane);
    dtcRenumberLabels();
    dtcActivateTab(idx);
    dtcCheckReady();
  }

  function dtcRemoveTab(idx) {
    if (document.querySelectorAll('.dtc-pane').length <= 1) return;
    var pane = document.querySelector('.dtc-pane[data-dtctab="' + idx + '"]');
    var btn  = document.querySelector('.dtc-tab-btn[data-dtctab="' + idx + '"]');
    var wasActive = btn && btn.classList.contains('active');
    if (pane) pane.remove();
    if (btn)  btn.remove();
    dtcRenumberLabels();
    if (wasActive) {
      var first = document.querySelector('.dtc-tab-btn[data-dtctab]');
      if (first) dtcActivateTab(first.getAttribute('data-dtctab'));
    }
    dtcCheckReady();
  }

  function dtcResetAll() {
    var panes = document.getElementById('dtcTabPanes');
    panes.innerHTML = '';
    var bar = document.getElementById('dtcTabBar');
    bar.querySelectorAll('.dtc-tab-btn').forEach(function(b) { b.remove(); });
    dtcTabCounter = 1;
    // Recreate initial tab
    var btn0 = document.createElement('button');
    btn0.type = 'button'; btn0.className = 'dtc-tab-btn active';
    btn0.setAttribute('data-dtctab', '0');
    btn0.textContent = 'Clone 1 ';
    btn0.addEventListener('click', function() { dtcActivateTab('0'); });
    bar.insertBefore(btn0, document.getElementById('dtcAddBtn'));
    var pane0 = document.createElement('div');
    pane0.className = 'dtc-pane active'; pane0.setAttribute('data-dtctab', '0');
    pane0.innerHTML =
      '<div class="dtc-name-row">' +
        '<label>Clone Name:</label>' +
        '<input type="text" class="dtc-name-input" placeholder="Enter clone model name (required)..." autocomplete="off">' +
      '</div>' +
      '<textarea class="dtc-paste-area" placeholder="Paste your Excel cells here...\\nName\\tUnit\\tDataType\\tSkipKey\\tTopicExpression\\tSchemaID\\tHierarchy" spellcheck="false"></textarea>' +
      '<div class="dtc-row-count"></div>' +
      '<div class="dtc-preview-wrap" style="max-height:200px;overflow-y:auto">' +
        '<table class="dtc-preview-table" style="display:none">' +
          '<thead><tr><th>Name</th><th>Unit</th><th>DataType</th><th>Skip Key</th><th>Topic Expr</th><th>Schema ID</th><th>Hierarchy</th></tr></thead>' +
          '<tbody></tbody>' +
        '</table>' +
      '</div>';
    pane0.querySelector('.dtc-paste-area').addEventListener('input', function() { dtcUpdatePreview(pane0); });
    pane0.querySelector('.dtc-name-input').addEventListener('input', function() { dtcCheckReady(); });
    panes.appendChild(pane0);
    document.getElementById('dtcPushResults').style.display = 'none';
    document.getElementById('dtcPushResults').innerHTML = '';
    dtcCheckReady();
  }

  // Wire initial tab 0 listeners
  (function() {
    var pane0 = document.querySelector('.dtc-pane[data-dtctab="0"]');
    if (pane0) {
      pane0.querySelector('.dtc-paste-area').addEventListener('input', function() { dtcUpdatePreview(pane0); });
      pane0.querySelector('.dtc-name-input').addEventListener('input', function() { dtcCheckReady(); });
    }
  })();

  document.getElementById('dtcAddBtn').addEventListener('click', dtcAddTab);

  // ── Push Clones ──
  document.getElementById('dtcPushBtn').addEventListener('click', function() {
    var pushBtn = this;
    var resultsDiv = document.getElementById('dtcPushResults');
    resultsDiv.style.display = 'flex'; resultsDiv.innerHTML = '';
    pushBtn.disabled = true; pushBtn.textContent = 'Pushing clones\\u2026';

    var templateModel = dtModels[selectedDtModelIdx];
    if (!templateModel) {
      resultsDiv.innerHTML = '<div class="dtc-push-result-item fail">No template model selected</div>';
      pushBtn.disabled = false; pushBtn.textContent = 'Push Clones to Litmus Edge';
      return;
    }

    var clones = [];
    document.querySelectorAll('.dtc-pane').forEach(function(pane) {
      var name = pane.querySelector('.dtc-name-input').value.trim();
      var rows = dtcParsePasted(pane.querySelector('.dtc-paste-area').value);
      if (name && rows.length > 0) {
        clones.push({ name: name, attributes: rows });
      }
    });

    if (clones.length === 0) {
      resultsDiv.innerHTML = '<div class="dtc-push-result-item fail">No valid clone tabs configured</div>';
      pushBtn.disabled = false; pushBtn.textContent = 'Push Clones to Litmus Edge';
      return;
    }

    var payload = {
      template_model_id: templateModel.ID,
      clones: clones
    };

    var base = dtApiParams();
    fetch('/api/dt/push-clones?' + base, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    })
    .then(function(r) { return r.json(); })
    .then(function(data) {
      if (data.results) {
        data.results.forEach(function(r) {
          var div = document.createElement('div');
          div.className = 'dtc-push-result-item ' + (r.ok ? 'ok' : 'fail');
          div.textContent = (r.ok ? '\\u2713 ' : '\\u2717 ') + r.name + ': ' + r.message;
          resultsDiv.appendChild(div);
        });
      } else if (data.error) {
        resultsDiv.innerHTML = '<div class="dtc-push-result-item fail">' + escHtml(data.error) + '</div>';
      }
    })
    .catch(function(err) {
      resultsDiv.innerHTML = '<div class="dtc-push-result-item fail">Network error: ' + escHtml(err.message) + '</div>';
    })
    .finally(function() {
      pushBtn.disabled = false; pushBtn.textContent = 'Push Clones to Litmus Edge';
      dtcCheckReady();
    });
  });

  function fetchDtPreview(modelId) {
    const status = document.getElementById('dtPreviewStatus');
    status.textContent = 'Loading model data\u2026';
    status.className = 'dt-preview-status';
    // Reset all tabs
    document.querySelector('#dtStaticTable tbody').innerHTML = '';
    document.querySelector('#dtDynamicTable tbody').innerHTML = '';
    document.getElementById('dtHierarchyTree').innerHTML = '';
    document.getElementById('dtStaticEmpty').style.display = 'none';
    document.getElementById('dtDynamicEmpty').style.display = 'none';
    document.getElementById('dtHierarchyEmpty').style.display = 'none';

    const base = dtApiParams() + '&model_id=' + encodeURIComponent(modelId);

    // Fetch all three in parallel
    Promise.all([
      fetch('/api/dt/static-attrs?' + base).then(r => r.ok ? r.json() : []),
      fetch('/api/dt/dynamic-attrs?' + base).then(r => r.ok ? r.json() : []),
      fetch('/api/dt/hierarchy?' + base).then(r => r.ok ? r.json() : {}),
      fetch('/api/dt/transformations?' + base).then(r => r.ok ? r.json() : [])
    ]).then(([staticAttrs, dynamicAttrs, hierarchy, transformations]) => {
      window._dtRawData = {
        static: staticAttrs,
        dynamic: dynamicAttrs,
        hierarchy: hierarchy,
        transformations: transformations
      };
      renderDtStaticAttrs(staticAttrs);
      renderDtDynamicAttrs(dynamicAttrs, transformations);
      renderDtHierarchy(hierarchy);
      status.textContent = '';
    }).catch(err => {
      status.textContent = 'Error loading preview: ' + err.message;
      status.className = 'dt-preview-status error';
    });
  }

  function renderDtStaticAttrs(attrs) {
    const tbody = document.querySelector('#dtStaticTable tbody');
    const empty = document.getElementById('dtStaticEmpty');
    if (!attrs || !attrs.length) { empty.style.display = 'block'; return; }
    empty.style.display = 'none';
    attrs.forEach(a => {
      const tr = document.createElement('tr');
      tr.innerHTML = '<td>' + escHtml(a.Key || '') + '</td><td>' + escHtml(a.Value || '') + '</td>';
      tbody.appendChild(tr);
    });
  }

  function renderDtDynamicAttrs(attrs, transformations) {
    const tbody = document.querySelector('#dtDynamicTable tbody');
    const empty = document.getElementById('dtDynamicEmpty');
    if (!attrs || !attrs.length) { empty.style.display = 'block'; return; }
    empty.style.display = 'none';
    var schemaMap = {};
    if (transformations && transformations.length) {
      transformations.forEach(t => { schemaMap[t.ID] = t.Name; });
    }
    attrs.forEach(a => {
      const tr = document.createElement('tr');
      var skip = a.SkipParentKey === true || a.SkipParentKey === 'true' ? '\u2611' : '\u2610';
      var topicExpr = a.TopicExpression || a.Topic || '\u2014';
      var schemaName = a.SchemaID ? (schemaMap[a.SchemaID] || a.SchemaID) : '\u2014';
      tr.innerHTML = '<td>' + escHtml(a.Name || '') + '</td>' +
        '<td>' + escHtml(a.Unit || '\u2014') + '</td>' +
        '<td>' + escHtml(a.DataType || 'JSON') + '</td>' +
        '<td style="text-align:center">' + skip + '</td>' +
        '<td>' + escHtml(topicExpr) + '</td>' +
        '<td>' + escHtml(schemaName) + '</td>';
      tbody.appendChild(tr);
    });
  }

  function renderDtHierarchy(node) {
    const tree = document.getElementById('dtHierarchyTree');
    const empty = document.getElementById('dtHierarchyEmpty');
    if (!node || (!node.Name && !node.Childs)) { empty.style.display = 'block'; return; }
    empty.style.display = 'none';
    tree.innerHTML = buildTreeHtml(node, 0);
  }

  function buildTreeHtml(node, depth) {
    if (!node) return '';
    var isFolder = !node.Node || (node.Node && node.Node.IsFolder);
    var attrType = node.Node && node.Node.AttributeType ? node.Node.AttributeType : '';
    var cls = isFolder ? 'dt-tree-folder' : (attrType === 'dynamic' ? 'dt-tree-attr-dynamic' : 'dt-tree-attr-static');
    var icon = isFolder ? '\u25b8' : (attrType === 'dynamic' ? '\u26a1' : '\u2022');
    var html = '<div class="dt-tree-node" style="padding-left:' + (depth * 18) + 'px">';
    html += '<span class="dt-tree-label ' + cls + '"><span class="dt-tree-icon">' + icon + '</span>' + escHtml(node.Name || '') + '</span>';
    html += '</div>';
    if (node.Childs && node.Childs.length) {
      node.Childs.forEach(function(child) { html += buildTreeHtml(child, depth + 1); });
    }
    return html;
  }

  // Reset DT pane when switching back to manual mode
  function resetDtPane() {
    dtModels = [];
    selectedDtModelIdx = -1;
    document.getElementById('dtModelCard').style.display   = 'none';
    document.getElementById('dtPreviewCard').style.display  = 'none';
    document.getElementById('dtCloneCard').style.display    = 'none';
    document.getElementById('dtNoConnection').style.display = 'block';
    var list = document.getElementById('dtModelList');
    if (list) list.innerHTML = '';
    var status = document.getElementById('dtStatus');
    if (status) { status.style.display = 'none'; status.textContent = ''; }
    dtcResetAll();
  }

  // ══════════════════════════════════════════════════════════════════════════
  //  JSON HELPERS
  // ══════════════════════════════════════════════════════════════════════════
  function findTableInJson(obj) {
    if (Array.isArray(obj)) {
      for (const item of obj) { const r = findTableInJson(item); if (r) return r; }
    } else if (obj && typeof obj === 'object') {
      for (const [k, v] of Object.entries(obj)) {
        if (typeof v === 'string') {
          try { const inner = JSON.parse(v); if (inner && typeof inner === 'object' && inner.table) return String(inner.table); } catch(_) {}
        }
        if (v && typeof v === 'object') { const r = findTableInJson(v); if (r) return r; }
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


def replace_mapping_in_json(obj, new_mapping: dict, new_table: str = "", new_name: str = ""):
    """Recursively find and replace (or inject) every 'mapping' key in the
    structure, including inside JSON-encoded 'config' strings.
    Optionally also replace 'table' and 'name' inside config strings."""
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
                        if new_table:
                            cfg["table"] = new_table
                        if new_name:
                            cfg["name"] = new_name
                        obj[k] = json.dumps(cfg, ensure_ascii=False)
                except (json.JSONDecodeError, TypeError):
                    pass
            else:
                replace_mapping_in_json(v, new_mapping, new_table, new_name)
    elif isinstance(obj, list):
        for item in obj:
            replace_mapping_in_json(item, new_mapping, new_table, new_name)
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
        elif self.path.startswith("/api/ping"):
            self._handle_ping()
        elif self.path.startswith("/api/deviceinfo"):
            self._handle_le_deviceinfo()
        elif self.path.startswith("/api/instances"):
            self._handle_le_instances()
        elif self.path.startswith("/api/dt/models"):
            self._handle_dt_models()
        elif self.path.startswith("/api/dt/static-attrs"):
            self._handle_dt_static_attrs()
        elif self.path.startswith("/api/dt/dynamic-attrs"):
            self._handle_dt_dynamic_attrs()
        elif self.path.startswith("/api/dt/hierarchy"):
            self._handle_dt_hierarchy()
        elif self.path.startswith("/api/dt/transformations"):
            self._handle_dt_transformations()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_ping(self):
        """Ping an IP address to check basic network reachability."""
        import subprocess, platform
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        if not ip:
            self._send_json_error(400, "Missing 'ip' parameter")
            return
        # Validate IP: only allow dotted notation or simple hostnames
        if not re.match(r'^[a-zA-Z0-9._-]+$', ip):
            self._send_json_error(400, "Invalid IP address")
            return
        if ip.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            self._send_json_error(400, "Cannot ping localhost")
            return

        # Platform-aware ping: -n on Windows, -c on Unix
        count_flag = "-n" if platform.system().lower() == "windows" else "-c"
        try:
            result = subprocess.run(
                ["ping", count_flag, "1", "-w", "3000", ip],
                capture_output=True, text=True, timeout=10
            )
            reachable = result.returncode == 0
            # Extract just the summary lines
            lines = result.stdout.strip().splitlines()
            detail = "\\n".join(lines[-3:]) if len(lines) >= 3 else result.stdout.strip()
            if not reachable and result.stderr.strip():
                detail = result.stderr.strip()
        except subprocess.TimeoutExpired:
            reachable = False
            detail = "Ping timed out after 10 seconds"
        except Exception as e:
            reachable = False
            detail = str(e)

        self._send_json_resp({"reachable": reachable, "detail": detail})

    def _handle_le_deviceinfo(self):
        """Proxy endpoint: fetch device info (firmware version) from a Litmus Edge device."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        token = qs.get("token", [""])[0].strip()
        if not ip or not token:
            self._send_json_error(400, "Missing ip or token")
            return
        if ip.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            self._send_json_error(400, "Cannot connect to localhost")
            return

        url = f"https://{ip}/dm/deviceinfo"
        try:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
            auth_str = base64.b64encode(f"{token}:".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {auth_str}")
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except Exception:
            self._send_json_error(502, "Could not fetch device info")
            return

        try:
            info = json.loads(raw)
        except json.JSONDecodeError:
            self._send_json_error(502, "Invalid JSON from device info endpoint")
            return

        result = {"firmwareVersion": info.get("firmwareVersion", "")}
        self._send_json_resp(result)

    def _handle_le_instances(self):
        """Proxy endpoint: fetch connector instances from a Litmus Edge device."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        token = qs.get("token", [""])[0].strip()
        if not ip:
            self._send_json_error(400, "Missing 'ip' parameter")
            return
        if not token:
            self._send_json_error(400, "Missing API token")
            return

        # Basic IP/hostname validation — prevent SSRF to internal services
        if ip.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            self._send_json_error(400, "Cannot connect to localhost")
            return

        url = f"https://{ip}/cc/instances"
        try:
            # LE uses self-signed certs — skip verification for local network
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
            # LE 3.16.x Basic Auth: username=API token, password=empty
            auth_str = base64.b64encode(f"{token}:".encode("utf-8")).decode("ascii")
            req.add_header("Authorization", f"Basic {auth_str}")
            with urllib.request.urlopen(req, timeout=10, context=ctx) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge at {ip}: {e}")
            return

        # Parse and extract just table names + provider for the frontend list
        try:
            instances = json.loads(raw)
            if not isinstance(instances, list):
                instances = [instances]
        except json.JSONDecodeError:
            self._send_json_error(502, "Invalid JSON response from Litmus Edge")
            return

        result = []
        for inst in instances:
            table_name = ""
            cfg_str = inst.get("config", "")
            if isinstance(cfg_str, str):
                try:
                    cfg = json.loads(cfg_str)
                    table_name = cfg.get("table", "") or cfg.get("name", "")
                except (json.JSONDecodeError, TypeError):
                    pass
            result.append({
                "_tableName": table_name,
                "providerId": inst.get("providerId", ""),
                "_original": inst,
            })

        out = json.dumps(result, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    def _handle_dt_models(self):
        """Proxy endpoint: fetch Digital Twin models from a Litmus Edge device via GraphQL."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        token = qs.get("token", [""])[0].strip()
        if not ip:
            self._send_json_error(400, "Missing 'ip' parameter")
            return
        if not token:
            self._send_json_error(400, "Missing API token")
            return
        if ip.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            self._send_json_error(400, "Cannot connect to localhost")
            return
        try:
            payload = self._dt_graphql(ip, token,
                "query ListModels { ListModels { ID Name } }", {})
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge at {ip}: {e}")
            return
        models = []
        if isinstance(payload, dict):
            data = payload.get("data", {})
            if isinstance(data, dict):
                models = data.get("ListModels", [])
        if not isinstance(models, list):
            models = []
        self._send_json_resp(models)

    def _dt_graphql(self, ip, token, query, variables):
        """Helper: execute a GraphQL call to the Digital Twins endpoint."""
        url = f"https://{ip}/digital-twins"
        gql_body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(url, data=gql_body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        auth_str = base64.b64encode(f"{token}:".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", f"Basic {auth_str}")
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    def _dt_parse_qs(self):
        """Parse and validate ip, token, model_id from query string."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        token = qs.get("token", [""])[0].strip()
        model_id = qs.get("model_id", [""])[0].strip()
        if not ip:
            self._send_json_error(400, "Missing 'ip' parameter")
            return None
        if not token:
            self._send_json_error(400, "Missing API token")
            return None
        if ip.lower() in ("localhost", "127.0.0.1", "::1", "0.0.0.0"):
            self._send_json_error(400, "Cannot connect to localhost")
            return None
        if not model_id:
            self._send_json_error(400, "Missing 'model_id' parameter")
            return None
        return ip, token, model_id

    def _handle_dt_static_attrs(self):
        """Fetch static attributes for a Digital Twin model."""
        params = self._dt_parse_qs()
        if not params:
            return
        ip, token, model_id = params
        query = ("query ListStaticAttributes($input:ListStaticAttributeRequest!) {"
                 "  ListStaticAttributes(input: $input) {"
                 "    ID ModelID InstanceID Key Value CreatedAt UpdatedAt"
                 "  }}") 
        variables = {"input": {"ModelID": model_id, "InstanceID": None}}
        try:
            payload = self._dt_graphql(ip, token, query, variables)
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge: {e}")
            return
        attrs = payload.get("data", {}).get("ListStaticAttributes", [])
        self._send_json_resp(attrs if isinstance(attrs, list) else [])

    def _handle_dt_dynamic_attrs(self):
        """Fetch dynamic attributes for a Digital Twin model."""
        params = self._dt_parse_qs()
        if not params:
            return
        ip, token, model_id = params
        query = ("query ListDynamicAttributes($input: ListDynamicAttributeRequest!) {"
                 "  ListDynamicAttributes(input:$input) {"
                 "    ID ModelID InstanceID Topic TopicExpression Name Unit DataType SkipParentKey SchemaID CreatedAt UpdatedAt"
                 "  }}")
        variables = {"input": {"ModelID": model_id}}
        try:
            payload = self._dt_graphql(ip, token, query, variables)
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge: {e}")
            return
        attrs = payload.get("data", {}).get("ListDynamicAttributes", [])
        self._send_json_resp(attrs if isinstance(attrs, list) else [])

    def _handle_dt_hierarchy(self):
        """Fetch schema hierarchy for a Digital Twin model."""
        params = self._dt_parse_qs()
        if not params:
            return
        ip, token, model_id = params
        query = ("query GetHierarchy($input : GetHierarchyRequest!) {"
                 "  GetHierarchy(input: $input)}") 
        variables = {"input": {"ModelID": model_id}}
        try:
            payload = self._dt_graphql(ip, token, query, variables)
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge: {e}")
            return
        hierarchy = payload.get("data", {}).get("GetHierarchy", {})
        self._send_json_resp(hierarchy if isinstance(hierarchy, dict) else {})

    def _handle_dt_transformations(self):
        """Fetch transformations for a Digital Twin model."""
        params = self._dt_parse_qs()
        if not params:
            return
        ip, token, model_id = params
        query = ("query ListTransformations($input: ListTransformationRequest!) {"
                 "  ListTransformations(input: $input) {"
                 "    ID Name Schema"
                 "  }}")
        variables = {"input": {"ModelID": model_id}}
        try:
            payload = self._dt_graphql(ip, token, query, variables)
        except urllib.error.HTTPError as e:
            self._send_json_error(502, f"Litmus Edge returned HTTP {e.code}")
            return
        except Exception as e:
            self._send_json_error(502, f"Cannot reach Litmus Edge: {e}")
            return
        transforms = payload.get("data", {}).get("ListTransformations", [])
        self._send_json_resp(transforms if isinstance(transforms, list) else [])

    def _handle_dt_push_clones(self):
        """Clone template model, add dynamic attributes, update hierarchy for each clone tab."""
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(self.path).query)
        ip = qs.get("ip", [""])[0].strip()
        token = qs.get("token", [""])[0].strip()
        if not ip or not token:
            self._send_json_error(400, "Missing ip or token")
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        try:
            data = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self._send_json_error(400, f"Invalid JSON: {e}")
            return

        template_id = data.get("template_model_id", "")
        clones = data.get("clones", [])
        if not template_id or not clones:
            self._send_json_error(400, "Missing template_model_id or clones")
            return

        # Fetch transformations to resolve schema names → IDs
        try:
            tf_payload = self._dt_graphql(ip, token,
                "query ListTransformations($input: ListTransformationRequest!) {"
                "  ListTransformations(input: $input) { ID Name } }",
                {"input": {"ModelID": template_id}})
            transforms = tf_payload.get("data", {}).get("ListTransformations", [])
            schema_name_to_id = {t["Name"].lower(): t["ID"] for t in transforms if t.get("Name")}
        except Exception:
            schema_name_to_id = {}

        results = []
        for clone in clones:
            clone_name = clone.get("name", "").strip()
            attributes = clone.get("attributes", [])
            if not clone_name:
                results.append({"name": clone_name or "(unnamed)", "ok": False, "message": "Clone name is required"})
                continue

            # Step 1: Clone the template model
            try:
                clone_resp = self._dt_graphql(ip, token,
                    "mutation CloneModel($input: CloneModelRequest!) {"
                    "  CloneModel(input: $input) { ID Assets Name Description } }",
                    {"input": {"ID": template_id, "Name": clone_name}})
                clone_data = clone_resp.get("data", {}).get("CloneModel")
                if not clone_data or not clone_data.get("ID"):
                    errors = clone_resp.get("errors", [{}])
                    msg = errors[0].get("message", "Unknown error") if errors else "No ID returned"
                    results.append({"name": clone_name, "ok": False, "message": f"Clone failed: {msg}"})
                    continue
                new_model_id = clone_data["ID"]
            except Exception as e:
                results.append({"name": clone_name, "ok": False, "message": f"Clone failed: {e}"})
                continue

            # Step 2: Fetch cloned model's transformations to get schema IDs
            try:
                tf2 = self._dt_graphql(ip, token,
                    "query ListTransformations($input: ListTransformationRequest!) {"
                    "  ListTransformations(input: $input) { ID Name } }",
                    {"input": {"ModelID": new_model_id}})
                new_transforms = tf2.get("data", {}).get("ListTransformations", [])
                new_schema_map = {t["Name"].lower(): t["ID"] for t in new_transforms if t.get("Name")}
            except Exception:
                new_schema_map = {}

            # Step 3: Create dynamic attributes
            created_attrs = []  # list of {name, id, hierarchy_path}
            attr_errors = []
            for attr in attributes:
                attr_name = attr.get("Name", "").strip()
                if not attr_name:
                    continue
                # Resolve SchemaID: try by name first, then by UUID
                schema_val = attr.get("SchemaID", "").strip()
                schema_id = ""
                if schema_val:
                    schema_id = new_schema_map.get(schema_val.lower(), "")
                    if not schema_id:
                        # Try as direct UUID
                        schema_id = schema_val if len(schema_val) > 30 else ""

                # DataType must be lowercase for the AttrDataType enum
                data_type_raw = attr.get("DataType", "json").strip()
                data_type_lower = data_type_raw.lower() if data_type_raw else "json"

                create_vars = {
                    "input": {
                        "ModelID": new_model_id,
                        "Name": attr_name,
                        "Unit": attr.get("Unit", ""),
                        "DataType": data_type_lower,
                        "SchemaID": schema_id,
                        "Topic": "",
                        "TopicExpression": attr.get("TopicExpression", "")
                    }
                }
                try:
                    ca_resp = self._dt_graphql(ip, token,
                        "mutation CreateDynamicAttribute($input: CreateDynamicAttributeRequest!) {"
                        "  CreateDynamicAttribute(input: $input) { ID ModelID Name } }",
                        create_vars)
                    ca_data = ca_resp.get("data", {}).get("CreateDynamicAttribute")
                    if ca_data and ca_data.get("ID"):
                        created_attrs.append({
                            "id": ca_data["ID"],
                            "name": attr_name,
                            "hierarchy": attr.get("Hierarchy", "").strip()
                        })
                    else:
                        errors = ca_resp.get("errors", [{}])
                        msg = errors[0].get("message", "Unknown") if errors else "No ID"
                        attr_errors.append(f"{attr_name}: {msg}")
                except Exception as e:
                    attr_errors.append(f"{attr_name}: {e}")

            # Step 4: Fetch existing hierarchy of cloned model
            try:
                h_resp = self._dt_graphql(ip, token,
                    "query GetHierarchy($input: GetHierarchyRequest!) {"
                    "  GetHierarchy(input: $input) }",
                    {"input": {"ModelID": new_model_id}})
                hierarchy = h_resp.get("data", {}).get("GetHierarchy", {})
            except Exception:
                hierarchy = {}

            # Step 5: Convert GetHierarchy format to SaveAllHierarchy format
            def convert_hier_node(src):
                """Convert {Name, Node:{...}, Attr, Childs} → {Node:{Position,Name,IsFolder,AttributeID,AttributeType,NodeType}, Childs:[...]}"""
                node_data = src.get("Node") or {}
                is_folder = node_data.get("IsFolder", False)
                attr_id = node_data.get("AttributeID")
                # Null-UUID means no attribute
                if attr_id and attr_id == "00000000-0000-0000-0000-000000000000":
                    attr_id = None
                result = {
                    "Node": {
                        "Position": node_data.get("Position", 0),
                        "Name": node_data.get("Name") or src.get("Name", ""),
                        "IsFolder": is_folder,
                        "AttributeID": attr_id,
                        "AttributeType": node_data.get("AttributeType"),
                        "NodeType": node_data.get("NodeType", "array" if is_folder else "attribute")
                    },
                    "Childs": []
                }
                for child in src.get("Childs", []):
                    result["Childs"].append(convert_hier_node(child))
                return result

            hier_input = []
            if isinstance(hierarchy, dict) and hierarchy.get("Childs"):
                for child in hierarchy["Childs"]:
                    hier_input.append(convert_hier_node(child))

            # Insert new dynamic attrs into hierarchy
            def find_or_create_folder(node_list, path_parts):
                """Navigate/create folder path and return the target Childs list."""
                current = node_list
                for part in path_parts:
                    if not part or part.lower() == "root":
                        continue
                    found = None
                    for child in current:
                        n = child.get("Node", {})
                        if n.get("Name", "").lower() == part.lower() and n.get("IsFolder", False):
                            found = child
                            break
                    if found is None:
                        new_folder = {
                            "Node": {
                                "Position": len(current),
                                "Name": part,
                                "IsFolder": True,
                                "AttributeID": None,
                                "AttributeType": None,
                                "NodeType": "array"
                            },
                            "Childs": []
                        }
                        current.append(new_folder)
                        found = new_folder
                    current = found["Childs"]
                return current

            for ca in created_attrs:
                path = ca["hierarchy"]
                path_parts = [p.strip() for p in path.replace("\\", "/").split("/") if p.strip()] if path else []
                target = find_or_create_folder(hier_input, path_parts)
                target.append({
                    "Node": {
                        "Position": len(target),
                        "Name": ca["name"],
                        "IsFolder": False,
                        "AttributeID": ca["id"],
                        "AttributeType": "dynamic",
                        "NodeType": "attribute"
                    },
                    "Childs": []
                })

            # Step 6: Save hierarchy
            hier_msg = ""
            if hier_input:
                try:
                    save_resp = self._dt_graphql(ip, token,
                        "mutation SaveAllHierarchy($modelId: UUID!, $input: [SaveAllHierarchyRequest]!) {"
                        "  SaveAllHierarchy(modelId: $modelId, input: $input) }",
                        {"modelId": new_model_id, "input": hier_input})
                    if save_resp.get("errors"):
                        hier_msg = "; Hierarchy warning: " + save_resp["errors"][0].get("message", "")
                except Exception as e:
                    hier_msg = f"; Hierarchy save failed: {e}"

            # Build result message
            msg = f"Created with {len(created_attrs)} new attribute(s)"
            if attr_errors:
                msg += f", {len(attr_errors)} failed"
            msg += hier_msg
            results.append({"name": clone_name, "ok": len(attr_errors) == 0, "message": msg})

        self._send_json_resp({"results": results})

    def _handle_le_push(self):
        """Process mapping tabs and POST each instance to Litmus Edge."""
        content_type = self.headers.get("Content-Type", "")
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            fields = parse_multipart(body, content_type)

            # LE credentials
            _, ip_b = fields.get("le_ip", (None, b""))
            _, token_b = fields.get("le_token", (None, b""))
            le_ip = ip_b.decode("utf-8", errors="replace").strip()
            le_token = token_b.decode("utf-8", errors="replace").strip()
            if not le_ip or not le_token:
                self._send_json_resp({"error": "Missing LE IP or API token"})
                return

            # Source instance
            _, le_json_b = fields.get("le_instance_json", (None, b""))
            le_json_str = le_json_b.decode("utf-8", errors="replace").strip()
            if not le_json_str:
                self._send_json_resp({"error": "No template instance selected"})
                return
            try:
                template_instance = json.loads(le_json_str)
            except json.JSONDecodeError as e:
                self._send_json_resp({"error": f"Invalid instance data: {e}"})
                return

            # Detected table name
            _, det_tbl_b = fields.get("detected_table", (None, b""))
            detected_table = det_tbl_b.decode("utf-8", errors="replace").strip()

            # Number of tabs
            _, tc_b = fields.get("tab_count", (None, b"1"))
            try:
                tab_count = max(1, int(tc_b.decode("utf-8", errors="replace").strip()))
            except ValueError:
                tab_count = 1

            # Build instances from tabs
            new_instances = []
            for i in range(tab_count):
                _, mapping_b = fields.get(f"mapping_text_{i}", (None, b""))
                mapping_text = mapping_b.decode("utf-8", errors="replace")
                new_mapping = parse_mapping_text(mapping_text)
                if not new_mapping:
                    continue

                _, replace_flag_b = fields.get(f"replace_table_{i}", (None, b""))
                _, new_tbl_b = fields.get(f"new_table_{i}", (None, b""))
                replace_flag = replace_flag_b.decode("utf-8", errors="replace").strip()
                new_tbl = new_tbl_b.decode("utf-8", errors="replace").strip()

                if replace_flag == "1" and new_tbl:
                    # User checked replace: set both table and name to new value
                    inst_copy = copy.deepcopy(template_instance)
                    replace_mapping_in_json(inst_copy, new_mapping, new_table=new_tbl, new_name=new_tbl)
                    inst_name = new_tbl
                else:
                    # No replace: auto-suffix the name to avoid conflicts
                    suffix = random.randint(1, 100)
                    # Read the existing name from config
                    existing_name = ""
                    cfg_str = template_instance.get("config", "")
                    if isinstance(cfg_str, str):
                        try:
                            cfg_obj = json.loads(cfg_str)
                            existing_name = cfg_obj.get("name", "")
                        except (json.JSONDecodeError, TypeError):
                            pass
                    auto_name = f"{existing_name}_{suffix}" if existing_name else f"instance_{i+1}_{suffix}"
                    inst_copy = copy.deepcopy(template_instance)
                    replace_mapping_in_json(inst_copy, new_mapping, new_name=auto_name)
                    inst_name = auto_name

                new_instances.append((inst_name, inst_copy))

            if not new_instances:
                self._send_json_resp({"error": "No tabs contained valid mapping data"})
                return

            # POST each instance to LE
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            auth_str = base64.b64encode(f"{le_token}:".encode("utf-8")).decode("ascii")
            push_url = f"https://{le_ip}/cc/instances"

            results = []
            for name, inst in new_instances:
                payload = json.dumps(inst, ensure_ascii=False).encode("utf-8")
                try:
                    req = urllib.request.Request(push_url, data=payload, method="POST")
                    req.add_header("Content-Type", "application/json")
                    req.add_header("Accept", "application/json")
                    req.add_header("Authorization", f"Basic {auth_str}")
                    with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
                        resp_body = resp.read().decode("utf-8", errors="replace")
                        results.append({"name": name, "ok": True, "message": f"Created (HTTP {resp.status})"})
                except urllib.error.HTTPError as e:
                    err_body = e.read().decode("utf-8", errors="replace")[:200]
                    results.append({"name": name, "ok": False, "message": f"HTTP {e.code}: {err_body}"})
                except Exception as e:
                    results.append({"name": name, "ok": False, "message": str(e)})

            self._send_json_resp({"results": results})

        except Exception as e:
            self._send_json_resp({"error": str(e)})

    def _send_json_resp(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json_error(self, status, msg):
        body = json.dumps({"error": msg}).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if self.path == "/api/push":
            self._handle_le_push()
            return
        if self.path.startswith("/api/dt/push-clones"):
            self._handle_dt_push_clones()
            return
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

            # Determine source: file upload (manual) or LE instance JSON
            _, le_json_b = fields.get("le_instance_json", (None, b""))
            le_json_str = le_json_b.decode("utf-8", errors="replace").strip()

            if le_json_str:
                # LE mode: wrap selected instance in cc structure
                try:
                    le_instance = json.loads(le_json_str)
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid LE instance data: {e}")
                connector = {"cc": {"instances": [le_instance]}}
            elif "json_file" in fields:
                # Manual mode: read uploaded file
                _, json_data = fields["json_file"]
                try:
                    connector = json.loads(json_data.decode("utf-8-sig", errors="replace"))
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON file: {e}")
            else:
                raise ValueError("No JSON source. Upload a file or connect to Litmus Edge.")

            # Detected table name (shared across tabs)
            _, det_tbl_b = fields.get("detected_table", (None, b""))
            detected_table = det_tbl_b.decode("utf-8", errors="replace").strip()

            # Determine number of tabs
            _, tc_b = fields.get("tab_count", (None, b"1"))
            try:
                tab_count = max(1, int(tc_b.decode("utf-8", errors="replace").strip()))
            except ValueError:
                tab_count = 1

            # Find the instances array and use first instance as template
            instances = None
            if isinstance(connector.get("cc"), dict):
                instances = connector["cc"].get("instances")
            if not isinstance(instances, list) or not instances:
                raise ValueError("JSON structure error: could not find cc.instances array.")
            template_instance = instances[0]

            # Process each tab → one instance per tab
            new_instances = []
            for i in range(tab_count):
                # Read pasted mapping text for this tab
                _, mapping_b = fields.get(f"mapping_text_{i}", (None, b""))
                mapping_text = mapping_b.decode("utf-8", errors="replace")

                new_mapping = parse_mapping_text(mapping_text)
                if not new_mapping:
                    continue  # skip tabs with no mapping

                # Optional: replace table name for this tab
                _, replace_flag_b = fields.get(f"replace_table_{i}", (None, b""))
                _, new_tbl_b = fields.get(f"new_table_{i}", (None, b""))
                replace_flag = replace_flag_b.decode("utf-8", errors="replace").strip()
                new_tbl = new_tbl_b.decode("utf-8", errors="replace").strip()

                # Clone the template instance and apply mapping + name/table
                inst_copy = copy.deepcopy(template_instance)
                if replace_flag == "1" and new_tbl:
                    # User checked replace: set both table and name to new value
                    replace_mapping_in_json(inst_copy, new_mapping, new_table=new_tbl, new_name=new_tbl)
                else:
                    # No replace: auto-suffix the name to avoid conflicts
                    suffix = random.randint(1, 100)
                    existing_name = ""
                    cfg_str = template_instance.get("config", "")
                    if isinstance(cfg_str, str):
                        try:
                            cfg_obj = json.loads(cfg_str)
                            existing_name = cfg_obj.get("name", "")
                        except (json.JSONDecodeError, TypeError):
                            pass
                    auto_name = f"{existing_name}_{suffix}" if existing_name else f"instance_{i+1}_{suffix}"
                    replace_mapping_in_json(inst_copy, new_mapping, new_name=auto_name)

                new_instances.append(inst_copy)

            if not new_instances:
                raise ValueError("No tabs contained valid key-value mapping data.")

            # Build final output: replace instances array with all processed tabs
            output = copy.deepcopy(connector)
            output["cc"]["instances"] = new_instances
            output_str = json.dumps(output, indent=2, ensure_ascii=False)
            output_bytes = output_str.encode("utf-8")

            # Determine filename
            filename = "updated_connector.json"
            if len(new_instances) == 1:
                # For single tab, try to name after the table
                _, rf_b = fields.get("replace_table_0", (None, b""))
                _, nt_b = fields.get("new_table_0", (None, b""))
                rf = rf_b.decode("utf-8", errors="replace").strip()
                nt = nt_b.decode("utf-8", errors="replace").strip()
                if rf == "1" and nt:
                    filename = nt + ".json"
                elif detected_table:
                    filename = detected_table + ".json"

            # Send single file download
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
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
║       Litmus Deployment Assist — Ready       ║
╠══════════════════════════════════════════════╣
║  Open:  http://localhost:{port}                 ║
║  Stop:  Ctrl+C                               ║
╠══════════════════════════════════════════════╣
║  Apps:                                       ║
║    • Integration Connector Assist            ║
║    • Digital Twin Model Assist (coming soon) ║
╚══════════════════════════════════════════════╝
""")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()