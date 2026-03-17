"""Pipeline run manifest and HTML DAG report helpers."""

from __future__ import annotations

import csv
import json as pyjson
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4
from research_loom.output.html_theme import build_html_document, get_shared_html_css


REQUIRED_REPORT_KEYS = {
    "enabled",
    "format",
    "output_dir",
    "keep_last_n",
    "include_edge_payloads",
    "write_latest_pointer",
    "layout",
}
ALLOWED_FORMATS = {"json", "html", "both"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_run_id() -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}_{uuid4().hex[:8]}"


def validate_report_config(report_cfg: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(report_cfg, dict):
        raise ValueError("logging.pipeline_report must be an object")
    missing = sorted(REQUIRED_REPORT_KEYS - set(report_cfg.keys()))
    if missing:
        raise ValueError(f"logging.pipeline_report missing required keys: {missing}")
    unknown = sorted(set(report_cfg.keys()) - REQUIRED_REPORT_KEYS)
    if unknown:
        raise ValueError(f"logging.pipeline_report contains unknown keys: {unknown}")

    enabled = report_cfg["enabled"]
    fmt = report_cfg["format"]
    output_dir = report_cfg["output_dir"]
    keep_last_n = report_cfg["keep_last_n"]
    include_edge_payloads = report_cfg["include_edge_payloads"]
    write_latest_pointer = report_cfg["write_latest_pointer"]
    layout = report_cfg["layout"]

    if not isinstance(enabled, bool):
        raise ValueError("logging.pipeline_report.enabled must be boolean")
    if fmt not in ALLOWED_FORMATS:
        raise ValueError(f"logging.pipeline_report.format must be one of {sorted(ALLOWED_FORMATS)}")
    if not isinstance(output_dir, str) or not output_dir.strip():
        raise ValueError("logging.pipeline_report.output_dir must be a non-empty string")
    if not isinstance(keep_last_n, int) or keep_last_n < 1:
        raise ValueError("logging.pipeline_report.keep_last_n must be an integer >= 1")
    if not isinstance(include_edge_payloads, bool):
        raise ValueError("logging.pipeline_report.include_edge_payloads must be boolean")
    if not isinstance(write_latest_pointer, bool):
        raise ValueError("logging.pipeline_report.write_latest_pointer must be boolean")
    if not isinstance(layout, dict):
        raise ValueError("logging.pipeline_report.layout must be an object")

    layout_keys = {"node_spacing", "layer_spacing", "edge_node_spacing"}
    missing_layout = sorted(layout_keys - set(layout.keys()))
    if missing_layout:
        raise ValueError(f"logging.pipeline_report.layout missing required keys: {missing_layout}")
    unknown_layout = sorted(set(layout.keys()) - layout_keys)
    if unknown_layout:
        raise ValueError(f"logging.pipeline_report.layout contains unknown keys: {unknown_layout}")

    node_spacing = layout["node_spacing"]
    layer_spacing = layout["layer_spacing"]
    edge_node_spacing = layout["edge_node_spacing"]
    if not isinstance(node_spacing, (int, float)) or node_spacing < 20:
        raise ValueError("logging.pipeline_report.layout.node_spacing must be a number >= 20")
    if not isinstance(layer_spacing, (int, float)) or layer_spacing < 40:
        raise ValueError("logging.pipeline_report.layout.layer_spacing must be a number >= 40")
    if not isinstance(edge_node_spacing, (int, float)) or edge_node_spacing < 0:
        raise ValueError("logging.pipeline_report.layout.edge_node_spacing must be a number >= 0")

    return {
        "enabled": enabled,
        "format": fmt,
        "output_dir": output_dir.strip(),
        "keep_last_n": keep_last_n,
        "include_edge_payloads": include_edge_payloads,
        "write_latest_pointer": write_latest_pointer,
        "layout": {
            "node_spacing": float(node_spacing),
            "layer_spacing": float(layer_spacing),
            "edge_node_spacing": float(edge_node_spacing),
        },
    }


def _fmt_bytes(n: Optional[int]) -> str:
    if n is None:
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(n)
    idx = 0
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024
        idx += 1
    return f"{value:.1f} {units[idx]}"


def _escape(s: Any) -> str:
    text = "" if s is None else str(s)
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def _tooltip_lines(node: Dict[str, Any]) -> List[str]:
    outputs = node.get("output_artifacts", [])
    inputs = node.get("input_artifacts", [])
    lines = [
        f"Node: {node.get('name')}",
        f"Status: {node.get('status')}",
        f"Refreshed: {node.get('refreshed')}",
        f"Reason: {node.get('reason')}",
        f"Elapsed: {node.get('elapsed_s', 0.0):.2f}s",
        f"Output bytes: {_fmt_bytes(node.get('output_total_bytes'))}",
        f"Input bytes: {_fmt_bytes(node.get('input_total_bytes'))}",
        f"Cache file: {node.get('cache_file_path', 'n/a')}",
        f"Cache bytes: {_fmt_bytes(node.get('cache_file_bytes'))}",
        f"Outputs: {len(outputs)} artifact(s)",
        f"Inputs: {len(inputs)} artifact(s)",
    ]
    for art in outputs[:3]:
        lines.append(f"OUT {art.get('key')}: {_fmt_bytes(art.get('bytes'))} @ {art.get('path')}")
    if len(outputs) > 3:
        lines.append(f"... +{len(outputs)-3} more outputs")
    for art in inputs[:3]:
        lines.append(f"IN {art.get('key')}: {_fmt_bytes(art.get('bytes'))} @ {art.get('path')}")
    if len(inputs) > 3:
        lines.append(f"... +{len(inputs)-3} more inputs")
    return lines


def _edge_tooltip_lines(edge: Dict[str, Any]) -> List[str]:
    return [
        f"Edge: {edge['source']} -> {edge['target']}",
        f"Payload bytes: {_fmt_bytes(edge.get('payload_bytes'))}",
    ]


def _slugify(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", text).strip("-").lower() or "node"


def _preview_eligible(fmt: Optional[str]) -> bool:
    return (fmt or "").lower() in {"parquet", "json.zst", "jsonl.zst", "csv", "json"}


def _rows_from_object(payload: Any, max_rows: int = 40) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    columns: List[str] = []

    def append_row(item: Any) -> None:
        nonlocal columns
        if isinstance(item, dict):
            row = {str(k): item.get(k) for k in item.keys()}
        else:
            row = {"value": item}
        for c in row.keys():
            if c not in columns:
                columns.append(c)
        rows.append(row)

    if isinstance(payload, list):
        for item in payload[:max_rows]:
            append_row(item)
        total = len(payload)
    elif isinstance(payload, dict):
        list_key = None
        for k, v in payload.items():
            if isinstance(v, list):
                list_key = k
                break
        if list_key is not None:
            src = payload.get(list_key) or []
            for item in src[:max_rows]:
                append_row(item)
            total = len(src)
        else:
            append_row(payload)
            total = 1
    else:
        append_row(payload)
        total = 1

    return {"columns": columns, "rows": rows, "row_count": total}


def _extract_preview_rows(artifact_path: Path, fmt: str, max_rows: int = 40) -> Optional[Dict[str, Any]]:
    lower = fmt.lower()
    try:
        if lower == "csv":
            with artifact_path.open("r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                rows = []
                for i, row in enumerate(reader):
                    if i >= max_rows:
                        break
                    rows.append(dict(row))
                return {"columns": list(reader.fieldnames or []), "rows": rows, "row_count": len(rows)}
        if lower == "json":
            payload = pyjson.loads(artifact_path.read_text(encoding="utf-8"))
            return _rows_from_object(payload, max_rows=max_rows)
        if lower in {"json.zst", "jsonl.zst"}:
            import zstandard as zstd

            with artifact_path.open("rb") as fh:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(fh) as reader:
                    raw = reader.read().decode("utf-8")
            if lower == "jsonl.zst":
                out = []
                for line in raw.splitlines():
                    if not line.strip():
                        continue
                    try:
                        out.append(pyjson.loads(line))
                    except Exception:
                        continue
                    if len(out) >= max_rows:
                        break
                return _rows_from_object(out, max_rows=max_rows)
            payload = pyjson.loads(raw)
            return _rows_from_object(payload, max_rows=max_rows)
        if lower == "parquet":
            import pandas as pd

            df = pd.read_parquet(artifact_path)
            head = df.head(max_rows)
            rows = head.to_dict(orient="records")
            return {"columns": [str(c) for c in head.columns], "rows": rows, "row_count": int(len(df))}
    except Exception:
        return None
    return None


def _write_preview_html(
    preview_path: Path,
    node_name: str,
    artifact: Dict[str, Any],
    preview_data: Dict[str, Any],
) -> None:
    columns = [str(c) for c in (preview_data.get("columns") or [])]
    rows = preview_data.get("rows") or []
    row_count = int(preview_data.get("row_count") or 0)
    table_head = "".join(f"<th>{_escape(c)}</th>" for c in columns) if columns else "<th>value</th>"
    table_rows: List[str] = []
    for row in rows:
        table_rows.append("<tr>" + "".join(f"<td>{_escape(row.get(c, ''))}</td>" for c in columns) + "</tr>")
    if not table_rows:
        table_rows.append("<tr><td><i>No preview rows available</i></td></tr>")

    body_html = f"""
  <h2>Preview: {_escape(node_name)} :: {_escape(str(artifact.get("key")))}</h2>
  <div class="meta rl-card">
    <b>Format:</b> {_escape(artifact.get("format"))} |
    <b>Rows shown:</b> {len(rows)} |
    <b>Total rows:</b> {row_count} |
    <b>Source:</b> {_escape(artifact.get("abs_path"))}
  </div>
  <table class="simpletable">
    <thead><tr>{table_head}</tr></thead>
    <tbody>{''.join(table_rows)}</tbody>
  </table>
"""
    html = build_html_document(
        title=f"Artifact Preview - {node_name}::{artifact.get('key')}",
        body_html=body_html,
        extra_css=".meta { margin-bottom: 12px; font-size: 13px; }",
    )
    preview_path.write_text(html, encoding="utf-8")


def build_node_inspector_html(envelope: Dict[str, Any], node_name: str, title: Optional[str] = None) -> str:
    outputs = envelope.get("outputs") if isinstance(envelope, dict) else {}
    output_rows: List[str] = []
    for key, payload in (outputs or {}).items():
        if not isinstance(payload, dict):
            continue
        val = payload.get("value")
        rendered = ""
        if isinstance(val, dict) and val.get("__artifact__"):
            rendered = f"artifact: {_escape(val.get('path'))} ({_escape(val.get('format'))})"
        elif isinstance(val, (dict, list)):
            rendered = _escape(pyjson.dumps(val, ensure_ascii=False)[:1000])
        else:
            rendered = _escape(str(val))
        output_rows.append(
            "<tr>"
            f"<td><b>{_escape(key)}</b></td>"
            f"<td>{_escape(payload.get('type'))}</td>"
            f"<td>{_escape(payload.get('storage', ''))}</td>"
            f"<td>{_escape(payload.get('preview', ''))}</td>"
            f"<td><code>{rendered}</code></td>"
            "</tr>"
        )
    if not output_rows:
        output_rows.append("<tr><td colspan='5'><i>No outputs</i></td></tr>")

    summary_json = _escape(pyjson.dumps(envelope.get("summary", {}), ensure_ascii=False, indent=2))
    metadata_json = _escape(pyjson.dumps(envelope.get("metadata", {}), ensure_ascii=False, indent=2))
    page_title = title or f"Node Inspector - {node_name}"
    body_html = f"""
  <h2>{_escape(page_title)}</h2>
  <div class="meta rl-card"><b>Node:</b> {_escape(node_name)} | <b>Status:</b> {_escape(envelope.get("status"))}</div>
  <h3>Summary</h3>
  <pre>{summary_json}</pre>
  <h3>Outputs</h3>
  <table class="simpletable">
    <thead><tr><th>Key</th><th>Type</th><th>Storage</th><th>Preview</th><th>Value</th></tr></thead>
    <tbody>{''.join(output_rows)}</tbody>
  </table>
  <h3>Metadata</h3>
  <pre>{metadata_json}</pre>
"""
    return build_html_document(
        title=page_title,
        body_html=body_html,
        extra_css=".meta { margin-bottom: 12px; font-size: 13px; } table { margin-top: 10px; }",
    )


def _select_primary_artifact(artifact_links: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not artifact_links:
        return None
    format_priority = {"html": 0, "png": 1, "jpg": 1, "jpeg": 1, "webp": 1, "gif": 1}
    best = None
    best_score = (99, 99)
    for idx, link in enumerate(artifact_links):
        fmt = str(link.get("format") or "").lower()
        has_preview = bool(link.get("preview_href"))
        open_href = bool(link.get("open_href"))
        if fmt in format_priority:
            score = (format_priority[fmt], idx)
        elif has_preview:
            score = (2, idx)
        elif open_href:
            score = (3, idx)
        else:
            score = (8, idx)
        if best is None or score < best_score:
            best = link
            best_score = score
    return best


def _build_enriched_manifest(manifest: Dict[str, Any], output_dir: Path, include_previews: bool) -> Dict[str, Any]:
    data = pyjson.loads(pyjson.dumps(manifest))
    run_id = str(data.get("run_id") or "run")
    previews_dir = output_dir / "previews"
    if include_previews:
        previews_dir.mkdir(parents=True, exist_ok=True)

    for node in data.get("nodes", []):
        artifact_links: List[Dict[str, Any]] = []
        inspector_href = None
        cache_file_path = str(node.get("cache_file_path") or "").strip()
        if include_previews and cache_file_path:
            cache_path = Path(cache_file_path)
            if cache_path.exists():
                try:
                    cache_blob = pyjson.loads(cache_path.read_text(encoding="utf-8"))
                    envelope = cache_blob.get("_result") if isinstance(cache_blob, dict) else None
                    if isinstance(envelope, dict) and "outputs" in envelope and "status" in envelope:
                        inspector_name = f"{run_id}_{_slugify(str(node.get('name')))}_inspector.html"
                        inspector_path = previews_dir / inspector_name
                        inspector_html = build_node_inspector_html(envelope, str(node.get("name")))
                        inspector_path.write_text(inspector_html, encoding="utf-8")
                        inspector_href = inspector_path.resolve().as_uri()
                except Exception:
                    inspector_href = None
        for idx, artifact in enumerate(node.get("output_artifacts", []) or []):
            abs_path = str(artifact.get("abs_path") or "").strip()
            fmt = str(artifact.get("format") or "")
            link = {
                "key": artifact.get("key"),
                "format": fmt,
                "bytes": artifact.get("bytes"),
                "path": artifact.get("path"),
                "abs_path": abs_path,
                "open_href": None,
                "preview_href": None,
            }
            if abs_path:
                p = Path(abs_path)
                if p.exists():
                    link["open_href"] = p.resolve().as_uri()
                    if include_previews and _preview_eligible(fmt):
                        preview_data = _extract_preview_rows(p, fmt)
                        if preview_data is not None:
                            preview_name = f"{run_id}_{_slugify(str(node.get('name')))}_{idx}.html"
                            preview_path = previews_dir / preview_name
                            _write_preview_html(preview_path, str(node.get("name")), artifact, preview_data)
                            link["preview_href"] = preview_path.resolve().as_uri()
            artifact_links.append(link)

        node["artifact_links"] = artifact_links
        primary = _select_primary_artifact(artifact_links)
        if primary:
            node["primary_artifact_href"] = primary.get("open_href")
            node["primary_preview_href"] = primary.get("preview_href")
            node["primary_artifact_key"] = primary.get("key")
        else:
            node["primary_artifact_href"] = None
            node["primary_preview_href"] = None
            node["primary_artifact_key"] = None
        node["inspector_href"] = inspector_href
        if inspector_href and not node.get("primary_preview_href"):
            node["primary_preview_href"] = inspector_href
    return data


def render_html_report(manifest: Dict[str, Any], output_path: Path) -> None:
    nodes = manifest.get("nodes", [])
    edges = manifest.get("edges", [])
    nodes_json = json.dumps(nodes, ensure_ascii=False)
    edges_json = json.dumps(edges, ensure_ascii=False)
    layout_settings_json = json.dumps(
        manifest.get("layout", {"node_spacing": 55.0, "layer_spacing": 120.0, "edge_node_spacing": 30.0}),
        ensure_ascii=False,
    )

    table_rows = []
    for node in sorted(nodes, key=lambda item: float(item.get("elapsed_s", 0.0)), reverse=True):
        table_rows.append(
            "<tr>"
            f"<td>{_escape(node.get('name'))}</td>"
            f"<td>{_escape(node.get('status'))}</td>"
            f"<td>{_escape(str(node.get('refreshed')))}</td>"
            f"<td>{_escape(node.get('reason'))}</td>"
            f"<td data-sort='{float(node.get('elapsed_s', 0.0)):.6f}'>{float(node.get('elapsed_s', 0.0)):.2f}</td>"
            f"<td data-sort='{int(node.get('output_total_bytes') or 0)}'>{_escape(_fmt_bytes(node.get('output_total_bytes')))}</td>"
            f"<td data-sort='{int(node.get('input_total_bytes') or 0)}'>{_escape(_fmt_bytes(node.get('input_total_bytes')))}</td>"
            f"<td>{_escape(node.get('cache_file_path', ''))}</td>"
            "</tr>"
        )

    meta = manifest.get("summary", {})
    project_name = manifest.get("project_name", "unknown_project")
    materialize = manifest.get("materialize")
    materialize_text = materialize if isinstance(materialize, str) else ", ".join(materialize or [])
    outputs = manifest.get("outputs", [])
    shared_css = get_shared_html_css()
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Pipeline DAG Report { _escape(manifest.get("run_id")) }</title>
  <style>
    {shared_css}
    h1,h2 {{ margin: 0 0 8px 0; }}
    h1 {{
      font-size: 18px;
      font-weight: 500;
      color: #cbd5e1;
      letter-spacing: 0.1px;
    }}
    h2 {{
      font-size: 18px;
      font-weight: 500;
      color: #cbd5e1;
      letter-spacing: 0.1px;
    }}
    .meta {{ color: #cbd5e1; }}
    .card {{ background: #1f2937; border: 1px solid #374151; border-radius: 12px; padding: 8px; margin-bottom: 12px; }}
    .runbar {{
      margin: 4px 0 8px 0;
      font-size: 12px;
      line-height: 1.3;
      color: #cbd5e1;
      background: #0f172a;
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 6px 8px;
      white-space: normal;
      word-break: break-word;
    }}
    .runbar b {{ color: #93c5fd; }}
    #nodeTable th {{ cursor: pointer; }}
    .dag-tooltip {{
      position: fixed;
      z-index: 9600;
      display: none;
      top: 92px;
      left: 20px;
      width: min(560px, calc(100vw - 40px));
      max-height: 42vh;
      overflow-y: auto;
      overflow-x: hidden;
      background: rgba(15, 23, 42, 0.97);
      border: 1px solid #60a5fa;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.35;
      color: #e2e8f0;
      pointer-events: none;
      box-shadow: 0 10px 28px rgba(0, 0, 0, 0.42);
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      word-break: break-word;
    }}
    .dag-tooltip .row {{ margin: 0 0 2px 0; }}
    .dag-tooltip .k {{ color: #93c5fd; font-weight: 700; }}
    .drawer-backdrop {{
      position: fixed;
      inset: 0;
      background: rgba(2, 6, 23, 0.35);
      opacity: 0;
      visibility: hidden;
      pointer-events: none;
      transition: opacity 140ms ease, visibility 140ms ease;
      z-index: 8998;
    }}
    .drawer-backdrop.open {{
      opacity: 1;
      visibility: visible;
      pointer-events: auto;
    }}
    .artifact-panel {{
      position: fixed;
      right: 12px;
      top: 12px;
      width: min(460px, calc(100vw - 24px));
      height: calc(100vh - 24px);
      overflow: auto;
      background: rgba(15, 23, 42, 0.98);
      border: 1px solid #334155;
      border-radius: 14px;
      box-shadow: 0 8px 30px rgba(0, 0, 0, 0.45);
      padding: 10px 12px;
      z-index: 9001;
      transform: translateX(calc(100% + 20px));
      opacity: 0;
      visibility: hidden;
      pointer-events: none;
      will-change: transform, opacity;
      transition: transform 150ms cubic-bezier(0.2, 0.8, 0.2, 1), opacity 120ms ease, visibility 120ms ease;
    }}
    .artifact-panel.open {{
      transform: translateX(0);
      opacity: 1;
      visibility: visible;
      pointer-events: auto;
    }}
    .artifact-panel h3 {{ margin: 2px 0 8px 0; font-size: 15px; color: #93c5fd; }}
    .artifact-panel .meta {{ font-size: 12px; margin-bottom: 10px; color: #cbd5e1; }}
    .artifact-list {{ display: grid; gap: 8px; }}
    .artifact-item {{
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 8px;
      background: #111827;
      font-size: 12px;
    }}
    .artifact-item .top {{ display: flex; justify-content: space-between; gap: 8px; margin-bottom: 6px; }}
    .artifact-item .fmt {{ color: #93c5fd; font-weight: 700; }}
    .artifact-item .key {{ color: #e2e8f0; font-weight: 600; }}
    .artifact-actions a {{
      color: #93c5fd;
      text-decoration: none;
      margin-right: 10px;
      font-weight: 600;
    }}
    .artifact-actions a:hover {{ text-decoration: underline; }}
    .artifact-close {{
      float: right;
      border: 1px solid #475569;
      background: #1e293b;
      color: #cbd5e1;
      border-radius: 6px;
      padding: 3px 8px;
      cursor: pointer;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <h1>Pipeline DAG Report - {_escape(project_name)}</h1>
  <div class="runbar">
    <b>Run:</b> {_escape(manifest.get("run_id"))} |
    <b>Status:</b> {_escape(manifest.get("run_status"))} |
    <b>Elapsed:</b> {float(manifest.get("elapsed_s", 0.0)):.2f}s |
    <b>Materialize:</b> {_escape(materialize_text)} |
    <b>Nodes:</b> {meta.get("n_nodes")} (c={meta.get("n_computed")}, k={meta.get("n_cached")}, f={meta.get("n_failed")}) |
    <b>Outputs:</b> {len(outputs)} |
    <b>Started:</b> {_escape(manifest.get("started_at"))} |
    <b>Ended:</b> {_escape(manifest.get("ended_at"))}
  </div>
  <div class="card">
    <svg id="dagSvg" viewBox="0 0 1800 900" width="100%" height="780" role="img"></svg>
  </div>
  <div class="card">
    <h2>Node Metrics</h2>
    <table id="nodeTable" class="simpletable">
      <thead>
        <tr>
          <th onclick="sortTable(0)">Node</th>
          <th onclick="sortTable(1)">Status</th>
          <th onclick="sortTable(2)">Refreshed</th>
          <th onclick="sortTable(3)">Reason</th>
          <th onclick="sortTable(4,true)">Elapsed(s)</th>
          <th onclick="sortTable(5,true)">Output Size</th>
          <th onclick="sortTable(6,true)">Input Size</th>
          <th>Cache Path</th>
        </tr>
      </thead>
      <tbody>
        {''.join(table_rows)}
      </tbody>
    </table>
  </div>
  <div id="dagTooltip" class="dag-tooltip"></div>
  <div id="drawerBackdrop" class="drawer-backdrop"></div>
  <div id="artifactPanel" class="artifact-panel">
    <button id="artifactPanelClose" class="artifact-close">Close</button>
    <div id="artifactPanelBody"></div>
  </div>
  <script src="https://unpkg.com/elkjs/lib/elk.bundled.js"></script>
  <script>
    const manifestNodes = {nodes_json};
    const manifestEdges = {edges_json};
    const manifestLayout = {layout_settings_json};
    const NS = 'http://www.w3.org/2000/svg';

    function nodeColor(status) {{
      if (status === 'computed') return '#2f855a';
      if (status === 'cached') return '#2b6cb0';
      if (status === 'failed') return '#c53030';
      return '#4a5568';
    }}

    function fmtBytes(n) {{
      if (n === null || n === undefined) return 'n/a';
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let v = Number(n);
      let i = 0;
      while (v >= 1024 && i < units.length - 1) {{ v /= 1024; i += 1; }}
      return `${{v.toFixed(1)}} ${{units[i]}}`;
    }}

    function nodeTooltip(node) {{
      const lines = [
        `Node: ${{node.name}}`,
        `Status: ${{node.status}}`,
        `Refreshed: ${{node.refreshed}}`,
        `Reason: ${{node.reason}}`,
        `Elapsed: ${{Number(node.elapsed_s || 0).toFixed(2)}}s`,
        `Output bytes: ${{fmtBytes(node.output_total_bytes)}}`,
        `Input bytes: ${{fmtBytes(node.input_total_bytes)}}`,
        `Cache file: ${{node.cache_file_path || 'n/a'}}`,
        `Cache bytes: ${{fmtBytes(node.cache_file_bytes)}}`,
        `Outputs: ${{(node.output_artifacts || []).length}} artifact(s)`,
        `Inputs: ${{(node.input_artifacts || []).length}} artifact(s)`,
      ];
      (node.output_artifacts || []).slice(0, 3).forEach((a) => {{
        lines.push(`OUT ${{a.key}}: ${{fmtBytes(a.bytes)}} @ ${{a.path}}`);
      }});
      if ((node.output_artifacts || []).length > 3) {{
        lines.push(`... +${{(node.output_artifacts || []).length - 3}} more outputs`);
      }}
      (node.input_artifacts || []).slice(0, 3).forEach((a) => {{
        lines.push(`IN ${{a.key}}: ${{fmtBytes(a.bytes)}} @ ${{a.path}}`);
      }});
      if ((node.input_artifacts || []).length > 3) {{
        lines.push(`... +${{(node.input_artifacts || []).length - 3}} more inputs`);
      }}
      return lines.join('\\n');
    }}

    function edgeTooltip(edge) {{
      return [`Edge: ${{edge.source}} -> ${{edge.target}}`, `Payload bytes: ${{fmtBytes(edge.payload_bytes)}}`].join('\\n');
    }}

    function escapeHtml(s) {{
      return String(s ?? '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    function renderArtifactPanel(node) {{
      const panel = document.getElementById('artifactPanel');
      const body = document.getElementById('artifactPanelBody');
      const backdrop = document.getElementById('drawerBackdrop');
      if (!panel || !body || !node) return;
      const links = Array.isArray(node.artifact_links) ? node.artifact_links : [];
      let html = `
        <h3>${{escapeHtml(node.name)}}</h3>
        <div class="meta">
          Status: <b>${{escapeHtml(node.status)}}</b> |
          Elapsed: <b>${{Number(node.elapsed_s || 0).toFixed(2)}}s</b> |
          Outputs: <b>${{links.length}}</b>
        </div>
      `;
      if (node.inspector_href) {{
        html += `<div class="artifact-actions" style="margin-bottom:8px;"><a href="${{escapeHtml(node.inspector_href)}}" target="_blank" rel="noopener noreferrer">Open Inspector</a></div>`;
      }}
      if (!links.length) {{
        html += `<div class="artifact-item">No output artifacts available for this node.</div>`;
      }} else {{
        html += '<div class="artifact-list">';
        links.forEach((a) => {{
          html += `
            <div class="artifact-item">
              <div class="top">
                <span class="key">${{escapeHtml(a.key || 'artifact')}}</span>
                <span class="fmt">${{escapeHtml(a.format || 'unknown')}}</span>
              </div>
              <div>Size: ${{fmtBytes(a.bytes)}}</div>
              <div style="margin-top:4px;color:#94a3b8;">${{escapeHtml(a.path || a.abs_path || 'n/a')}}</div>
              <div class="artifact-actions" style="margin-top:6px;">
                ${{a.open_href ? `<a href="${{escapeHtml(a.open_href)}}" target="_blank" rel="noopener noreferrer">Open</a>` : ''}}
                ${{a.preview_href ? `<a href="${{escapeHtml(a.preview_href)}}" target="_blank" rel="noopener noreferrer">Preview</a>` : ''}}
              </div>
            </div>
          `;
        }});
        html += '</div>';
      }}
      body.innerHTML = html;
      panel.classList.add('open');
      if (backdrop) backdrop.classList.add('open');
    }}

    function closeArtifactPanel() {{
      const panel = document.getElementById('artifactPanel');
      const backdrop = document.getElementById('drawerBackdrop');
      if (panel) panel.classList.remove('open');
      if (backdrop) backdrop.classList.remove('open');
    }}

    function arrowPoints(x2, y2, c2x, c2y, size = 11.5) {{
      const angle = Math.atan2(y2 - c2y, x2 - c2x);
      const ux = Math.cos(angle);
      const uy = Math.sin(angle);
      const bx = x2 - ux * size;
      const by = y2 - uy * size;
      const px = -uy;
      const py = ux;
      const w = size * 0.6;
      const lx = bx + px * w;
      const ly = by + py * w;
      const rx = bx - px * w;
      const ry = by - py * w;
      return `${{x2}},${{y2}} ${{lx}},${{ly}} ${{rx}},${{ry}}`;
    }}

    function buildDims(nodes) {{
      const maxElapsed = Math.max(...nodes.map((n) => Number(n.elapsed_s || 0)), 1);
      const map = new Map();
      nodes.forEach((n) => {{
        const e = Number(n.elapsed_s || 0);
        map.set(n.name, {{
          w: 180 + 140 * Math.sqrt(Math.max(e, 0) / Math.max(maxElapsed, 1e-9)),
          h: 62,
        }});
      }});
      return map;
    }}

    async function elkLayout(nodes, edges, dims, layoutSettings) {{
      if (!window.ELK) return null;
      const elk = new ELK();
      const nodeSpacing = Number(layoutSettings.node_spacing || 55);
      const layerSpacing = Number(layoutSettings.layer_spacing || 120);
      const edgeNodeSpacing = Number(layoutSettings.edge_node_spacing || 30);
      const graph = {{
        id: 'root',
        layoutOptions: {{
          'elk.algorithm': 'layered',
          'elk.direction': 'RIGHT',
          'elk.edgeRouting': 'SPLINES',
          'elk.spacing.nodeNode': String(nodeSpacing),
          'elk.layered.spacing.nodeNodeBetweenLayers': String(layerSpacing),
          'elk.spacing.edgeNode': String(edgeNodeSpacing),
          'elk.layered.crossingMinimization.strategy': 'LAYER_SWEEP',
        }},
        children: nodes.map((n) => {{
          const d = dims.get(n.name);
          return {{ id: n.name, width: d.w, height: d.h }};
        }}),
        edges: edges.map((e, i) => ({{
          id: `e${{i}}`,
          sources: [e.source],
          targets: [e.target],
        }})),
      }};
      const out = await elk.layout(graph);
      const pos = new Map();
      (out.children || []).forEach((c) => {{
        pos.set(c.id, {{
          x: Number(c.x || 0) + Number(c.width || 0) / 2,
          y: Number(c.y || 0) + Number(c.height || 0) / 2,
          w: Number(c.width || 0),
          h: Number(c.height || 0),
        }});
      }});
      const edgeSections = new Map();
      (out.edges || []).forEach((e) => {{
        if (e.sections && e.sections.length) edgeSections.set(e.id, e.sections[0]);
      }});
      return {{ width: Number(out.width || 1600), height: Number(out.height || 900), positions: pos, edgeSections }};
    }}

    function renderDag(layout, nodes, edges, dims) {{
      const svg = document.getElementById('dagSvg');
      while (svg.firstChild) svg.removeChild(svg.firstChild);
      const pad = 60;
      svg.setAttribute('viewBox', `0 0 ${{Math.ceil(layout.width + pad * 2)}} ${{Math.ceil(layout.height + pad * 2)}}`);
      const nodeByName = new Map(nodes.map((n) => [n.name, n]));
      const closeBtn = document.getElementById('artifactPanelClose');
      if (closeBtn) closeBtn.onclick = closeArtifactPanel;
      const backdrop = document.getElementById('drawerBackdrop');
      if (backdrop) backdrop.onclick = closeArtifactPanel;

      const g = document.createElementNS(NS, 'g');
      g.setAttribute('transform', `translate(${{pad}}, ${{pad}})`);
      svg.appendChild(g);

      const maxPayload = Math.max(...edges.map((e) => Number(e.payload_bytes || 0)), 0);
      const edgePathEls = new Map();
      const edgeArrowEls = new Map();
      const edgeMeta = new Map();

      edges.forEach((edge, idx) => {{
        const edgeId = `e${{idx}}`;
        edgeMeta.set(edgeId, edge);
        const src = layout.positions.get(edge.source);
        const dst = layout.positions.get(edge.target);
        if (!src || !dst) return;

        const payload = Number(edge.payload_bytes || 0);
        let stroke = 1.0;
        if (maxPayload > 0 && payload > 0) stroke = 1.0 + 5.0 * Math.sqrt(payload / maxPayload);

        let x1 = src.x + src.w / 2, y1 = src.y, x2 = dst.x - dst.w / 2, y2 = dst.y;
        let c1x, c1y, c2x, c2y, pathD;

        const sec = layout.edgeSections.get(edgeId);
        if (sec && sec.startPoint && sec.endPoint) {{
          const pts = [sec.startPoint].concat(sec.bendPoints || []).concat([sec.endPoint]);
          const p0 = pts[0], pN = pts[pts.length - 1];
          x1 = Number(p0.x); y1 = Number(p0.y); x2 = Number(pN.x); y2 = Number(pN.y);
          pathD = `M ${{x1}} ${{y1}}`;
          for (let i = 1; i < pts.length; i += 1) pathD += ` L ${{Number(pts[i].x)}} ${{Number(pts[i].y)}}`;
          const pPrev = pts.length > 1 ? pts[pts.length - 2] : p0;
          c2x = Number(pPrev.x); c2y = Number(pPrev.y);
        }} else {{
          const dx = Math.max((x2 - x1) * 0.45, 40.0);
          c1x = x1 + dx; c1y = y1 + (y2 - y1) * 0.2;
          c2x = x2 - dx; c2y = y2 - (y2 - y1) * 0.2;
          pathD = `M ${{x1}} ${{y1}} C ${{c1x}} ${{c1y}}, ${{c2x}} ${{c2y}}, ${{x2}} ${{y2}}`;
        }}

        const path = document.createElementNS(NS, 'path');
        path.setAttribute('class', 'dag-hover dag-edge-path');
        path.setAttribute('data-edge-id', edgeId);
        path.setAttribute('data-source', edge.source);
        path.setAttribute('data-target', edge.target);
        path.setAttribute('data-tooltip', edgeTooltip(edge));
        path.setAttribute('d', pathD);
        path.setAttribute('fill', 'none');
        path.setAttribute('stroke', '#a0aec0');
        path.setAttribute('stroke-width', stroke.toFixed(2));
        g.appendChild(path);
        edgePathEls.set(edgeId, path);

        const arrow = document.createElementNS(NS, 'polygon');
        arrow.setAttribute('class', 'dag-hover dag-edge-arrow');
        arrow.setAttribute('data-edge-id', edgeId);
        arrow.setAttribute('data-source', edge.source);
        arrow.setAttribute('data-target', edge.target);
        arrow.setAttribute('data-tooltip', edgeTooltip(edge));
        arrow.setAttribute('points', arrowPoints(x2, y2, c2x ?? x1, c2y ?? y1, 11.5));
        arrow.setAttribute('fill', '#a0aec0');
        g.appendChild(arrow);
        edgeArrowEls.set(edgeId, arrow);
      }});

      const labelMap = new Map();
      const subMap = new Map();
      const nodeRectMap = new Map();
      nodes.forEach((node) => {{
        const p = layout.positions.get(node.name);
        if (!p) return;
        const rect = document.createElementNS(NS, 'rect');
        rect.setAttribute('class', 'dag-hover dag-node');
        rect.setAttribute('data-node', node.name);
        rect.setAttribute('data-cx', String(p.x));
        rect.setAttribute('data-cy', String(p.y));
        rect.setAttribute('data-w', String(p.w));
        rect.setAttribute('data-h', String(p.h));
        rect.setAttribute('data-tooltip', nodeTooltip(node));
        rect.setAttribute('x', String(p.x - p.w / 2));
        rect.setAttribute('y', String(p.y - p.h / 2));
        rect.setAttribute('width', String(p.w));
        rect.setAttribute('height', String(p.h));
        rect.setAttribute('rx', '10');
        rect.setAttribute('fill', nodeColor(String(node.status || 'unknown')));
        rect.setAttribute('opacity', '0.95');
        rect.setAttribute('stroke', '#1a202c');
        rect.setAttribute('stroke-width', '1.0');
        rect.style.cursor = 'grab';
        g.appendChild(rect);
        nodeRectMap.set(node.name, rect);

        const t1 = document.createElementNS(NS, 'text');
        t1.setAttribute('data-node-label', node.name);
        t1.setAttribute('x', String(p.x));
        t1.setAttribute('y', String(p.y - 6));
        t1.setAttribute('text-anchor', 'middle');
        t1.setAttribute('fill', '#f7fafc');
        t1.setAttribute('font-size', '13');
        t1.setAttribute('font-family', 'Segoe UI, Arial');
        t1.setAttribute('font-weight', '700');
        t1.style.pointerEvents = 'none';
        t1.textContent = node.name;
        g.appendChild(t1);
        labelMap.set(node.name, t1);

        const t2 = document.createElementNS(NS, 'text');
        t2.setAttribute('data-node-sub', node.name);
        t2.setAttribute('x', String(p.x));
        t2.setAttribute('y', String(p.y + 14));
        t2.setAttribute('text-anchor', 'middle');
        t2.setAttribute('fill', '#edf2f7');
        t2.setAttribute('font-size', '11');
        t2.setAttribute('font-family', 'Segoe UI, Arial');
        t2.style.pointerEvents = 'none';
        t2.textContent = `${{node.status}} | ${{Number(node.elapsed_s || 0).toFixed(2)}}s`;
        g.appendChild(t2);
        subMap.set(node.name, t2);
      }});

      // Tooltip
      const tooltip = document.getElementById('dagTooltip');
      const hoverEls = svg.querySelectorAll('.dag-hover[data-tooltip]');
      const renderTooltip = (raw) => {{
        const lines = String(raw || '').split('\\n');
        const html = lines.map((line) => {{
          const idx = line.indexOf(':');
          if (idx > 0) {{
            const k = line.slice(0, idx + 1);
            const v = line.slice(idx + 1);
            if (k === 'Node:') return `<div class="row"><span class="k">${{k}}</span><b>${{v}}</b></div>`;
            return `<div class="row"><span class="k">${{k}}</span>${{v}}</div>`;
          }}
          return `<div class="row">${{line}}</div>`;
        }}).join('');
        tooltip.innerHTML = html;
      }};
      hoverEls.forEach((el) => {{
        el.addEventListener('mouseenter', () => {{
          renderTooltip(el.getAttribute('data-tooltip'));
          tooltip.style.display = 'block';
        }});
        el.addEventListener('mouseleave', () => {{ tooltip.style.display = 'none'; }});
      }});

      // Drag support
      const getState = (name) => {{
        const r = nodeRectMap.get(name);
        if (!r) return null;
        return {{
          cx: parseFloat(r.getAttribute('data-cx') || '0'),
          cy: parseFloat(r.getAttribute('data-cy') || '0'),
          w: parseFloat(r.getAttribute('data-w') || '0'),
          h: parseFloat(r.getAttribute('data-h') || '0'),
        }};
      }};
      const setState = (name, cx, cy) => {{
        const r = nodeRectMap.get(name);
        if (!r) return;
        const w = parseFloat(r.getAttribute('data-w') || '0');
        const h = parseFloat(r.getAttribute('data-h') || '0');
        r.setAttribute('data-cx', String(cx));
        r.setAttribute('data-cy', String(cy));
        r.setAttribute('x', String(cx - w / 2));
        r.setAttribute('y', String(cy - h / 2));
        const t1 = labelMap.get(name);
        if (t1) {{ t1.setAttribute('x', String(cx)); t1.setAttribute('y', String(cy - 6)); }}
        const t2 = subMap.get(name);
        if (t2) {{ t2.setAttribute('x', String(cx)); t2.setAttribute('y', String(cy + 14)); }}
      }};

      const rerouteEdgeById = (edgeId) => {{
        const e = edgeMeta.get(edgeId);
        if (!e) return;
        const src = getState(e.source);
        const dst = getState(e.target);
        if (!src || !dst) return;
        const x1 = src.cx + src.w / 2, y1 = src.cy;
        const x2 = dst.cx - dst.w / 2, y2 = dst.cy;
        const dx = Math.max((x2 - x1) * 0.45, 40.0);
        const c1x = x1 + dx, c1y = y1 + (y2 - y1) * 0.2;
        const c2x = x2 - dx, c2y = y2 - (y2 - y1) * 0.2;
        const d = `M ${{x1}} ${{y1}} C ${{c1x}} ${{c1y}}, ${{c2x}} ${{c2y}}, ${{x2}} ${{y2}}`;
        const p = edgePathEls.get(edgeId);
        const a = edgeArrowEls.get(edgeId);
        if (p) p.setAttribute('d', d);
        if (a) a.setAttribute('points', arrowPoints(x2, y2, c2x, c2y, 11.5));
      }};

      const rerouteConnectedEdges = (name) => {{
        edgeMeta.forEach((e, edgeId) => {{
          if (e.source === name || e.target === name) rerouteEdgeById(edgeId);
        }});
      }};

      // Normalize first paint to the same smooth cubic routing used during drag.
      // This avoids the initial ELK polyline look before any interaction.
      edgeMeta.forEach((_e, edgeId) => rerouteEdgeById(edgeId));

      const pt = svg.createSVGPoint();
      const toSvgPoint = (clientX, clientY) => {{
        pt.x = clientX; pt.y = clientY;
        const m = svg.getScreenCTM();
        return m ? pt.matrixTransform(m.inverse()) : {{ x: clientX, y: clientY }};
      }};
      let drag = null;
      let suppressNodeClickUntil = 0;
      nodeRectMap.forEach((rect, name) => {{
        rect.addEventListener('mousedown', (ev) => {{
          ev.preventDefault();
          const s = getState(name);
          if (!s) return;
          const p = toSvgPoint(ev.clientX, ev.clientY);
          drag = {{ name, dx: p.x - s.cx, dy: p.y - s.cy, startX: ev.clientX, startY: ev.clientY, moved: false }};
          rect.style.cursor = 'grabbing';
        }});
        rect.addEventListener('click', (ev) => {{
          if (Date.now() < suppressNodeClickUntil) return;
          ev.stopPropagation();
          const node = nodeByName.get(name);
          renderArtifactPanel(node);
        }});
        rect.addEventListener('dblclick', (ev) => {{
          ev.stopPropagation();
          const node = nodeByName.get(name);
          if (!node) return;
          const href = node.primary_artifact_href || node.primary_preview_href;
          if (href) window.open(href, '_blank', 'noopener,noreferrer');
        }});
      }});
      window.addEventListener('mousemove', (ev) => {{
        if (!drag) return;
        const p = toSvgPoint(ev.clientX, ev.clientY);
        const movedPx = Math.hypot(ev.clientX - drag.startX, ev.clientY - drag.startY);
        if (movedPx > 4) drag.moved = true;
        setState(drag.name, p.x - drag.dx, p.y - drag.dy);
        rerouteConnectedEdges(drag.name);
      }});
      window.addEventListener('mouseup', () => {{
        if (!drag) return;
        const r = nodeRectMap.get(drag.name);
        if (r) r.style.cursor = 'grab';
        if (drag.moved) suppressNodeClickUntil = Date.now() + 200;
        drag = null;
      }});
      svg.addEventListener('click', () => closeArtifactPanel());
      window.addEventListener('keydown', (ev) => {{
        if (ev.key === 'Escape') closeArtifactPanel();
      }});
    }}

    function sortTable(col, numeric) {{
      const table = document.getElementById('nodeTable');
      const rows = Array.from(table.querySelectorAll('tbody tr'));
      const sorted = rows.sort((a, b) => {{
        const ac = a.children[col];
        const bc = b.children[col];
        const av = ac.getAttribute('data-sort') || ac.innerText;
        const bv = bc.getAttribute('data-sort') || bc.innerText;
        if (numeric) return parseFloat(bv) - parseFloat(av);
        return av.localeCompare(bv);
      }});
      const body = table.querySelector('tbody');
      sorted.forEach(r => body.appendChild(r));
    }}

    (async function initDag() {{
      const dims = buildDims(manifestNodes);
      try {{
        const layout = await elkLayout(manifestNodes, manifestEdges, dims, manifestLayout);
        if (!layout) throw new Error('ELK layout unavailable');
        renderDag(layout, manifestNodes, manifestEdges, dims);
      }} catch (err) {{
        const card = document.querySelector('.card');
        if (card) {{
          card.innerHTML = `<div style="padding:16px;color:#fca5a5;">Failed to initialize ELK DAG layout. Please check network access for elkjs bundle.</div>`;
        }}
      }}
    }})();
  </script>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def prune_old_reports(output_dir: Path, keep_last_n: int, suffix: str) -> None:
    files = sorted(output_dir.glob(f"*{suffix}"), key=lambda p: p.stat().st_mtime, reverse=True)
    for stale in files[keep_last_n:]:
        try:
            stale.unlink()
        except Exception:
            pass


def write_report_bundle(report_cfg: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, str]:
    output_dir = Path(report_cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id = manifest["run_id"]
    written: Dict[str, str] = {}
    fmt = report_cfg["format"]
    enriched_manifest = _build_enriched_manifest(
        manifest=manifest,
        output_dir=output_dir,
        include_previews=fmt in {"html", "both"},
    )

    if fmt in {"json", "both"}:
        json_path = output_dir / f"{run_id}.json"
        _write_json(json_path, enriched_manifest)
        written["json"] = str(json_path)
        prune_old_reports(output_dir, report_cfg["keep_last_n"], ".json")
        if report_cfg["write_latest_pointer"]:
            latest_json = output_dir / "latest.json"
            _write_json(latest_json, enriched_manifest)
            written["latest_json"] = str(latest_json)

    if fmt in {"html", "both"}:
        html_path = output_dir / f"{run_id}.html"
        render_html_report(enriched_manifest, html_path)
        written["html"] = str(html_path)
        prune_old_reports(output_dir, report_cfg["keep_last_n"], ".html")
        if report_cfg["write_latest_pointer"]:
            latest_html = output_dir / "latest.html"
            shutil.copyfile(html_path, latest_html)
            written["latest_html"] = str(latest_html)

    return written
