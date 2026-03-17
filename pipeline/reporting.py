"""Pipeline run manifest and HTML DAG report helpers."""

from __future__ import annotations

import json
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


REQUIRED_REPORT_KEYS = {
    "enabled",
    "format",
    "output_dir",
    "keep_last_n",
    "include_edge_payloads",
    "write_latest_pointer",
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

    return {
        "enabled": enabled,
        "format": fmt,
        "output_dir": output_dir.strip(),
        "keep_last_n": keep_last_n,
        "include_edge_payloads": include_edge_payloads,
        "write_latest_pointer": write_latest_pointer,
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


def _encode_tooltip(lines: List[str]) -> str:
    return _escape("\n".join(lines))


def _arrow_points(x2: float, y2: float, c2x: float, c2y: float, size: float = 10.0) -> str:
    angle = math.atan2(y2 - c2y, x2 - c2x)
    ux = math.cos(angle)
    uy = math.sin(angle)
    bx = x2 - ux * size
    by = y2 - uy * size
    px = -uy
    py = ux
    w = size * 0.6
    lx = bx + px * w
    ly = by + py * w
    rx = bx - px * w
    ry = by - py * w
    return f"{x2:.1f},{y2:.1f} {lx:.1f},{ly:.1f} {rx:.1f},{ry:.1f}"


def render_html_report(manifest: Dict[str, Any], output_path: Path) -> None:
    nodes = manifest.get("nodes", [])
    edges = manifest.get("edges", [])
    node_by_name = {n["name"]: n for n in nodes}

    # Layer nodes by dependency depth.
    parents: Dict[str, List[str]] = {n["name"]: [] for n in nodes}
    for edge in edges:
        parents.setdefault(edge["target"], []).append(edge["source"])
    memo: Dict[str, int] = {}

    def depth(name: str) -> int:
        if name in memo:
            return memo[name]
        p = parents.get(name, [])
        if not p:
            memo[name] = 0
            return 0
        d = 1 + max(depth(parent) for parent in p)
        memo[name] = d
        return d

    layers: Dict[int, List[str]] = {}
    for n in node_by_name:
        layers.setdefault(depth(n), []).append(n)
    for level_nodes in layers.values():
        level_nodes.sort()

    children: Dict[str, List[str]] = {n["name"]: [] for n in nodes}
    for edge in edges:
        children.setdefault(edge["source"], []).append(edge["target"])

    max_level = max(layers.keys(), default=0)
    # Barycentric sweeps reduce crossings/overlap by ordering each layer
    # based on neighboring layers' current order.
    for _ in range(6):
        # Forward sweep (parents)
        for level in range(1, max_level + 1):
            prev_nodes = layers.get(level - 1, [])
            prev_rank = {name: idx for idx, name in enumerate(prev_nodes)}

            def parent_score(name: str):
                p = parents.get(name, [])
                vals = [prev_rank[x] for x in p if x in prev_rank]
                if vals:
                    return sum(vals) / len(vals)
                return float("inf")

            layers[level] = sorted(layers.get(level, []), key=lambda n: (parent_score(n), n))

        # Backward sweep (children)
        for level in range(max_level - 1, -1, -1):
            next_nodes = layers.get(level + 1, [])
            next_rank = {name: idx for idx, name in enumerate(next_nodes)}

            def child_score(name: str):
                c = children.get(name, [])
                vals = [next_rank[x] for x in c if x in next_rank]
                if vals:
                    return sum(vals) / len(vals)
                return float("inf")

            layers[level] = sorted(layers.get(level, []), key=lambda n: (child_score(n), n))

    max_elapsed = max((float(n.get("elapsed_s", 0.0)) for n in nodes), default=1.0)
    width = max(1200, 320 * max(len(layers), 1))
    height = max(700, 170 * max((len(v) for v in layers.values()), default=1))
    x_step = width / max(len(layers), 1)
    positions: Dict[str, Dict[str, float]] = {}

    for level, level_nodes in sorted(layers.items()):
        y_step = height / (len(level_nodes) + 1)
        for idx, name in enumerate(level_nodes, start=1):
            n = node_by_name[name]
            elapsed = float(n.get("elapsed_s", 0.0))
            w = 180 + 140 * math.sqrt(max(elapsed, 0.0) / max(max_elapsed, 1e-9))
            h = 62
            x = (level + 0.5) * x_step
            y = idx * y_step
            positions[name] = {"x": x, "y": y, "w": w, "h": h}

    def node_color(status: str) -> str:
        if status == "computed":
            return "#2f855a"
        if status == "cached":
            return "#2b6cb0"
        if status == "failed":
            return "#c53030"
        return "#4a5568"

    payload_values = [float(edge.get("payload_bytes") or 0) for edge in edges]
    max_payload = max(payload_values, default=0.0)

    svg_lines: List[str] = []
    svg_lines.append(f'<svg viewBox="0 0 {int(width)} {int(height)}" width="100%" height="780" role="img">')

    for edge_idx, edge in enumerate(edges):
        src = positions.get(edge["source"])
        dst = positions.get(edge["target"])
        if not src or not dst:
            continue
        payload = float(edge.get("payload_bytes") or 0)
        stroke = 1.0
        if max_payload > 0 and payload > 0:
            # Sqrt-scaled linear sizing keeps large payloads visibly thicker
            # while avoiding extreme line widths.
            stroke = 1.0 + 5.0 * math.sqrt(payload / max_payload)
        tooltip = _encode_tooltip(_edge_tooltip_lines(edge))
        x1 = src["x"] + src["w"] / 2.0
        y1 = src["y"]
        x2 = dst["x"] - dst["w"] / 2.0
        y2 = dst["y"]
        # Gentle cubic routing reduces visual collisions with nodes/labels
        # versus strict straight-line edges.
        dx = max((x2 - x1) * 0.45, 40.0)
        c1x = x1 + dx
        c1y = y1 + (y2 - y1) * 0.2
        c2x = x2 - dx
        c2y = y2 - (y2 - y1) * 0.2
        path_d = f"M {x1:.1f} {y1:.1f} C {c1x:.1f} {c1y:.1f}, {c2x:.1f} {c2y:.1f}, {x2:.1f} {y2:.1f}"
        points = _arrow_points(x2, y2, c2x, c2y, size=11.5)
        edge_id = f"e{edge_idx}"
        svg_lines.append(
            f'<path class="dag-hover dag-edge-path" data-edge-id="{edge_id}" data-source="{_escape(edge["source"])}" data-target="{_escape(edge["target"])}" data-tooltip="{tooltip}" d="{path_d}" fill="none" stroke="#a0aec0" stroke-width="{stroke:.2f}"></path>'
        )
        svg_lines.append(
            f'<polygon class="dag-hover dag-edge-arrow" data-edge-id="{edge_id}" data-source="{_escape(edge["source"])}" data-target="{_escape(edge["target"])}" data-tooltip="{tooltip}" points="{points}" fill="#a0aec0"></polygon>'
        )

    for node in nodes:
        name = node["name"]
        pos = positions.get(name)
        if not pos:
            continue
        fill = node_color(str(node.get("status", "unknown")))
        tooltip = _encode_tooltip(_tooltip_lines(node))
        x = pos["x"] - pos["w"] / 2.0
        y = pos["y"] - pos["h"] / 2.0
        svg_lines.append(
            f'<rect class="dag-hover dag-node" data-node="{_escape(name)}" data-cx="{pos["x"]:.1f}" data-cy="{pos["y"]:.1f}" data-w="{pos["w"]:.1f}" data-h="{pos["h"]:.1f}" data-tooltip="{tooltip}" x="{x:.1f}" y="{y:.1f}" width="{pos["w"]:.1f}" height="{pos["h"]:.1f}" rx="10" fill="{fill}" opacity="0.95" stroke="#1a202c" stroke-width="1.0" style="cursor: grab;"></rect>'
        )
        label = _escape(name)
        sub = _escape(f"{node.get('status')} | {node.get('elapsed_s', 0.0):.2f}s")
        svg_lines.append(f'<text data-node-label="{_escape(name)}" x="{pos["x"]:.1f}" y="{(pos["y"]-6):.1f}" text-anchor="middle" fill="#f7fafc" font-size="13" font-family="Segoe UI, Arial" style="pointer-events:none;">{label}</text>')
        svg_lines.append(f'<text data-node-sub="{_escape(name)}" x="{pos["x"]:.1f}" y="{(pos["y"]+14):.1f}" text-anchor="middle" fill="#edf2f7" font-size="11" font-family="Segoe UI, Arial" style="pointer-events:none;">{sub}</text>')

    svg_lines.append("</svg>")
    svg = "\n".join(svg_lines)

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
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Pipeline DAG Report { _escape(manifest.get("run_id")) }</title>
  <style>
    body {{ background: #111827; color: #e5e7eb; font-family: Segoe UI, Arial, sans-serif; margin: 18px; }}
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
    table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
    th, td {{ padding: 8px; border-bottom: 1px solid #374151; text-align: left; vertical-align: top; }}
    th {{ cursor: pointer; color: #93c5fd; }}
    .dag-tooltip {{
      position: fixed;
      z-index: 9999;
      display: none;
      max-width: 520px;
      background: rgba(15, 23, 42, 0.97);
      border: 1px solid #60a5fa;
      border-radius: 10px;
      padding: 8px 10px;
      font-size: 12px;
      line-height: 1.35;
      color: #e2e8f0;
      pointer-events: none;
      box-shadow: 0 6px 24px rgba(0, 0, 0, 0.4);
      white-space: pre-wrap;
    }}
    .dag-tooltip .row {{ margin: 0 0 2px 0; }}
    .dag-tooltip .k {{ color: #93c5fd; font-weight: 700; }}
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
    {svg}
  </div>
  <div class="card">
    <h2>Node Metrics</h2>
    <table id="nodeTable">
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
  <script>
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

    (function setupDagTooltip() {{
      const tooltip = document.getElementById('dagTooltip');
      const hoverEls = document.querySelectorAll('.dag-hover[data-tooltip]');
      const renderTooltip = (raw) => {{
        const lines = String(raw || '').split('\\n');
        const html = lines.map((line) => {{
          const idx = line.indexOf(':');
          if (idx > 0) {{
            const k = line.slice(0, idx + 1);
            const v = line.slice(idx + 1);
            return `<div class="row"><span class="k">${{k}}</span>${{v}}</div>`;
          }}
          return `<div class="row">${{line}}</div>`;
        }}).join('');
        tooltip.innerHTML = html;
      }};
      const move = (ev) => {{
        const x = ev.clientX + 14;
        const y = ev.clientY + 14;
        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
      }};
      hoverEls.forEach((el) => {{
        el.addEventListener('mouseenter', (ev) => {{
          renderTooltip(el.getAttribute('data-tooltip'));
          tooltip.style.display = 'block';
          move(ev);
        }});
        el.addEventListener('mousemove', move);
        el.addEventListener('mouseleave', () => {{
          tooltip.style.display = 'none';
        }});
      }});
    }})();

    (function setupDagDrag() {{
      const svg = document.querySelector('svg');
      if (!svg) return;
      const nodeEls = Array.from(svg.querySelectorAll('.dag-node[data-node]'));
      const edgePathEls = Array.from(svg.querySelectorAll('.dag-edge-path[data-edge-id]'));
      const edgeArrowEls = Array.from(svg.querySelectorAll('.dag-edge-arrow[data-edge-id]'));
      const edgePathById = new Map();
      const edgeArrowById = new Map();
      edgePathEls.forEach((el) => edgePathById.set(el.getAttribute('data-edge-id'), el));
      edgeArrowEls.forEach((el) => edgeArrowById.set(el.getAttribute('data-edge-id'), el));
      const labelMap = new Map();
      const subMap = new Map();
      svg.querySelectorAll('text[data-node-label]').forEach((el) => labelMap.set(el.getAttribute('data-node-label'), el));
      svg.querySelectorAll('text[data-node-sub]').forEach((el) => subMap.set(el.getAttribute('data-node-sub'), el));

      const nodeMap = new Map();
      nodeEls.forEach((el) => nodeMap.set(el.getAttribute('data-node'), el));

      const getState = (name) => {{
        const rect = nodeMap.get(name);
        if (!rect) return null;
        return {{
          cx: parseFloat(rect.getAttribute('data-cx') || '0'),
          cy: parseFloat(rect.getAttribute('data-cy') || '0'),
          w: parseFloat(rect.getAttribute('data-w') || '0'),
          h: parseFloat(rect.getAttribute('data-h') || '0'),
        }};
      }};

      const setState = (name, cx, cy) => {{
        const rect = nodeMap.get(name);
        if (!rect) return;
        const w = parseFloat(rect.getAttribute('data-w') || '0');
        const h = parseFloat(rect.getAttribute('data-h') || '0');
        rect.setAttribute('data-cx', String(cx));
        rect.setAttribute('data-cy', String(cy));
        rect.setAttribute('x', String(cx - w / 2));
        rect.setAttribute('y', String(cy - h / 2));
        const label = labelMap.get(name);
        if (label) {{
          label.setAttribute('x', String(cx));
          label.setAttribute('y', String(cy - 6));
        }}
        const sub = subMap.get(name);
        if (sub) {{
          sub.setAttribute('x', String(cx));
          sub.setAttribute('y', String(cy + 14));
        }}
      }};

      const arrowPoints = (x2, y2, c2x, c2y, size = 11.5) => {{
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
      }};

      const rerouteEdgeById = (edgeId) => {{
        const edgeEl = edgePathById.get(edgeId);
        if (!edgeEl) return;
        const source = edgeEl.getAttribute('data-source');
        const target = edgeEl.getAttribute('data-target');
        const src = getState(source);
        const dst = getState(target);
        if (!src || !dst) return;
        const x1 = src.cx + src.w / 2;
        const y1 = src.cy;
        const x2 = dst.cx - dst.w / 2;
        const y2 = dst.cy;
        const dx = Math.max((x2 - x1) * 0.45, 40.0);
        const c1x = x1 + dx;
        const c1y = y1 + (y2 - y1) * 0.2;
        const c2x = x2 - dx;
        const c2y = y2 - (y2 - y1) * 0.2;
        edgeEl.setAttribute('d', `M ${{x1}} ${{y1}} C ${{c1x}} ${{c1y}}, ${{c2x}} ${{c2y}}, ${{x2}} ${{y2}}`);
        const arrowEl = edgeArrowById.get(edgeId);
        if (arrowEl) {{
          arrowEl.setAttribute('points', arrowPoints(x2, y2, c2x, c2y));
        }}
      }};

      const rerouteConnectedEdges = (name) => {{
        edgePathEls.forEach((edgeEl) => {{
          if (edgeEl.getAttribute('data-source') === name || edgeEl.getAttribute('data-target') === name) {{
            rerouteEdgeById(edgeEl.getAttribute('data-edge-id'));
          }}
        }});
      }};

      let drag = null;
      const pt = svg.createSVGPoint();
      const toSvgPoint = (clientX, clientY) => {{
        pt.x = clientX;
        pt.y = clientY;
        const m = svg.getScreenCTM();
        return m ? pt.matrixTransform(m.inverse()) : {{ x: clientX, y: clientY }};
      }};

      nodeEls.forEach((rect) => {{
        rect.addEventListener('mousedown', (ev) => {{
          ev.preventDefault();
          const name = rect.getAttribute('data-node');
          const s = getState(name);
          if (!s) return;
          const p = toSvgPoint(ev.clientX, ev.clientY);
          drag = {{ name, dx: p.x - s.cx, dy: p.y - s.cy }};
          rect.style.cursor = 'grabbing';
        }});
      }});

      window.addEventListener('mousemove', (ev) => {{
        if (!drag) return;
        const p = toSvgPoint(ev.clientX, ev.clientY);
        const nx = p.x - drag.dx;
        const ny = p.y - drag.dy;
        setState(drag.name, nx, ny);
        rerouteConnectedEdges(drag.name);
      }});

      window.addEventListener('mouseup', () => {{
        if (!drag) return;
        const rect = nodeMap.get(drag.name);
        if (rect) rect.style.cursor = 'grab';
        drag = null;
      }});
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

    if fmt in {"json", "both"}:
        json_path = output_dir / f"{run_id}.json"
        _write_json(json_path, manifest)
        written["json"] = str(json_path)
        prune_old_reports(output_dir, report_cfg["keep_last_n"], ".json")
        if report_cfg["write_latest_pointer"]:
            latest_json = output_dir / "latest.json"
            _write_json(latest_json, manifest)
            written["latest_json"] = str(latest_json)

    if fmt in {"html", "both"}:
        html_path = output_dir / f"{run_id}.html"
        render_html_report(manifest, html_path)
        written["html"] = str(html_path)
        prune_old_reports(output_dir, report_cfg["keep_last_n"], ".html")
        if report_cfg["write_latest_pointer"]:
            latest_html = output_dir / "latest.html"
            shutil.copyfile(html_path, latest_html)
            written["latest_html"] = str(latest_html)

    return written
