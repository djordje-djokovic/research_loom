"""
Shared HTML styling framework for research_loom outputs.

This is the single source of truth for dark-theme document styling, table
formatting, and chart container look-and-feel across reports, previews,
inspectors, and model artifacts.
"""

from __future__ import annotations

from html import escape


def get_shared_html_css() -> str:
    return """
/* research_loom shared theme - aligned to original cox_model HTML style */
html, body {
  margin: 0;
  padding: 0;
}
body {
  font-family: 'Consolas', 'Courier New', monospace;
  background-color: #1e1e1e;
  color: #d4d4d4;
  padding: 20px;
  font-size: 12px;
}
h1 {
  color: #ffffff;
  border-bottom: 1px solid #ffffff;
  padding-bottom: 10px;
  font-size: 18px;
  margin: 0 0 20px 0;
}
h2 {
  color: #d4d4d4;
  margin-top: 30px;
  margin-bottom: 10px;
  font-size: 14px;
  border-bottom: 1px solid #3e3e42;
  padding-bottom: 5px;
}
h3, h4 {
  color: #d4d4d4;
  margin-top: 20px;
  margin-bottom: 8px;
  font-size: 13px;
  border-bottom: 1px solid #3e3e42;
  padding-bottom: 3px;
}
p, li, .meta, .note {
  color: #d4d4d4;
}
a {
  color: #93c5fd;
}
code, pre {
  background-color: #252526;
  border: 1px solid #3e3e42;
  border-radius: 4px;
}
pre {
  padding: 12px;
  overflow: auto;
}
table, .simpletable {
  border-collapse: collapse;
  width: 100%;
  margin-bottom: 20px;
  background-color: #252526;
  font-size: 11px;
}
table caption, .simpletable caption {
  text-align: left;
  color: #d4d4d4;
  font-weight: bold;
  margin-bottom: 5px;
}
table th, table td, .simpletable th, .simpletable td {
  padding: 6px 8px;
  border-bottom: 1px solid #3e3e42;
  text-align: left;
  vertical-align: top;
}
table th, .simpletable th {
  background-color: #2d2d30;
  color: #cccccc;
  font-weight: bold;
}
table tr:hover td, .simpletable tr:hover td {
  background-color: #2a2d2e;
}
table tr:nth-child(even) td, .simpletable tr:nth-child(even) td {
  background-color: #1e1e1e;
}
table tr:nth-child(even):hover td, .simpletable tr:nth-child(even):hover td {
  background-color: #2a2d2e;
}
.rl-card {
  background-color: #252526;
  border: 1px solid #3e3e42;
  border-radius: 4px;
  padding: 12px;
}
.info {
  color: #858585;
  font-size: 10px;
  margin-bottom: 8px;
}
.stats-section {
  background-color: #252526;
  padding: 12px;
  margin-bottom: 20px;
  border-radius: 4px;
}
.stats-row {
  display: table-row;
}
.stats-key {
  display: table-cell;
  padding: 4px 12px 4px 0;
  color: #d4d4d4;
  font-weight: bold;
  min-width: 200px;
}
.stats-value {
  display: table-cell;
  padding: 4px 0;
  color: #d4d4d4;
}
.model-summary-html {
  background-color: #252526;
  padding: 12px;
  margin-bottom: 20px;
  overflow-x: auto;
}
.model-summary-text {
  background-color: #252526;
  padding: 12px;
  margin-bottom: 20px;
  white-space: pre-wrap;
  font-family: 'Consolas', 'Courier New', monospace;
  font-size: 11px;
}
.figure-container {
  background-color: #252526;
  border: 1px solid #3e3e42;
  border-radius: 4px;
  padding: 10px;
  margin-bottom: 20px;
}
.rl-center {
  text-align: center !important;
}
.rl-note {
  font-size: 11px;
  color: #a6a6a6;
}
.rl-note-warn {
  font-size: 11px;
  color: #e3b341;
}
.rl-muted {
  color: #a6a6a6;
}
.rl-alert-high {
  color: #f87171;
  font-weight: 700;
}
.rl-alert-med {
  color: #f59e0b;
}
.rl-sig-positive {
  color: #4ade80;
  font-weight: 700;
}
"""


def build_html_document(title: str, body_html: str, extra_css: str = "") -> str:
    title_safe = escape(title or "research_loom output")
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>{title_safe}</title>
  <style>
{get_shared_html_css()}
{extra_css or ""}
  </style>
</head>
<body>
{body_html}
</body>
</html>
"""

