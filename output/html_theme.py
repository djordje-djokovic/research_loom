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
/* wrap_node_output model reports: sections + tables use full viewport width */
section.section {
  display: block;
  width: 100%;
  max-width: none;
  box-sizing: border-box;
  overflow-x: auto;
}
section.section table {
  width: 100% !important;
  max-width: none;
  box-sizing: border-box;
}
.rl-hr-by-age {
  display: block;
  width: 100%;
  max-width: none;
  box-sizing: border-box;
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
/* Tables: full width; compact row height (original padding); col1 left, rest right */
table,
table.dataframe,
.simpletable {
  border-collapse: collapse;
  width: 100%;
  margin-bottom: 20px;
  background-color: #252526;
  font-size: 11px;
  border: none;
}
table caption,
table.dataframe caption,
.simpletable caption {
  text-align: left;
  color: #d4d4d4;
  font-weight: bold;
  margin-bottom: 5px;
}
table th,
table td,
table.dataframe th,
table.dataframe td,
.simpletable th,
.simpletable td {
  padding: 6px 8px;
  border: none;
  vertical-align: top;
}
table th:first-child,
table td:first-child,
table.dataframe th:first-child,
table.dataframe td:first-child,
.simpletable th:first-child,
.simpletable td:first-child {
  text-align: left;
}
table th:not(:first-child),
table td:not(:first-child),
table.dataframe th:not(:first-child),
table.dataframe td:not(:first-child),
.simpletable th:not(:first-child),
.simpletable td:not(:first-child) {
  text-align: right;
}
/* Column headers only — not pandas row-index cells (tbody th), which should zebra like td */
table thead th,
table.dataframe thead th,
.simpletable thead th {
  background-color: #2d2d30;
  color: #cccccc;
  font-weight: bold;
}
/*
 * First column must zebra with the row (same as td), whether the cell is <th> (statsmodels stub /
 * pandas index) or <td>. Statsmodels summary tables have NO <thead>: row 1 is all <th> headers;
 * data rows are <th> stub + <td>… — old "tbody th" zebra hit every <th> in an odd row and broke
 * the header row. Use :first-child per row + thead / not-first-row guards.
 */
table tbody th,
table.dataframe tbody th,
.simpletable tbody th {
  color: #d4d4d4;
  font-weight: 600;
}
/* Explicit <thead> (e.g. pandas): only tbody rows; first cell th or td */
table thead + tbody tr:nth-child(odd) > :first-child,
table.dataframe thead + tbody tr:nth-child(odd) > :first-child,
.simpletable thead + tbody tr:nth-child(odd) > :first-child {
  background-color: #252526;
}
table thead + tbody tr:nth-child(even) > :first-child,
table.dataframe thead + tbody tr:nth-child(even) > :first-child,
.simpletable thead + tbody tr:nth-child(even) > :first-child {
  background-color: #1e1e1e;
}
table thead + tbody tr:hover > :first-child,
table.dataframe thead + tbody tr:hover > :first-child,
.simpletable thead + tbody tr:hover > :first-child {
  background-color: #2a2d2e;
}
/* No <thead> (statsmodels SimpleTable): row 1 = full header bar; from row 2, zebra first column */
table:not(:has(thead)) > tbody > tr:first-child > th,
table:not(:has(thead)) > tbody > tr:first-child > td,
table:not(:has(thead)) > tr:first-child > th,
table:not(:has(thead)) > tr:first-child > td {
  background-color: #2d2d30;
  color: #cccccc;
  font-weight: bold;
}
table:not(:has(thead)) > tbody > tr:nth-child(odd):not(:first-child) > :first-child,
table.dataframe:not(:has(thead)) > tbody > tr:nth-child(odd):not(:first-child) > :first-child,
.simpletable:not(:has(thead)) > tbody > tr:nth-child(odd):not(:first-child) > :first-child {
  background-color: #252526;
  color: #d4d4d4;
  font-weight: 600;
}
table:not(:has(thead)) > tbody > tr:nth-child(even):not(:first-child) > :first-child,
table.dataframe:not(:has(thead)) > tbody > tr:nth-child(even):not(:first-child) > :first-child,
.simpletable:not(:has(thead)) > tbody > tr:nth-child(even):not(:first-child) > :first-child {
  background-color: #1e1e1e;
  color: #d4d4d4;
  font-weight: 600;
}
table:not(:has(thead)) > tbody > tr:not(:first-child):hover > :first-child,
table.dataframe:not(:has(thead)) > tbody > tr:not(:first-child):hover > :first-child,
.simpletable:not(:has(thead)) > tbody > tr:not(:first-child):hover > :first-child {
  background-color: #2a2d2e;
}
/*
 * Statsmodels PHReg summary: no :has() / file:// reliance — cox_model wraps output in this div.
 * SimpleTable uses row 1 = all <th> headers; data rows = <th> stub + <td>. Striping uses nth-child(n+2)
 * so it matches global even/odd row indexing used by td rules (no :has(thead) needed).
 */
.rl-statsmodels-summary table tbody tr:first-child > th,
.rl-statsmodels-summary table tbody tr:first-child > td {
  background-color: #2d2d30;
  color: #cccccc;
  font-weight: bold;
}
.rl-statsmodels-summary table tbody tr:nth-child(n+2):nth-child(odd) > :first-child {
  background-color: #252526;
  color: #d4d4d4;
  font-weight: 600;
}
.rl-statsmodels-summary table tbody tr:nth-child(n+2):nth-child(even) > :first-child {
  background-color: #1e1e1e;
  color: #d4d4d4;
  font-weight: 600;
}
.rl-statsmodels-summary table tbody tr:nth-child(n+2):hover > :first-child {
  background-color: #2a2d2e;
}
table tr:hover td,
table.dataframe tr:hover td,
.simpletable tr:hover td {
  background-color: #2a2d2e;
}
table tr:nth-child(even) td,
table.dataframe tr:nth-child(even) td,
.simpletable tr:nth-child(even) td {
  background-color: #1e1e1e;
}
table tr:nth-child(even):hover td,
table.dataframe tr:nth-child(even):hover td,
.simpletable tr:nth-child(even):hover td {
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
  text-align: left;
  vertical-align: top;
}
.stats-value {
  display: table-cell;
  padding: 4px 0;
  color: #d4d4d4;
  text-align: right;
  vertical-align: top;
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
/* Staged Cox: full-width block; title/note/table body left-aligned; equal width for stage columns */
.rl-staged-cox {
  display: block;
  width: 100%;
  margin-bottom: 20px;
  text-align: left;
}
.rl-staged-cox h4 {
  margin-top: 0;
  text-align: left;
}
.rl-staged-cox > table,
.rl-staged-cox table.simpletable,
.rl-staged-cox table.dataframe {
  table-layout: fixed;
  width: 100%;
  /* Match other simpletables: odd rows use table #252526; even rows use tr:nth-child(even) td #1e1e1e */
  background-color: #252526;
}
.rl-staged-cox table th,
.rl-staged-cox table td {
  overflow-wrap: anywhere;
  word-wrap: break-word;
}
.rl-staged-cox p.rl-note {
  text-align: left;
  max-width: none;
  margin-left: 0;
  margin-right: 0;
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

