"""Matrix summary/reporting helpers."""

from __future__ import annotations

from typing import Any, Dict, Iterable


def summarize_matrix_results(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    total = 0
    ok = 0
    error = 0
    for row in rows:
        total += 1
        if row.get("status") == "ok":
            ok += 1
        else:
            error += 1
    return {"n_total": total, "n_ok": ok, "n_error": error}
