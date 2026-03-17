"""Generic staged matrix executor."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping


def run_spec_matrix(
    *,
    specs: Iterable[Mapping[str, Any]],
    run_one: Callable[[Mapping[str, Any]], Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for spec in specs:
        try:
            result = dict(run_one(spec))
            row = {"status": "ok", "spec": dict(spec), "result": result}
        except Exception as exc:
            row = {"status": "error", "spec": dict(spec), "error": str(exc)}
        rows.append(row)
    return rows
