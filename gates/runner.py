"""Policy-aware gate runner for study-specific checks."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List


CheckFn = Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]]


def _normalize_policy(policy: str) -> str:
    p = (policy or "exploratory").strip().lower()
    if p == "strict":
        return "confirmatory"
    if p == "warn":
        return "exploratory"
    if p not in {"exploratory", "confirmatory"}:
        raise ValueError(f"Unsupported gate policy: {policy}")
    return p


def run_gate_checks(
    payload: Dict[str, Any],
    checks: Iterable[CheckFn],
    *,
    policy: str = "exploratory",
    context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_policy = _normalize_policy(policy)
    ctx = context or {}
    results: List[Dict[str, Any]] = []
    warnings: List[str] = []
    failures: List[str] = []

    for check in checks:
        row = check(payload, ctx) or {}
        ok = bool(row.get("ok", True))
        severity = str(row.get("severity", "error")).lower()
        if not ok:
            message = row.get("message") or f"Gate failed: {row.get('name', check.__name__)}"
            if normalized_policy == "exploratory" or severity == "warn":
                warnings.append(message)
            else:
                failures.append(message)
        results.append(
            {
                "name": row.get("name", check.__name__),
                "ok": ok,
                "severity": severity,
                "message": row.get("message"),
                "metrics": row.get("metrics", {}),
            }
        )

    return {
        "policy": normalized_policy,
        "status": "failed" if failures else ("warn" if warnings else "passed"),
        "warnings": warnings,
        "failures": failures,
        "checks": results,
    }
