"""Common gate check helpers."""

from __future__ import annotations

from typing import Any, Dict


def check_minimum(name: str, value: float, minimum: float, *, severity: str = "error") -> Dict[str, Any]:
    ok = value >= minimum
    return {
        "name": name,
        "severity": severity,
        "ok": ok,
        "message": None if ok else f"{name}={value} < minimum={minimum}",
        "metrics": {name: value, "minimum": minimum},
    }


def check_maximum(name: str, value: float, maximum: float, *, severity: str = "error") -> Dict[str, Any]:
    ok = value <= maximum
    return {
        "name": name,
        "severity": severity,
        "ok": ok,
        "message": None if ok else f"{name}={value} > maximum={maximum}",
        "metrics": {name: value, "maximum": maximum},
    }
