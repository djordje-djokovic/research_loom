"""Validation helpers for canonical modeling payloads."""

from __future__ import annotations

from typing import Any, Dict


REQUIRED_RESULT_KEYS = {
    "status",
    "error",
    "model_family",
    "sample_metrics",
    "n_observations",
    "n_events",
    "n_companies_used",
    "coefficients",
    "metadata",
}


def validate_canonical_result(payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ValueError("Model payload must be a dict")
    missing = sorted(REQUIRED_RESULT_KEYS - set(payload.keys()))
    if missing:
        raise ValueError(f"Model payload missing required keys: {missing}")
    sample = payload.get("sample_metrics")
    if not isinstance(sample, dict):
        raise ValueError("sample_metrics must be a dict")
    for key in ["n_rows_input", "n_companies_input", "n_rows_model", "n_companies_model", "n_events_model"]:
        if key not in sample:
            raise ValueError(f"sample_metrics missing required key: {key}")
