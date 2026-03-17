"""Generic builders for canonical model outputs."""

from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from research_loom.modeling.contracts import CanonicalModelResult, CoefficientRow, ModelStatus, SampleMetrics


def build_sample_metrics(
    df_input: pd.DataFrame,
    df_model: pd.DataFrame,
    *,
    company_col_candidates: Iterable[str] = ("company_id", "cluster_id"),
    event_col: Optional[str] = "event",
) -> SampleMetrics:
    company_col: Optional[str] = None
    for col in company_col_candidates:
        if col in df_input.columns:
            company_col = col
            break
    n_companies_input = int(df_input[company_col].nunique()) if company_col and not df_input.empty else None
    n_companies_model = int(df_model[company_col].nunique()) if company_col and not df_model.empty else None
    n_events_model: Optional[int] = None
    if event_col and event_col in df_model.columns:
        n_events_model = int(pd.to_numeric(df_model[event_col], errors="coerce").fillna(0).sum())
    return {
        "n_rows_input": int(len(df_input)),
        "n_companies_input": n_companies_input,
        "n_rows_model": int(len(df_model)),
        "n_companies_model": n_companies_model,
        "n_events_model": n_events_model,
    }


def empty_sample_metrics(*, n_rows_input: int = 0, n_companies_input: Optional[int] = None) -> SampleMetrics:
    return {
        "n_rows_input": int(n_rows_input),
        "n_companies_input": n_companies_input,
        "n_rows_model": 0,
        "n_companies_model": None,
        "n_events_model": None,
    }


def normalize_coefficients(rows: Optional[List[Dict[str, Any]]]) -> List[CoefficientRow]:
    out: List[CoefficientRow] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        out.append(
            {
                "term": str(row.get("term", "")),
                "coef": row.get("coef"),
                "se": row.get("se"),
                "z": row.get("z"),
                "p_value": row.get("p_value"),
                "effect": row.get("effect", row.get("hr")),
                "ci_low": row.get("ci_low", row.get("hr_ci_low")),
                "ci_high": row.get("ci_high", row.get("hr_ci_high")),
                "hr": row.get("hr"),
                "hr_ci_low": row.get("hr_ci_low"),
                "hr_ci_high": row.get("hr_ci_high"),
            }
        )
    return out


def build_model_result(
    *,
    status: ModelStatus,
    model_family: str,
    sample_metrics: SampleMetrics,
    error: Optional[str] = None,
    n_observations: Optional[int] = None,
    n_events: Optional[int] = None,
    n_companies_used: Optional[int] = None,
    coefficients: Optional[List[Dict[str, Any]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> CanonicalModelResult:
    n_obs = int(sample_metrics["n_rows_model"] if n_observations is None else n_observations)
    return {
        "status": status,
        "error": error,
        "model_family": model_family,
        "n_observations": n_obs,
        "n_events": n_events,
        "n_companies_used": sample_metrics.get("n_companies_model") if n_companies_used is None else n_companies_used,
        "sample_metrics": sample_metrics,
        "coefficients": normalize_coefficients(coefficients),
        "metadata": metadata or {},
    }


def wrap_node_output(result_key: str, result_payload: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    return {
        result_key: result_payload,
        "config_hash": hashlib.md5(str(config).encode()).hexdigest()[:8],
        "timestamp": time.time(),
    }
