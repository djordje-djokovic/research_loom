"""Canonical model-output contracts for study nodes."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, TypedDict


ModelStatus = Literal[
    "completed",
    "completed_fallback",
    "empty",
    "failed",
    "missing_rpy2",
    "missing_r_packages",
]


class SampleMetrics(TypedDict):
    n_rows_input: int
    n_companies_input: Optional[int]
    n_rows_model: int
    n_companies_model: Optional[int]
    n_events_model: Optional[int]


class CoefficientRow(TypedDict, total=False):
    term: str
    coef: float
    se: Optional[float]
    z: Optional[float]
    p_value: Optional[float]
    effect: Optional[float]
    ci_low: Optional[float]
    ci_high: Optional[float]
    hr: Optional[float]
    hr_ci_low: Optional[float]
    hr_ci_high: Optional[float]


class CanonicalModelResult(TypedDict):
    status: ModelStatus
    error: Optional[str]
    model_family: str
    n_observations: int
    n_events: Optional[int]
    n_companies_used: Optional[int]
    sample_metrics: SampleMetrics
    coefficients: List[CoefficientRow]
    metadata: Dict[str, Any]
