"""Generic builders for canonical model outputs."""

from __future__ import annotations

import hashlib
import time
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from research_loom.modeling.contracts import CanonicalModelResult, CoefficientRow, ModelStatus, SampleMetrics
from research_loom.output.html_theme import build_html_document


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


def output_item(
    output_type: str,
    value: Any,
    *,
    storage: Optional[str] = None,
    preview: Optional[str] = None,
) -> Dict[str, Any]:
    item: Dict[str, Any] = {"type": output_type, "value": value}
    if storage is not None:
        item["storage"] = storage
    if preview is not None:
        item["preview"] = preview
    return item


def build_node_output(
    *,
    status: str,
    summary: Dict[str, Any],
    outputs: Dict[str, Dict[str, Any]],
    metadata: Optional[Dict[str, Any]] = None,
    artifacts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "status": status,
        "summary": summary,
        "outputs": outputs,
        "metadata": metadata or {},
    }
    if artifacts:
        payload["artifacts"] = artifacts
    return payload


def _ensure_html_document(title: str, html_fragment: str) -> str:
    fragment = (html_fragment or "").strip()
    lowered = fragment.lower()
    if "<html" in lowered and "<body" in lowered:
        return fragment
    return build_html_document(
        title=title,
        body_html=fragment,
        extra_css=".section { margin-top: 14px; }\n.section + .section { margin-top: 8px; }",
    )


def _build_model_report_html(result_key: str, result_payload: Dict[str, Any]) -> Optional[str]:
    metadata = result_payload.get("metadata", {}) if isinstance(result_payload, dict) else {}
    model_outputs = metadata.get("model_outputs") if isinstance(metadata, dict) else None
    if not isinstance(model_outputs, dict) or not model_outputs:
        return None
    sections: List[str] = []
    for label, content in model_outputs.items():
        if not isinstance(content, str) or not content.strip():
            continue
        sections.append(f"<section class='section'><h2>{label}</h2>{content}</section>")
    if not sections:
        return None
    title = f"{result_key} report"
    html = f"<h1>{title}</h1>{''.join(sections)}"
    return _ensure_html_document(title, html)


def wrap_node_output(result_key: str, result_payload: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
    model_status = str(result_payload.get("status", "completed"))
    envelope_status = "completed" if model_status in {"completed", "completed_fallback"} else "failed"
    outputs: Dict[str, Dict[str, Any]] = {
        result_key: output_item("json", result_payload, storage="json.zst", preview="model"),
    }
    report_html = _build_model_report_html(result_key, result_payload)
    if report_html:
        report_key = "report_html" if result_key == "result" else f"{result_key}_report_html"
        outputs[report_key] = output_item("html", report_html, storage="html", preview="report")
    return build_node_output(
        status=envelope_status,
        summary={
            "result_key": result_key,
            "model_status": model_status,
            "config_hash": hashlib.md5(str(config).encode()).hexdigest()[:8],
            "timestamp": time.time(),
        },
        outputs=outputs,
        metadata={"node_contract": "typed_output_envelope_v1"},
    )
