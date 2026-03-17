import pandas as pd
import pytest

from research_loom.config.composer import load_composed_config
from research_loom.gates.runner import run_gate_checks
from research_loom.matrix.executor import run_spec_matrix
from research_loom.matrix.reporting import summarize_matrix_results
from research_loom.modeling.artifact_resolver import resolve_artifacts
from research_loom.modeling.result_builders import (
    build_model_result,
    build_sample_metrics,
    empty_sample_metrics,
    wrap_node_output,
)
from research_loom.provenance.capture import capture_run_provenance

pytestmark = pytest.mark.core


def test_build_sample_metrics_and_model_result_contract():
    df_in = pd.DataFrame(
        [{"company_id": "c1", "event": 0}, {"company_id": "c1", "event": 1}, {"company_id": "c2", "event": 0}]
    )
    df_model = df_in.iloc[:2].copy()
    sm = build_sample_metrics(df_in, df_model, company_col_candidates=("company_id",), event_col="event")
    result = build_model_result(
        status="completed",
        model_family="unit_test",
        sample_metrics=sm,
        n_observations=2,
        n_events=1,
        coefficients=[{"term": "x", "coef": 0.1}],
        metadata={"m": 1},
    )
    assert result["sample_metrics"]["n_rows_input"] == 3
    assert result["sample_metrics"]["n_rows_model"] == 2
    assert result["n_companies_used"] == 1
    wrapped = wrap_node_output("model_results", result, {"x": 1})
    assert "outputs" in wrapped and "model_results" in wrapped["outputs"]
    assert "config_hash" in wrapped["summary"]
    assert "timestamp" in wrapped["summary"]


def test_empty_sample_metrics_contract():
    sm = empty_sample_metrics(n_rows_input=5, n_companies_input=3)
    assert sm["n_rows_model"] == 0
    assert sm["n_companies_model"] is None


def test_artifact_resolver_recurses_nested_values():
    class P:
        def _load_artifact(self, ptr):
            return {"loaded": ptr.get("key")}

    payload = {"a": {"__artifact__": True, "key": "x"}, "b": [{"__artifact__": True, "key": "y"}]}
    out = resolve_artifacts(payload, P())
    assert out["a"]["loaded"] == "x"
    assert out["b"][0]["loaded"] == "y"


def test_config_composer_supports_composition(tmp_path):
    config_dir = tmp_path / "config"
    (config_dir / "overrides" / "sample").mkdir(parents=True)
    (config_dir / "base.yaml").write_text("a: 1\nsection:\n  x: 1\n", encoding="utf-8")
    (config_dir / "overrides" / "sample" / "ai.yaml").write_text("section:\n  y: 2\n", encoding="utf-8")
    out = load_composed_config(config_dir, "base+sample/ai")
    assert out["a"] == 1
    assert out["section"]["x"] == 1
    assert out["section"]["y"] == 2


def test_gate_runner_policy_behavior():
    checks = [
        lambda payload, _ctx: {"name": "fatal", "ok": payload["ok"], "severity": "error", "message": "fatal"},
        lambda _payload, _ctx: {"name": "warn", "ok": False, "severity": "warn", "message": "warn"},
    ]
    exploratory = run_gate_checks({"ok": False}, checks, policy="exploratory")
    confirmatory = run_gate_checks({"ok": False}, checks, policy="confirmatory")
    assert exploratory["status"] == "warn"
    assert confirmatory["status"] == "failed"


def test_matrix_and_provenance_helpers():
    rows = run_spec_matrix(specs=[{"x": 1}, {"x": 2}], run_one=lambda spec: {"y": spec["x"] * 2})
    summary = summarize_matrix_results(rows)
    assert summary["n_total"] == 2
    assert summary["n_ok"] == 2
    prov = capture_run_provenance(repo_root=".", config={"a": 1}, seeds={"s": 1}, input_paths=[])
    assert "config_hash" in prov
