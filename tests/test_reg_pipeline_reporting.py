import argparse
import json
from pathlib import Path

import pytest

from research_loom.cli import apply_pipeline_report_overrides
from research_loom.pipeline.core import Node, ResearchPipeline


pytestmark = pytest.mark.core


def _build_pipeline(cache_dir: Path) -> ResearchPipeline:
    p = ResearchPipeline(cache_dir=str(cache_dir))
    p.add_node(Node(name="source", func=lambda _ins, _cfg: {"items": [{"x": 1}, {"x": 2}]}, inputs=[], config_section="data"))
    p.add_node(
        Node(
            name="sink",
            func=lambda ins, _cfg: {"n": len(ins["source"]["items"])},
            inputs=["source"],
            config_section="model",
        )
    )
    return p


def _strict_cfg() -> dict:
    return {
        "data": {},
        "model": {},
        "logging": {
            "pipeline_report": {
                "enabled": True,
                "format": "both",
                "output_dir": "reports",
                "keep_last_n": 2,
                "include_edge_payloads": True,
                "write_latest_pointer": True,
            }
        },
    }


def test_strict_pipeline_report_contract_required(tmp_path):
    p = _build_pipeline(tmp_path / "cache")
    with pytest.raises(ValueError, match="pipeline_report"):
        p.run_pipeline({"data": {}, "model": {}, "logging": {}}, materialize=["sink"])


def test_pipeline_report_outputs_and_retention(tmp_path):
    p = _build_pipeline(tmp_path / "cache")
    cfg = _strict_cfg()

    for _ in range(3):
        p.run_pipeline(cfg, materialize=["sink"])

    report_dir = (tmp_path / "cache" / "reports").resolve()
    html_files = [p for p in report_dir.glob("*.html") if p.name != "latest.html"]
    json_files = [p for p in report_dir.glob("*.json") if p.name != "latest.json"]

    assert (report_dir / "latest.html").exists()
    assert (report_dir / "latest.json").exists()
    assert len(html_files) <= 2
    assert len(json_files) <= 2

    latest_html = (report_dir / "latest.html").read_text(encoding="utf-8")
    assert "<svg" in latest_html
    assert "source" in latest_html
    assert "sink" in latest_html
    assert "Refreshed" in latest_html

    latest_json = json.loads((report_dir / "latest.json").read_text(encoding="utf-8"))
    assert latest_json["nodes"]
    assert all(isinstance(node.get("refreshed"), bool) for node in latest_json["nodes"])


def test_cli_pipeline_report_overrides():
    cfg = _strict_cfg()
    args = argparse.Namespace(
        pipeline_report="off",
        pipeline_report_format="json",
        pipeline_report_dir="custom_reports",
        pipeline_report_keep_last=5,
    )
    out = apply_pipeline_report_overrides(cfg, args)
    report_cfg = out["logging"]["pipeline_report"]
    assert report_cfg["enabled"] is False
    assert report_cfg["format"] == "json"
    assert report_cfg["output_dir"] == "custom_reports"
    assert report_cfg["keep_last_n"] == 5
