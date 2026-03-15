import json
from pathlib import Path

import pytest

from pipeline.core import Node, ResearchPipeline


def _source(inputs, config):
    return {"records": [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]}


def _sink(inputs, config):
    # Preserve artifacts from source in result lineage while adding a new artifact.
    source_rows = inputs["source"]["records"]
    return {
        "source_records": source_rows,
        "summary": {"count": len(source_rows)},
    }


@pytest.mark.core
@pytest.mark.integration
def test_export_node_with_and_without_ancestors(tmp_path):
    pytest.importorskip("zstandard")

    cache_dir = tmp_path / "cache"
    p = ResearchPipeline(cache_dir=str(cache_dir))
    p.add_node(
        Node(
            name="source",
            func=_source,
            inputs=[],
            config_section="source",
            storage_formats={"records": "json.zst"},
        )
    )
    p.add_node(
        Node(
            name="sink",
            func=_sink,
            inputs=["source"],
            config_section="sink",
            storage_formats={"summary": "json"},
            materialize_by_default=True,
        )
    )

    cfg = {"source": {}, "sink": {}}
    p.run_pipeline(cfg, materialize=["sink"])

    out_without = tmp_path / "export_without_anc"
    res_without = p.export_node(
        "sink",
        cfg,
        str(out_without),
        include_ancestors=False,
        keep_tree=True,
        link=False,
    )
    idx_without = json.loads(Path(res_without["index"]).read_text(encoding="utf-8"))
    assert len(idx_without["items"]) >= 1

    out_with = tmp_path / "export_with_anc"
    res_with = p.export_node(
        "sink",
        cfg,
        str(out_with),
        include_ancestors=True,
        keep_tree=True,
        link=False,
    )
    idx_with = json.loads(Path(res_with["index"]).read_text(encoding="utf-8"))
    prov_with = json.loads(Path(res_with["provenance"]).read_text(encoding="utf-8"))

    assert len(idx_with["items"]) >= len(idx_without["items"])
    assert "entries" in prov_with
    assert any(entry["node"] == "sink" for entry in prov_with["entries"])
    assert any(entry["node"] == "source" for entry in prov_with["entries"])


@pytest.mark.core
@pytest.mark.integration
def test_export_keep_tree_false_flattens_paths(tmp_path):
    pytest.importorskip("zstandard")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    p.add_node(
        Node(
            name="source",
            func=_source,
            inputs=[],
            config_section="source",
            storage_formats={"records": "json.zst"},
        )
    )
    cfg = {"source": {}}
    p.run_pipeline(cfg, materialize=["source"])

    out = tmp_path / "flat_export"
    res = p.export_node(
        "source",
        cfg,
        str(out),
        include_ancestors=False,
        keep_tree=False,
        link=False,
    )
    idx = json.loads(Path(res["index"]).read_text(encoding="utf-8"))

    assert idx["items"], "Expected at least one exported artifact"
    for item in idx["items"]:
        # Flattened mode should not preserve nested cache tree under export root.
        rel = Path(item["dest"]).relative_to(out)
        assert len(rel.parts) == 1


@pytest.mark.core
@pytest.mark.integration
def test_export_missing_cache_raises_file_not_found(tmp_path):
    p = ResearchPipeline(cache_dir=str(tmp_path / "cache_missing"))
    p.add_node(Node(name="source", func=_source, inputs=[], config_section="source"))
    cfg = {"source": {}}

    with pytest.raises(FileNotFoundError):
        p.export_node("source", cfg, str(tmp_path / "export_missing"))


@pytest.mark.core
@pytest.mark.integration
def test_provenance_metadata_contains_expected_shape(tmp_path):
    pytest.importorskip("zstandard")

    def redact(section, cfg):
        return {"section": section, "keys": sorted((cfg or {}).keys())}

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache_prov"), redact_config=redact)
    p.add_node(
        Node(
            name="source",
            func=_source,
            inputs=[],
            config_section="source",
            storage_formats={"records": "json.zst"},
        )
    )
    p.add_node(
        Node(
            name="sink",
            func=_sink,
            inputs=["source"],
            config_section="sink",
            storage_formats={"summary": "json"},
        )
    )

    cfg = {"source": {"token": "abc", "param": 1}, "sink": {"mode": "x"}}
    p.run_pipeline(cfg, materialize=["sink"])

    sink_key = p.get_node_cache_key(cfg, "sink")
    cache_json = p.get_cache_path("sink", sink_key)
    cache_payload = json.loads(cache_json.read_text(encoding="utf-8"))
    meta = cache_payload["_metadata"]
    assert "input_keys" in meta
    assert "ancestor_keys" in meta
    assert "section_hashes" in meta
    assert "effective_configs" in meta
    assert meta["effective_configs"]["source"] == {"section": "source", "keys": ["param", "token"]}
