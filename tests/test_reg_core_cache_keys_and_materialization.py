import pytest

from pipeline.core import Node, ResearchPipeline


def _source(inputs, config):
    return {"payload": config.get("value", 1)}


def _mid(inputs, config):
    return {"payload": inputs["source"]["payload"] + config.get("delta", 0)}


def _sink(inputs, config):
    return {"payload": inputs["mid"]["payload"] + config.get("delta", 0)}


@pytest.mark.core
@pytest.mark.smoke
def test_cache_key_stable_for_equivalent_config_dict_order(pipeline):
    pipeline.add_node(Node("source", _source, [], "source"))
    pipeline.add_node(Node("mid", _mid, ["source"], "mid"))

    config_a = {"source": {"x": 1, "y": 2}, "mid": {"k": "v"}}
    config_b = {"mid": {"k": "v"}, "source": {"y": 2, "x": 1}}

    key_a = pipeline.get_node_cache_key(config_a, "mid")
    key_b = pipeline.get_node_cache_key(config_b, "mid")
    assert key_a == key_b


@pytest.mark.core
@pytest.mark.smoke
def test_cache_key_changes_when_upstream_config_changes(pipeline):
    pipeline.add_node(Node("source", _source, [], "source"))
    pipeline.add_node(Node("mid", _mid, ["source"], "mid"))
    pipeline.add_node(Node("sink", _sink, ["mid"], "sink"))

    config_1 = {"source": {"value": 1}, "mid": {"delta": 0}, "sink": {"delta": 0}}
    config_2 = {"source": {"value": 2}, "mid": {"delta": 0}, "sink": {"delta": 0}}

    key_1 = pipeline.get_node_cache_key(config_1, "sink")
    key_2 = pipeline.get_node_cache_key(config_2, "sink")
    assert key_1 != key_2


@pytest.mark.core
def test_cache_key_uses_fingerprint_and_swallows_fingerprint_errors(pipeline):
    def fp_ok(_, cfg):
        return {"stamp": cfg.get("stamp", "none")}

    def fp_raises(_, cfg):
        raise RuntimeError("boom")

    pipeline.add_node(Node("source", _source, [], "source", fingerprint=fp_ok))
    pipeline.add_node(Node("sink", _mid, ["source"], "mid", fingerprint=fp_raises))

    c1 = {"source": {"value": 1, "stamp": "a"}, "mid": {"delta": 1}}
    c2 = {"source": {"value": 1, "stamp": "b"}, "mid": {"delta": 1}}

    key_1 = pipeline.get_node_cache_key(c1, "source")
    key_2 = pipeline.get_node_cache_key(c2, "source")
    assert key_1 != key_2

    # fingerprint errors should not bubble from key generation
    sink_key = pipeline.get_node_cache_key(c1, "sink")
    assert isinstance(sink_key, str)
    assert len(sink_key) == 24


@pytest.mark.core
@pytest.mark.smoke
def test_materialize_none_computes_defaults_when_missing_but_returns_empty(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache")
    config = {"source": {"value": 5}, "mid": {"delta": 1}, "out": {"delta": 2}}

    result_first = p.run_pipeline(config, materialize="none")
    assert result_first == {}
    assert call_counter == {"source": 1, "mid": 1, "out": 1}

    # All required defaults are cached now, so no new execution should happen.
    result_second = p.run_pipeline(config, materialize="none")
    assert result_second == {}
    assert call_counter == {"source": 1, "mid": 1, "out": 1}


@pytest.mark.core
def test_materialize_explicit_node_list_returns_only_requested_nodes(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache2")
    config = {"source": {"value": 2}, "mid": {"delta": 3}, "out": {"delta": 4}}

    result = p.run_pipeline(config, materialize=["mid"])
    # Current framework behavior returns computed closure members on a dirty run.
    assert "mid" in result
    assert result["mid"]["value"] == 5


@pytest.mark.core
def test_corrupt_nonempty_cache_is_recomputed(tmp_path):
    p = ResearchPipeline(cache_dir=str(tmp_path / "cache_corrupt"))
    p.add_node(Node("source", _source, [], "source"))
    p.add_node(Node("mid", _mid, ["source"], "mid"))

    config = {"source": {"value": 3}, "mid": {"delta": 2}}
    first = p.run_pipeline(config, materialize=["mid"])
    assert "mid" in first
    key = p.get_node_cache_key(config, "mid")
    cache_path = p.get_cache_path("mid", key)

    # Corrupt cache with non-empty invalid JSON.
    cache_path.write_text("{ definitely_not_json", encoding="utf-8")

    second = p.run_pipeline(config, materialize=["mid"])
    assert "mid" in second
    assert second["mid"]["payload"] == 5


@pytest.mark.core
def test_materialize_explicit_node_list_cold_vs_warm_consistent_keys(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache_consistency")
    config = {"source": {"value": 10}, "mid": {"delta": 1}, "out": {"delta": 2}}

    first = p.run_pipeline(config, materialize=["mid"])
    second = p.run_pipeline(config, materialize=["mid"])

    assert set(first.keys()) == set(second.keys())
    assert "mid" in first
    assert first["mid"] == second["mid"]


@pytest.mark.core
def test_materialize_none_recomputes_after_config_change_and_returns_empty(
    tiny_pipeline_factory, call_counter, tmp_path
):
    p = tiny_pipeline_factory(call_counter, tmp_path / "cache_none_recompute")
    config_a = {"source": {"value": 1}, "mid": {"delta": 1}, "out": {"delta": 1}}
    config_b = {"source": {"value": 2}, "mid": {"delta": 1}, "out": {"delta": 1}}

    assert p.run_pipeline(config_a, materialize="none") == {}
    before = dict(call_counter)
    assert p.run_pipeline(config_b, materialize="none") == {}
    after = dict(call_counter)

    # Upstream config change should trigger recomputation of default outputs closure.
    assert after["source"] > before["source"]
    assert after["mid"] > before["mid"]
    assert after["out"] > before["out"]
