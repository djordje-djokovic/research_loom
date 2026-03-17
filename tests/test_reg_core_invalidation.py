import pytest

from pipeline.core import Node, ResearchPipeline
from modeling.result_builders import build_node_output, output_item


def _source(inputs, config):
    return build_node_output(
        status="completed",
        summary={"status": "ok"},
        outputs={"value": output_item("json", config.get("v", 1))},
    )


def _add(input_name):
    def _fn(inputs, config):
        return build_node_output(
            status="completed",
            summary={"status": "ok"},
            outputs={"value": output_item("json", inputs[input_name]["value"] + config.get("d", 0))},
        )
    return _fn


def _build_pipeline(cache_dir):
    p = ResearchPipeline(cache_dir=str(cache_dir))
    p.add_node(Node("a", _source, [], "a"))
    p.add_node(Node("b", _add("a"), ["a"], "b"))
    p.add_node(Node("c", _add("b"), ["b"], "c"))
    p.add_node(Node("d", _add("b"), ["b"], "d"))
    return p


@pytest.mark.core
@pytest.mark.smoke
def test_invalidate_node_is_config_specific(tmp_path):
    p = _build_pipeline(tmp_path / "cache")

    cfg1 = {"a": {"v": 1}, "b": {"d": 1}, "c": {"d": 1}, "d": {"d": 1}}
    cfg2 = {"a": {"v": 99}, "b": {"d": 1}, "c": {"d": 1}, "d": {"d": 1}}

    p.run_pipeline(cfg1, materialize="all")
    p.run_pipeline(cfg2, materialize="all")

    key1_b = p.get_node_cache_key(cfg1, "b")
    key2_b = p.get_node_cache_key(cfg2, "b")
    assert (p.cache_dir / "b" / key1_b).exists()
    assert (p.cache_dir / "b" / key2_b).exists()

    deleted = p.invalidate_node(cfg1, "b")
    assert deleted is True
    assert not (p.cache_dir / "b" / key1_b).exists()
    assert (p.cache_dir / "b" / key2_b).exists()


@pytest.mark.core
def test_invalidate_upstream_removes_dependencies_and_target(tmp_path):
    p = _build_pipeline(tmp_path / "cache_up")
    cfg = {"a": {"v": 1}, "b": {"d": 1}, "c": {"d": 1}, "d": {"d": 1}}
    p.run_pipeline(cfg, materialize="all")

    key_a = p.get_node_cache_key(cfg, "a")
    key_b = p.get_node_cache_key(cfg, "b")
    key_c = p.get_node_cache_key(cfg, "c")
    key_d = p.get_node_cache_key(cfg, "d")

    p.invalidate_upstream(cfg, "c")

    assert not (p.cache_dir / "a" / key_a).exists()
    assert not (p.cache_dir / "b" / key_b).exists()
    assert not (p.cache_dir / "c" / key_c).exists()
    assert (p.cache_dir / "d" / key_d).exists()


@pytest.mark.core
def test_invalidate_downstream_removes_dependents_and_target(tmp_path):
    p = _build_pipeline(tmp_path / "cache_down")
    cfg = {"a": {"v": 1}, "b": {"d": 1}, "c": {"d": 1}, "d": {"d": 1}}
    p.run_pipeline(cfg, materialize="all")

    key_a = p.get_node_cache_key(cfg, "a")
    key_b = p.get_node_cache_key(cfg, "b")
    key_c = p.get_node_cache_key(cfg, "c")
    key_d = p.get_node_cache_key(cfg, "d")

    p.invalidate_downstream(cfg, "b")

    assert (p.cache_dir / "a" / key_a).exists()
    assert not (p.cache_dir / "b" / key_b).exists()
    assert not (p.cache_dir / "c" / key_c).exists()
    assert not (p.cache_dir / "d" / key_d).exists()


@pytest.mark.core
def test_invalidate_node_unknown_raises_clear_error(tmp_path):
    p = _build_pipeline(tmp_path / "cache_unknown")
    cfg = {"a": {"v": 1}, "b": {"d": 1}, "c": {"d": 1}, "d": {"d": 1}}

    with pytest.raises(ValueError, match="Node 'missing' not found"):
        p.invalidate_node(cfg, "missing")
