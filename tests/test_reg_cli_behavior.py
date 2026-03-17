import sys

import pytest

from cli import handle_invalidation, handle_view, normalize_materialize, run_cli
from pipeline.core import Node, ResearchPipeline


class _FakePipeline:
    def __init__(self):
        self.nodes = {"a": object(), "b": object(), "c": object()}
        self.calls = []

    def invalidate_node(self, config, node_name):
        self.calls.append(("invalidate_node", node_name))
        return True

    def invalidate_upstream(self, config, node_name):
        self.calls.append(("invalidate_upstream", node_name))

    def invalidate_downstream(self, config, node_name):
        self.calls.append(("invalidate_downstream", node_name))

    def view_cached_node(self, config, node_name, open_browser=True, config_name=None):
        self.calls.append(("view_cached_node", node_name, open_browser, config_name))

    def get_all_downstream_nodes(self, node_name):
        return ["c"] if node_name == "b" else []

    def get_dependencies(self, node_name):
        return ["a"] if node_name == "b" else []


@pytest.mark.cli
@pytest.mark.smoke
def test_normalize_materialize_behavior():
    assert normalize_materialize(["results"]) == "none"
    assert normalize_materialize([]) == "none"
    assert normalize_materialize(["all"]) == "all"
    assert normalize_materialize(["node_a"]) == ["node_a"]


@pytest.mark.cli
def test_handle_invalidation_dispatches_expected_methods():
    p = _FakePipeline()
    cfg = {}

    handle_invalidation(
        p,
        cfg,
        force_downstream_nodes=["b"],
        force_upstream_nodes=["b"],
        force_all=False,
        force_only_nodes=["a"],
        config_name="base",
    )
    assert ("invalidate_downstream", "b") in p.calls
    assert ("invalidate_upstream", "b") in p.calls
    assert ("invalidate_node", "a") in p.calls


@pytest.mark.cli
def test_handle_view_calls_view_cached_node():
    p = _FakePipeline()
    handle_view(p, {}, ["a"], open_browser=False, config_name="dev")
    assert ("view_cached_node", "a", False, "dev") in p.calls


@pytest.mark.cli
def test_handle_invalidation_unknown_nodes_warn_and_skip(capfd):
    p = _FakePipeline()
    cfg = {}

    handle_invalidation(
        p,
        cfg,
        force_downstream_nodes=["missing_x"],
        force_upstream_nodes=["missing_y"],
        force_all=False,
        force_only_nodes=["missing_z"],
        config_name="base",
    )
    out = capfd.readouterr().out
    assert "Warning: Node 'missing_x' not found, skipping" in out
    assert "Warning: Node 'missing_y' not found, skipping" in out
    assert "Warning: Node 'missing_z' not found, skipping" in out
    assert not p.calls


@pytest.mark.cli
@pytest.mark.smoke
def test_run_cli_default_materialize_passes_none(monkeypatch, tmp_path):
    calls = {"materialize": None}

    class _CapturePipeline:
        def __init__(self):
            self.nodes = {"a": object()}

        def run_pipeline(self, config, materialize="none"):
            calls["materialize"] = materialize
            return {}

    def create_pipeline():
        return _CapturePipeline()

    def load_config(name):
        assert name == "base"
        return {"a": {}}

    def run_study(config_name, materialize):
        return ({}, create_pipeline(), {"a": {}})

    monkeypatch.setattr(sys, "argv", ["prog"])
    run_cli(create_pipeline, load_config, run_study)

    assert calls["materialize"] == "none"


@pytest.mark.cli
def test_run_cli_view_mode_exits(monkeypatch, tmp_path):
    events = {}

    def create_pipeline():
        rp = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
        rp.add_node(Node("a", lambda i, c: {"v": 1}, [], "a", materialize_by_default=True))
        # Ensure view handler can call method safely without real cache.
        def _view_cached_node(config, node_name, open_browser=True, config_name=None):
            events["viewed"] = (node_name, open_browser, config_name)
            return None

        rp.view_cached_node = _view_cached_node
        return rp

    def load_config(name):
        return {"a": {}}

    def run_study(config_name, materialize):
        raise AssertionError("run_study should not be called in --view mode")

    monkeypatch.setattr(sys, "argv", ["prog", "--view", "a", "--config", "base"])
    with pytest.raises(SystemExit) as exc:
        run_cli(create_pipeline, load_config, run_study)
    assert exc.value.code == 0
    assert events["viewed"] == ("a", True, "base")


@pytest.mark.cli
def test_run_cli_view_unknown_node_exits_without_run_study(monkeypatch, capfd):
    called = {"run": False}

    def create_pipeline():
        return _FakePipeline()

    def load_config(name):
        return {}

    def run_study(config_name, materialize):
        called["run"] = True
        return ({}, None, {})

    monkeypatch.setattr(sys, "argv", ["prog", "--view", "missing_node"])
    with pytest.raises(SystemExit) as exc:
        run_cli(create_pipeline, load_config, run_study)
    assert exc.value.code == 0
    assert called["run"] is False
    out = capfd.readouterr().out
    assert "Warning: Node 'missing_node' not found, skipping" in out


@pytest.mark.cli
def test_run_cli_merges_invalidated_nodes_into_materialize(monkeypatch, tmp_path):
    calls = {}

    class _CapturePipeline(_FakePipeline):
        def run_pipeline(self, config, materialize="none"):
            calls["materialize"] = materialize
            return {}

    def create_pipeline():
        return _CapturePipeline()

    def load_config(name):
        return {"a": {}, "b": {}, "c": {}}

    def run_study(config_name, materialize):
        return ({}, create_pipeline(), {})

    monkeypatch.setattr(
        sys,
        "argv",
        ["prog", "--force-downstream", "b", "--materialize", "a"],
    )
    run_cli(create_pipeline, load_config, run_study)

    # Requested "a" should remain, and invalidated set should be merged in.
    assert "a" in calls["materialize"]
    assert "b" in calls["materialize"]
    assert "c" in calls["materialize"]
