from pathlib import Path

import pytest

from pipeline.core import Node, ResearchPipeline


def _node_with_output(key, value):
    def _fn(inputs, config):
        return {key: value}
    return _fn


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_json_zst_roundtrip(tmp_path):
    pytest.importorskip("zstandard")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    payload = {"nested": {"a": 1, "b": [1, 2, 3]}, "txt": "hello"}
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("result", payload),
            inputs=[],
            config_section="n",
            storage_formats={"result": "json.zst"},
        )
    )
    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["result"]
    assert ptr["__artifact__"] is True
    assert ptr["format"] == "json.zst"
    loaded = p._load_artifact(ptr)
    assert loaded == payload


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_jsonl_zst_roundtrip(tmp_path):
    pytest.importorskip("zstandard")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    rows = [{"id": i, "v": f"x{i}"} for i in range(50)]
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("rows", rows),
            inputs=[],
            config_section="n",
            storage_formats={"rows": "jsonl.zst"},
        )
    )
    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["rows"]
    assert ptr["format"] == "jsonl.zst"
    loaded = p._load_artifact(ptr)
    assert loaded == rows


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_parquet_roundtrip(tmp_path):
    pd = pytest.importorskip("pandas")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    df = pd.DataFrame({"id": [1, 2, 3], "val": [10.0, 20.0, 30.0]})
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("df", df),
            inputs=[],
            config_section="n",
            storage_formats={"df": "parquet"},
        )
    )

    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["df"]
    assert ptr["format"] == "parquet"
    loaded = p._load_artifact(ptr)
    assert list(loaded.columns) == ["id", "val"]
    assert loaded.shape == (3, 2)


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_csv_roundtrip(tmp_path):
    pytest.importorskip("pandas")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    rows = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("rows", rows),
            inputs=[],
            config_section="n",
            storage_formats={"rows": "csv"},
        )
    )

    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["rows"]
    assert ptr["format"] == "csv"
    loaded = p._load_artifact(ptr)
    assert loaded.shape[0] == 2


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_plotly_html_roundtrip(tmp_path):
    go = pytest.importorskip("plotly.graph_objects")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    fig = go.Figure(data=go.Scatter(x=[1, 2], y=[3, 4]))
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("fig", fig),
            inputs=[],
            config_section="n",
            storage_formats={"fig": "html"},
        )
    )

    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["fig"]
    assert ptr["format"] == "html"
    html_path = Path(p._load_artifact(ptr))
    assert html_path.exists()
    assert html_path.suffix == ".html"


@pytest.mark.core
@pytest.mark.artifact
@pytest.mark.full_matrix
def test_matplotlib_png_roundtrip(tmp_path):
    plt = pytest.importorskip("matplotlib.pyplot")

    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot([1, 2], [2, 3])
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("fig", fig),
            inputs=[],
            config_section="n",
            storage_formats={"fig": "png"},
        )
    )

    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    ptr = out["n"]["fig"]
    assert ptr["format"] == "png"
    png_path = Path(p._load_artifact(ptr))
    assert png_path.exists()
    assert png_path.suffix == ".png"


@pytest.mark.core
@pytest.mark.artifact
def test_invalid_storage_format_falls_back_to_inline(tmp_path):
    p = ResearchPipeline(cache_dir=str(tmp_path / "cache"))
    p.add_node(
        Node(
            name="n",
            func=_node_with_output("x", {"a": 1}),
            inputs=[],
            config_section="n",
            storage_formats={"x": "not_a_real_format"},
        )
    )

    cfg = {"n": {}}
    out = p.run_pipeline(cfg, materialize=["n"])
    assert out["n"]["x"] == {"a": 1}
