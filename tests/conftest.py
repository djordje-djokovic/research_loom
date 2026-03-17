from pathlib import Path
import sys
import types

import pytest

from pipeline.core import Node, ResearchPipeline
from modeling.result_builders import build_node_output, output_item


# Ensure imports resolve for tests run from repository root.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture()
def cache_dir(tmp_path) -> Path:
    return tmp_path / "cache"


@pytest.fixture()
def pipeline(cache_dir: Path) -> ResearchPipeline:
    return ResearchPipeline(cache_dir=str(cache_dir))


@pytest.fixture()
def call_counter() -> dict:
    return {}


def _counted_loader(counter: dict, name: str):
    def _fn(inputs, config):
        counter[name] = counter.get(name, 0) + 1
        return build_node_output(
            status="completed",
            summary={"status": "ok"},
            outputs={"value": output_item("json", config.get("value", 1))},
        )
    return _fn


def _counted_passthrough(counter: dict, name: str, input_key: str):
    def _fn(inputs, config):
        counter[name] = counter.get(name, 0) + 1
        return build_node_output(
            status="completed",
            summary={"status": "ok"},
            outputs={"value": output_item("json", inputs[input_key]["value"] + config.get("delta", 0))},
        )
    return _fn


@pytest.fixture()
def tiny_pipeline_factory():
    """
    Build a small deterministic pipeline:
      source -> mid -> out
    """
    def _factory(counter: dict, cache_dir: Path) -> ResearchPipeline:
        p = ResearchPipeline(cache_dir=str(cache_dir))
        p.add_node(
            Node(
                name="source",
                func=_counted_loader(counter, "source"),
                inputs=[],
                config_section="source",
            )
        )
        p.add_node(
            Node(
                name="mid",
                func=_counted_passthrough(counter, "mid", "source"),
                inputs=["source"],
                config_section="mid",
            )
        )
        p.add_node(
            Node(
                name="out",
                func=_counted_passthrough(counter, "out", "mid"),
                inputs=["mid"],
                config_section="out",
                materialize_by_default=True,
            )
        )
        return p

    return _factory


@pytest.fixture()
def optional_deps():
    """
    Report optional dependency availability in one place.
    """
    return {
        "pandas": bool(sys.modules.get("pandas")) or _can_import("pandas"),
        "pyarrow": bool(sys.modules.get("pyarrow")) or _can_import("pyarrow"),
        "zstandard": bool(sys.modules.get("zstandard")) or _can_import("zstandard"),
        "plotly": bool(sys.modules.get("plotly")) or _can_import("plotly"),
        "matplotlib": bool(sys.modules.get("matplotlib")) or _can_import("matplotlib"),
    }


def _can_import(module_name: str) -> bool:
    try:
        __import__(module_name)
        return True
    except Exception:
        return False
