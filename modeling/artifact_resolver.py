"""Generic artifact-resolution helper."""

from __future__ import annotations

from typing import Any


def resolve_artifacts(value: Any, pipeline) -> Any:
    if isinstance(value, dict) and value.get("__artifact__"):
        return pipeline._load_artifact(value)  # noqa: SLF001
    if isinstance(value, dict):
        return {k: resolve_artifacts(v, pipeline) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_artifacts(v, pipeline) for v in value]
    if isinstance(value, tuple):
        return tuple(resolve_artifacts(v, pipeline) for v in value)
    return value
