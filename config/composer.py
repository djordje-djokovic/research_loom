"""Generic composable config loader."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import yaml

from research_loom.config.schema import ConfigError, validate_config_schema


def deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for key, value in override.items():
            out[key] = deep_merge(out[key], value) if key in out else value
        return out
    return override


def _read_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        payload = yaml.safe_load(f) or {}
    if not isinstance(payload, dict):
        raise ConfigError(f"Top-level YAML in {path} must be a mapping/dict")
    return payload


def _compose_tokens(expr: str) -> List[str]:
    parts = [p.strip() for p in str(expr).split("+")]
    tokens = [p for p in parts if p]
    if not tokens:
        raise ConfigError("Config expression is empty")
    return tokens


def _token_to_path(config_dir: Path, token: str, aliases: Dict[str, str]) -> Path:
    alias = aliases.get(token, token)
    if alias.endswith(".yaml"):
        if "/" in alias or "\\" in alias:
            return (config_dir / alias).resolve()
        return (config_dir / alias).resolve()
    if "/" in alias or "\\" in alias:
        return (config_dir / "overrides" / f"{alias}.yaml").resolve()
    return (config_dir / f"{alias}.yaml").resolve()


def _resolve_extends(config_dir: Path, file_path: Path, aliases: Dict[str, str], stack: Optional[List[Path]] = None) -> Dict[str, Any]:
    stack = stack or []
    file_path = file_path.resolve()
    if file_path in stack:
        cycle = " -> ".join(str(p) for p in stack + [file_path])
        raise ConfigError(f"Detected _extends cycle: {cycle}")

    payload = _read_yaml(file_path)
    parent_token = payload.pop("_extends", None)
    if parent_token is None:
        return payload
    if not isinstance(parent_token, str) or not parent_token.strip():
        raise ConfigError(f"_extends must be a non-empty string in {file_path}")

    parent_path = _token_to_path(config_dir, parent_token.strip(), aliases)
    parent_cfg = _resolve_extends(config_dir, parent_path, aliases, stack + [file_path])
    return deep_merge(parent_cfg, payload)


def load_composed_config(
    config_dir: Path,
    config_expr: str,
    *,
    aliases: Optional[Dict[str, str]] = None,
    allowed_top_level_keys: Optional[Iterable[str]] = None,
    required_top_level_keys: Optional[Iterable[str]] = None,
    section_type_map: Optional[Dict[str, type]] = None,
) -> Dict[str, Any]:
    alias_map = {"base": "base", "dev": "dev", "robustness": "robustness"}
    if aliases:
        alias_map.update(aliases)

    expanded_tokens: List[str] = []
    for token in _compose_tokens(config_expr):
        aliased = alias_map.get(token, token)
        token_list = _compose_tokens(aliased) if "+" in aliased else [aliased]
        expanded_tokens.extend(token_list)

    resolved: Dict[str, Any] = {}
    for token in expanded_tokens:
        file_path = _token_to_path(config_dir, token, alias_map)
        cfg = _resolve_extends(config_dir, file_path, alias_map)
        resolved = deep_merge(resolved, cfg)

    if allowed_top_level_keys is not None:
        validate_config_schema(
            resolved,
            allowed_top_level_keys=allowed_top_level_keys,
            required_top_level_keys=required_top_level_keys or [],
            section_type_map=section_type_map or {},
        )
    return resolved
