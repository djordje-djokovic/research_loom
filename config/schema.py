"""Config schema validation helpers."""

from __future__ import annotations

from typing import Dict, Iterable


class ConfigError(ValueError):
    """Raised when config loading/validation fails."""


def validate_config_schema(
    config: Dict,
    *,
    allowed_top_level_keys: Iterable[str],
    required_top_level_keys: Iterable[str],
    section_type_map: Dict[str, type],
) -> None:
    allowed = set(allowed_top_level_keys)
    required = set(required_top_level_keys)
    keys = set(config.keys())

    unknown = sorted(keys - allowed)
    if unknown:
        raise ConfigError(f"Unknown top-level config keys: {unknown}")

    missing = sorted(required - keys)
    if missing:
        raise ConfigError(f"Missing required top-level config keys: {missing}")

    for section, expected in section_type_map.items():
        if section in config and not isinstance(config[section], expected):
            raise ConfigError(
                f"Invalid type for top-level section '{section}': expected {expected.__name__}, got {type(config[section]).__name__}"
            )
