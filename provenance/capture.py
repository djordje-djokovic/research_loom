"""Capture reproducibility metadata for a run."""

from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping


def _safe_cmd(args: list[str], cwd: Path) -> str | None:
    try:
        out = subprocess.check_output(args, cwd=str(cwd), stderr=subprocess.DEVNULL, text=True)
        return out.strip() or None
    except Exception:
        return None


def _hash_json(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _file_fingerprint(path: Path) -> Dict[str, Any]:
    try:
        stat = path.stat()
        return {
            "path": str(path),
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        }
    except Exception:
        return {"path": str(path), "error": "unreadable"}


def capture_run_provenance(
    *,
    repo_root: str | Path,
    config: Mapping[str, Any],
    seeds: Mapping[str, Any] | None = None,
    input_paths: Iterable[str | Path] | None = None,
) -> Dict[str, Any]:
    root = Path(repo_root).resolve()
    seed_payload = dict(seeds or {})
    path_payload = [Path(p).resolve() for p in (input_paths or [])]

    commit = _safe_cmd(["git", "rev-parse", "HEAD"], root)
    status = _safe_cmd(["git", "status", "--porcelain"], root)
    package_list = _safe_cmd(["python", "-m", "pip", "freeze"], root)

    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(root),
        "git": {"commit": commit, "dirty": bool(status)},
        "runtime": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cwd": os.getcwd(),
        },
        "packages": package_list.splitlines() if package_list else [],
        "seeds": seed_payload,
        "config_hash": _hash_json(dict(config)),
        "input_fingerprints": [_file_fingerprint(p) for p in path_payload],
    }
