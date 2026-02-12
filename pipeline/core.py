#!/usr/bin/env python3
"""
Custom Research Pipeline Framework
A parsimonious implementation of all research pipeline requirements in one file.
"""

import hashlib
import json
import os
import time
import contextlib
import logging
import io
import re
from collections import deque
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Union, Sequence, Callable
from dataclasses import dataclass
from functools import lru_cache
import shutil
import yaml

# Optional imports for artifact handling
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

try:
    from matplotlib.figure import Figure as MPLFigure
    HAS_MPL = True
except ImportError:
    HAS_MPL = False

try:
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False

@dataclass
class Node:
    name: str
    func: Callable[[Dict[str, Any], Dict[str, Any]], Any]
    inputs: List[str]  # Input node names
    config_section: str  # Which config section affects this node
    version: str = "v1"  # Bump when changing node logic
    materialize_by_default: bool = False  # Mark real outputs
    fingerprint: Optional[Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, str]]] = None  # Optional extra key bits
    storage_formats: Optional[Dict[str, str]] = None  # Override storage formats for specific outputs

def _blake12(s: str) -> str:
    """Generate a 24-character blake2b hash (12 bytes = 24 hex chars)"""
    return hashlib.blake2b(s.encode(), digest_size=12).hexdigest()

class ResearchPipeline:
    def __init__(self, cache_dir: str = "cache", logger: Optional[logging.Logger] = None, 
                 redact_config: Optional[Callable[[str, Dict], Dict]] = None):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.nodes = {}
        self.dag = {}
        self.reverse_deps = {}  # Track what depends on each node
        self.logger = logger or self._setup_default_logger()
        self.redact_config = redact_config or (lambda sec, cfg: cfg)
        self._recent_cache_keys = set()  # Track recently used cache keys
        self._recent_queue = deque(maxlen=512)  # Bounded LRU for recent keys
    
    def _setup_default_logger(self) -> logging.Logger:
        """Setup default logger with pretty output"""
        logger = logging.getLogger(f"pipeline_{id(self)}")
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        logger.propagate = False  # Prevent double-printing
        return logger
    
    def _mark_recent(self, node_name: str, cache_key: str):
        """Mark a cache key as recently used with bounded LRU"""
        token = f"{node_name}_{cache_key}"
        if token in self._recent_cache_keys:
            # refresh recency
            try:
                self._recent_queue.remove(token)
            except ValueError:
                pass
            self._recent_queue.append(token)
            return
        # if full, pop *before* append so we know what was evicted
        if len(self._recent_queue) == self._recent_queue.maxlen:
            evicted = self._recent_queue.popleft()
            self._recent_cache_keys.discard(evicted)
        self._recent_queue.append(token)
        self._recent_cache_keys.add(token)
        
    def add_node(self, node: Node):
        """Add a node to the pipeline"""
        # Validate inputs (fail early)
        missing = [i for i in node.inputs if i not in self.nodes]
        if missing:
            raise ValueError(f"Unknown inputs for {node.name}: {missing}")
        
        self.nodes[node.name] = node
        self.dag[node.name] = {
            'inputs': node.inputs,
            'config_section': node.config_section
        }
        
        # Build reverse dependency map
        for input_name in node.inputs:
            if input_name not in self.reverse_deps:
                self.reverse_deps[input_name] = []
            self.reverse_deps[input_name].append(node.name)
        
        # Clear memoized graph queries since DAG structure changed
        self._section_closure.cache_clear()
        self.get_dependencies.cache_clear()
        
        # Early cycle detection
        try:
            self._get_execution_order()
        except ValueError as e:
            # Remove the node we just added to clean up state
            del self.nodes[node.name]
            del self.dag[node.name]
            for input_name in node.inputs:
                if input_name in self.reverse_deps:
                    self.reverse_deps[input_name].remove(node.name)
                    if not self.reverse_deps[input_name]:
                        del self.reverse_deps[input_name]
            raise e
    
    def get_config_hash(self, config: Dict, section: str) -> str:
        """Generate hash for specific config section"""
        section_config = config.get(section, {})
        config_str = json.dumps(section_config, sort_keys=True)
        return hashlib.md5(config_str.encode()).hexdigest()[:8]
    
    @lru_cache(maxsize=None)
    def _section_closure(self, node_name: str) -> Set[str]:
        """Get all config sections that affect this node (including upstream dependencies)"""
        sections = {self.nodes[node_name].config_section}
        for dep in self.nodes[node_name].inputs:
            if dep in self.nodes:
                sections |= self._section_closure(dep)
        return sections
    
    def get_node_cache_key(self, config: Dict, node_name: str) -> str:
        """Generate composite cache key including all upstream config sections, node version, and input provenance"""
        @lru_cache(None)
        def key_of(n: str) -> str:
            node = self.nodes[n]
            sections = sorted(self._section_closure(n))
            section_hashes = {sec: self.get_config_hash(config, sec) for sec in sections}
            input_keys = {i: key_of(i) for i in node.inputs if i in self.nodes}
            
            # Add fingerprint if provided (for external artifacts like file mtimes, env vars)
            # Note: fingerprints are called with empty inputs - they're for external state, not upstream values
            fingerprint_data = {}
            if node.fingerprint:
                try:
                    fingerprint_data = node.fingerprint({}, config.get(node.config_section, {}))
                except Exception:
                    pass  # Ignore fingerprint errors
            
            payload = {
                "ver": node.version, 
                "sections": section_hashes, 
                "inputs": input_keys,
                "fingerprint": fingerprint_data
            }
            return _blake12(json.dumps(payload, sort_keys=True))
        return key_of(node_name)
    
    def get_cache_path(self, node_name: str, cache_key: str) -> Path:
        """Get cache path for a node (nested layout)"""
        return self.cache_dir / node_name / cache_key / "cache.json"
    
    def get_artifacts_dir(self, node_name: str, cache_key: str) -> Path:
        """Get the artifacts directory for a node"""
        return self.cache_dir / node_name / cache_key / "artifacts"
    
    def _sanitize_key(self, s: str) -> str:
        """Sanitize key for use in filenames"""
        s = re.sub(r'[^A-Za-z0-9._-]+', '_', str(s)).strip('_')
        return s or "item"
    
    def _is_dataframe_or_arrow(self, val) -> bool:
        """Check if value is a pandas DataFrame or pyarrow Table"""
        if HAS_PANDAS and isinstance(val, pd.DataFrame):
            return True
        if HAS_PYARROW and isinstance(val, pa.Table):
            return True
        return False
    
    def _looks_like_uniform_rows(self, val) -> bool:
        """Check if value looks like uniform rows (list of dicts)"""
        if not isinstance(val, list) or len(val) < 100:  # threshold for parquet
            return False
        if not all(isinstance(item, dict) for item in val[:10]):  # sample check
            return False
        # Check if keys are mostly uniform
        sample_keys = [set(item.keys()) for item in val[:10]]
        if not all(keys == sample_keys[0] for keys in sample_keys):
            return False
        # Check if values are flat (not nested structures) - if nested, don't treat as tabular
        # Sample a few items to check for nested dicts/lists
        for item in val[:5]:  # Check first 5 items
            for key, value in item.items():
                # If any value is a dict or list, this is nested data, not tabular
                if isinstance(value, (dict, list)):
                    return False
        return True
    
    def _is_large_nested(self, val) -> bool:
        """Check if value is a large nested dict"""
        if not isinstance(val, dict):
            return False
        # Rough size estimate (8MB threshold)
        try:
            size = len(json.dumps(val, default=str))
            return size > 8 * 1024 * 1024  # 8MB
        except:
            return False
    
    def _is_big_object_list(self, val) -> bool:
        """Check if value is a big list of objects"""
        if not isinstance(val, list) or len(val) < 1000:
            return False
        # Check if it's a list of dicts/objects
        return all(isinstance(item, (dict, list)) for item in val[:10])
    
    def _artifact_ptr(self, fmt: str, cache_root: Path, path: Path, key: str, **hints) -> Dict:
        """Create an artifact pointer"""
        rel_path = path.relative_to(cache_root).as_posix()
        ptr = {
            "__artifact__": True,
            "format": fmt,
            "path": rel_path,
            "key": key
        }
        ptr.update(hints)
        return ptr
    
    def _spill_with_format(self, val: Any, artifacts_dir: Path, stem: str, key: str, format: str) -> Any:
        """Spill a value to artifacts with a specific format"""
        path = artifacts_dir / f"{stem}.{format}"
        
        try:
            if format == "parquet":
                if HAS_PANDAS and isinstance(val, pd.DataFrame):
                    val.to_parquet(path, index=False)
                elif HAS_PYARROW and isinstance(val, pa.Table):
                    pq.write_table(val, path)
                elif isinstance(val, list):  # list of dicts
                    if HAS_PANDAS:
                        df = pd.DataFrame(val)
                        df.to_parquet(path, index=False)
                    elif HAS_PYARROW:
                        table = pa.Table.from_pylist(val)
                        pq.write_table(table, path)
                    else:
                        raise ValueError("Parquet format requires pandas or pyarrow")
                else:
                    raise ValueError(f"Cannot convert {type(val)} to parquet")
                    
            elif format == "json.zst":
                if not HAS_ZSTD:
                    raise ValueError("JSON.zst format requires zstandard")
                self.logger.info(f"[PHASE: SAVE] Starting json.zst save for {key}")
                try:
                    # Test JSON serialization first with detailed error reporting
                    # Note: json is already imported at module level
                    self.logger.info(f"[PHASE: SAVE] Serializing {key} to JSON string...")
                    json_str = json.dumps(val, default=str)
                    json_bytes = json_str.encode('utf-8')
                    self.logger.info(f"[PHASE: SAVE] JSON serialization successful: {len(json_bytes)} bytes")
                except TypeError as e:
                    self.logger.error(f"[PHASE: SAVE] JSON serialization TypeError for {key}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Error type: {type(e).__name__}, args: {e.args}")
                    raise
                except ValueError as e:
                    self.logger.error(f"[PHASE: SAVE] JSON serialization ValueError for {key}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Error type: {type(e).__name__}, args: {e.args}")
                    raise
                except MemoryError as e:
                    self.logger.error(f"[PHASE: SAVE] MemoryError during JSON serialization for {key}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Data too large to serialize in memory. Size estimate: {len(str(val)) if hasattr(val, '__len__') else 'N/A'}")
                    raise
                except Exception as e:
                    self.logger.error(f"[PHASE: SAVE] JSON serialization error for {key}: {type(e).__name__}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Error args: {e.args}")
                    raise
                
                try:
                    self.logger.info(f"[PHASE: SAVE] Compressing and writing {key} to {path}...")
                    with open(path, 'wb') as f:
                        cctx = zstd.ZstdCompressor()
                        compressed = cctx.compress(json_bytes)
                        f.write(compressed)
                    self.logger.info(f"[PHASE: SAVE] Successfully saved {key} as json.zst: {len(compressed)} bytes compressed from {len(json_bytes)} bytes")
                except IOError as e:
                    self.logger.error(f"[PHASE: SAVE] IOError saving {key} as json.zst to {path}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Error type: {type(e).__name__}, errno: {getattr(e, 'errno', 'N/A')}")
                    raise
                except MemoryError as e:
                    self.logger.error(f"[PHASE: SAVE] MemoryError during compression/write for {key}: {e}")
                    raise
                except Exception as e:
                    self.logger.error(f"[PHASE: SAVE] Error compressing/writing {key} as json.zst: {type(e).__name__}: {e}")
                    self.logger.error(f"[PHASE: SAVE] Error args: {e.args}")
                    raise
                    
            elif format == "jsonl.zst":
                if not HAS_ZSTD:
                    raise ValueError("JSONL.zst format requires zstandard")
                if not isinstance(val, list):
                    raise ValueError("JSONL.zst format requires a list")
                self.logger.info(f"[PHASE: SAVE] Starting jsonl.zst save for {key} (streaming, {len(val)} items)")
                try:
                    cctx = zstd.ZstdCompressor()
                    item_count = 0
                    with open(path, 'wb') as f, cctx.stream_writer(f) as writer:
                        for item in val:
                            writer.write((json.dumps(item, default=str) + '\n').encode())
                            item_count += 1
                            if item_count % 1000 == 0:
                                self.logger.debug(f"[PHASE: SAVE] Written {item_count}/{len(val)} items to jsonl.zst")
                    self.logger.info(f"[PHASE: SAVE] Successfully saved {key} as jsonl.zst: {item_count} items")
                except MemoryError as e:
                    self.logger.error(f"[PHASE: SAVE] MemoryError during jsonl.zst streaming for {key}: {e}")
                    raise
                except Exception as e:
                    self.logger.error(f"[PHASE: SAVE] Error writing jsonl.zst for {key}: {type(e).__name__}: {e}")
                    raise
                        
            elif format == "json":
                with open(path, 'w') as f:
                    json.dump(val, f, default=str, indent=2)
                    
            elif format == "csv":
                if isinstance(val, list) and len(val) > 0:
                    import csv
                    with open(path, "w", newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=list(val[0].keys()))
                        writer.writeheader()
                        writer.writerows(val)
                elif HAS_PANDAS and isinstance(val, pd.DataFrame):
                    val.to_csv(path, index=False)
                else:
                    raise ValueError(f"Cannot convert {type(val)} to CSV")
                    
            elif format == "png":
                if HAS_MPL and isinstance(val, MPLFigure):
                    val.savefig(path, dpi=150, bbox_inches="tight")
                elif HAS_PLOTLY and isinstance(val, go.Figure):
                    val.write_image(str(path))
                else:
                    raise ValueError(f"Cannot convert {type(val)} to PNG")
                    
            elif format == "html":
                if HAS_PLOTLY and isinstance(val, go.Figure):
                    val.write_html(str(path))
                    # Inject dark background CSS and JavaScript for Plotly figures
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            html_content = f.read()
                        
                        # Add CSS in <head> to set dark background and remove margins
                        css_code = """
<style>
    html, body {
        background-color: #101010 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    body > div {
        background-color: #101010 !important;
    }
</style>
"""
                        # Inject CSS before </head>
                        if "</head>" in html_content:
                            html_content = html_content.replace("</head>", css_code + "</head>")
                        else:
                            # Fallback: inject at start of <body>
                            html_content = html_content.replace("<body>", "<head>" + css_code + "</head><body>")
                        
                        # Add JavaScript as fallback (runs after page load)
                        js_code = """
<script>
    document.body.style.backgroundColor = "#101010";
    if (document.body.parentElement) {
        document.body.parentElement.style.backgroundColor = "#101010";
    }
</script>
"""
                        html_content = html_content.replace("</body>", js_code + "</body>")
                        
                        with open(path, 'w', encoding='utf-8') as f:
                            f.write(html_content)
                    except Exception as e:
                        # If injection fails, continue anyway (HTML is still valid)
                        self.logger.warning(f"Could not inject dark background styles: {e}")
                else:
                    raise ValueError(f"Cannot convert {type(val)} to HTML")
                    
            else:
                raise ValueError(f"Unsupported storage format: {format}")
                
            return self._artifact_ptr(format, self.cache_dir, path, key, 
                                   n_rows=len(val) if hasattr(val, '__len__') else None)
                                   
        except Exception as e:
            # Fallback to inline storage if configured format fails
            error_type = type(e).__name__
            error_msg = str(e) if str(e) else "No error message"
            error_args = e.args if hasattr(e, 'args') else None
            self.logger.warning(f"Failed to save {key} as {format}: {error_type}: {error_msg}")
            if error_args:
                self.logger.warning(f"  Error args: {error_args}")
            
            # Safely get size info without causing MemoryError
            try:
                if isinstance(val, list):
                    size_info = f"list with {len(val)} items"
                elif hasattr(val, '__len__'):
                    # Try to get length, but don't convert to string
                    try:
                        size_info = f"{type(val).__name__} with length {len(val)}"
                    except Exception:
                        size_info = f"{type(val).__name__} (length unknown)"
                else:
                    size_info = f"{type(val).__name__} (no length)"
            except Exception:
                size_info = f"{type(val).__name__} (size unknown)"
            
            self.logger.warning(f"  Storing inline instead. Data type: {type(val).__name__}, {size_info}")
            return val
    
    def _maybe_spill(self, node_name: str, cache_key: str, key: str, val) -> Any:
        """Maybe spill a value to artifacts, return pointer or original value"""
        artifacts_dir = self.get_artifacts_dir(node_name, cache_key)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        
        stem = self._sanitize_key(key)
        
        # Check if there's a configured storage format for this output
        node = self.nodes[node_name]
        if node.storage_formats and key in node.storage_formats:
            return self._spill_with_format(val, artifacts_dir, stem, key, node.storage_formats[key])
        
        # Matplotlib Figure → PNG
        if HAS_MPL and isinstance(val, MPLFigure):
            path = artifacts_dir / f"{stem}.png"
            try:
                val.savefig(path, dpi=150, bbox_inches="tight")
                return self._artifact_ptr("png", self.cache_dir, path, key)
            except Exception:
                return val
        
        # Plotly Figure → PNG (fallback HTML)
        if HAS_PLOTLY and isinstance(val, go.Figure):
            path = artifacts_dir / f"{stem}.png"
            try:
                val.write_image(str(path))
                return self._artifact_ptr("png", self.cache_dir, path, key)
            except Exception:
                html_path = artifacts_dir / f"{stem}.html"
                val.write_html(str(html_path))
                # Inject dark background CSS and JavaScript for Plotly figures
                try:
                    with open(html_path, 'r', encoding='utf-8') as f:
                        html_content = f.read()
                    
                    # Add CSS in <head> to set dark background and remove margins
                    css_code = """
<style>
    html, body {
        background-color: #101010 !important;
        margin: 0 !important;
        padding: 0 !important;
    }
    body > div {
        background-color: #101010 !important;
    }
</style>
"""
                    # Inject CSS before </head>
                    if "</head>" in html_content:
                        html_content = html_content.replace("</head>", css_code + "</head>")
                    else:
                        # Fallback: inject at start of <body>
                        html_content = html_content.replace("<body>", "<head>" + css_code + "</head><body>")
                    
                    # Add JavaScript as fallback (runs after page load)
                    js_code = """
<script>
    document.body.style.backgroundColor = "#101010";
    if (document.body.parentElement) {
        document.body.parentElement.style.backgroundColor = "#101010";
    }
</script>
"""
                    html_content = html_content.replace("</body>", js_code + "</body>")
                    
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(html_content)
                except Exception as e:
                    # If injection fails, continue anyway (HTML is still valid)
                    self.logger.warning(f"Could not inject dark background styles: {e}")
                return self._artifact_ptr("html", self.cache_dir, html_path, key)
        
        # Tabular → Parquet (only for explicit DataFrames/Arrow, not auto-detected list-of-dicts)
        # Removed automatic conversion of list-of-dicts to avoid converting nested structures
        if self._is_dataframe_or_arrow(val):
            path = artifacts_dir / f"{stem}.parquet"
            try:
                if HAS_PANDAS and isinstance(val, pd.DataFrame):
                    val.to_parquet(path, index=False)
                elif HAS_PYARROW and isinstance(val, pa.Table):
                    pq.write_table(val, path)
                else:
                    return val
                
                return self._artifact_ptr("parquet", self.cache_dir, path, key, 
                                        n_rows=len(val) if hasattr(val, '__len__') else None)
            except Exception:
                # Fallback to CSV only for explicit DataFrames (not auto-converted lists)
                path = artifacts_dir / f"{stem}.csv"
                try:
                    if HAS_PANDAS and isinstance(val, pd.DataFrame):
                        val.to_csv(path, index=False)
                        return self._artifact_ptr("csv", self.cache_dir, path, key)
                    else:
                        # Don't auto-convert lists to CSV - return original value
                        return val
                except Exception:
                    return val  # fallback to inline
        
        # Large nested JSON → JSON.zst
        if self._is_large_nested(val):
            if not HAS_ZSTD:
                self.logger.debug(f"zstandard not available, storing {key} inline")
                return val  # fallback to inline
            
            path = artifacts_dir / f"{stem}.json.zst"
            try:
                import json
                # Test JSON serialization first
                try:
                    json_str = json.dumps(val, default=str)
                    json_bytes = json_str.encode('utf-8')
                    self.logger.debug(f"JSON serialization for {key}: {len(json_bytes)} bytes")
                except Exception as json_err:
                    self.logger.warning(f"JSON serialization failed for {key}: {type(json_err).__name__}: {json_err}")
                    return val  # fallback to inline
                
                # Compress and write
                try:
                    with open(path, 'wb') as f:
                        cctx = zstd.ZstdCompressor()
                        compressed = cctx.compress(json_bytes)
                        f.write(compressed)
                    self.logger.debug(f"Successfully saved {key} as json.zst: {len(compressed)} bytes compressed")
                    return self._artifact_ptr("json.zst", self.cache_dir, path, key)
                except IOError as io_err:
                    self.logger.warning(f"IOError saving {key} as json.zst to {path}: {io_err}")
                    return val  # fallback to inline
                except Exception as comp_err:
                    self.logger.warning(f"Compression/write error for {key}: {type(comp_err).__name__}: {comp_err}")
                    return val  # fallback to inline
            except Exception as e:
                self.logger.warning(f"Unexpected error saving {key} as json.zst: {type(e).__name__}: {e}")
                return val  # fallback to inline
        
        # Big object list → JSONL.zst (fixed streaming)
        if self._is_big_object_list(val):
            if not HAS_ZSTD:
                return val  # fallback to inline
            
            path = artifacts_dir / f"{stem}.jsonl.zst"
            try:
                cctx = zstd.ZstdCompressor()
                with open(path, 'wb') as f, cctx.stream_writer(f) as writer:
                    for item in val:
                        writer.write((json.dumps(item, default=str) + '\n').encode())
                return self._artifact_ptr("jsonl.zst", self.cache_dir, path, key)
            except Exception:
                return val  # fallback to inline
        
        # Keep inline
        return val
    
    def _load_artifact(self, ptr: Dict) -> Any:
        """Load an artifact from a pointer"""
        path = (self.cache_dir / ptr["path"]).resolve()
        fmt = ptr["format"]
        
        try:
            if fmt == "parquet":
                if HAS_PANDAS:
                    return pd.read_parquet(path)
                elif HAS_PYARROW:
                    return pq.read_table(path).to_pandas()
                else:
                    raise ValueError("No parquet support available")
            
            elif fmt == "csv":
                if not HAS_PANDAS:
                    raise ValueError("CSV load requires pandas in this framework")
                return pd.read_csv(path)
            
            elif fmt == "json.zst":
                if not HAS_ZSTD:
                    raise ValueError("zstandard not available")
                with open(path, 'rb') as f:
                    dctx = zstd.ZstdDecompressor()
                    decompressed = dctx.decompress(f.read())
                    return json.loads(decompressed.decode())
            
            elif fmt == "jsonl.zst":
                if not HAS_ZSTD:
                    raise ValueError("zstandard not available")
                dctx = zstd.ZstdDecompressor()
                def gen():
                    with open(path, 'rb') as f, dctx.stream_reader(f) as reader:
                        for line in io.TextIOWrapper(reader, encoding='utf-8'):
                            yield json.loads(line)
                return list(gen())  # or return gen() if you prefer lazy iteration
            
            elif fmt in ("png", "html"):
                return str(path)  # return path; caller can open/view
            
            else:
                raise ValueError(f"Unknown artifact format: {fmt}")
        
        except Exception as e:
            self.logger.warning(f"Failed to load artifact {ptr['path']}: {e}")
            raise
    
    def is_cache_valid(self, node_name: str, cache_key: str) -> bool:
        """
        Check if cache exists and is valid.
        
        NOTE: This implementation is intentionally lightweight for large-result nodes:
        it only checks that the cache file exists and is non-empty, instead of fully
        json-loading potentially huge results (which can be very slow).
        """
        cache_path = self.get_cache_path(node_name, cache_key)
        if not cache_path.exists():
            return False
        try:
            # Lightweight sanity: file is non-empty and readable
            with open(cache_path, "rb") as f:
                first_byte = f.read(1)
            if not first_byte:
                # Empty / truncated file – treat as invalid
                return False
            return True
        except Exception:
            # Best-effort cleanup of corrupted cache
            try:
                cache_path.unlink()
            except Exception:
                pass
            return False
    
    def load_cache(self, node_name: str, cache_key: str) -> Any:
        """Load cached result with lazy artifact loading"""
        cache_path = self.get_cache_path(node_name, cache_key)
        if not cache_path.exists():
            raise FileNotFoundError(f"Cache file not found: {cache_path}")
        with open(cache_path, 'r') as f:
            data = json.load(f)
            # Handle both old format (direct result) and new format (with metadata)
            if isinstance(data, dict) and "_result" in data:
                return data["_result"]  # Return as-is (may contain artifact pointers)
            # Handle old format
            return data
    
    def _has_artifacts(self, obj) -> bool:
        """Check if object contains artifact pointers"""
        if isinstance(obj, dict):
            if obj.get("__artifact__"):
                return True
            return any(self._has_artifacts(v) for v in obj.values())
        elif isinstance(obj, (list, tuple)):
            return any(self._has_artifacts(item) for item in obj)
        return False
    
    def save_cache(self, node_name: str, cache_key: str, result: Any, config: Dict = None):
        """Save result to cache atomically with provenance metadata and file locking"""
        final = self.get_cache_path(node_name, cache_key)
        tmp = final.with_suffix(final.suffix + ".tmp")
        lock = final.with_suffix(".lock")

        # Ensure the directory structure exists
        final.parent.mkdir(parents=True, exist_ok=True)

        # Acquire lock with retry
        for _ in range(100):
            try:
                fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.close(fd)
                break
            except FileExistsError:
                time.sleep(0.01)
        else:
            raise RuntimeError(f"Could not acquire lock for {final}")

        try:
            # Double-check after acquiring lock (avoid wasted recompute)
            if final.exists() and self.is_cache_valid(node_name, cache_key):
                return  # someone else finished; skip writing
            
            # Create metadata header
            node = self.nodes[node_name]
            
            # Build flattened ancestor list for easy inspection (topo-ordered upstream)
            ancestor_keys = []
            def collect_ancestors(n: str, visited: set = None):
                if visited is None:
                    visited = set()
                if n in visited:
                    return
                visited.add(n)
                for input_node in self.nodes[n].inputs:
                    if input_node in self.nodes:
                        ancestor_keys.append({
                            "node": input_node,
                            "key": self.get_node_cache_key(config or {}, input_node)
                        })
                        collect_ancestors(input_node, visited)
            
            collect_ancestors(node_name)
            
            metadata = {
                "node": node_name,
                "version": node.version,
                "cache_key": cache_key,
                "created_at": time.time(),
                "input_keys": {i: self.get_node_cache_key(config or {}, i) for i in node.inputs if i in self.nodes},
                "ancestor_keys": ancestor_keys,  # topo-ordered upstream for easy inspection
                "section_hashes": {sec: self.get_config_hash(config or {}, sec) for sec in sorted(self._section_closure(node_name))},
                "effective_configs": {sec: self.redact_config(sec, (config or {}).get(sec, {})) for sec in sorted(self._section_closure(node_name))}
            }
            
            # Process result for artifact spilling
            processed_result = result
            # Handle direct DataFrame/Arrow returns (must be before dict/list checks)
            if self._is_dataframe_or_arrow(result):
                processed_result = self._maybe_spill(node_name, cache_key, "result", result)
            elif isinstance(result, dict):
                processed_result = {}
                for key, val in result.items():
                    processed_result[key] = self._maybe_spill(node_name, cache_key, key, val)
            elif isinstance(result, (list, tuple)) and len(result) > 0:
                # For list/tuple results, check if we should spill the whole thing
                processed_result = self._maybe_spill(node_name, cache_key, "result", result)
            
            # Wrap result with metadata
            cache_data = {
                "_metadata": metadata,
                "_result": processed_result
            }

            with open(tmp, 'w') as f:
                json.dump(cache_data, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            tmp.replace(final)  # atomic on same filesystem
        finally:
            with contextlib.suppress(Exception):
                lock.unlink()
    
    @lru_cache(maxsize=None)
    def get_dependencies(self, node_name: str) -> List[str]:
        """Get all nodes that this node depends on (recursive, optimized)"""
        visited, deps = set(), set()
        
        def walk(n: str):
            for dep in self.nodes[n].inputs:
                if dep in self.nodes and dep not in visited:
                    visited.add(dep)
                    deps.add(dep)
                    walk(dep)
        
        walk(node_name)
        return sorted(list(deps))
    
    def _cached_frontier(self, plan: Dict[str, str], required: Set[str], dirty: Set[str]) -> Set[str]:
        """Nearest cached parents needed to feed dirty nodes; stop at first cached parent."""
        frontier, seen = set(), set()
        
        def walk(n: str):
            for p in self.nodes[n].inputs:
                if p not in required or p in seen:
                    continue
                seen.add(p)
                status = plan.get(p, "CACHED")
                if status == "MISSING":
                    walk(p)        # keep walking up through dirty parents
                else:
                    frontier.add(p) # cached parent needed; don't go past it
        
        for n in dirty:
            walk(n)
        return frontier
    
    def _sink_nodes(self) -> List[str]:
        """Nodes with no dependents."""
        return [n for n in self.nodes if n not in self.reverse_deps]
    
    def _default_outputs(self) -> List[str]:
        """Prefer nodes explicitly tagged as outputs; otherwise use sinks."""
        tagged = [n for n, node in self.nodes.items()
                  if getattr(node, "materialize_by_default", False)]
        return tagged if tagged else self._sink_nodes()
    
    def _resolve_materialize_set(self, materialize: Union[str, Sequence[str]]) -> List[str]:
        """
        Accepts:
          - list/tuple/set of node names -> use exactly those
          - "sinks"  -> auto outputs (default finals)
          - "all"    -> every node
          - "none"   -> materialize nothing
        """
        if isinstance(materialize, (list, tuple, set)):
            return [n for n in materialize if n in self.nodes]
        if materialize == "sinks":
            return self._default_outputs()
        if materialize == "all":
            return list(self.nodes.keys())
        # "none" or anything else -> no outputs
        return []
    
    def get_all_downstream_nodes(self, node_name: str) -> List[str]:
        """Get ALL nodes that depend on this node (recursive)"""
        downstream = set()
        
        def find_downstream(current_node):
            if current_node in self.reverse_deps:
                for dependent in self.reverse_deps[current_node]:
                    downstream.add(dependent)
                    find_downstream(dependent)  # Recursive
        
        find_downstream(node_name)
        return sorted(list(downstream))
    
    def determine_execution_plan(self, config: Dict, required: Optional[Set[str]] = None) -> Dict[str, str]:
        """Determine which nodes need to be executed"""
        if required is None:
            required = set(self.nodes.keys())
        
        execution_plan = {}
        
        # Check cache validity for required nodes
        # With provenance-based keys, no second pass needed - keys automatically
        # reflect upstream changes, so CACHED means consistent with current state
        for node_name in required:
            cache_key = self.get_node_cache_key(config, node_name)
            
            if self.is_cache_valid(node_name, cache_key):
                execution_plan[node_name] = "CACHED"
            else:
                execution_plan[node_name] = "MISSING"
        
        return execution_plan
    
    def get_execution_reason(self, node_name: str, config: Dict, execution_plan: Dict) -> str:
        """Get the reason why a node needs to be executed"""
        cache_key = self.get_node_cache_key(config, node_name)
        
        if not self.is_cache_valid(node_name, cache_key):
            return "MISSING_CACHE"
        
        # Check if any dependencies are being processed
        for dep in self.get_dependencies(node_name):
            if execution_plan.get(dep) == "MISSING":
                return f"DEPENDENCY_CHANGED({dep})"
        
        return "CACHED"
    
    def _required_nodes(self, targets: Optional[List[str]]) -> Set[str]:
        """Get the set of nodes required to compute the given targets"""
        if not targets:
            return set(self.nodes.keys())
        
        required = set()
        def add_node(node_name):
            if node_name in required or node_name not in self.nodes:
                return
            required.add(node_name)
            for dep in self.nodes[node_name].inputs:
                add_node(dep)
        
        for target in targets:
            add_node(target)
        
        return required
    
    def execute_node(self, node_name: str, config: Dict, inputs: Dict[str, Any]) -> Any:
        """Execute a single node"""
        cache_key = self.get_node_cache_key(config, node_name)
        
        # Check cache first
        if self.is_cache_valid(node_name, cache_key):
            self.logger.info(f"    {node_name}: [LOADED FROM CACHE] - Using cached result")
            # Track this cache key as recently used
            self._mark_recent(node_name, cache_key)
            return self.load_cache(node_name, cache_key)
        
        self.logger.info(f"    {node_name}: [PROCESSING] - Computing new result")
        
        # Execute the node function with timing
        node = self.nodes[node_name]
        
        # Add _load helper to inputs for lazy artifact loading
        inputs_with_load = dict(inputs)
        inputs_with_load["_load"] = self._load_artifact
        
        t0 = time.time()
        result = node.func(inputs_with_load, config.get(node.config_section, {}))
        dt = time.time() - t0
        
        # Persist in 'wire format' (with artifact pointers) and return the same shape
        self.save_cache(node_name, cache_key, result, config)
        self._mark_recent(node_name, cache_key)
        self.logger.info(f"    {node_name}: [SAVED TO CACHE] - Result cached for future use ({dt:.3f}s)")
        return self.load_cache(node_name, cache_key)
    
    def run_pipeline(self, config: Dict, materialize: Union[str, Sequence[str]] = "sinks") -> Dict[str, Any]:
        """
        materialize:
          - list of node names (e.g., ["cox_model"]) -> plan their closure, return only those
          - "sinks" (default) -> plan closure for default outputs (tagged or graph sinks)
          - "all"   -> plan everything, return everything
          - "none"  -> plan nothing, return {}
        """
        self.logger.info("="*80)
        self.logger.info("RESEARCH PIPELINE EXECUTION")
        self.logger.info("="*80)

        # 1) Decide outputs we actually want to return
        self.logger.info("[DEBUG] Starting _resolve_materialize_set")
        original_materialize = materialize
        outputs = self._resolve_materialize_set(materialize)
        self.logger.info(f"[DEBUG] _resolve_materialize_set completed -> outputs={outputs}")

        # 2) Decide what we must plan/compute (ancestor closure of outputs)
        self.logger.info("[DEBUG] Computing required node set")
        auto_computed_outputs = []  # Track outputs we auto-compute due to config changes
        if not outputs:
            # Even when materialize="none", we should still check if any default outputs
            # are invalid (e.g., due to config changes) and compute them
            default_outputs = self._default_outputs()
            if default_outputs:
                # Check if any default outputs are invalid
                invalid_defaults = []
                for node_name in default_outputs:
                    cache_key = self.get_node_cache_key(config, node_name)
                    if not self.is_cache_valid(node_name, cache_key):
                        invalid_defaults.append(node_name)
                
                if invalid_defaults:
                    self.logger.info(f"[DEBUG] Found {len(invalid_defaults)} invalid default output(s) due to config/code changes: {invalid_defaults}")
                    self.logger.info("[DEBUG] Computing invalid nodes even though materialize='none' (config changed)")
                    # Use invalid defaults as outputs to trigger computation, but mark them as auto-computed
                    outputs = invalid_defaults
                    auto_computed_outputs = invalid_defaults
                else:
                    self.logger.info("\n[OPTIMIZATION] No outputs requested and all default outputs are cached → no work")
                    return {}
            else:
                self.logger.info("\n[OPTIMIZATION] No outputs requested → no work")
                return {}
        required = self._required_nodes(outputs)
        self.logger.info(f"[DEBUG] Required nodes resolved: {sorted(required)}")

        # 3) Build plan on the required subgraph
        self.logger.info("[DEBUG] Starting determine_execution_plan")
        execution_plan = self.determine_execution_plan(config, required)
        self.logger.info("[DEBUG] determine_execution_plan completed")

        self.logger.info("\nEXECUTION PLAN:")
        for node_name, status in execution_plan.items():
            reason = self.get_execution_reason(node_name, config, execution_plan)
            downstream = [d for d in self.get_all_downstream_nodes(node_name) if d in execution_plan]
            dependencies = self.get_dependencies(node_name)
            self.logger.info(f"  {node_name}: {status} (Reason: {reason})")
            if dependencies:
                self.logger.info(f"    -> Depends on: {dependencies}")
            if downstream:
                self.logger.info(f"    -> Affects downstream (required): {downstream}")

        # 4) Execution order in required subgraph
        execution_order = [n for n in self._get_execution_order() if n in required]
        all_cached = all(execution_plan[n] == "CACHED" for n in execution_order)

        # 5) Fast path when everything is cached:
        #    load ONLY the requested outputs (not every ancestor)
        if all_cached:
            self.logger.info(f"\n[OPTIMIZATION] All {len(execution_order)} required nodes cached; materializing outputs only")
            results = {}
            for n in outputs:
                key = self.get_node_cache_key(config, n)
                results[n] = self.load_cache(n, key)
                self._mark_recent(n, key)
                self.logger.info(f"    {n}: [LOADED FROM CACHE]")
            return results

        # 6) Dirty path: compute dirty, load only nearest cached parents (frontier)
        to_process = {n for n, s in execution_plan.items() if s == "MISSING"}
        frontier = self._cached_frontier(execution_plan, required, to_process)
        work_set = to_process | frontier
        filtered_order = [n for n in execution_order if n in work_set]

        results = {}
        self.logger.info(f"\nEXECUTING {sum(execution_plan[n] == 'MISSING' for n in filtered_order)} NODES:")
        if any(execution_plan[n] == "CACHED" for n in filtered_order):
            self.logger.info(f"LOADING {sum(execution_plan[n]=='CACHED' for n in filtered_order)} NODES FROM CACHE:")

        start_time = time.time()
        for n in filtered_order:
            if execution_plan[n] == "MISSING":
                # Resolve artifact pointers in inputs automatically for downstream nodes
                ins = {}
                for i in self.nodes[n].inputs:
                    val = results[i]
                    # If input is an artifact pointer, resolve it automatically
                    if isinstance(val, dict) and val.get("__artifact__"):
                        ins[i] = self._load_artifact(val)
                    else:
                        ins[i] = val
                results[n] = self.execute_node(n, config, ins)
            else:
                key = self.get_node_cache_key(config, n)
                results[n] = self.load_cache(n, key)
                self._mark_recent(n, key)
                self.logger.info(f"    {n}: [LOADED FROM CACHE]")

        # 7) Ensure requested outputs are returned (load if not produced above)
        for n in outputs:
            if n not in results:
                key = self.get_node_cache_key(config, n)
                results[n] = self.load_cache(n, key)
                self._mark_recent(n, key)
                self.logger.info(f"    {n}: [LOADED FROM CACHE] (output)")

        # 8) If we auto-computed nodes due to config changes but materialize was "none",
        # don't return their results (they were computed but not requested)
        if auto_computed_outputs and original_materialize == "none":
            self.logger.info(f"[DEBUG] Filtering out auto-computed nodes from results (materialize='none'): {auto_computed_outputs}")
            # Only return results for nodes that were actually requested (none in this case)
            return {}

        self.logger.info(f"\nEXECUTION COMPLETED in {time.time() - start_time:.2f} seconds")
        return results
    
    def _get_execution_order(self) -> List[str]:
        """
        Get execution order using topological sort with cycle detection.
        Uses insertion order (order nodes were added) as tie-breaker instead of alphabetical order.
        This makes execution order match the logical flow in the pipeline definition.
        """
        temp, perm, order = set(), set(), []
        
        def visit(node_name: str):
            if node_name in perm:
                return
            if node_name in temp:
                raise ValueError(f"Cycle detected at '{node_name}'")
            temp.add(node_name)
            
            # Use insertion order of inputs (preserves order specified in node definition)
            for dep in self.nodes[node_name].inputs:
                if dep in self.nodes:
                    visit(dep)
            
            temp.remove(node_name)
            perm.add(node_name)
            order.append(node_name)
        
        # Use insertion order of nodes (preserves order nodes were added to pipeline)
        # Python 3.7+ dicts maintain insertion order
        for node_name in self.nodes.keys():
            if node_name not in perm:
                visit(node_name)
        
        return order
    
    def clear_cache(self):
        """Clear all cache files"""
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Clear recent cache tracking
        self._recent_cache_keys.clear()
        self._recent_queue.clear()
        
        # Clear memoized graph queries for good measure
        self._section_closure.cache_clear()
        self.get_dependencies.cache_clear()
    
    def get_cache_info(self) -> Dict[str, List[str]]:
        """Get information about cached files (nested layout)"""
        cache_info = {}
        for node_name in self.nodes:
            node_dir = self.cache_dir / node_name
            if node_dir.exists():
                cache_dirs = [d.name for d in node_dir.iterdir() if d.is_dir()]
                cache_info[node_name] = cache_dirs
            else:
                cache_info[node_name] = []
        return cache_info
    
    def prune_cache(self, keep_per_node: int = 5):
        """Remove old cache files, keeping only the most recent ones per node (nested layout)"""
        for node_name in self.nodes:
            node_dir = self.cache_dir / node_name
            if not node_dir.exists():
                continue
                
            # Get all cache key directories for this node
            cache_dirs = [d for d in node_dir.iterdir() if d.is_dir()]
            if len(cache_dirs) > keep_per_node:
                # Sort by modification time, newest first
                cache_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
                # Remove oldest directories, but protect recently used ones
                for old_dir in cache_dirs[keep_per_node:]:
                    cache_key = old_dir.name
                    recent_key = f"{node_name}_{cache_key}"
                    
                    # Skip if this cache directory was recently used
                    if recent_key in self._recent_cache_keys:
                        self.logger.info(f"    Protected recent cache: {old_dir.name}")
                        continue
                    
                    # Remove the entire directory (cache.json + artifacts/)
                    shutil.rmtree(old_dir)
                    self.logger.info(f"    Removed old cache: {old_dir.name}")
    
    def plan(self, config: Dict, materialize: Union[str, Sequence[str]] = "sinks") -> Dict[str, str]:
        """Preview what would run without loading/processing"""
        outputs = self._resolve_materialize_set(materialize)
        if not outputs:
            return {}
        required = self._required_nodes(outputs)
        return self.determine_execution_plan(config, required)
    
    def bump_version(self, node_name: str) -> None:
        """Bump version of a node to invalidate it and all downstream nodes"""
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")
        
        # Bump the version
        current_version = self.nodes[node_name].version
        if current_version.startswith("v") and current_version[1:].isdigit():
            new_version = f"v{int(current_version[1:]) + 1}"
        else:
            new_version = f"v{int(time.time())}"
        
        self.nodes[node_name].version = new_version
        self.logger.info(f"Bumped {node_name} version: {current_version} → {new_version}")
        
        # Clear memoized graph queries since version changed
        self._section_closure.cache_clear()
        self.get_dependencies.cache_clear()
    
    def invalidate(self, node_name: str) -> None:
        """Delete all cache files for a specific node"""
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")
        
        # Find and delete all cache directories for this node
        node_dir = self.cache_dir / node_name
        deleted_count = 0
        if node_dir.exists():
            for cache_dir in node_dir.iterdir():
                if cache_dir.is_dir():
                    try:
                        shutil.rmtree(cache_dir)
                        deleted_count += 1
                    except Exception as e:
                        self.logger.warning(f"Failed to delete {cache_dir}: {e}")
        
        self.logger.info(f"Invalidated {node_name}: deleted {deleted_count} cache files")
    
    def invalidate_upstream(self, config: Dict, target_node: str) -> None:
        """
        Invalidate cache for target node and all its upstream dependencies.
        Only deletes cache entries for the current config (not all configs).
        
        Args:
            config: Current configuration dictionary
            target_node: Name of the target node (and its dependencies will be invalidated)
        """
        if target_node not in self.nodes:
            raise ValueError(f"Node '{target_node}' not found")
        
        # Get all upstream dependencies (including the target node itself)
        dependencies = self.get_dependencies(target_node)
        nodes_to_invalidate = dependencies + [target_node]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_nodes = []
        for node in nodes_to_invalidate:
            if node not in seen:
                seen.add(node)
                unique_nodes.append(node)
        
        deleted_count = 0
        for node_name in unique_nodes:
            # Use invalidate_node for each node (it returns True if cache was deleted)
            if self.invalidate_node(config, node_name):
                deleted_count += 1
        
        self.logger.info(f"Invalidated {len(unique_nodes)} nodes ({deleted_count} cache entries) for current config")
    
    def invalidate_downstream(self, config: Dict, target_node: str) -> None:
        """
        Invalidate cache for target node and all its downstream dependents.
        Only deletes cache entries for the current config (not all configs).
        
        Args:
            config: Current configuration dictionary
            target_node: Name of the target node (and its dependents will be invalidated)
        """
        if target_node not in self.nodes:
            raise ValueError(f"Node '{target_node}' not found")
        
        # Get all downstream dependents (including the target node itself)
        dependents = self.get_all_downstream_nodes(target_node)
        nodes_to_invalidate = [target_node] + dependents
        
        # Remove duplicates while preserving order
        seen = set()
        unique_nodes = []
        for node in nodes_to_invalidate:
            if node not in seen:
                seen.add(node)
                unique_nodes.append(node)
        
        deleted_count = 0
        for node_name in unique_nodes:
            # Use invalidate_node for each node (it returns True if cache was deleted)
            if self.invalidate_node(config, node_name):
                deleted_count += 1
        
        self.logger.info(f"Invalidated {len(unique_nodes)} nodes ({deleted_count} cache entries) for current config")
    
    def invalidate_node(self, config: Dict, node_name: str) -> bool:
        """
        Invalidate cache for a single node only (config-specific, no dependencies).
        Only deletes cache entries for the current config (not all configs).
        
        Args:
            config: Current configuration dictionary
            node_name: Name of the target node to invalidate
            
        Returns:
            True if cache was deleted, False if no cache found
        """

        # Get the cache key for this node with the current config
        cache_key = self.get_node_cache_key(config, node_name)

        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found")
        
        # Get the specific cache directory for this config
        cache_dir = self.cache_dir / node_name / cache_key
        
        if cache_dir.exists():
            try:
                shutil.rmtree(cache_dir)
                self.logger.info(f"Invalidated {node_name} (cache key: {cache_key[:12]}...)")
                return True
            except Exception as e:
                self.logger.warning(f"Failed to delete {cache_dir}: {e}")
                return False
        else:
            self.logger.info(f"No cache found for {node_name} (cache key: {cache_key[:12]}...)")
            return False
    
    def view_cached_node(self, config: Dict, node_name: str, open_browser: bool = True, 
                         formatter: Optional[Callable[[str, Any], None]] = None,
                         config_name: Optional[str] = None) -> Optional[Path]:
        """
        View cached node result - generic implementation that works for any node type.
        
        For visualization nodes (detected by HTML artifacts or visualization_path keys):
        - Opens HTML files in browser
        
        For data/model nodes:
        - Prints summary information (can be customized with formatter)
        
        Args:
            config: Configuration dictionary
            node_name: Name of the node to view
            open_browser: Whether to open visualization HTML in browser (default: True)
            formatter: Optional custom formatter function(node_name, result) -> None
                       If provided, this will be called instead of default formatting
            
        Returns:
            Path to visualization file if applicable, None otherwise
        """
        import webbrowser
        
        if node_name not in self.nodes:
            raise ValueError(f"Node '{node_name}' not found in pipeline")
        
        # Get cache key
        cache_key = self.get_node_cache_key(config, node_name)
        
        # Check if cache exists
        if not self.is_cache_valid(node_name, cache_key):
            print(f"Error: No cached result found for node '{node_name}'")
            print(f"Cache key: {cache_key[:12]}...")
            print(f"Run the pipeline first to generate this node's output.")
            return None
        
        # Load cached result
        try:
            result = self.load_cache(node_name, cache_key)
        except Exception as e:
            print(f"Error loading cache for '{node_name}': {e}")
            return None
        
        # Use custom formatter if provided
        if formatter:
            formatter(node_name, result)
            return None
        
        # Try to find and render DataFrames/JSON/Figures as HTML (generic for any model)
        # This creates a combined view if there are multiple artifact types
        df_html_path = self._find_and_render_dataframes(result, node_name, cache_key, config_name)
        
        if df_html_path and df_html_path.exists():
            print(f"\n{'='*60}")
            print(f"Results View: {node_name}")
            print(f"File: {df_html_path}")
            print(f"{'='*60}")
            if open_browser:
                # Convert Path to absolute and use proper file:// URL format
                abs_path = df_html_path.resolve().as_uri()
                webbrowser.open(abs_path)
                print(f"Opened in browser")
            else:
                print(f"Path: {df_html_path}")
            return df_html_path
        
        # Fallback: if no combined view was created, try to open standalone visualization
        # (e.g., a single Plotly figure with no other artifacts)
        html_path = self._find_visualization_path(result)
        
        if html_path and html_path.exists():
            print(f"\n{'='*60}")
            print(f"Visualization: {node_name}")
            print(f"File: {html_path}")
            print(f"{'='*60}")
            if open_browser:
                # Convert Path to absolute and use proper file:// URL format
                abs_path = html_path.resolve().as_uri()
                webbrowser.open(abs_path)
                print(f"Opened in browser")
            else:
                print(f"Path: {html_path}")
            return html_path
        
        # Default: print summary
        self._print_node_summary(node_name, cache_key, result)
        return None
    
    def _find_and_render_dataframes(self, result: Any, node_name: str, cache_key: str, config_name: Optional[str] = None) -> Optional[Path]:
        """
        Generic function to find and render DataFrames/JSON artifacts as HTML.
        Works with any model/package - no assumptions about structure.
        """
        if not HAS_PANDAS:
            return None
        
        # Handle direct artifact pointer (e.g., direct DataFrame return)
        if isinstance(result, dict) and result.get("__artifact__"):
            if result.get("format") == "parquet":
                try:
                    loaded_df = self._load_artifact(result)
                    if isinstance(loaded_df, pd.DataFrame) and not loaded_df.empty:
                        dataframes = [loaded_df]
                        df_names = ["result"]
                        # Create HTML with this DataFrame
                        cache_dir = Path(self.cache_dir)
                        node_cache_dir = cache_dir / node_name / cache_key
                        html_path = node_cache_dir / "model_results.html"
                        html_content = self._create_generic_results_html(
                            dataframes, df_names, {}, [], [], node_name, html_path, config_name
                        )
                        return html_path
                except Exception:
                    pass
            return None
        
        if not isinstance(result, dict):
            return None
        
        dataframes = []
        df_names = []
        json_data = {}
        
        # Find all artifacts (parquet DataFrames, JSON, HTML, PNG figures, etc.)
        # Generic: handles single items and lists of items
        html_content_list = []  # List of (name, html_string) tuples
        figure_list = []  # List of (name, figure_path) tuples for PNG/HTML figures
        
        def process_artifact(key: str, val: Any, index: int = None):
            """Process a single artifact (called recursively for lists)"""
            if not isinstance(val, dict) or not val.get("__artifact__"):
                return
            
            format_type = val.get("format")
            display_name = f"{key}[{index}]" if index is not None else key
            
            if format_type == "parquet":
                try:
                    loaded_df = self._load_artifact(val)
                    if isinstance(loaded_df, pd.DataFrame) and not loaded_df.empty:
                        dataframes.append(loaded_df)
                        df_names.append(display_name)
                except Exception:
                    pass
            elif format_type == "html":
                # HTML artifacts - load and render directly
                try:
                    html_path = self._load_artifact(val)
                    if isinstance(html_path, str) and Path(html_path).exists():
                        with open(html_path, 'r', encoding='utf-8') as f:
                            html_content_list.append((display_name, f.read()))
                except Exception:
                    pass
            elif format_type == "png":
                # PNG figures (Plotly, matplotlib, etc.) - embed as images
                try:
                    figure_path = self._load_artifact(val)
                    if isinstance(figure_path, str) and Path(figure_path).exists():
                        figure_list.append((display_name, figure_path))
                except Exception:
                    pass
            elif format_type in ["json.zst", "jsonl.zst"]:
                # Load JSON artifacts (model statistics, summaries, etc.)
                try:
                    loaded_json = self._load_artifact(val)
                    if isinstance(loaded_json, dict) and key not in ["config_hash", "timestamp"]:
                        json_key = display_name if index is not None else key
                        json_data[json_key] = loaded_json
                except Exception:
                    pass
        
        # Process all artifacts (handle both single items and lists)
        for key, val in result.items():
            if key in ["config_hash", "timestamp"]:
                continue
            
            # Check if value is a list of artifacts
            if isinstance(val, list):
                for idx, item in enumerate(val):
                    if isinstance(item, dict) and item.get("__artifact__"):
                        process_artifact(key, item, index=idx)
            else:
                # Single artifact
                process_artifact(key, val)
        
        # If we found DataFrames, JSON data, HTML, or figures, create an HTML file
        if dataframes or json_data or html_content_list or figure_list:
            cache_dir = Path(self.cache_dir)
            node_cache_dir = cache_dir / node_name / cache_key
            html_path = node_cache_dir / "model_results.html"
            
            try:
                # Create HTML with all DataFrames, JSON, HTML content, and figures
                html_content = self._create_generic_results_html(
                    dataframes, df_names, json_data, html_content_list, figure_list, node_name, html_path, config_name
                )
                html_path.parent.mkdir(parents=True, exist_ok=True)
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(html_content)
                return html_path
            except Exception as e:
                print(f"  Note: Could not create model results HTML view: {e}")
                return None
        
        return None
    
    def _is_html_string(self, s: str) -> bool:
        """Check if a string contains HTML (generic detection)"""
        if not isinstance(s, str) or len(s) < 10:
            return False
        # Simple heuristic: contains HTML tags
        s_lower = s.lower().strip()
        return s_lower.startswith('<') and ('<table' in s_lower or '<div' in s_lower or '<html' in s_lower or '<body' in s_lower)
    
    def _create_generic_results_html(self, dataframes: List[pd.DataFrame], df_names: List[str], 
                                      json_data: Dict[str, Any], html_content_list: List[tuple],
                                      figure_list: List[tuple], node_name: str, html_path: Path,
                                      config_name: Optional[str] = None) -> str:
        """
        Generic HTML renderer for model results.
        Works with any structure - renders DataFrames as tables, JSON as formatted sections, HTML directly.
        No special cases - completely generic.
        """
        html_parts = ["""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Results: """ + node_name + (f" ({config_name})" if config_name else "") + """</title>
    <style>
        body {
            font-family: 'Consolas', 'Courier New', monospace;
            background-color: #1e1e1e;
            color: #d4d4d4;
            margin: 0;
            padding: 20px;
            font-size: 12px;
        }
        h1 {
            color: #ffffff;
            border-bottom: 1px solid #ffffff;
            padding-bottom: 10px;
            font-size: 18px;
            margin-bottom: 20px;
        }
        h2 {
            color: #d4d4d4;
            margin-top: 30px;
            margin-bottom: 10px;
            font-size: 14px;
            border-bottom: 1px solid #3e3e42;
            padding-bottom: 5px;
        }
        h3 {
            color: #d4d4d4;
            margin-top: 20px;
            margin-bottom: 8px;
            font-size: 13px;
            border-bottom: 1px solid #3e3e42;
            padding-bottom: 3px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            margin-bottom: 20px;
            background-color: #252526;
            font-size: 11px;
        }
        th {
            background-color: #2d2d30;
            color: #cccccc;
            font-weight: bold;
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #3e3e42;
            position: sticky;
            top: 0;
            z-index: 10;
            font-size: 11px;
        }
        td {
            padding: 6px 8px;
            border-bottom: 1px solid #3e3e42;
            font-size: 11px;
        }
        tr:hover {
            background-color: #2a2d2e;
        }
        tr:nth-child(even) {
            background-color: #1e1e1e;
        }
        tr:nth-child(even):hover {
            background-color: #2a2d2e;
        }
        .info {
            color: #858585;
            font-size: 10px;
            margin-bottom: 8px;
        }
        .stats-section {
            background-color: #252526;
            padding: 12px;
            margin-bottom: 20px;
            border-radius: 4px;
        }
        .stats-row {
            display: table-row;
        }
        .stats-key {
            display: table-cell;
            padding: 4px 12px 4px 0;
            color: #d4d4d4;
            font-weight: bold;
            min-width: 200px;
        }
        .stats-value {
            display: table-cell;
            padding: 4px 0;
            color: #d4d4d4;
        }
        .model-summary-html {
            background-color: #252526;
            padding: 12px;
            margin-bottom: 20px;
            overflow-x: auto;
        }
        .model-summary-text {
            background-color: #252526;
            padding: 12px;
            margin-bottom: 20px;
            white-space: pre-wrap;
            font-family: 'Consolas', 'Courier New', monospace;
            font-size: 11px;
        }
    </style>
</head>
<body>
    <h1>Results: """ + node_name + (f" ({config_name})" if config_name else "") + """</h1>
"""]
        
        # Render HTML artifacts first (if any)
        for name, html_str in html_content_list:
            html_parts.append(f'    <h2>{name}</h2>')
            html_parts.append('    <div class="model-summary-html">')
            html_parts.append(html_str)
            html_parts.append('    </div>')
            html_parts.append('')
        
        # Render figures (PNG images) - embed as images
        for name, figure_path in figure_list:
            html_parts.append(f'    <h2>{name}</h2>')
            # Convert to relative path for embedding
            try:
                figure_rel_path = Path(figure_path).relative_to(Path(html_path).parent)
                html_parts.append(f'    <div class="figure-container">')
                html_parts.append(f'        <img src="{figure_rel_path.as_posix()}" style="max-width: 100%; height: auto;" />')
                html_parts.append(f'    </div>')
            except Exception:
                # If relative path fails, use absolute path
                html_parts.append(f'    <div class="figure-container">')
                html_parts.append(f'        <img src="file://{figure_path}" style="max-width: 100%; height: auto;" />')
                html_parts.append(f'    </div>')
            html_parts.append('')
        
        # Render JSON data (generic - no special cases)
        for key, data in json_data.items():
            if key == "cox_results" and isinstance(data, dict):
                # Remove feature_info from cox_results (not needed for display)
                data = {k: v for k, v in data.items() if k != "feature_info"}
            if data:  # Only render if data exists
                html_parts.append(f'    <h2>{key}</h2>')
                html_parts.append(self._render_dict_generic(data, indent=4))
                html_parts.append('')
        
        # Render DataFrames (generic - all DataFrames are rendered, order preserved)
        for df, name in zip(dataframes, df_names):
            html_parts.append(f'    <h2>{name}</h2>')
            html_parts.append(f'    <div class="info">Shape: {df.shape[0]} rows × {df.shape[1]} columns</div>')
            
            # Prepare DataFrame for display: if 'feature' column exists, it's already visible
            # Otherwise, make index a column if it has meaningful names
            display_df = df.copy()
            if 'feature' not in display_df.columns and (display_df.index.name or 
                (len(display_df) > 0 and any(not str(idx).isdigit() for idx in display_df.index[:5]))):
                display_df = display_df.reset_index()
                if display_df.columns[0] == 'index':
                    display_df.rename(columns={'index': 'variable'}, inplace=True)
            
            # Convert DataFrame to HTML table
            html_table = display_df.to_html(
                classes=None,
                table_id=None,
                escape=False,
                index=False,  # Index is now a column, so don't show it again
                float_format=lambda x: f'{x:.6f}' if isinstance(x, (int, float)) else str(x),
                max_rows=None
            )
            
            # Add custom styling
            html_table = html_table.replace('<table', '<table style="width:100%; border-collapse:collapse;"')
            html_parts.append('    ' + html_table)
            html_parts.append('')
        
        html_parts.append("</body>\n</html>")
        
        return '\n'.join(html_parts)
    
    def _render_dict_generic(self, data: Any, indent: int = 0) -> str:
        """Recursively render any data structure as HTML (completely generic)"""
        indent_str = ' ' * indent
        
        if isinstance(data, dict):
            html_parts = [f'{indent_str}<div class="stats-section">']
            for key, value in data.items():
                if isinstance(value, (dict, list)):
                    html_parts.append(f'{indent_str}  <h3>{key}</h3>')
                    html_parts.append(self._render_dict_generic(value, indent + 4))
                elif isinstance(value, str) and self._is_html_string(value):
                    # Generic HTML detection - if it's HTML, render it directly
                    html_parts.append(f'{indent_str}  <h3>{key}</h3>')
                    html_parts.append(f'{indent_str}  <div class="model-summary-html">')
                    html_parts.append(value)
                    html_parts.append(f'{indent_str}  </div>')
                else:
                    html_parts.append(f'{indent_str}  <div class="stats-row">')
                    html_parts.append(f'{indent_str}    <span class="stats-key">{key}:</span>')
                    # Format numbers nicely
                    if isinstance(value, float):
                        formatted = f'{value:.6f}' if abs(value) < 1e-3 or abs(value) > 1e6 else f'{value:.4f}'
                    else:
                        formatted = str(value)
                    html_parts.append(f'{indent_str}    <span class="stats-value">{formatted}</span>')
                    html_parts.append(f'{indent_str}  </div>')
            html_parts.append(f'{indent_str}</div>')
            return '\n'.join(html_parts)
        elif isinstance(data, list):
            html_parts = [f'{indent_str}<div class="stats-section">']
            html_parts.append(f'{indent_str}  <div class="stats-row">')
            html_parts.append(f'{indent_str}    <span class="stats-value">{len(data)} items</span>')
            html_parts.append(f'{indent_str}  </div>')
            html_parts.append(f'{indent_str}</div>')
            return '\n'.join(html_parts)
        else:
            # Primitive value
            formatted = f'{data:.6f}' if isinstance(data, float) else str(data)
            return f'{indent_str}<div class="stats-value">{formatted}</div>'
    
    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters"""
        return (text.replace('&', '&amp;')
                   .replace('<', '&lt;')
                   .replace('>', '&gt;')
                   .replace('"', '&quot;')
                   .replace("'", '&#39;'))
    
    def _find_visualization_path(self, result: Any) -> Optional[Path]:
        """Find HTML visualization file path from result"""
        if isinstance(result, dict):
            # Check for visualization_path key (legacy format - can be string or artifact pointer)
            if "visualization_path" in result:
                path_val = result["visualization_path"]
                if path_val:
                    if isinstance(path_val, str):
                        html_path = Path(path_val)
                        if html_path.exists():
                            return html_path
                    elif isinstance(path_val, dict) and path_val.get("__artifact__"):
                        # It's an artifact pointer - resolve it
                        if path_val.get("format") == "html":
                            artifact_path = (self.cache_dir / path_val["path"]).resolve()
                            if artifact_path.exists():
                                return artifact_path
            
            # Check for figure artifact (HTML format) - new format
            if "figure" in result:
                fig_path = result["figure"]
                if isinstance(fig_path, str):
                    html_path = Path(fig_path)
                    if html_path.exists():
                        return html_path
                elif isinstance(fig_path, dict) and fig_path.get("__artifact__"):
                    # It's an artifact pointer - resolve it
                    if fig_path.get("format") == "html":
                        artifact_path = (self.cache_dir / fig_path["path"]).resolve()
                        if artifact_path.exists():
                            return artifact_path
            
            # Check all values for HTML artifacts (fallback)
            for val in result.values():
                if isinstance(val, dict) and val.get("__artifact__"):
                    if val.get("format") == "html":
                        artifact_path = (self.cache_dir / val["path"]).resolve()
                        if artifact_path.exists():
                            return artifact_path
        
        return None
    
    def _print_node_summary(self, node_name: str, cache_key: str, result: Any) -> None:
        """Print summary of cached node result"""
        cache_path = self.get_cache_path(node_name, cache_key)
        print(f"\n{'='*60}")
        print(f"Node: {node_name}")
        print(f"Cache key: {cache_key[:12]}...")
        print(f"Cache file: {cache_path.resolve()}")
        print(f"{'='*60}")
        
        if isinstance(result, dict):
            for key, val in result.items():
                if key in ["config_hash", "timestamp"]:
                    continue
                
                # Handle artifact pointers
                if isinstance(val, dict) and val.get("__artifact__"):
                    artifact_path = (self.cache_dir / val["path"]).resolve()
                    print(f"\n{key}:")
                    print(f"  Type: Artifact ({val.get('format', 'unknown')})")
                    print(f"  Path: {artifact_path}")
                    if artifact_path.exists():
                        size = artifact_path.stat().st_size
                        if size > 1024 * 1024:
                            print(f"  Size: {size / (1024*1024):.2f} MB")
                        else:
                            print(f"  Size: {size / 1024:.2f} KB")
                    
                    # Try to load and show preview for certain formats
                    if val.get("format") in ["json.zst", "jsonl.zst"]:
                        try:
                            loaded_data = self._load_artifact(val)
                            if isinstance(loaded_data, list):
                                print(f"  Preview: List with {len(loaded_data)} items")
                                if len(loaded_data) > 0:
                                    first = loaded_data[0]
                                    if isinstance(first, dict):
                                        print(f"  First item keys: {', '.join(list(first.keys())[:10])}")
                            elif isinstance(loaded_data, dict):
                                print(f"  Preview: Dict with keys: {', '.join(list(loaded_data.keys())[:10])}")
                        except Exception as e:
                            print(f"  Note: Could not load artifact for preview: {e}")
                
                elif isinstance(val, list):
                    print(f"\n{key}:")
                    print(f"  Type: List")
                    print(f"  Length: {len(val)} items")
                    if len(val) > 0:
                        first_item = val[0]
                        if isinstance(first_item, dict):
                            print(f"  First item keys: {', '.join(list(first_item.keys())[:10])}")
                            if len(list(first_item.keys())) > 10:
                                print(f"  ... ({len(list(first_item.keys())) - 10} more keys)")
                
                elif hasattr(val, 'shape'):  # DataFrame
                    if HAS_PANDAS and isinstance(val, pd.DataFrame):
                        print(f"\n{key}:")
                        print(f"  Type: DataFrame")
                        print(f"  Shape: {val.shape[0]} rows × {val.shape[1]} columns")
                        print(f"  Columns: {', '.join(list(val.columns)[:10])}")
                        if len(val.columns) > 10:
                            print(f"  ... ({len(val.columns) - 10} more columns)")
                        print(f"\n  First 5 rows:")
                        print(val.head().to_string())
                
                else:
                    print(f"\n{key}: {type(val).__name__}")
                    if isinstance(val, (str, int, float, bool)):
                        print(f"  Value: {val}")
                    elif isinstance(val, dict):
                        print(f"  Keys: {', '.join(list(val.keys())[:10])}")
                        if len(val) > 10:
                            print(f"  ... ({len(val) - 10} more keys)")
        
        else:
            print(f"\nResult type: {type(result).__name__}")
            if isinstance(result, (str, int, float, bool)):
                print(f"Value: {result}")

    def _iter_artifact_ptrs(self, obj):
        """Yield every artifact pointer found recursively in obj."""
        if isinstance(obj, dict):
            if obj.get("__artifact__"):
                yield obj
            for v in obj.values():
                yield from self._iter_artifact_ptrs(v)
        elif isinstance(obj, (list, tuple)):
            for it in obj:
                yield from self._iter_artifact_ptrs(it)

    def export_node(
        self,
        node_name: str,
        config: Dict,
        dest_root: str,
        cache_key: Optional[str] = None,
        include_ancestors: bool = False,
        keep_tree: bool = True,
        link: bool = True,
    ) -> Dict[str, Any]:
        """
        Export artifacts for a node (and optionally all ancestors) to dest_root.
        - keep_tree=True keeps the node/cache_key/artifacts/ subtree in exports.
        - link=True uses hardlinks when possible; False copies bytes.
        Writes:
            exports/.../index.json       # list of exported files with origins
            exports/.../provenance.json  # per-node cache metadata for reproducibility
        Returns paths to the index & provenance files plus counts.
        """
        dest_root = Path(dest_root)
        dest_root.mkdir(parents=True, exist_ok=True)

        # Resolve cache key & read this node's cache
        if cache_key is None:
            cache_key = self.get_node_cache_key(config, node_name)
        cache_path = self.get_cache_path(node_name, cache_key)
        if not cache_path.exists():
            raise FileNotFoundError(f"No cache for {node_name}:{cache_key}")

        with open(cache_path, "r") as f:
            node_cache = json.load(f)
        node_result = node_cache.get("_result", node_cache)
        provenance_entries = []
        items = []

        def export_ptrs_for(node: str, key: str):
            cp = self.get_cache_path(node, key)
            if not cp.exists():
                return
            with open(cp, "r") as f2:
                data = json.load(f2)
            result = data.get("_result", data)

            # record provenance for this (node, key)
            provenance_entries.append({
                "node": node,
                "cache_key": key,
                "_metadata": data.get("_metadata", {})
            })

            # export each artifact pointer found in result
            for ptr in self._iter_artifact_ptrs(result):
                rel = ptr["path"]                  # relative to self.cache_dir
                src = (self.cache_dir / rel).resolve()
                if keep_tree:
                    dest = dest_root / rel         # preserves node/cache_key/...
                else:
                    dest = dest_root / f"{node}__{key}__{src.name}"

                dest.parent.mkdir(parents=True, exist_ok=True)
                if link:
                    with contextlib.suppress(FileExistsError):
                        os.link(src, dest)
                else:
                    shutil.copy2(src, dest)

                items.append({
                    "src": str(src),
                    "dest": str(dest),
                    "node": node,
                    "cache_key": key,
                    "format": ptr.get("format"),
                    "key": ptr.get("key")
                })

        # always export the requested node
        export_ptrs_for(node_name, cache_key)

        # optionally include all ancestors' artifacts
        if include_ancestors:
            for anc in node_cache.get("_metadata", {}).get("ancestor_keys", []):
                export_ptrs_for(anc["node"], anc["key"])

        # write indexes
        index_path = dest_root / "index.json"
        prov_path = dest_root / "provenance.json"
        index_path.write_text(json.dumps({"items": items}, indent=2))
        prov_path.write_text(json.dumps({
            "created_at": time.time(),
            "cache_dir": str(self.cache_dir),
            "entries": provenance_entries
        }, indent=2))
        return {"index": str(index_path), "provenance": str(prov_path), "count": len(items)}

# Dummy functions for testing (used by test system)
def load_raw_data(inputs: Dict, config: Dict) -> Dict:
    """Dummy raw data loading - returns config as output"""
    return {
        "data": f"Raw data for {config}",
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8],
        "timestamp": time.time()
    }

def calculate_variables(inputs: Dict, config: Dict) -> Dict:
    """Dummy variable calculation"""
    raw_data = inputs.get("raw_data", {})
    return {
        "variables": f"Variables for {raw_data.get('config_hash', 'unknown')} with {config}",
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8],
        "timestamp": time.time()
    }

def calculate_global_variables(inputs: Dict, config: Dict) -> Dict:
    """Dummy global variable calculation"""
    variables = inputs.get("variables", {})
    return {
        "global_variables": f"Global variables for {variables.get('config_hash', 'unknown')} with {config}",
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8],
        "timestamp": time.time()
    }

def prepare_cox_data(inputs: Dict, config: Dict) -> Dict:
    """Dummy Cox data preparation"""
    variables = inputs.get("variables", {})
    global_vars = inputs.get("global_variables", {})
    return {
        "cox_data": f"Cox data for {variables.get('config_hash', 'unknown')} + {global_vars.get('config_hash', 'unknown')} with {config}",
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8],
        "timestamp": time.time()
    }

def fit_cox_model(inputs: Dict, config: Dict) -> Dict:
    """Dummy Cox model fitting"""
    cox_data = inputs.get("cox_data", {})
    return {
        "cox_results": f"Cox results for {cox_data.get('config_hash', 'unknown')} with {config}",
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8],
        "timestamp": time.time()
    }


def create_study_structure(study_name: str, study_dir: str = None) -> Path:
    """
    Create a standardized study folder structure with full template.
    
    Args:
        study_name: Name of the study (will be used as folder name)
        study_dir: Parent directory where to create the study (default: current directory)
    
    Returns:
        Path to the created study directory
    """
    if study_dir is None:
        study_dir = "."
    
    study_path = Path(study_dir) / study_name
    study_path.mkdir(parents=True, exist_ok=True)
    
    # Create standard directories (full template)
    directories = [
        "config",
        "data", 
        "exports",
        "notebooks",
        "cache",
        "scripts",
        "tests", 
        "docs",
        "logs",
        "modules",  # Study-specific modules
        "viz"  # Visualization scripts and outputs (html/ subfolder created by visualizations)
    ]
    
    for dir_name in directories:
        (study_path / dir_name).mkdir(exist_ok=True)
    
    # Create __init__.py files for Python packages (modules and viz)
    # These are needed for proper Python package imports
    python_packages = ["modules", "viz", "tests"]
    for package_name in python_packages:
        init_file = study_path / package_name / "__init__.py"
        if not init_file.exists():
            with open(init_file, 'w') as f:
                f.write(f'"""\n{package_name.title()} package for {study_name} study.\n"""\n')
    
    # Create sample data file to show what goes in data/ folder
    sample_data_content = """# Sample Data File
# Replace this with your actual data file

# This file shows the expected format for your raw data
# The actual data should be in CSV format with columns like:
# - patient_id, age, gender, treatment_group, survival_time, event_occurred

# Example structure:
# patient_id,age,gender,treatment_group,survival_time,event_occurred
# 001,45,Male,Treatment,365,1
# 002,52,Female,Control,180,0
# 003,38,Male,Treatment,500,1

# Add your actual data file here and update config/base.yaml accordingly
"""
    
    with open(study_path / "data" / "README.txt", 'w') as f:
        f.write(sample_data_content)
    
    # Create configuration files (full template)
    base_config = {
            "data": {
                "source": "data/raw_data.csv",
                "format": "csv",
                "preprocessing": {
                    "clean_missing": True,
                    "normalize": False
                }
            },
            "variables": {
                "method": "advanced",
                "parameters": {
                    "feature_selection": True,
                    "cross_validation": True
                }
            },
            "global_variables": {
                "parameters": {
                    "scaling": "standard",
                    "outlier_detection": True
                }
            },
            "cox_data": {
                "parameters": {
                    "time_variable": "survival_time",
                    "event_variable": "event_occurred"
                }
            },
            "models": {
                "parameters": {
                    "max_iter": 1000,
                    "regularization": "l1",
                    "alpha": 0.01
                }
            },
            "results": {
                "format": "json",
                "include_plots": True,
                "export_format": ["json", "csv", "html"]
            },
            "logging": {
                "level": "INFO",
                "file": "logs/study.log"
            },
            "validation": {
                "cross_validation_folds": 5,
                "test_size": 0.2
            }
        }
    
    dev_config = base_config.copy()
    dev_config["models"]["parameters"]["max_iter"] = 100
    dev_config["validation"]["cross_validation_folds"] = 3
    
    # Write configuration files
    config_dir = study_path / "config"
    config_dir.mkdir(exist_ok=True)
    
    with open(config_dir / "base.yaml", 'w') as f:
        yaml.dump(base_config, f, default_flow_style=False)
    
    with open(config_dir / "dev.yaml", 'w') as f:
        yaml.dump(dev_config, f, default_flow_style=False)
    
    # Create study_pipeline.py template
    pipeline_template = '''#!/usr/bin/env python3
"""
{study_name} Study Pipeline
Generated study pipeline using the research pipeline framework.
"""

import sys
from pathlib import Path

# Add the framework root to the path
framework_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(framework_root))

from pipeline.core import ResearchPipeline, Node
import yaml
import time
import hashlib
import json


def create_pipeline(cache_dir: str = None) -> ResearchPipeline:
    """Create and configure the {study_name} study pipeline"""
    if cache_dir is None:
        cache_dir = "cache"  # Relative to the study directory
    
    # Initialize pipeline
    pipeline = ResearchPipeline(cache_dir=cache_dir)
    
    # Add your study-specific nodes here
    # Example:
    # pipeline.add_node(Node(
    #     name="raw_data",
    #     func=your_data_loading_function,
    #     inputs=[],
    #     config_section="data"
    # ))
    
    return pipeline


def load_study_config(config_name: str = "base") -> dict:
    """Load study configuration"""
    config_path = Path(__file__).parent / "config" / f"{{config_name}}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {{config_path}}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run_study(config_name: str = "base", materialize: list = None):
    """Run the {study_name} study"""
    if materialize is None:
        materialize = ["results"]
    
    # Load configuration
    config = load_study_config(config_name)
    
    # Create pipeline
    pipeline = create_pipeline()
    
    # Run pipeline
    print(f"Running {study_name} study with config: {{config_name}}")
    print(f"Materializing: {{materialize}}")
    
    results = pipeline.run_pipeline(config, materialize=materialize)
    
    return results, pipeline, config


if __name__ == "__main__":
    # Run the study
    results, pipeline, config = run_study()
    
    print("\\nStudy completed!")
    print(f"Results: {{list(results.keys())}}")
'''.format(study_name=study_name)
    
    with open(study_path / "study_pipeline.py", 'w') as f:
        f.write(pipeline_template)
    
    # Create test_study.py template
    test_template = '''#!/usr/bin/env python3
"""
Test script for {study_name} study
"""

import sys
import tempfile
import shutil
from pathlib import Path

# Add the framework root to the path
framework_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(framework_root))

import study_pipeline


def test_study():
    """Test the {study_name} study"""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir) / "cache"
        
        # Create pipeline with explicit cache directory
        pipe = study_pipeline.create_pipeline(cache_dir=str(cache_dir))
        
        # Load configuration
        config = study_pipeline.load_study_config("base")
        
        # Test that all nodes are properly configured
        assert pipe is not None
        assert config is not None
        
        print("Study test passed!")


if __name__ == "__main__":
    test_study()
'''
    
    with open(study_path / "test_study.py", 'w') as f:
        f.write(test_template)
    
    # Create README.md
    readme_content = f"""# {study_name} Study

This study was created using the research pipeline framework.

## Structure

- `config/` - Configuration files (base.yaml, dev.yaml)
- `data/` - Raw data files
- `exports/` - Exported results and artifacts
- `notebooks/` - Jupyter notebooks for analysis
- `cache/` - Pipeline cache (auto-generated)
- `modules/` - Study-specific modules and utilities
- `scripts/` - Study-specific scripts
- `tests/` - Test files for the study
- `docs/` - Study documentation
- `logs/` - Study log files
- `study_pipeline.py` - Main study pipeline definition
- `test_study.py` - Test script for the study

## Usage

```bash
# Run the study
python study_pipeline.py

# Run with specific configuration
python study_pipeline.py --config dev

# Test the study
python test_study.py
```

## Configuration

Edit the files in `config/` to customize your study parameters.

## Modules

Study-specific modules are located in the `modules/` folder. These contain:
- Database queries and data access
- Variable calculation logic
- Statistical modeling functions
- Utility functions

See `modules/README.md` for more details.
"""
    
    with open(study_path / "README.md", 'w') as f:
        f.write(readme_content)
    
    # Create additional files (full template)
    # Create requirements.txt
    requirements_content = """# Study Dependencies
# Add your study-specific dependencies here

# Core framework (already included)
# research-pipeline-framework

# Data processing
pandas>=1.5.0
numpy>=1.21.0

# Machine learning
scikit-learn>=1.1.0
scipy>=1.9.0

# Visualization
matplotlib>=3.5.0
seaborn>=0.11.0

# Jupyter notebooks
jupyter>=1.0.0
ipykernel>=6.0.0
"""
    with open(study_path / "requirements.txt", 'w') as f:
        f.write(requirements_content)
    
    # Create .gitignore
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Jupyter Notebook
.ipynb_checkpoints

# Study-specific
cache/
logs/
exports/
data/raw_*
!data/raw_data.csv

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db
"""
    with open(study_path / ".gitignore", 'w') as f:
        f.write(gitignore_content)
    
    # Create setup script
    setup_script = '''#!/usr/bin/env python3
"""
Setup script for {study_name} study
"""

import subprocess
import sys
from pathlib import Path

def setup_study():
    """Setup the study environment"""
    print("Setting up {study_name} study...")
    
    # Install requirements
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("+ Dependencies installed")
    except subprocess.CalledProcessError:
        print("- Failed to install dependencies")
        return False
    
    # Create log directory
    Path("logs").mkdir(exist_ok=True)
    print("+ Log directory created")
    
    print("\\nStudy setup complete!")
    print("Next steps:")
    print("1. Add your data to the data/ directory")
    print("2. Edit config/base.yaml to configure your study")
    print("3. Implement your pipeline nodes in study_pipeline.py")
    print("4. Run: python study_pipeline.py")
    
    return True

if __name__ == "__main__":
    setup_study()
'''.format(study_name=study_name)
        
    with open(study_path / "setup.py", 'w') as f:
        f.write(setup_script)
    
    # Create modules README
    modules_readme = """# Study Modules

This folder contains study-specific modules and utilities.

## Purpose

- **Study-specific code**: Custom functions, classes, and utilities for this study
- **Self-contained**: All dependencies should be within this study
- **Portable**: Study can be moved/copied independently

## Common Module Types

- `database_queries.py` - Database access and queries
- `variable_calculator.py` - Variable calculation logic
- `global_variables.py` - Global variable computation
- `cox_data_preparation.py` - Cox regression data preparation
- `cox_regression.py` - Cox regression modeling
- `utils.py` - General utility functions

## Usage

Import modules in your `study_pipeline.py`:

```python
from modules.database_queries import DatabaseQueries
from modules.variable_calculator import VariableCalculator
```

## Best Practices

- Keep modules focused and single-purpose
- Include proper docstrings and type hints
- Add unit tests in the `tests/` folder
- Document any external dependencies
"""
    
    with open(study_path / "modules" / "README.md", 'w') as f:
        f.write(modules_readme)
    
    # Create viz README
    viz_readme = """# Visualization Scripts

This folder contains visualization scripts and generated visualizations for your study.

## Purpose

- **Visualization scripts**: Python scripts to create plots, charts, and interactive visualizations
- **Output files**: Generated HTML, PNG, SVG, or other visualization files
- **Analysis visuals**: Charts and graphs for exploring your data and results

## Common Visualization Types

- `plot_results.py` - Visualize study results and outputs
- `exploratory_analysis.py` - EDA visualizations
- `model_diagnostics.py` - Model performance and diagnostic plots
- `interactive_dashboards.py` - Interactive dashboards (Plotly, Bokeh, etc.)

## Usage

Create visualization scripts that:
1. Load data from `exports/` or `cache/`
2. Generate plots using matplotlib, plotly, seaborn, etc.
3. Save outputs to this `viz/` folder

Example:
```python
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Load results
results = pd.read_parquet("../exports/results.parquet")

# Create visualization
plt.figure(figsize=(10, 6))
plt.plot(results['x'], results['y'])
plt.savefig('results_plot.png')
```

## Best Practices

- Use consistent naming conventions (e.g., `plot_<feature>_<metric>.py`)
- Save outputs with descriptive names
- Include docstrings explaining what each visualization shows
- Consider using interactive libraries (Plotly, Bokeh) for exploration
- Document any external dependencies in requirements.txt
"""
    
    with open(study_path / "viz" / "README.md", 'w') as f:
        f.write(viz_readme)
    
    print(f"\n{'='*60}")
    print(f"STUDY CREATED: {study_name}")
    print(f"Template: FULL (with all features)")
    print(f"Location: {study_path}")
    print(f"{'='*60}")
    
    print(f"\nDIRECTORIES CREATED:")
    for dir_name in directories:
        print(f"  + {dir_name}/")
    
    print(f"\nFILES CREATED:")
    print(f"  + config/base.yaml")
    print(f"  + config/dev.yaml") 
    print(f"  + study_pipeline.py")
    print(f"  + test_study.py")
    print(f"  + README.md")
    print(f"  + data/README.txt (sample data format)")
    print(f"  + modules/README.md (modules documentation)")
    print(f"  + viz/README.md (visualization guide)")
    print(f"  + requirements.txt")
    print(f"  + .gitignore")
    print(f"  + setup.py")
    
    print(f"\nCONFIGURATION FEATURES:")
    print(f"  + Advanced preprocessing options")
    print(f"  + Feature selection & cross-validation")
    print(f"  + Logging configuration")
    print(f"  + Multiple export formats")
    print(f"  + Outlier detection settings")
    
    print(f"\nIMPORTANT NOTES:")
    print(f"  * data/ folder is for YOUR raw data files")
    print(f"  * cache/ folder is for pipeline cache (auto-generated)")
    print(f"  * exports/ folder is for your study results")
    print(f"  * notebooks/ folder is for Jupyter analysis")
    
    return study_path

if __name__ == "__main__":
    # Example usage
    print("Research Pipeline Framework")
    print("For testing, use: python tests/run_all_tests.py")
