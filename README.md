# Research Loom

A parsimonious research pipeline framework that provides caching, artifact management, and export capabilities in a single file.

## Project Structure

```
research_loom/
├── pipeline/
│   └── core.py              # ResearchPipeline (your one-file framework) 
├── studies/
│   └── example_study/
│       ├── config/
│       │   ├── base.yaml
│       │   └── dev.yaml
│       ├── data/            # raw/external/processed if you keep data per study
│       ├── cache/           # ephemeral pipeline cache (auto-pruned)
│       ├── exports/         # durable, shareable outputs (never pruned)
│       ├── notebooks/       # optional
│       └── README.md
├── scripts/
│   └── run.py               # CLI entrypoint
├── tests/                   # All test files
├── pyproject.toml
└── README.md
```

## Key Features

- **Single-file framework**: All pipeline logic in `pipeline/core.py`
- **Intelligent caching**: Automatic cache invalidation based on dependencies
- **Configurable storage formats**: Explicit control over how data is stored (parquet, JSON.zst, etc.)
- **Artifact management**: Automatic spilling of large objects to files
- **Export capabilities**: Export nodes with provenance tracking
- **Study organization**: Per-study cache and exports directories

## Storage Formats

Research Loom supports multiple storage formats for different data types. You can explicitly control how each output is stored by specifying `storage_formats` in your Node definition:

### Supported Formats

| Format | Description | Best For | Requirements |
|--------|-------------|----------|-------------|
| `parquet` | Columnar format | Tabular data, DataFrames | pandas/pyarrow |
| `json.zst` | Compressed JSON | Nested dictionaries, complex objects | zstandard |
| `jsonl.zst` | Compressed JSON Lines | Large lists of objects | zstandard |
| `json` | Plain JSON | Small objects, configuration | - |
| `csv` | Comma-separated values | Simple tabular data | - |
| `png` | PNG image | Matplotlib/Plotly figures | matplotlib/plotly |
| `html` | HTML | Interactive plots | plotly |

### Example Usage

```python
# Explicitly control storage formats
pipeline.add_node(Node(
    name="raw_data",
    func=load_data,
    inputs=[],
    config_section="data",
    storage_formats={
        "companies": "json.zst",      # Preserve nested structure
        "metadata": "json"            # Simple metadata
    }
))

pipeline.add_node(Node(
    name="analysis",
    func=analyze_data,
    inputs=["raw_data"],
    config_section="analysis",
    storage_formats={
        "results": "parquet",        # Tabular results
        "plots": "png"               # Generated plots
    }
))
```

### Automatic Format Selection

If no `storage_formats` are specified, the framework automatically chooses based on data characteristics:

- **Lists of dicts (≥100 items)** → `parquet`
- **Large nested dicts (>8MB)** → `json.zst`
- **Large object lists (≥1000 items)** → `jsonl.zst`
- **Matplotlib figures** → `png`
- **Plotly figures** → `png` (fallback to `html`)

## Quick Start

### 1. Define Your Pipeline

```python
from pipeline.core import ResearchPipeline, Node

# Create pipeline instance
pipe = ResearchPipeline(cache_dir="studies/my_study/cache")

# Define nodes
pipe.add_node(Node(
    name="load_data",
    func=load_data_function,
    inputs=[],
    config_section="data"
))

pipe.add_node(Node(
    name="process_data", 
    func=process_data_function,
    inputs=["load_data"],
    config_section="processing"
))

pipe.add_node(Node(
    name="report",
    func=create_report_function,
    inputs=["process_data"],
    config_section="reporting",
    materialize_by_default=True  # Mark as output
))
```

### 2. Run Pipeline

```python
# Load configuration
import yaml
with open("studies/my_study/config/base.yaml") as f:
    config = yaml.safe_load(f)

# Run pipeline
results = pipe.run_pipeline(config, materialize=["report"])
```

### 3. Export Results

```python
# Export just the report
pipe.export_node("report", config, "studies/my_study/exports/report_2025-01-17")

# Export report with all upstream artifacts
pipe.export_node("report", config, "studies/my_study/exports/full_bundle_2025-01-17", 
                 include_ancestors=True)
```

## Configuration

Studies use YAML configuration files:

```yaml
# studies/my_study/config/base.yaml
data:
  source: "studies/my_study/data/raw"
  format: "csv"

model:
  type: "cox_regression"
  parameters:
    alpha: 0.05
    max_iter: 1000
```

### Required Pipeline Reporting Contract

`ResearchPipeline.run_pipeline()` now requires a strict reporting block in config:

```yaml
logging:
  pipeline_report:
    enabled: true
    format: "both"            # json | html | both
    output_dir: "run_reports"
    keep_last_n: 20
    include_edge_payloads: true
    write_latest_pointer: true
```

Projects consuming `research_loom` (for example `secondary_sale`) must add this block before upgrading to this version.

## Cache Management

- **Cache location**: `studies/<study>/cache/` (per study)
- **Automatic pruning**: Use `pipe.prune_cache()` to remove old cache files
- **Cache invalidation**: Dependencies automatically invalidate downstream caches

## Export System

Exports are stored in `studies/<study>/exports/` with:

- `index.json`: List of exported files with origins
- `provenance.json`: Cache metadata for reproducibility
- Artifact files: The actual exported data/plots/models

## CLI Usage

```bash
# Run a study
python scripts/run.py studies/my_study --config base --materialize report

# Show execution plan
python scripts/run.py studies/my_study --plan

# Export results
python scripts/run.py studies/my_study --export studies/my_study/exports/report_2025-01-17
```

## Installation

```bash
# Install in development mode (from research_loom root directory)
pip install -e .

# Install with optional dependencies
pip install -e .[data,viz,dev]

# Or install all optional dependencies
pip install -e .[all]
```

After installation, you can import the framework from anywhere:

```python
from research_loom.pipeline.core import ResearchPipeline, Node
from research_loom.cli import run_cli
```

## Dependencies

- **Core**: `pyyaml` (configuration)
- **Data**: `pandas`, `pyarrow`, `zstandard` (optional)
- **Visualization**: `matplotlib`, `plotly` (optional)

## Examples

See `studies/example_study/` for a complete example study structure.

## API Reference

### ResearchPipeline

Main pipeline class with methods:
- `add_node()`: Add a processing node
- `run_pipeline()`: Execute the pipeline
- `export_node()`: Export node artifacts
- `prune_cache()`: Clean up old cache files
- `invalidate()`: Remove cache for specific node

### Node

Processing node definition:
- `name`: Node identifier
- `func`: Processing function
- `inputs`: List of input node names
- `config_section`: Configuration section name
- `version`: Node version (for cache invalidation)
- `materialize_by_default`: Mark as output node

## Testing

Run the test suite:

```bash
# Run all tests
python -m pytest tests/

# Run with coverage
python -m pytest tests/ --cov=pipeline
```

## License

MIT License - see LICENSE file for details.