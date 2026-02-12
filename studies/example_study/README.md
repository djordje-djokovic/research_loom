# Example Study - Comprehensive Framework Demonstration

This example study showcases **all key features** of the Research Loom framework. It's designed to be a perfect starting point for understanding how to use the framework effectively.

## 🎯 What This Example Demonstrates

### 1. **Multiple Storage Formats**
- **Parquet**: Efficient storage for DataFrames (`processed_dataframe` node)
- **JSON.zst**: Compressed JSON for nested data (`raw_data`, `statistics`, `report` nodes)
- **HTML**: Interactive Plotly visualizations (`visualization` node)

### 2. **Node Dependencies (DAG)**
```
raw_data
  ├── processed_dataframe
  │     ├── visualization
  │     └── statistics
  └── report (depends on all)
```

### 3. **Config-Driven Behavior**
Each node uses a specific config section, allowing independent configuration:
- `data` → `raw_data` node
- `processing` → `processed_dataframe` node
- `visualization` → `visualization` node
- `statistics` → `statistics` node
- `report` → `report` node

### 4. **Cache Management**
- Automatic cache invalidation when configs change
- Selective cache invalidation with `--force-downstream`, `--force-only`, etc.
- Cache reuse when configs are unchanged

### 5. **Materialization Options**
- `--materialize all`: Return all nodes
- `--materialize report visualization`: Return specific nodes
- `--materialize sinks`: Return only default outputs (nodes with `materialize_by_default=True`)

### 6. **Viewing Results**
- `--view visualization`: Opens Plotly HTML in browser
- `--view report`: Shows JSON data summary
- `--view processed_dataframe`: Displays DataFrame as HTML table

## 📁 Project Structure

```
example_study/
├── config/
│   ├── base.yaml          # Base configuration
│   └── dev.yaml           # Development overrides
├── data/                   # Raw data files (add your data here)
├── cache/                  # Pipeline cache (auto-generated, safe to delete)
├── exports/                # Durable outputs (never auto-deleted)
├── notebooks/              # Jupyter notebooks for exploration
├── study_pipeline.py       # Main pipeline definition
├── test_study.py           # Test script
└── README.md               # This file
```

## 🚀 Quick Start

### Installation

First, install the Research Loom framework:

```bash
# From the research_loom root directory
pip install -e .

# Or with optional dependencies
pip install -e .[data,viz]
```

### Running the Study

```bash
# Run with default config (base.yaml)
python study_pipeline.py

# Run with development config (faster, smaller dataset)
python study_pipeline.py --config dev

# View cached visualization
python study_pipeline.py --view visualization

# View cached report
python study_pipeline.py --view report

# Force re-run a node and all downstream nodes
python study_pipeline.py --force-downstream processed_dataframe

# Force re-run only a specific node
python study_pipeline.py --force-only statistics

# Materialize specific nodes
python study_pipeline.py --materialize visualization report
```

## 📊 Understanding the Nodes

### 1. `raw_data` Node
- **Inputs**: None (entry point)
- **Output**: Dictionary with metadata
- **Storage**: `json.zst` (default for dictionaries)
- **Config Section**: `data`

### 2. `processed_dataframe` Node
- **Inputs**: `raw_data`
- **Output**: Pandas DataFrame + summary dictionary
- **Storage**: `parquet` for DataFrame, `json.zst` for summary
- **Config Section**: `processing`
- **Demonstrates**: Explicit storage format specification

### 3. `visualization` Node
- **Inputs**: `processed_dataframe`
- **Output**: Plotly figure
- **Storage**: `html` (interactive Plotly figure)
- **Config Section**: `visualization`
- **Demonstrates**: HTML figure storage and viewing

### 4. `statistics` Node
- **Inputs**: `processed_dataframe`
- **Output**: Nested statistics dictionary
- **Storage**: `json.zst` (default)
- **Config Section**: `statistics`
- **Demonstrates**: Complex nested data storage

### 5. `report` Node
- **Inputs**: `raw_data`, `processed_dataframe`, `statistics`, `visualization`
- **Output**: Comprehensive report combining all inputs
- **Storage**: `json.zst` (default)
- **Config Section**: `report`
- **Materialize**: `True` (default output)
- **Demonstrates**: Combining multiple inputs

## 🔧 Configuration

### Base Configuration (`config/base.yaml`)

Each config section controls a specific node:

```yaml
data:                    # Controls raw_data node
  source: "sample_data"
  sample_size: 100

processing:              # Controls processed_dataframe node
  method: "standard"
  transformations: [...]

visualization:           # Controls visualization node
  theme: "plotly_dark"
  width: 800
  height: 600

statistics:              # Controls statistics node
  calculate_descriptive: true

report:                  # Controls report node
  output_format: "json"
```

### Development Configuration (`config/dev.yaml`)

Overrides base.yaml for faster development:

```yaml
data:
  sample_size: 50  # Smaller dataset
```

## 💾 Storage Formats

The framework automatically chooses storage formats, but you can override:

```python
pipeline.add_node(Node(
    name="my_node",
    func=my_function,
    inputs=["other_node"],
    storage_formats={
        "dataframe": "parquet",    # DataFrame → parquet
        "figure": "html",          # Plotly figure → HTML
        "metadata": "json.zst",    # Dictionary → compressed JSON
    }
))
```

**Supported Formats:**
- `parquet`: DataFrames (requires pandas, pyarrow)
- `json.zst`: Compressed JSON (requires zstandard)
- `json`: Plain JSON
- `html`: HTML files (for Plotly figures)
- `png`: PNG images (for matplotlib figures)

## 🔄 Cache Management

### Automatic Cache Invalidation

The framework automatically invalidates cache when:
- Node code changes (version bump)
- Config section changes
- Input node cache changes

### Manual Cache Invalidation

```bash
# Force re-run node and all downstream nodes
python study_pipeline.py --force-downstream processed_dataframe

# Force re-run node and all upstream dependencies
python study_pipeline.py --force-upstream statistics

# Force re-run only specific node (no dependencies)
python study_pipeline.py --force-only statistics

# Force re-run everything
python study_pipeline.py --force-all
```

## 👀 Viewing Results

### View Visualizations

```bash
# Opens Plotly HTML in browser
python study_pipeline.py --view visualization
```

### View DataFrames

```bash
# Displays DataFrame as HTML table
python study_pipeline.py --view processed_dataframe
```

### View JSON Data

```bash
# Shows JSON summary
python study_pipeline.py --view report
```

## 📦 Materialization

Control what nodes are returned:

```bash
# Return all nodes
python study_pipeline.py --materialize all

# Return specific nodes
python study_pipeline.py --materialize visualization report

# Return only default outputs (materialize_by_default=True)
python study_pipeline.py --materialize sinks

# Return nothing (just execute)
python study_pipeline.py --materialize none
```

## 🧪 Testing

Run the test script:

```bash
python test_study.py
```

## 📚 Key Concepts

### 1. **Config Sections**
Each node uses a specific config section. This allows:
- Independent configuration per node
- Cache invalidation only when relevant config changes
- Clear separation of concerns

### 2. **Storage Formats**
Explicitly specify how data should be stored:
- DataFrames → `parquet` (efficient, columnar)
- Figures → `html` or `png` (viewable)
- Dictionaries → `json.zst` (compressed, readable)

### 3. **Cache Keys**
Cache keys are automatically generated from:
- Node name
- Node version
- Config section content
- Input node cache keys

### 4. **Dependencies**
The framework automatically:
- Determines execution order (topological sort)
- Only executes required nodes
- Reuses cached results when possible

## 🎓 Next Steps

1. **Modify the nodes**: Change the functions to do your own processing
2. **Add new nodes**: Create additional nodes with different storage formats
3. **Create new configs**: Add `config/production.yaml` for production settings
4. **Add real data**: Put your data files in the `data/` directory
5. **Create visualizations**: Use Plotly or matplotlib to create custom plots

## 📖 Additional Resources

- Framework documentation: See `research_loom/README.md`
- Framework code: See `research_loom/pipeline/core.py`
- CLI help: `python study_pipeline.py --help`

## 💡 Tips

1. **Use `--view` frequently**: Quickly check cached results without re-running
2. **Use `--force-only` for debugging**: Re-run just the node you're working on
3. **Use `dev.yaml` for development**: Faster iteration with smaller datasets
4. **Check cache directory**: See what's cached and understand cache structure
5. **Use materialization**: Only return what you need to save memory

## 🐛 Troubleshooting

### "Module not found" errors
- Make sure research_loom is installed: `pip install -e .`
- Or ensure the framework root is in your Python path

### Cache not updating
- Check if config actually changed (cache keys are based on config content)
- Use `--force-only <node>` to force re-run
- Check node version (bump version to invalidate cache)

### Visualization not opening
- Check if plotly is installed: `pip install plotly`
- Check browser default application
- Try `--view visualization` to see the file path

---

**Happy Researching! 🚀**
