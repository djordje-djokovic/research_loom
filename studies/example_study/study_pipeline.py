#!/usr/bin/env python3
"""
Example Study Pipeline - Comprehensive Framework Demonstration

This example study showcases all key features of the Research Loom framework:
- Multiple storage formats (parquet, json.zst, html)
- Visualizations (Plotly figures)
- DataFrames and tabular data
- Cache invalidation strategies
- Config-driven behavior
- Materialization options
- Viewing cached results
"""

import sys
from pathlib import Path

# Add the framework root to the path
# If research_loom is pip-installed, this can be simplified to just imports
framework_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(framework_root))

from research_loom.pipeline.core import ResearchPipeline, Node
import yaml
import time
import hashlib
import json
import numpy as np

# Optional imports for enhanced features
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import plotly.graph_objects as go
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False


# ============================================================================
# Node Functions - Demonstrating Different Data Types and Storage Formats
# ============================================================================

def load_raw_data(inputs, config):
    """
    Load raw data - demonstrates basic data loading.
    Returns a simple dictionary (stored as json.zst by default).
    """
    source = config.get('source', 'sample_data')
    sample_size = config.get('sample_size', 100)
    
    print(f"Loading raw data from {source}...")
    time.sleep(0.1)  # Simulate data loading
    
    return {
        "data": f"Raw data from {source}",
        "sample_size": sample_size,
        "source": source,
        "timestamp": time.time(),
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
    }


def process_dataframe(inputs, config):
    """
    Process data into a DataFrame - demonstrates parquet storage.
    Returns a DataFrame that will be stored as parquet.
    """
    raw_data = inputs.get("raw_data", {})
    sample_size = raw_data.get("sample_size", 100)
    
    if not HAS_PANDAS:
        return {"error": "pandas not available"}
    
    print(f"Processing DataFrame with {sample_size} rows...")
    
    # Create sample DataFrame
    np.random.seed(42)
    df = pd.DataFrame({
        "id": range(sample_size),
        "value": np.random.randn(sample_size),
        "category": np.random.choice(["A", "B", "C"], sample_size),
        "score": np.random.uniform(0, 100, sample_size),
    })
    
    return {
        "processed_df": df,  # Will be stored as parquet
        "summary": {
            "rows": len(df),
            "columns": list(df.columns),
            "mean_score": float(df["score"].mean()),
        },
        "timestamp": time.time(),
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
    }


def create_visualization(inputs, config):
    """
    Create a Plotly visualization - demonstrates HTML figure storage.
    Returns a Plotly figure that will be stored as HTML.
    """
    processed = inputs.get("processed_dataframe", {})
    
    if not HAS_PLOTLY:
        return {"error": "plotly not available"}
    
    if not HAS_PANDAS:
        return {"error": "pandas not available"}
    
    df = processed.get("processed_df")
    if df is None or not isinstance(df, pd.DataFrame):
        return {"error": "No DataFrame available"}
    
    print("Creating visualization...")
    
    # Create a simple scatter plot
    fig = go.Figure()
    
    for category in df["category"].unique():
        cat_data = df[df["category"] == category]
        fig.add_trace(go.Scatter(
            x=cat_data["value"],
            y=cat_data["score"],
            mode="markers",
            name=f"Category {category}",
            marker=dict(size=8, opacity=0.7)
        ))
    
    fig.update_layout(
        title="Example Visualization: Score vs Value by Category",
        xaxis_title="Value",
        yaxis_title="Score",
        template="plotly_dark",
        width=800,
        height=600,
    )
    
    return {
        "figure": fig,  # Will be stored as HTML
        "timestamp": time.time(),
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
    }


def calculate_statistics(inputs, config):
    """
    Calculate statistics - demonstrates JSON storage with nested data.
    Returns complex nested data (stored as json.zst).
    """
    processed = inputs.get("processed_dataframe", {})
    df = processed.get("processed_df")
    
    if df is None or not HAS_PANDAS:
        return {
            "error": "No DataFrame available",
            "timestamp": time.time(),
        }
    
    print("Calculating statistics...")
    
    stats = {
        "descriptive": {
            "mean": float(df["value"].mean()),
            "std": float(df["value"].std()),
            "min": float(df["value"].min()),
            "max": float(df["value"].max()),
        },
        "by_category": {},
    }
    
    for category in df["category"].unique():
        cat_data = df[df["category"] == category]
        stats["by_category"][category] = {
            "count": int(len(cat_data)),
            "mean_score": float(cat_data["score"].mean()),
            "mean_value": float(cat_data["value"].mean()),
        }
    
    return {
        "statistics": stats,
        "timestamp": time.time(),
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
    }


def generate_report(inputs, config):
    """
    Generate final report - demonstrates combining multiple inputs.
    Returns a comprehensive report (stored as json.zst).
    """
    raw_data = inputs.get("raw_data", {})
    processed = inputs.get("processed_dataframe", {})
    stats = inputs.get("statistics", {})
    viz = inputs.get("visualization", {})
    
    print("Generating final report...")
    
    report = {
        "study_info": {
            "name": config.get("study", {}).get("name", "example_study"),
            "version": config.get("study", {}).get("version", "1.0.0"),
            "description": config.get("study", {}).get("description", ""),
        },
        "data_summary": {
            "source": raw_data.get("source", "unknown"),
            "sample_size": raw_data.get("sample_size", 0),
        },
        "processing_summary": processed.get("summary", {}),
        "statistics": stats.get("statistics", {}),
        "visualization_created": viz.get("figure") is not None,
        "timestamp": time.time(),
    }
    
    return {
        "report": report,
        "timestamp": time.time(),
        "config_hash": hashlib.md5(json.dumps(config, sort_keys=True).encode()).hexdigest()[:8]
    }


# ============================================================================
# Pipeline Definition
# ============================================================================

def create_pipeline(cache_dir: str = None) -> ResearchPipeline:
    """
    Create and configure the example study pipeline.
    
    This demonstrates:
    - Node dependencies (DAG structure)
    - Different storage formats (parquet, json.zst, html)
    - Config sections for each node
    - Materialization flags
    """
    if cache_dir is None:
        cache_dir = "cache"  # Relative to the study directory
    
    # Initialize pipeline
    pipeline = ResearchPipeline(cache_dir=cache_dir)
    
    # Node 1: Load raw data (stored as json.zst by default)
    pipeline.add_node(Node(
        name="raw_data",
        func=load_raw_data,
        inputs=[],  # No dependencies
        config_section="data"
    ))
    
    # Node 2: Process into DataFrame (explicitly stored as parquet)
    pipeline.add_node(Node(
        name="processed_dataframe",
        func=process_dataframe,
        inputs=["raw_data"],
        config_section="processing",
        storage_formats={"processed_df": "parquet"}  # DataFrame stored as parquet
    ))
    
    # Node 3: Create visualization (stored as HTML)
    pipeline.add_node(Node(
        name="visualization",
        func=create_visualization,
        inputs=["processed_dataframe"],
        config_section="visualization",
        storage_formats={"figure": "html"}  # Plotly figure stored as HTML
    ))
    
    # Node 4: Calculate statistics (stored as json.zst by default)
    pipeline.add_node(Node(
        name="statistics",
        func=calculate_statistics,
        inputs=["processed_dataframe"],
        config_section="statistics"
    ))
    
    # Node 5: Generate final report (combines multiple inputs)
    pipeline.add_node(Node(
        name="report",
        func=generate_report,
        inputs=["raw_data", "processed_dataframe", "statistics", "visualization"],
        config_section="report",
        materialize_by_default=True  # Mark as default output
    ))
    
    return pipeline


def load_study_config(config_name: str = "base") -> dict:
    """Load study configuration from YAML file"""
    config_path = Path(__file__).parent / "config" / f"{config_name}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def run_study(config_name: str = "base", materialize: list = None):
    """
    Run the example study.
    
    Args:
        config_name: Name of config file (without .yaml extension)
        materialize: What to return - can be:
            - List of node names: ["report", "visualization"]
            - "all": Return all nodes
            - "sinks": Return only default outputs (materialize_by_default=True)
            - "none": Return nothing
    
    Returns:
        Tuple of (results, pipeline, config)
    """
    if materialize is None:
        materialize = "sinks"  # Default: return nodes marked as outputs
    
    # Load configuration
    config = load_study_config(config_name)
    
    # Create pipeline
    pipeline = create_pipeline()
    
    # Run pipeline
    print(f"Running example study with config: {config_name}")
    print(f"Materializing: {materialize}")
    
    results = pipeline.run_pipeline(config, materialize=materialize)
    
    return results, pipeline, config


if __name__ == "__main__":
    # Import CLI handler
    from research_loom.cli import run_cli
    
    # Use the generic CLI
    run_cli(
        create_pipeline=create_pipeline,
        load_config=load_study_config,
        run_study=run_study,
        description="Example Study - Comprehensive Framework Demonstration",
        epilog="""
Examples:
  # Run full pipeline with default config
  python study_pipeline.py
  
  # Run with specific config
  python study_pipeline.py --config dev
  
  # View cached visualization (opens in browser)
  python study_pipeline.py --view visualization
  
  # View cached report
  python study_pipeline.py --view report
  
  # Force re-run processing node and all downstream nodes
  python study_pipeline.py --force-downstream processed_dataframe
  
  # Force re-run only statistics node
  python study_pipeline.py --force-only statistics
  
  # Materialize specific nodes
  python study_pipeline.py --materialize visualization report
        """
    )
