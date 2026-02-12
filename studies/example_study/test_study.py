#!/usr/bin/env python3
"""
Test script for example study
Demonstrates how to test pipeline functionality
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
    """Test the example study pipeline"""
    print("Testing example study...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir) / "cache"
        
        # Create pipeline with explicit cache directory
        pipe = study_pipeline.create_pipeline(cache_dir=str(cache_dir))
        
        # Load configuration
        config = study_pipeline.load_study_config("base")
        
        # Test that all nodes are properly configured
        assert pipe is not None
        assert config is not None
        assert len(pipe.nodes) == 5, f"Expected 5 nodes, got {len(pipe.nodes)}"
        
        # Test node names
        expected_nodes = {"raw_data", "processed_dataframe", "visualization", "statistics", "report"}
        actual_nodes = set(pipe.nodes.keys())
        assert actual_nodes == expected_nodes, f"Node mismatch: {actual_nodes} vs {expected_nodes}"
        
        # Test dependencies
        assert pipe.get_dependencies("raw_data") == []
        assert "raw_data" in pipe.get_dependencies("processed_dataframe")
        assert "processed_dataframe" in pipe.get_dependencies("visualization")
        assert "processed_dataframe" in pipe.get_dependencies("statistics")
        
        # Test running the pipeline
        print("  Running pipeline...")
        results, pipeline, config = study_pipeline.run_study("base", materialize=["report"])
        
        assert "report" in results, "Report should be in results"
        assert results["report"] is not None, "Report should not be None"
        
        print("  ✓ Pipeline execution successful")
        print("  ✓ All nodes properly configured")
        print("  ✓ Dependencies correctly set up")
        print("  ✓ Results generated successfully")
        
        print("\nExample study test passed! ✅")


if __name__ == "__main__":
    test_study()
