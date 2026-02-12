#!/usr/bin/env python3
"""
Basic tests for the research pipeline framework.
"""

import tempfile
import shutil
from pathlib import Path
import sys

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pipeline.core import ResearchPipeline, Node


def test_data_loader(inputs, config):
    """Test data loading function"""
    return {
        "data": f"Loaded data with config: {config}",
        "count": 100
    }


def test_processor(inputs, config):
    """Test data processing function"""
    data = inputs.get("test_data", {})
    return {
        "processed": f"Processed {data.get('count', 0)} items",
        "result": "success"
    }


def test_pipeline_basic():
    """Test basic pipeline functionality"""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir) / "cache"
        
        # Create pipeline
        pipe = ResearchPipeline(cache_dir=str(cache_dir))
        
        # Add nodes
        pipe.add_node(Node(
            name="test_data",
            func=test_data_loader,
            inputs=[],
            config_section="data"
        ))
        
        pipe.add_node(Node(
            name="test_processor",
            func=test_processor,
            inputs=["test_data"],
            config_section="processing"
        ))
        
        # Test configuration
        config = {
            "data": {"source": "test.csv"},
            "processing": {"method": "test"}
        }
        
        # Run pipeline
        results = pipe.run_pipeline(config, materialize=["test_processor"])
        
        # Verify results
        assert "test_processor" in results
        assert results["test_processor"]["result"] == "success"
        
        # Test cache
        assert pipe.is_cache_valid("test_data", pipe.get_node_cache_key(config, "test_data"))
        assert pipe.is_cache_valid("test_processor", pipe.get_node_cache_key(config, "test_processor"))
        
        print("✓ Basic pipeline test passed")


def test_export_functionality():
    """Test export functionality"""
    with tempfile.TemporaryDirectory() as temp_dir:
        cache_dir = Path(temp_dir) / "cache"
        export_dir = Path(temp_dir) / "exports"
        
        # Create pipeline
        pipe = ResearchPipeline(cache_dir=str(cache_dir))
        
        # Add a simple node
        pipe.add_node(Node(
            name="test_export",
            func=lambda inputs, config: {"message": "Hello, World!"},
            inputs=[],
            config_section="test"
        ))
        
        config = {"test": {}}
        
        # Run pipeline to create cache
        pipe.run_pipeline(config, materialize=["test_export"])
        
        # Test export
        try:
            result = pipe.export_node("test_export", config, str(export_dir))
            assert "index" in result
            assert "provenance" in result
            assert "count" in result
            print("✓ Export functionality test passed")
        except Exception as e:
            print(f"✗ Export test failed: {e}")
            raise


if __name__ == "__main__":
    print("Running pipeline tests...")
    test_pipeline_basic()
    test_export_functionality()
    print("All tests passed!")
