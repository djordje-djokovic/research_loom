#!/usr/bin/env python3
"""
Quick test of the example study
"""

import sys
from pathlib import Path

# Add the framework root to the path
sys.path.insert(0, str(Path(__file__).parent))

from studies.example_study.study_pipeline import create_pipeline, load_study_config
import tempfile
import shutil


def test_example_study():
    """Test the example study pipeline"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create temporary cache directory
        cache_dir = Path(temp_dir) / "cache"
        
        # Create pipeline with explicit cache directory
        pipe = create_pipeline(cache_dir=str(cache_dir))
        
        # Load configuration
        config = load_study_config("base")
        
        # Test that all nodes are properly configured
        expected_nodes = ["raw_data", "variables", "global_variables", "cox_data", "cox_model", "results"]
        for node_name in expected_nodes:
            assert node_name in pipe.nodes, f"Node {node_name} not found"
        
        # Test that results node is marked as output
        assert pipe.nodes["results"].materialize_by_default == True
        
        print("✓ Pipeline structure test passed")
        
        # Test running the pipeline
        results = pipe.run_pipeline(config, materialize=["results"])
        
        # Verify results
        assert "results" in results
        results_data = results["results"]
        
        # Check that results have the expected structure
        assert "model_summary" in results_data
        assert "coefficients" in results_data
        assert "model_fit" in results_data
        
        print("✓ Pipeline execution test passed")
        
        # Test export functionality
        export_dir = Path(temp_dir) / "exports" / "test_export"
        export_result = pipe.export_node("results", config, str(export_dir))
        
        assert "index" in export_result
        assert "provenance" in export_result
        assert "count" in export_result
        
        # Check that export files exist
        assert (export_dir / "index.json").exists()
        assert (export_dir / "provenance.json").exists()
        
        print("✓ Export functionality test passed")
        
        return True


if __name__ == "__main__":
    print("Testing example study...")
    
    try:
        success = test_example_study()
        if success:
            print("\n✅ Example study test passed!")
        else:
            print("\n❌ Example study test failed!")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Example study test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
