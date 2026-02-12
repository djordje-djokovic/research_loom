#!/usr/bin/env python3
"""
Test that the cache directory fix works correctly
"""

import sys
from pathlib import Path
import tempfile
import shutil

# Add the framework root to the path
sys.path.insert(0, str(Path(__file__).parent))

from studies.example_study.study_pipeline import create_pipeline, load_study_config


def test_cache_directory_fix():
    """Test that cache directory is created in the right place"""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Change to the temp directory to simulate being in the study directory
        original_cwd = Path.cwd()
        temp_path = Path(temp_dir)
        
        try:
            # Create a cache subdirectory
            cache_dir = temp_path / "cache"
            cache_dir.mkdir()
            
            # Create pipeline with explicit cache directory
            pipe = create_pipeline(cache_dir=str(cache_dir))
            
            # Load configuration
            config = load_study_config("base")
            
            # Run a simple test
            results = pipe.run_pipeline(config, materialize=["raw_data"])
            
            # Check that cache was created in the right place
            assert cache_dir.exists(), "Cache directory should exist"
            assert (cache_dir / "raw_data").exists(), "Node cache directory should exist"
            
            # Check that no nested studies folder was created
            studies_folders = list(temp_path.glob("**/studies"))
            assert len(studies_folders) == 0, f"Found unexpected studies folders: {studies_folders}"
            
            print("✓ Cache directory fix test passed")
            return True
            
        finally:
            # Restore original working directory
            import os
            os.chdir(original_cwd)


if __name__ == "__main__":
    print("Testing cache directory fix...")
    
    try:
        success = test_cache_directory_fix()
        if success:
            print("✅ Cache directory fix works correctly!")
        else:
            print("❌ Cache directory fix failed!")
            sys.exit(1)
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
