#!/usr/bin/env python3
"""
Test that execute_node returns consistent wire format (artifact pointers)
"""
import json
from pipeline.core import ResearchPipeline, Node

def create_large_data(inputs, config):
    """Create large data that will be spilled to artifacts"""
    size = config.get('size', 1000)
    return {
        'large_list': [
            {'id': i, 'data': f'item_{i}'} 
            for i in range(size)
        ],
        'small_data': 'stays inline',
        'metadata': {'count': size}
    }

def process_data(inputs, config):
    """Process data and check for consistency"""
    raw_data = inputs['raw_data']
    
    # Check if we get artifact pointers (wire format)
    result = {
        'input_has_artifacts': False,
        'input_artifact_keys': [],
        'input_small_keys': [],
        'processed': True
    }
    
    if isinstance(raw_data, dict):
        for key, value in raw_data.items():
            if isinstance(value, dict) and value.get('__artifact__'):
                result['input_has_artifacts'] = True
                result['input_artifact_keys'].append(key)
            else:
                result['input_small_keys'].append(key)
    
    return result

def test_wire_format_consistency():
    """Test that execute_node returns consistent wire format"""
    print("="*60)
    print("TESTING WIRE FORMAT CONSISTENCY")
    print("="*60)
    
    # Create pipeline
    pipeline = ResearchPipeline(cache_dir="cache/test_wire_format")
    pipeline.clear_cache()
    
    # Add nodes
    pipeline.add_node(Node("raw_data", create_large_data, [], "data"))
    pipeline.add_node(Node("processed_data", process_data, ["raw_data"], "processing"))
    
    # Run pipeline twice to test consistency
    config = {"data": {"size": 500}}
    
    print("\n1. FIRST RUN (should create artifacts and return wire format):")
    results1 = pipeline.run_pipeline(config, materialize="all")
    
    print("\n2. SECOND RUN (should load from cache and return same wire format):")
    results2 = pipeline.run_pipeline(config, materialize="all")
    
    print("\n3. COMPARING RESULTS:")
    print(f"   First run processed_data: {results1['processed_data']}")
    print(f"   Second run processed_data: {results2['processed_data']}")
    
    # Check consistency
    first_has_artifacts = results1['processed_data']['input_has_artifacts']
    second_has_artifacts = results2['processed_data']['input_has_artifacts']
    
    print(f"\n4. CONSISTENCY CHECK:")
    print(f"   First run has artifacts: {first_has_artifacts}")
    print(f"   Second run has artifacts: {second_has_artifacts}")
    print(f"   Consistent: {first_has_artifacts == second_has_artifacts}")
    
    if first_has_artifacts and second_has_artifacts:
        print("   ✅ SUCCESS: Both runs return wire format with artifact pointers")
    elif not first_has_artifacts and not second_has_artifacts:
        print("   ⚠️  WARNING: No artifacts created (data might be too small)")
    else:
        print("   ❌ ERROR: Inconsistent wire format between runs!")
    
    print("\n5. CACHE STRUCTURE:")
    cache_info = pipeline.get_cache_info()
    for node_name, cache_dirs in cache_info.items():
        print(f"   {node_name}: {len(cache_dirs)} cache(s)")
        for cache_dir in cache_dirs:
            node_cache_dir = pipeline.cache_dir / node_name / cache_dir
            cache_file = node_cache_dir / "cache.json"
            artifacts_dir = node_cache_dir / "artifacts"
            
            print(f"     {cache_dir}/")
            print(f"       cache.json ({cache_file.stat().st_size} bytes)")
            
            if artifacts_dir.exists():
                artifacts = list(artifacts_dir.iterdir())
                print(f"       artifacts/ ({len(artifacts)} files)")
                for artifact in artifacts:
                    print(f"         {artifact.name} ({artifact.stat().st_size} bytes)")
            else:
                print(f"       artifacts/ (none - all data inline)")
    
    print("\n" + "="*60)
    print("WIRE FORMAT CONSISTENCY TEST COMPLETE!")
    if first_has_artifacts == second_has_artifacts:
        print("✅ SUCCESS: Consistent wire format across runs")
    else:
        print("❌ FAILED: Inconsistent wire format")
    print("="*60)
    
    # Clean up
    pipeline.clear_cache()

if __name__ == "__main__":
    test_wire_format_consistency()
