#!/usr/bin/env python3
"""
Simple demonstration of nested cache layout (no external dependencies)
"""
import json, sys, os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.core import ResearchPipeline, Node

def create_large_list(inputs, config):
    """Create a large list that should be spilled to JSONL.zst"""
    size = config.get('size', 2000)
    return [
        {
            'id': i,
            'data': f'item_{i}',
            'metadata': {f'key_{j}': f'value_{j}' for j in range(5)},
            'nested': {'level1': {'level2': f'deep_value_{i}'}}
        }
        for i in range(size)
    ]

def create_large_dict(inputs, config):
    """Create a large nested dict that should be spilled to JSON.zst"""
    depth = config.get('depth', 3)
    size = config.get('size', 100)
    
    def create_nested(d, current_depth):
        if current_depth <= 0:
            return {f'leaf_{i}': f'value_{i}' for i in range(size)}
        return {f'level_{current_depth}_{i}': create_nested(d, current_depth-1) 
                for i in range(10)}
    
    return create_nested({}, depth)

def process_data(inputs, config):
    """Process data and return mixed results"""
    large_list = inputs['large_list']
    large_dict = inputs['large_dict']
    
    # Handle artifact pointers - load if needed
    load_artifact = inputs.get('_load')
    
    # Check if large_list is an artifact pointer
    if isinstance(large_list, dict) and large_list.get('__artifact__'):
        # Load the actual data for processing
        actual_list = load_artifact(large_list)
        list_size = len(actual_list)
        small_sample = actual_list[:5]  # small subset
    else:
        # It's already loaded
        actual_list = large_list
        list_size = len(actual_list)
        small_sample = actual_list[:5]  # small subset
    
    # Check if large_dict is an artifact pointer
    if isinstance(large_dict, dict) and large_dict.get('__artifact__'):
        # Load the actual data for processing
        actual_dict = load_artifact(large_dict)
        dict_keys = len(actual_dict)
    else:
        # It's already loaded
        actual_dict = large_dict
        dict_keys = len(actual_dict)
    
    # Small results stay inline
    summary = {
        'list_size': list_size,
        'dict_keys': dict_keys,
        'small_string': 'This stays inline',
        'small_list': [1, 2, 3, 4, 5]
    }
    
    return {
        'summary': summary,  # stays inline
        'small_sample': small_sample,  # small subset, stays inline
        'large_list': large_list,  # will be spilled to jsonl.zst (or already is)
        'large_dict': large_dict  # will be spilled to json.zst (or already is)
    }

def demonstrate_loading(inputs, config):
    """Demonstrate lazy loading of artifacts"""
    load_artifact = inputs.get('_load')
    if not load_artifact:
        return {"error": "No _load helper provided"}
    
    result = {
        'message': 'Demonstrating lazy loading',
        'available_inputs': list(inputs.keys())
    }
    
    # Check if we have artifacts to load
    if 'processed_data' in inputs:
        processed = inputs['processed_data']
        
        # Check for artifacts
        if isinstance(processed, dict):
            for key, value in processed.items():
                if isinstance(value, dict) and value.get('__artifact__'):
                    result[f'{key}_is_artifact'] = True
                    result[f'{key}_format'] = value.get('format')
                    result[f'{key}_path'] = value.get('path')
                    
                    # Demonstrate lazy loading
                    try:
                        loaded_data = load_artifact(value)
                        result[f'{key}_loaded'] = True
                        result[f'{key}_type'] = type(loaded_data).__name__
                        if hasattr(loaded_data, '__len__'):
                            result[f'{key}_length'] = len(loaded_data)
                    except Exception as e:
                        result[f'{key}_load_error'] = str(e)
                else:
                    result[f'{key}_is_inline'] = True
    
    return result

def main():
    """Demonstrate the nested cache layout"""
    print("="*80)
    print("SIMPLE NESTED CACHE LAYOUT DEMONSTRATION")
    print("="*80)
    
    # Create pipeline
    pipeline = ResearchPipeline(cache_dir="cache/simple_artifact_demo")
    # pipeline.clear_cache()
    
    # Add nodes
    pipeline.add_node(Node("large_list", create_large_list, [], "data"))
    pipeline.add_node(Node("large_dict", create_large_dict, [], "data"))
    pipeline.add_node(Node("processed_data", process_data, 
                          ["large_list", "large_dict"], "processing"))
    pipeline.add_node(Node("loading_demo", demonstrate_loading, 
                          ["processed_data"], "demo"))
    
    # Run pipeline
    config = {
        "data": {
            "size": 1000,
            "depth": 3
        }
    }
    
    print("\n1. RUNNING PIPELINE...")
    results = pipeline.run_pipeline(config, materialize="all")
    
    print("\n2. CACHE STRUCTURE:")
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
    
    print("\n3. LAZY LOADING DEMONSTRATION:")
    loading_result = results['loading_demo']
    for key, value in loading_result.items():
        print(f"   {key}: {value}")
    
    print("\n4. CACHE METADATA INSPECTION:")
    # Load and inspect cache metadata for processed_data
    if 'processed_data' in cache_info and cache_info['processed_data']:
        cache_key = cache_info['processed_data'][0]
        cache_file = pipeline.cache_dir / "processed_data" / cache_key / "cache.json"
        
        with open(cache_file, 'r') as f:
            cache_data = json.load(f)
            metadata = cache_data.get('_metadata', {})
            
            print(f"   Node: {metadata.get('node')}")
            print(f"   Version: {metadata.get('version')}")
            print(f"   Cache Key: {metadata.get('cache_key')}")
            print(f"   Created: {metadata.get('created_at')}")
            print(f"   Input Keys: {list(metadata.get('input_keys', {}).keys())}")
            print(f"   Ancestor Keys: {len(metadata.get('ancestor_keys', []))} ancestors")
    
    print("\n" + "="*80)
    print("DEMONSTRATION COMPLETE!")
    print("✅ Nested cache layout: cache/node_name/cache_key/")
    print("✅ Artifact spilling: large data → artifacts/")
    print("✅ Lazy loading: _load helper for on-demand access")
    print("✅ Parsimonious: only spill what's large")
    print("="*80)
    
    # Clean up
    # pipeline.clear_cache()

if __name__ == "__main__":
    main()
