#!/usr/bin/env python3
"""
Example demonstrating cache lineage and ancestor tracking
"""
import json
from pipeline.core import ResearchPipeline, Node

def dummy_raw_data(inputs, config):
    return {"data": f"raw_data_{config['industry']}"}

def dummy_variables(inputs, config):
    return {"variables": f"variables_{config['industry']}"}

def dummy_global_variables(inputs, config):
    return {"global_vars": f"global_{config['industry']}"}

def dummy_cox_data(inputs, config):
    return {"cox_data": f"cox_{config.get('covariates', ['default'])[0]}"}

def dummy_cox_model(inputs, config):
    return {"model": f"cox_model_{config.get('industry', 'default')}"}

def main():
    """Demonstrate lineage tracking"""
    print("="*60)
    print("CACHE LINEAGE DEMONSTRATION")
    print("="*60)
    
    # Create pipeline
    pipeline = ResearchPipeline(cache_dir="cache/lineage_demo")
    pipeline.clear_cache()
    
    # Add nodes with dependencies: raw_data -> variables -> global_variables -> cox_data -> cox_model
    pipeline.add_node(Node("raw_data", dummy_raw_data, [], "raw_data"))
    pipeline.add_node(Node("variables", dummy_variables, ["raw_data"], "variables"))
    pipeline.add_node(Node("global_variables", dummy_global_variables, ["variables"], "global_variables"))
    pipeline.add_node(Node("cox_data", dummy_cox_data, ["variables", "global_variables"], "cox_data"))
    pipeline.add_node(Node("cox_model", dummy_cox_model, ["cox_data"], "cox_model"))
    
    # Run pipeline to create cache
    config = {
        "raw_data": {"industry": "Software"},
        "variables": {"industry": "Software"},
        "global_variables": {"industry": "Software"},
        "cox_data": {"covariates": ["log_time"]},
        "models": {"industry": "Software"}
    }
    
    results = pipeline.run_pipeline(config, materialize="sinks")
    
    # Show lineage for cox_model (most downstream node)
    print("\n1. LINEAGE FOR cox_model:")
    print("   Dependencies: cox_data -> variables -> raw_data")
    print("   Global variables: global_variables -> variables -> raw_data")
    
    # Load and inspect cache metadata
    cache_files = list(pipeline.cache_dir.glob("cox_model_*.json"))
    if cache_files:
        with open(cache_files[0], 'r') as f:
            cache_data = json.load(f)
            metadata = cache_data.get("_metadata", {})
            
            print(f"\n2. CACHE METADATA FOR cox_model:")
            print(f"   Node: {metadata.get('node')}")
            print(f"   Version: {metadata.get('version')}")
            print(f"   Cache Key: {metadata.get('cache_key')}")
            
            print(f"\n3. DIRECT INPUTS (input_keys):")
            input_keys = metadata.get('input_keys', {})
            for node, key in input_keys.items():
                print(f"   {node}: {key}")
            
            print(f"\n4. FULL ANCESTOR PATH (ancestor_keys):")
            ancestor_keys = metadata.get('ancestor_keys', [])
            for ancestor in ancestor_keys:
                print(f"   {ancestor['node']}: {ancestor['key']}")
            
            print(f"\n5. CONFIG SECTIONS AFFECTING THIS NODE:")
            section_hashes = metadata.get('section_hashes', {})
            for section, hash_val in section_hashes.items():
                print(f"   {section}: {hash_val}")
    
    print(f"\n" + "="*60)
    print("LINEAGE SUMMARY:")
    print("✅ input_keys: Direct parent cache keys (immediate dependencies)")
    print("✅ ancestor_keys: Full upstream path (all ancestors)")
    print("✅ section_hashes: Config sections that affect this node")
    print("✅ Any node can be traced back to its full lineage!")
    print("="*60)
    
    # Clean up
    pipeline.clear_cache()

if __name__ == "__main__":
    main()
