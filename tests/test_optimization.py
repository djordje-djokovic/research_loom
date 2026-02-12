"""
Test 4: Cache Optimization
Tests that cache is only loaded when nodes actually need to be executed.
"""
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.core import ResearchPipeline, Node
import time

# Setup logging for tests
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def dummy_raw_data(inputs, config):
    """Dummy raw data function"""
    time.sleep(0.01)  # Simulate processing time
    return {"data": f"raw_data_{config['industry']}", "count": 100}

def dummy_variables(inputs, config):
    """Dummy variables function"""
    time.sleep(0.01)  # Simulate processing time
    return {"variables": f"variables_{config['industry']}", "count": 50}

def dummy_global_variables(inputs, config):
    """Dummy global variables function"""
    time.sleep(0.01)  # Simulate processing time
    return {"global_vars": f"global_{config['industry']}", "count": 25}

def dummy_cox_data(inputs, config):
    """Dummy Cox data function"""
    time.sleep(0.01)  # Simulate processing time
    return {"cox_data": f"cox_{config.get('covariates', ['default'])[0]}", "count": 20}

def dummy_cox_model(inputs, config):
    """Dummy Cox model function"""
    time.sleep(0.01)  # Simulate processing time
    return {"model": f"cox_model_{config.get('industry', 'default')}", "accuracy": 0.85}

def test_optimization():
    """Test cache optimization"""
    logger.info("="*80)
    logger.info("TEST 4: CACHE OPTIMIZATION")
    logger.info("="*80)
    
    # Create pipeline
    pipeline = ResearchPipeline(cache_dir="cache/test_optimization")
    pipeline.clear_cache()  # Start fresh
    
    # Add nodes
    pipeline.add_node(Node("raw_data", dummy_raw_data, [], "raw_data"))
    pipeline.add_node(Node("variables", dummy_variables, ["raw_data"], "variables"))
    pipeline.add_node(Node("global_variables", dummy_global_variables, ["variables"], "global_variables"))
    pipeline.add_node(Node("cox_data", dummy_cox_data, ["variables", "global_variables"], "cox_data"))
    pipeline.add_node(Node("cox_model", dummy_cox_model, ["cox_data"], "models"))
    
    # Test config
    config = {
        "raw_data": {"industry": "Software"},
        "variables": {"industry": "Software"},
        "global_variables": {"industry": "Software"},
        "models": {"industry": "Software"},
        "cox_data": {"covariates": ["log_time"]}
    }
    
    logger.info("\n1. First run - should process all nodes:")
    results1 = pipeline.run_pipeline(config)
    
    logger.info("\n2. Second run - should use cache for all nodes (no cache loading):")
    results2 = pipeline.run_pipeline(config)
    
    # Test: Change models config (should only process models, not load other cache)
    config_models_change = {
        "raw_data": {"industry": "Software"},  # Same
        "variables": {"industry": "Software"},  # Same
        "global_variables": {"industry": "Software"},  # Same
        "models": {"industry": "Hardware"},
        "cox_data": {"covariates": ["log_time"]}  # Changed
    }
    
    logger.info("\n3. Change models config - should only process models (load minimal cache):")
    results3 = pipeline.run_pipeline(config_models_change)
    
    # Test: Return to original config (should use cache for all)
    logger.info("\n4. Return to original config - should use cache for all nodes:")
    results4 = pipeline.run_pipeline(config)
    
    # Verify results (ignore timestamps)
    def compare_results(r1, r2):
        if not isinstance(r1, dict) or not isinstance(r2, dict):
            return r1 == r2
        if r1.keys() != r2.keys():
            return False
        for key in r1:
            if key == "timestamp":
                continue  # Skip timestamp comparison
            if isinstance(r1[key], dict) and isinstance(r2[key], dict):
                if not compare_results(r1[key], r2[key]):
                    return False
            elif r1[key] != r2[key]:
                return False
        return True
    
    # Compare only the materialized outputs (cox_model)
    cox_model_1 = results1.get("cox_model", {})
    cox_model_2 = results2.get("cox_model", {})
    cox_model_3 = results3.get("cox_model", {})
    cox_model_4 = results4.get("cox_model", {})
    
    assert compare_results(cox_model_1, cox_model_2), "Same config should produce identical results"
    assert not compare_results(cox_model_1, cox_model_3), "Models change should affect results"
    assert compare_results(cox_model_1, cox_model_4), "Return to original should be identical"
    
    logger.info(f"\n[PASS] TEST PASSED: Cache optimization works correctly")
    
    # Clean up
    pipeline.clear_cache()

if __name__ == "__main__":
    test_optimization()
