#!/usr/bin/env python3
"""
Test for cox_data config changes and plan-only functionality
"""

import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.core import ResearchPipeline, Node, load_raw_data as dummy_raw_data, \
                              calculate_variables as dummy_variables, \
                              calculate_global_variables as dummy_global_variables, \
                              prepare_cox_data as dummy_cox_data, \
                              fit_cox_model as dummy_cox_model
import time

# Setup logging for tests
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def test_cox_data_config_change():
    """Test that changing cox_data config only affects cox_data and downstream"""
    logger.info("="*80)
    logger.info("TEST: COX_DATA CONFIG CHANGE")
    logger.info("="*80)
    
    pipeline = ResearchPipeline(cache_dir="cache/test_cox_data_config")
    pipeline.clear_cache()  # Start fresh
    
    # Add nodes
    pipeline.add_node(Node("raw_data", dummy_raw_data, [], "raw_data"))
    pipeline.add_node(Node("variables", dummy_variables, ["raw_data"], "variables"))
    pipeline.add_node(Node("global_variables", dummy_global_variables, ["variables"], "global_variables"))
    pipeline.add_node(Node("cox_data", dummy_cox_data, ["variables", "global_variables"], "cox_data"))
    pipeline.add_node(Node("cox_model", dummy_cox_model, ["cox_data"], "models", materialize_by_default=True))
    
    # Initial config
    config_initial = {
        "raw_data": {"industry": "Software", "country": "GBR"},
        "variables": {"groups": ["funding", "founder"]},
        "global_variables": {"types": ["semantic_similarity"]},
        "cox_data": {"covariates": ["log_time"]},
        "models": {"cox_regression": {"covariates": ["log_time", "engineer"]}}
    }
    
    # Config with cox_data changed
    config_cox_data_changed = {
        "raw_data": {"industry": "Software", "country": "GBR"},
        "variables": {"groups": ["funding", "founder"]},
        "global_variables": {"types": ["semantic_similarity"]},
        "cox_data": {"feature_flags": ["new_transform"]},  # Changed!
        "models": {"cox_regression": {"covariates": ["log_time", "engineer"]}}
    }
    
    logger.info("\n1. First run with initial config:")
    results1 = pipeline.run_pipeline(config_initial, materialize="sinks")
    
    logger.info("\n2. Second run with initial config (should use cache):")
    results2 = pipeline.run_pipeline(config_initial, materialize="sinks")
    
    logger.info("\n3. Change cox_data config - should process cox_data + cox_model:")
    results3 = pipeline.run_pipeline(config_cox_data_changed, materialize="sinks")
    
    logger.info("\n4. Return to initial config (should use cache):")
    results4 = pipeline.run_pipeline(config_initial, materialize="sinks")
    
    # Verify results (compare only config_hash, ignore timestamps)
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
    
    # Compare only the cox_model results (the materialized output)
    cox_model_1 = results1.get("cox_model", {})
    cox_model_2 = results2.get("cox_model", {})
    cox_model_3 = results3.get("cox_model", {})
    cox_model_4 = results4.get("cox_model", {})
    
    assert compare_results(cox_model_1, cox_model_2), "Same config should produce identical cox_model results"
    assert not compare_results(cox_model_1, cox_model_3), "cox_data change should affect cox_model results"
    assert compare_results(cox_model_1, cox_model_4), "Return to initial should produce identical cox_model results"
    
    
    logger.info(f"\n[PASS] TEST PASSED: cox_data config change works correctly")
    
    # Clean up
    pipeline.clear_cache()

def test_plan_only():
    """Test the plan-only functionality"""
    logger.info("="*80)
    logger.info("TEST: PLAN-ONLY FUNCTIONALITY")
    logger.info("="*80)
    
    pipeline = ResearchPipeline(cache_dir="cache/test_cox_data_config")
    pipeline.clear_cache()  # Start fresh
    
    # Add nodes
    pipeline.add_node(Node("raw_data", dummy_raw_data, [], "raw_data"))
    pipeline.add_node(Node("variables", dummy_variables, ["raw_data"], "variables"))
    pipeline.add_node(Node("global_variables", dummy_global_variables, ["variables"], "global_variables"))
    pipeline.add_node(Node("cox_data", dummy_cox_data, ["variables", "global_variables"], "cox_data"))
    pipeline.add_node(Node("cox_model", dummy_cox_model, ["cox_data"], "models", materialize_by_default=True))
    
    config = {
        "raw_data": {"industry": "Software"},
        "variables": {"groups": ["funding"]},
        "global_variables": {"types": ["semantic_similarity"]},
        "cox_data": {"covariates": ["log_time"]},
        "models": {"cox_regression": {"covariates": ["log_time"]}}
    }
    
    logger.info("\n1. Plan for first run (no cache):")
    plan1 = pipeline.plan(config, materialize="sinks")
    logger.info("   Execution plan:")
    for node, status in plan1.items():
        logger.info(f"     {node}: {status}")
    
    logger.info("\n2. Run the pipeline to create cache:")
    results = pipeline.run_pipeline(config, materialize="sinks")
    
    logger.info("\n3. Plan for second run (with cache):")
    plan2 = pipeline.plan(config, materialize="sinks")
    logger.info("   Execution plan:")
    for node, status in plan2.items():
        logger.info(f"     {node}: {status}")
    
    logger.info("\n4. Plan for cox_data config change:")
    config_changed = config.copy()
    config_changed["cox_data"] = {"feature_flags": ["new_transform"]}
    plan3 = pipeline.plan(config_changed, materialize="sinks")
    logger.info("   Execution plan:")
    for node, status in plan3.items():
        logger.info(f"     {node}: {status}")
    
    # Verify plans
    assert all(status == "MISSING" for status in plan1.values()), "First run should have all MISSING"
    assert all(status == "CACHED" for status in plan2.values()), "Second run should have all CACHED"
    assert plan3["cox_data"] in ["MISSING", "PROCESS"], "cox_data should need processing"
    assert plan3["cox_model"] in ["MISSING", "PROCESS"], "cox_model should need processing"
    assert plan3["raw_data"] == "CACHED", "raw_data should be cached"
    assert plan3["variables"] == "CACHED", "variables should be cached"
    assert plan3["global_variables"] == "CACHED", "global_variables should be cached"
    
    logger.info(f"\n[PASS] TEST PASSED: Plan-only functionality works correctly")
    
    # Clean up
    pipeline.clear_cache()

def test_cox_data_config():
    """Main test function for cox_data config changes"""
    test_cox_data_config_change()
    logger.info("\n" + "="*80)
    test_plan_only()

if __name__ == "__main__":
    test_cox_data_config()
