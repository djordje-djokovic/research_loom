"""
Test Runner: Run all tests
"""
import sys
import os
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup logging for tests
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

def run_all_tests():
    """Run all tests"""
    logger.info("="*80)
    logger.info("RUNNING ALL TESTS")
    logger.info("="*80)
    
    tests = [
        ("Basic Caching", "test_basic_caching"),
        ("Config Changes", "test_config_changes"),
        ("Downstream Propagation", "test_downstream_propagation"),
        ("Cache Optimization", "test_optimization"),
        ("Comprehensive", "test_comprehensive"),
        ("No Execution & Minimal Cache", "test_no_execution_caching"),
        ("Cox Data Config Change", "test_cox_data_config")
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_module in tests:
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"RUNNING: {test_name}")
            logger.info(f"{'='*60}")
            
            # Import and run test
            module = __import__(test_module)
            test_func = getattr(module, test_module)
            test_func()
            
            logger.info(f"\n[PASS] {test_name}: PASSED")
            passed += 1
            
        except Exception as e:
            logger.error(f"\n[FAIL] {test_name}: FAILED")
            logger.error(f"   Error: {e}")
            failed += 1
    
    logger.info(f"\n{'='*80}")
    logger.info(f"TEST SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"[PASS] Passed: {passed}")
    logger.info(f"[FAIL] Failed: {failed}")
    logger.info(f"[INFO] Total: {passed + failed}")
    
    if failed == 0:
        logger.info(f"\n[SUCCESS] ALL TESTS PASSED!")
    else:
        logger.warning(f"\n[WARNING] {failed} TEST(S) FAILED")
    
    return failed == 0

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
