#!/usr/bin/env python3
"""
Test runner for all unit tests in the sc-topic-modelling-worker project.

This test suite provides comprehensive unit test coverage for:
- Supervisor class: Worker management, health checking, message routing
- Worker base class: Abstract interface testing
- All worker implementations: PreprocessingWorker, ETMWorker, LLMWorker, 
  RabbitMQWorker, DatabaseInteractionWorker
- Core patterns and logic used throughout the system

The tests are designed to work without external dependencies by focusing
on testing logic patterns, data structures, and core functionality.
"""

import unittest
import sys
import os

def run_all_tests():
    """Discover and run all tests in the tests directory."""
    print("=" * 70)
    print("SC Topic Modelling Worker - Unit Test Suite")
    print("=" * 70)
    print("Testing coverage:")
    print("- Supervisor class (worker management, health checks, message routing)")
    print("- Worker base class (abstract interface)")
    print("- PreprocessingWorker (Azure OpenAI integration patterns)")
    print("- ETMWorker (Embedded Topic Modeling patterns)")
    print("- LLMWorker (Language Model integration patterns)")
    print("- RabbitMQWorker (Message queue integration patterns)")
    print("- DatabaseInteractionWorker (MongoDB integration patterns)")
    print("- Core system patterns and logic")
    print("=" * 70)
    print()
    
    loader = unittest.TestLoader()
    start_dir = os.path.dirname(__file__)
    suite = loader.discover(start_dir, pattern='test_*.py')
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print()
    print("=" * 70)
    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED!")
        print(f"Ran {result.testsRun} tests successfully")
    else:
        print("❌ SOME TESTS FAILED!")
        print(f"Ran {result.testsRun} tests")
        print(f"Failures: {len(result.failures)}")
        print(f"Errors: {len(result.errors)}")
    print("=" * 70)
    
    return result.wasSuccessful()

if __name__ == '__main__':
    success = run_all_tests()
    sys.exit(0 if success else 1)