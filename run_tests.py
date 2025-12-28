#!/usr/bin/env python3
"""
Test Runner for ETL Framework

Runs comprehensive test suite with different configurations.
"""

import sys
import os
import argparse
import subprocess
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))


def run_unit_tests():
    """Run unit tests."""
    print("🧪 Running Unit Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/unit_tests.py",
        "-v", "--tb=short", "-m", "unit"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_integration_tests():
    """Run integration tests."""
    print("🔗 Running Integration Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/integration_tests.py",
        "-v", "--tb=short", "-m", "integration"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_data_validation_tests():
    """Run data validation tests."""
    print("✅ Running Data Validation Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/data_validation_tests.py",
        "-v", "--tb=short"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_performance_tests():
    """Run performance tests."""
    print("⚡ Running Performance Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/performance_tests.py",
        "-v", "--tb=short", "-m", "performance"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_all_tests():
    """Run all tests."""
    print("🚀 Running Complete Test Suite...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/",
        "-v", "--tb=short", "--cov=src", "--cov-report=html"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_tests_with_coverage():
    """Run tests with coverage reporting."""
    print("📊 Running Tests with Coverage...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/",
        "--cov=src",
        "--cov-report=html:htmlcov",
        "--cov-report=term-missing",
        "--cov-fail-under=80"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def run_slow_tests():
    """Run slow/performance tests."""
    print("🐌 Running Slow Tests...")
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "-m", "slow",
        "-v", "--tb=short"
    ], capture_output=True, text=True)

    print(result.stdout)
    if result.stderr:
        print("Errors:", result.stderr)
    return result.returncode == 0


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="ETL Framework Test Runner")
    parser.add_argument(
        "test_type",
        choices=["unit", "integration", "data_validation", "performance", "all", "coverage", "slow"],
        help="Type of tests to run"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failure"
    )

    args = parser.parse_args()

    # Set environment variables
    os.environ["PYTHONPATH"] = str(Path(__file__).parent / "src")

    # Run appropriate tests
    success = False

    if args.test_type == "unit":
        success = run_unit_tests()
    elif args.test_type == "integration":
        success = run_integration_tests()
    elif args.test_type == "data_validation":
        success = run_data_validation_tests()
    elif args.test_type == "performance":
        success = run_performance_tests()
    elif args.test_type == "all":
        success = run_all_tests()
    elif args.test_type == "coverage":
        success = run_tests_with_coverage()
    elif args.test_type == "slow":
        success = run_slow_tests()

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\run_tests.py