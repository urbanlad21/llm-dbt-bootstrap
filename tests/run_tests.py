#!/usr/bin/env python3
"""
Comprehensive test runner for DBT automation tool.

This script runs all unit tests with 100% coverage reporting and provides
detailed test results and coverage information.
"""

import sys
import os
import subprocess
import coverage
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


def run_tests_with_coverage():
    """Run all tests with coverage reporting."""
    print("🧪 Running comprehensive unit tests with coverage...")
    print("=" * 60)
    
    # Initialize coverage
    cov = coverage.Coverage(
        source=['src/dbt_automation'],
        omit=[
            '*/tests/*',
            '*/test_*',
            '*/__pycache__/*',
            '*/venv/*',
            '*/env/*'
        ]
    )
    
    cov.start()
    
    try:
        # Run pytest with verbose output
        result = subprocess.run([
            sys.executable, '-m', 'pytest',
            'tests/unit/',
            '-v',
            '--tb=short',
            '--strict-markers',
            '--disable-warnings'
        ], capture_output=True, text=True)
        
        # Stop coverage collection
        cov.stop()
        cov.save()
        
        # Print test results
        print("📊 Test Results:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️  Test Warnings/Errors:")
            print(result.stderr)
        
        # Generate coverage report
        print("\n📈 Coverage Report:")
        cov.report()
        
        # Generate HTML coverage report
        cov.html_report(directory='htmlcov')
        print(f"\n📄 HTML coverage report generated in: htmlcov/index.html")
        
        # Check coverage percentage
        total_coverage = cov.report()
        coverage_percentage = float(str(total_coverage).split('%')[0])
        
        print(f"\n🎯 Overall Coverage: {coverage_percentage:.1f}%")
        
        if coverage_percentage >= 100.0:
            print("✅ 100% code coverage achieved!")
            return True
        else:
            print(f"⚠️  Coverage below 100% ({coverage_percentage:.1f}%)")
            return False
            
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return False


def run_specific_test_module(module_name):
    """Run tests for a specific module."""
    print(f"🧪 Running tests for module: {module_name}")
    print("=" * 60)
    
    test_file = f"tests/unit/test_{module_name}.py"
    
    if not os.path.exists(test_file):
        print(f"❌ Test file not found: {test_file}")
        return False
    
    result = subprocess.run([
        sys.executable, '-m', 'pytest',
        test_file,
        '-v',
        '--tb=short'
    ], capture_output=True, text=True)
    
    print("📊 Test Results:")
    print(result.stdout)
    
    if result.stderr:
        print("⚠️  Test Warnings/Errors:")
        print(result.stderr)
    
    return result.returncode == 0


def list_available_tests():
    """List all available test modules."""
    print("📋 Available Test Modules:")
    print("=" * 60)
    
    test_dir = Path("tests/unit")
    if not test_dir.exists():
        print("❌ No test directory found")
        return
    
    test_files = list(test_dir.glob("test_*.py"))
    
    if not test_files:
        print("❌ No test files found")
        return
    
    for test_file in sorted(test_files):
        module_name = test_file.stem.replace("test_", "")
        print(f"  • {module_name}")
    
    print(f"\nTotal test modules: {len(test_files)}")


def main():
    """Main test runner function."""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "list":
            list_available_tests()
        elif command == "module" and len(sys.argv) > 2:
            module_name = sys.argv[2]
            success = run_specific_test_module(module_name)
            sys.exit(0 if success else 1)
        elif command == "coverage":
            success = run_tests_with_coverage()
            sys.exit(0 if success else 1)
        else:
            print("Usage:")
            print("  python tests/run_tests.py                    # Run all tests")
            print("  python tests/run_tests.py coverage          # Run tests with coverage")
            print("  python tests/run_tests.py list              # List available tests")
            print("  python tests/run_tests.py module <name>     # Run specific module tests")
            sys.exit(1)
    else:
        # Run all tests by default
        success = run_tests_with_coverage()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 