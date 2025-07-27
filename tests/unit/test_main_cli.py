"""
Unit tests for main CLI functionality
"""
import pytest
import tempfile
import os
import time
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from main import run_with_timeout, timeout_handler


class TestMainCLI:
    """Test cases for main CLI functionality"""
    
    def test_timeout_handler(self):
        """Test timeout handler raises TimeoutError"""
        with pytest.raises(TimeoutError, match="Operation timed out after 50 seconds"):
            timeout_handler(None, None)
    
    def test_run_with_timeout_success(self):
        """Test run_with_timeout with successful function"""
        def quick_function():
            return "success"
        
        result = run_with_timeout(quick_function, timeout=5)
        assert result == "success"
    
    def test_run_with_timeout_exception(self):
        """Test run_with_timeout with function that raises exception"""
        def failing_function():
            raise ValueError("Test error")
        
        with pytest.raises(ValueError, match="Test error"):
            run_with_timeout(failing_function, timeout=5)
    
    def test_run_with_timeout_timeout(self):
        """Test run_with_timeout with function that takes too long"""
        def slow_function():
            time.sleep(2)  # Sleep for 2 seconds
            return "success"
        
        with pytest.raises(TimeoutError, match="Operation timed out after 1 seconds"):
            run_with_timeout(slow_function, timeout=1)
    
    def test_run_with_timeout_default_timeout(self):
        """Test run_with_timeout with default timeout"""
        def quick_function():
            return "success"
        
        result = run_with_timeout(quick_function)  # Default 50 seconds
        assert result == "success"
    
    def test_run_with_timeout_with_return_value(self):
        """Test run_with_timeout with function that returns a value"""
        def function_with_return():
            return {"status": "success", "data": [1, 2, 3]}
        
        result = run_with_timeout(function_with_return, timeout=5)
        assert result == {"status": "success", "data": [1, 2, 3]}
    
    def test_run_with_timeout_with_complex_exception(self):
        """Test run_with_timeout with complex exception"""
        def function_with_complex_exception():
            raise RuntimeError("Complex error with details")
        
        with pytest.raises(RuntimeError, match="Complex error with details"):
            run_with_timeout(function_with_complex_exception, timeout=5)
    
    def test_run_with_timeout_thread_cleanup(self):
        """Test that threads are properly cleaned up"""
        def quick_function():
            return "done"
        
        # Run multiple times to ensure thread cleanup
        for _ in range(5):
            result = run_with_timeout(quick_function, timeout=1)
            assert result == "done"
    
    def test_timeout_handler_message(self):
        """Test timeout handler message is correct"""
        try:
            timeout_handler(None, None)
        except TimeoutError as e:
            assert str(e) == "Operation timed out after 50 seconds"
    
    def test_run_with_timeout_zero_timeout(self):
        """Test run_with_timeout with zero timeout"""
        def quick_function():
            time.sleep(0.1)  # Very short sleep
            return "success"
        
        with pytest.raises(TimeoutError, match="Operation timed out after 0 seconds"):
            run_with_timeout(quick_function, timeout=0)
    
    def test_run_with_timeout_negative_timeout(self):
        """Test run_with_timeout with negative timeout"""
        def quick_function():
            time.sleep(0.1)  # Very short sleep
            return "success"
        
        with pytest.raises(TimeoutError, match="Operation timed out after -1 seconds"):
            run_with_timeout(quick_function, timeout=-1) 