#!/usr/bin/env python3
"""
Test AWS Credentials Sanitization
==================================

This script tests that credentials are properly sanitized and 
can be used with boto3 without causing "Invalid header value" errors.
"""

import os
import sys
import tempfile
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_credential_sanitization():
    """Test that credentials with newlines are properly cleaned."""
    
    print("=" * 70)
    print("Testing AWS Credentials Sanitization")
    print("=" * 70)
    print()
    
    # Test 1: Credentials with newlines
    print("Test 1: Credentials with newlines")
    print("-" * 70)
    
    test_key = "AKIAIOSFODNN7EXAMPLE\n"
    test_secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY\n\r"
    
    os.environ['AWS_ACCESS_KEY_ID'] = test_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = test_secret
    
    print(f"Before: AWS_ACCESS_KEY_ID = {repr(test_key)}")
    print(f"Before: AWS_SECRET_ACCESS_KEY = {repr(test_secret)}")
    print()
    
    # Import and initialize S3DataStorage (which sanitizes)
    from src.data_ingestion.s3_storage import S3DataStorage
    
    try:
        # This should automatically sanitize credentials
        storage = S3DataStorage(
            bucket_name="test-bucket",
            region="ap-south-1"
        )
        
        print(f"After: AWS_ACCESS_KEY_ID = {repr(os.environ['AWS_ACCESS_KEY_ID'])}")
        print(f"After: AWS_SECRET_ACCESS_KEY = {repr(os.environ['AWS_SECRET_ACCESS_KEY'])}")
        print()
        
        # Verify no newlines
        if '\n' in os.environ['AWS_ACCESS_KEY_ID']:
            print("❌ FAILED: AWS_ACCESS_KEY_ID still contains newlines")
            return False
        
        if '\n' in os.environ['AWS_SECRET_ACCESS_KEY']:
            print("❌ FAILED: AWS_SECRET_ACCESS_KEY still contains newlines")
            return False
        
        print("✅ PASSED: Credentials sanitized successfully")
        print()
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    
    # Test 2: Already clean credentials
    print("Test 2: Already clean credentials")
    print("-" * 70)
    
    clean_key = "AKIAIOSFODNN7EXAMPLE"
    clean_secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
    
    os.environ['AWS_ACCESS_KEY_ID'] = clean_key
    os.environ['AWS_SECRET_ACCESS_KEY'] = clean_secret
    
    try:
        storage = S3DataStorage(
            bucket_name="test-bucket",
            region="ap-south-1"
        )
        
        if os.environ['AWS_ACCESS_KEY_ID'] != clean_key:
            print("❌ FAILED: Clean credentials were modified")
            return False
        
        print("✅ PASSED: Clean credentials left unchanged")
        print()
        
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False
    
    # Test 3: Python sanitizer script
    print("Test 3: Python sanitizer script")
    print("-" * 70)
    
    from scripts.sanitize_aws_credentials import sanitize_credential
    
    test_cases = [
        ("AKIATEST\n", "AKIATEST"),
        ("AKIATEST\r\n", "AKIATEST"),
        ("  AKIATEST  ", "AKIATEST"),
        ("AKIATEST", "AKIATEST"),
        ("AKIA\nTEST\r", "AKIATEST"),
    ]
    
    all_passed = True
    for input_val, expected in test_cases:
        result = sanitize_credential(input_val)
        if result == expected:
            print(f"✅ {repr(input_val)} → {repr(result)}")
        else:
            print(f"❌ {repr(input_val)} → {repr(result)} (expected {repr(expected)})")
            all_passed = False
    
    print()
    if not all_passed:
        return False
    
    print("=" * 70)
    print("✅ ALL TESTS PASSED")
    print("=" * 70)
    return True


if __name__ == '__main__':
    success = test_credential_sanitization()
    sys.exit(0 if success else 1)
