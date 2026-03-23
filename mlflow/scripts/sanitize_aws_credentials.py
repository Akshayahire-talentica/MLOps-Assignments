#!/usr/bin/env python3
"""
AWS Credentials Sanitizer
=========================

Utility to clean AWS credentials from environment variables.
Removes newlines, carriage returns, and whitespace that can cause
invalid HTTP headers in boto3 requests.

Usage:
    python scripts/sanitize_aws_credentials.py
    
Or source it in bash:
    eval "$(python scripts/sanitize_aws_credentials.py --export)"
"""

import os
import sys
import argparse


def sanitize_credential(value: str) -> str:
    """
    Sanitize a credential value by removing problematic characters.
    
    Args:
        value: The credential value to sanitize
        
    Returns:
        Cleaned credential value
    """
    if not value:
        return value
    
    # Remove newlines, carriage returns, null bytes, and strip whitespace
    cleaned = value.strip().replace('\n', '').replace('\r', '').replace('\0', '')
    
    return cleaned


def sanitize_aws_credentials(export_format: bool = False) -> dict:
    """
    Sanitize AWS credentials from environment variables.
    
    Args:
        export_format: If True, return shell export commands
        
    Returns:
        Dictionary of sanitized credentials or export commands
    """
    credentials = {
        'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
        'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
        'AWS_SESSION_TOKEN': os.environ.get('AWS_SESSION_TOKEN', ''),
    }
    
    results = {}
    changed = []
    
    for key, value in credentials.items():
        if value:
            cleaned = sanitize_credential(value)
            results[key] = cleaned
            
            if cleaned != value:
                changed.append(key)
                # Update environment variable
                os.environ[key] = cleaned
    
    if changed:
        print(f"✓ Sanitized credentials: {', '.join(changed)}", file=sys.stderr)
    else:
        print("✓ Credentials are clean", file=sys.stderr)
    
    if export_format:
        # Return shell export commands
        exports = []
        for key, value in results.items():
            if value:
                # Escape for shell
                escaped = value.replace("'", "'\\''")
                exports.append(f"export {key}='{escaped}'")
        return '\n'.join(exports)
    
    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Sanitize AWS credentials from environment variables'
    )
    parser.add_argument(
        '--export',
        action='store_true',
        help='Output shell export commands'
    )
    parser.add_argument(
        '--check',
        action='store_true',
        help='Check if credentials need sanitization (exit 1 if dirty)'
    )
    
    args = parser.parse_args()
    
    if args.check:
        # Check mode: exit with error if credentials are dirty
        credentials = {
            'AWS_ACCESS_KEY_ID': os.environ.get('AWS_ACCESS_KEY_ID', ''),
            'AWS_SECRET_ACCESS_KEY': os.environ.get('AWS_SECRET_ACCESS_KEY', ''),
            'AWS_SESSION_TOKEN': os.environ.get('AWS_SESSION_TOKEN', ''),
        }
        
        dirty = []
        for key, value in credentials.items():
            if value and sanitize_credential(value) != value:
                dirty.append(key)
        
        if dirty:
            print(f"⚠ Credentials need sanitization: {', '.join(dirty)}", file=sys.stderr)
            sys.exit(1)
        else:
            print("✓ Credentials are clean", file=sys.stderr)
            sys.exit(0)
    
    elif args.export:
        # Export mode: output shell commands
        print(sanitize_aws_credentials(export_format=True))
    
    else:
        # Normal mode: just sanitize in place
        sanitize_aws_credentials(export_format=False)


if __name__ == '__main__':
    main()
