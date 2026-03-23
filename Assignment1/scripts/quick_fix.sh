#!/bin/bash
# Quick Fix for AWS Credentials Issue
# ====================================
# Run this before executing any AWS operations

set -e

echo "🔧 AWS Credentials Quick Fix"
echo "=============================="
echo ""

# Check if credentials exist
if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "❌ AWS credentials not found in environment"
    echo ""
    echo "Please set your credentials:"
    echo "  export AWS_ACCESS_KEY_ID='your-access-key'"
    echo "  export AWS_SECRET_ACCESS_KEY='your-secret-key'"
    echo "  export AWS_DEFAULT_REGION='ap-south-1'"
    exit 1
fi

# Sanitize credentials
export AWS_ACCESS_KEY_ID="$(echo "$AWS_ACCESS_KEY_ID" | tr -d '\n\r' | xargs)"
export AWS_SECRET_ACCESS_KEY="$(echo "$AWS_SECRET_ACCESS_KEY" | tr -d '\n\r' | xargs)"

if [ -n "$AWS_SESSION_TOKEN" ]; then
    export AWS_SESSION_TOKEN="$(echo "$AWS_SESSION_TOKEN" | tr -d '\n\r' | xargs)"
fi

echo "✅ Credentials sanitized!"
echo ""
echo "Now you can run:"
echo "  python src/data_ingestion/run_ingestion.py"
echo "  python src/training/model_trainer.py"
echo "  aws s3 ls"
echo ""
