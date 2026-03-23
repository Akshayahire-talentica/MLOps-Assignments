#!/bin/bash
# AWS Credentials Fix Script
# ==========================
# This script sanitizes AWS credentials to prevent "Invalid header value" errors
# in boto3 when credentials contain newlines or whitespace.
#
# Usage:
#   source scripts/fix_aws_credentials.sh
#   # Or
#   . scripts/fix_aws_credentials.sh

echo "🔧 Sanitizing AWS Credentials..."

# Function to clean a credential value
clean_credential() {
    local value="$1"
    # Remove newlines, carriage returns, and trim whitespace
    echo "$value" | tr -d '\n\r' | xargs
}

# Check if AWS credentials exist
if [ -n "$AWS_ACCESS_KEY_ID" ]; then
    CLEANED_KEY=$(clean_credential "$AWS_ACCESS_KEY_ID")
    if [ "$CLEANED_KEY" != "$AWS_ACCESS_KEY_ID" ]; then
        export AWS_ACCESS_KEY_ID="$CLEANED_KEY"
        echo "✓ Cleaned AWS_ACCESS_KEY_ID"
    else
        echo "✓ AWS_ACCESS_KEY_ID is already clean"
    fi
else
    echo "⚠️  AWS_ACCESS_KEY_ID not set"
fi

if [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
    CLEANED_SECRET=$(clean_credential "$AWS_SECRET_ACCESS_KEY")
    if [ "$CLEANED_SECRET" != "$AWS_SECRET_ACCESS_KEY" ]; then
        export AWS_SECRET_ACCESS_KEY="$CLEANED_SECRET"
        echo "✓ Cleaned AWS_SECRET_ACCESS_KEY"
    else
        echo "✓ AWS_SECRET_ACCESS_KEY is already clean"
    fi
else
    echo "⚠️  AWS_SECRET_ACCESS_KEY not set"
fi

if [ -n "$AWS_SESSION_TOKEN" ]; then
    CLEANED_TOKEN=$(clean_credential "$AWS_SESSION_TOKEN")
    if [ "$CLEANED_TOKEN" != "$AWS_SESSION_TOKEN" ]; then
        export AWS_SESSION_TOKEN="$CLEANED_TOKEN"
        echo "✓ Cleaned AWS_SESSION_TOKEN"
    else
        echo "✓ AWS_SESSION_TOKEN is already clean"
    fi
fi

if [ -n "$AWS_DEFAULT_REGION" ]; then
    echo "✓ AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"
else
    echo "⚠️  AWS_DEFAULT_REGION not set, using ap-south-1"
    export AWS_DEFAULT_REGION="ap-south-1"
fi

echo ""
echo "✅ AWS Credentials sanitized and ready to use!"
echo ""
echo "💡 Tip: Always source this script before running AWS operations:"
echo "   source scripts/fix_aws_credentials.sh"
echo ""
