#!/bin/bash
set -e

# Create buckets
echo "Creating S3 buckets..."
awslocal s3 mb s3://mock-s3-bucket

# Add any bucket configurations if needed
awslocal s3api put-bucket-versioning \
    --bucket my-bucket \
    --versioning-configuration Status=Enabled

echo "S3 initialization completed!"
