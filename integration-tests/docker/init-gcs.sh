#!/bin/bash

# Wait for the fake-gcs-server to be ready
echo "Waiting for fake-gcs-server to be ready..."
until curl -s http://localhost:4443/storage/v1/b > /dev/null; do
    sleep 1
done

# Create the bucket using the REST API directly
curl -X POST "http://localhost:4443/storage/v1/b" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mock-gcs-bucket",
    "project": "my-test-project"
  }'

echo "Bucket 'mock-gcs-bucket' created successfully"
