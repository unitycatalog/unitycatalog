#!/bin/bash

# This script generates documentation for the Helm chart using helm-docs

# Check if helm-docs is installed
if ! command -v helm-docs &> /dev/null; then
    echo "helm-docs is not installed. Please install it first."
    echo "You can install it using:"
    echo "  brew install norwoodj/tap/helm-docs"
    exit 1
fi

# Run helm-docs to generate documentation
echo "Generating documentation for Helm chart..."
helm-docs

echo "Documentation generated successfully."
