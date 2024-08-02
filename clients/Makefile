# Makefile

# Default target
.DEFAULT_GOAL := help


# Help message
help:
	@echo "Usage: make python VERSION=a.b.c"
	@echo ""
	@echo "Generates Python client code using OpenAPI Generator and Docker."

.PHONY: python
# Rule for generating Python code
python:
	@echo "Generating Python client code..."
	@rm -rf python
	@docker run --rm \
		-v ${PWD}/../api/all.yaml:/api/all.yaml \
		-v ${PWD}:/local \
		openapitools/openapi-generator-cli:v7.7.0 generate \
			-i /api/all.yaml \
			-g python \
			-o /local/python \
			--package-name unitycatalog \
			--additional-properties=packageVersion=$(VERSION)
