# MinIO Integration Tests for Unity Catalog

This directory contains integration tests for Unity Catalog with MinIO, demonstrating support for S3-compatible object storage services.

## Overview

These tests validate:
1. **Server Configuration**: Unity Catalog server can be configured with custom S3 endpoints
2. **CLI Operations**: Creating, reading, and managing tables with external storage
3. **File-based Storage**: Testing with file:// paths as Unity Catalog doesn't support managed tables

Note: Due to Unity Catalog limitations, these tests use file:// paths for external tables rather than 
direct S3 integration. The MinIO container is included for future S3 credential vending support.

**Spark tests have been temporarily removed due to version compatibility issues between Spark 4.0 and PySpark 3.5.**

## Prerequisites

- Docker and Docker Compose
- Java 11+
- Unity Catalog built from source

## Test Structure

```
minio/
├── docker-compose.yml           # MinIO container configuration
├── server.properties.template   # UC server config for MinIO
├── test_cli_operations.sh      # CLI integration tests
├── run_tests.sh                # Main test runner
└── README.md                   # This file
```

## Running Tests

### Quick Start

Run all tests with default settings:
```bash
cd tests/integration/minio
./run_tests.sh
```

### Running Individual Components

#### 1. Start MinIO Only
```bash
docker compose up -d
```

#### 2. Run CLI Tests Only
```bash
./test_cli_operations.sh
```

### Test Options

```bash
# Skip Docker startup (if MinIO is already running)
./run_tests.sh --skip-docker

# Help
./run_tests.sh --help
```

## Configuration

### MinIO Settings

The default MinIO configuration:
- Endpoint: `http://localhost:9000`
- Console: `http://localhost:9001`
- Access Key: `minioadmin`
- Secret Key: `minioadmin`
- Buckets: `unity-catalog-test`, `delta-tables`

### Unity Catalog Server

The server is configured with two S3 configurations:
1. `s3://unity-catalog-test` - For catalog metadata
2. `s3://delta-tables` - For Delta table storage

See `server.properties.template` for the complete configuration.

## Test Scenarios

### CLI Tests (`test_cli_operations.sh`)

1. **Setup**
   - Start MinIO containers
   - Configure and start UC server
   - Create test catalog and schema

2. **Table Operations**
   - Create external Delta table in MinIO
   - List tables
   - Get table metadata
   - Write sample data
   - Read data from MinIO
   - Delete table

3. **Validation**
   - Verify table creation in MinIO bucket
   - Confirm data can be read through UC

### Spark Tests (Removed)

Spark integration tests have been temporarily removed due to version compatibility issues between Unity Catalog, Spark 4.0, and PySpark. They can be re-enabled once the version compatibility is resolved.

## Troubleshooting

### Common Issues

1. **MinIO Connection Failed**
   ```bash
   # Check if MinIO is running
   docker compose ps
   
   # View MinIO logs
   docker compose logs minio
   ```

2. **Unity Catalog Server Issues**
   ```bash
   # Check server logs
   tail -f /tmp/uc-server.log
   
   # Verify configuration
   cat etc/conf/server.properties
   ```


### Memory Optimization

For memory-constrained environments:

**Limit Docker Resources**
```yaml
# In docker-compose.yml
services:
  minio:
    mem_limit: 512m
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: MinIO Integration Tests

on: [push, pull_request]

jobs:
  minio-tests:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v2
    
    - name: Set up JDK 11
      uses: actions/setup-java@v2
      with:
        java-version: '11'
        
    - name: Set up Python
      uses: actions/setup-python@v2
      with:
        python-version: '3.9'
        
    - name: Install dependencies
      run: |
        pip install requests
        
    - name: Build Unity Catalog
      run: ./build/sbt compile
      
    - name: Run MinIO Integration Tests
      run: |
        cd tests/integration/minio
        ./run_tests.sh
```

## Extending Tests

To add new test scenarios:

1. **CLI Tests**: Add functions to `test_cli_operations.sh`
2. **Spark Tests**: Currently removed due to version compatibility issues
3. **New Storage Services**: Copy and modify the configuration for other S3-compatible services

## Clean Up

Remove all test artifacts:
```bash
# Stop and remove containers
docker compose down -v

# Kill UC server
pkill -f unity-catalog-server

# Remove test data
rm -rf /tmp/ucroot-test
rm -rf /tmp/uc-minio-tests
```

## Contributing

When adding S3-compatible storage support:
1. Test with these integration tests
2. Document any service-specific configuration
3. Update this README with new examples
4. Add service-specific test cases if needed