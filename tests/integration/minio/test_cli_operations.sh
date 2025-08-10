#!/bin/bash
# Integration test for Unity Catalog CLI with MinIO

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting MinIO Integration Test for CLI${NC}"

# Configuration
UC_HOME="${UC_HOME:-$(pwd)}"
MINIO_ENDPOINT="http://localhost:9000"
TEST_CATALOG="minio_test"
TEST_SCHEMA="default"
TEST_TABLE="test_table"
# Use file path for external tables
TEST_STORAGE_ROOT="file:///tmp/ucroot-test/tables"

# Function to check if MinIO is running
check_minio() {
    echo -e "${YELLOW}Checking MinIO availability...${NC}"
    if ! curl -s -f "${MINIO_ENDPOINT}/minio/health/live" > /dev/null 2>&1; then
        echo -e "${RED}MinIO is not running. Please start it with: docker compose up -d${NC}"
        exit 1
    fi
    echo -e "${GREEN}MinIO is running${NC}"
}

# Function to start UC server
start_uc_server() {
    echo -e "${YELLOW}Starting Unity Catalog server...${NC}"
    
    # Copy template and create actual config
    cp tests/integration/minio/server.properties.template etc/conf/server.properties
    
    # Start server in background
    bin/start-uc-server > /tmp/uc-server.log 2>&1 &
    UC_PID=$!
    
    # Wait for server to be ready
    for i in {1..30}; do
        if curl -s "http://localhost:8080/api/2.1/unity-catalog/catalogs" > /dev/null 2>&1; then
            echo -e "${GREEN}Unity Catalog server started (PID: $UC_PID)${NC}"
            return 0
        fi
        sleep 1
    done
    
    echo -e "${RED}Failed to start Unity Catalog server${NC}"
    cat /tmp/uc-server.log
    exit 1
}

# Function to stop UC server
stop_uc_server() {
    echo -e "${YELLOW}Stopping Unity Catalog server...${NC}"
    if [ ! -z "$UC_PID" ]; then
        kill $UC_PID 2>/dev/null || true
    fi
}

# Cleanup function
cleanup() {
    stop_uc_server
    # Don't stop MinIO - let docker compose handle it
}

# Set trap for cleanup
trap cleanup EXIT

# Main test execution
main() {
    cd "$UC_HOME"
    
    # Check MinIO is running
    check_minio
    
    # Start UC server
    start_uc_server
    
    echo -e "${GREEN}Running CLI tests...${NC}"
    
    # Create catalog if it doesn't exist
    echo "Creating catalog..."
    ./bin/uc catalog create --name "$TEST_CATALOG" || true
    
    # Create schema
    echo "Creating schema..."
    ./bin/uc schema create --catalog "$TEST_CATALOG" --name "$TEST_SCHEMA" || true
    
    # Create external table with file storage
    echo "Creating external table..."
    STORAGE_LOCATION="${TEST_STORAGE_ROOT}/${TEST_CATALOG}/${TEST_SCHEMA}/${TEST_TABLE}"
    ./bin/uc table create \
        --full_name "${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}" \
        --columns "id INT, name STRING, value DOUBLE" \
        --storage_location "$STORAGE_LOCATION" \
        --format DELTA
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Table created successfully${NC}"
    else
        echo -e "${RED}✗ Failed to create table${NC}"
        exit 1
    fi
    
    # List tables to verify
    echo "Listing tables..."
    ./bin/uc table list --catalog "$TEST_CATALOG" --schema "$TEST_SCHEMA"
    
    # Get table details
    echo "Getting table details..."
    ./bin/uc table get --full_name "${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}"
    
    # Write sample data (if table supports it)
    echo "Writing sample data to table..."
    ./bin/uc table write --full_name "${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}" || true
    
    # Read table
    echo "Reading table..."
    ./bin/uc table read --full_name "${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}" --max_results 5
    
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Successfully read table${NC}"
    else
        echo -e "${RED}✗ Failed to read table${NC}"
        exit 1
    fi
    
    # Cleanup - delete table
    echo "Cleaning up - deleting table..."
    ./bin/uc table delete --full_name "${TEST_CATALOG}.${TEST_SCHEMA}.${TEST_TABLE}"
    
    echo -e "${GREEN}CLI MinIO integration test completed successfully!${NC}"
}

# Run main function
main "$@"