#!/bin/bash
# Main test runner for MinIO integration tests

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
UC_ROOT="$SCRIPT_DIR/../../.."
LOG_DIR="/tmp/uc-minio-tests"
SKIP_DOCKER_START=${SKIP_DOCKER_START:-false}
# Spark tests removed due to version compatibility issues
# SKIP_SPARK_TEST=${SKIP_SPARK_TEST:-false}

# Create log directory
mkdir -p "$LOG_DIR"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Unity Catalog MinIO Integration Tests${NC}"
echo -e "${BLUE}========================================${NC}"

# Function to check Docker
check_docker() {
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Docker is not installed. Please install Docker to run these tests.${NC}"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        echo -e "${RED}Docker daemon is not running. Please start Docker.${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Docker is available${NC}"
}

# Function to start MinIO
start_minio() {
    echo -e "${YELLOW}Starting MinIO containers...${NC}"
    cd "$SCRIPT_DIR"
    
    if [ "$SKIP_DOCKER_START" = "false" ]; then
        docker compose down 2>/dev/null || true
        docker compose up -d
        
        # Wait for MinIO to be healthy
        echo -e "${YELLOW}Waiting for MinIO to be ready...${NC}"
        for i in {1..30}; do
            if docker compose ps | grep -q "healthy"; then
                echo -e "${GREEN}✓ MinIO is ready${NC}"
                return 0
            fi
            sleep 2
        done
        
        echo -e "${RED}MinIO failed to start${NC}"
        docker compose logs
        exit 1
    else
        echo -e "${YELLOW}Skipping Docker start (SKIP_DOCKER_START=true)${NC}"
    fi
}

# Function to stop MinIO
stop_minio() {
    if [ "$SKIP_DOCKER_START" = "false" ]; then
        echo -e "${YELLOW}Stopping MinIO containers...${NC}"
        cd "$SCRIPT_DIR"
        docker compose down
    fi
}

# Function to build Unity Catalog
build_unity_catalog() {
    echo -e "${YELLOW}Building Unity Catalog...${NC}"
    cd "$UC_ROOT"
    
    # Build with sbt
    if command -v sbt &> /dev/null; then
        ./build/sbt clean compile
    else
        echo -e "${YELLOW}SBT not found, assuming Unity Catalog is already built${NC}"
    fi
    
    echo -e "${GREEN}✓ Unity Catalog build complete${NC}"
}

# Function to run CLI tests
run_cli_tests() {
    echo -e "${BLUE}\n--- Running CLI Tests ---${NC}"
    cd "$UC_ROOT"
    
    # Make script executable
    chmod +x "$SCRIPT_DIR/test_cli_operations.sh"
    
    # Run CLI tests
    if "$SCRIPT_DIR/test_cli_operations.sh" > "$LOG_DIR/cli-test.log" 2>&1; then
        echo -e "${GREEN}✓ CLI tests passed${NC}"
        return 0
    else
        echo -e "${RED}✗ CLI tests failed${NC}"
        echo "Logs:"
        tail -n 50 "$LOG_DIR/cli-test.log"
        return 1
    fi
}

# Spark tests removed - version compatibility issues
# Can be re-enabled when Spark 4.0/PySpark compatibility is resolved

# Cleanup function
cleanup() {
    echo -e "${YELLOW}\nCleaning up...${NC}"
    
    # Stop UC server if running
    pkill -f "unity-catalog-server" 2>/dev/null || true
    
    # Stop MinIO
    stop_minio
    
    echo -e "${GREEN}✓ Cleanup complete${NC}"
}

# Set trap for cleanup
trap cleanup EXIT

# Main execution
main() {
    local exit_code=0
    
    # Check prerequisites
    check_docker
    
    # Start MinIO
    start_minio
    
    # Build Unity Catalog
    build_unity_catalog
    
    # Run CLI tests
    if ! run_cli_tests; then
        exit_code=1
    fi
    
    # Spark tests removed - version compatibility issues with Spark 4.0/PySpark 3.5
    # Can be re-enabled when version compatibility is resolved
    
    # Summary
    echo -e "\n${BLUE}========================================${NC}"
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ All MinIO integration tests passed!${NC}"
    else
        echo -e "${RED}✗ Some tests failed. Check logs in $LOG_DIR${NC}"
    fi
    echo -e "${BLUE}========================================${NC}"
    
    exit $exit_code
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-docker)
            SKIP_DOCKER_START=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --skip-docker    Skip starting Docker containers (assume already running)"
            echo "  --help          Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run main function
main