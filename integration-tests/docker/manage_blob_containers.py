# manage_blob_containers.py

from azure.storage.blob import BlobServiceClient, ContainerClient
import sys
import time
import os

# ============================
# Configuration
# ============================

# Retrieve connection details from environment variables
CONNECTION_STRING = os.getenv(
    "STORAGE_CONNECTION_STRING",
    "DefaultEndpointsProtocol=http;"
    "AccountName=devstoreaccount1;"
    "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
    "K1SZFPTOtr/KBHBeksoGMGw==;"
    "BlobEndpoint=http://azurite:10000/devstoreaccount1;"  # 'azurite' is the service name in docker-compose
)

# Name of the new container to be created
NEW_CONTAINER_NAME = "mock-azure-blob-storage"

# Number of retries to attempt connecting to the storage account
MAX_RETRIES = 10

# Delay (in seconds) between retries
RETRY_DELAY = 5

# ============================
# Functions
# ============================

def connect_blob_service(connection_string):
    """
    Attempts to create a BlobServiceClient using the provided connection string.
    Returns the BlobServiceClient object if successful.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        # Test the connection by listing containers
        blob_service_client.list_containers()
        print("Successfully connected to Azure Blob Storage.")
        return blob_service_client
    except Exception as e:
        print(f"Connection attempt failed: {e}")
        return None

def wait_for_storage_account(connection_string, max_retries=5, delay=5):
    """
    Waits until the Azure Storage account is ready by attempting to connect multiple times.
    """
    attempt = 0
    while attempt < max_retries:
        print(f"Attempt {attempt + 1} of {max_retries} to connect to Azure Blob Storage...")
        blob_service_client = connect_blob_service(connection_string)
        if blob_service_client:
            return blob_service_client
        attempt += 1
        print(f"Retrying in {delay} seconds...\n")
        time.sleep(delay)
    print("Failed to connect to Azure Blob Storage after multiple attempts.")
    sys.exit(1)

def create_blob_container(blob_service_client, container_name):
    """
    Creates a new blob container with the specified name.
    If the container already exists, it notifies the user.
    """
    try:
        blob_service_client.create_container(container_name)
        print(f"Container '{container_name}' created successfully.\n")
    except Exception as e:
        if "ContainerAlreadyExists" in str(e):
            print(f"Container '{container_name}' already exists.\n")
        else:
            print(f"Failed to create container '{container_name}': {e}")
            sys.exit(1)

def list_blob_containers(blob_service_client):
    """
    Lists all blob containers in the Azure Storage account.
    """
    try:
        containers = blob_service_client.list_containers()
        print("Available Blob Containers:")
        for container in containers:
            print(f"- {container.name}")
    except Exception as e:
        print(f"An error occurred while listing containers: {e}")
        sys.exit(1)

# ============================
# Main Execution Block
# ============================

def main():
    print("Starting Azure Blob Storage management script...\n")

    # Step 1: Wait until the storage account is ready
    blob_service_client = wait_for_storage_account(CONNECTION_STRING, MAX_RETRIES, RETRY_DELAY)

    # Step 2: Create a new blob container
    create_blob_container(blob_service_client, NEW_CONTAINER_NAME)

    # Step 3: List all blob containers to confirm creation
    list_blob_containers(blob_service_client)

if __name__ == "__main__":
    main()
