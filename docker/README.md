# Unity Catalog Dockerised Environment

This project provides a Dockerised environment for running Unity Catalog. It includes everything needed to set up and interact with the catalog for testing purposes.

## Prerequisites

* Docker installed on your system.

## Usage

### 1. Building the Image

The `build_docker.sh` script is responsible for building the Docker image for Unity Catalog. Run it from the project directory:

```bash
./build_docker.sh
```

This will create an image named `unitycatalog`.

### 2. Running the Catalog

The `run_docker.sh` script starts the Unity Catalog container. It also creates a network named `unitycatalog_network` for the container to use. Run it from the project directory:

```bash
./run_docker.sh
```

This will start the container and make it accessible on port `8080` within the `unitycatalog_network`.

### 3. Adding Custom Startup Code

Place any scripts you want to run during Unity Catalog startup in the `initialise.sh` file located in the project directory. These scripts will be executed automatically when the container starts.

### 4. Testing the Catalog

This project provides several examples using `curl` commands to interact with the Unity Catalog API and demonstrate basic functionalities.

#### 4.1. Store the Catalog Endpoint

The examples use a variable `unitycatalog_endpoint` to store the catalog's URL. Update this variable with the actual address based on your network configuration.

```bash
unitycatalog_endpoint="http://unitycatalog:8080/api/2.1/unity-catalog"
```

#### 4.2. Create a Catalog

This example demonstrates creating a new catalog named "MyCatalog" with a description and properties:

```bash
create_catalog_request_body='{
  "name": "MyCatalog",
  "comment": "An example of how to create your first catalog",
  "properties": {
    "Project": "Unity Catalog Demo",
    "Environment": "Development",
    "Access": "Public"
  }
}'

create_catalog_request=$(printf "curl -s -X POST \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json' \
  -d '%s' \
  %s/catalogs" "$create_catalog_request_body" "$unitycatalog_endpoint")

docker run --rm --network unitycatalog_network alpine/curl sh -c "$create_catalog_request" | jq .
```

#### 4.3. List Catalogs

This example lists all available catalogs:

```bash
list_catalog_request_body="curl -s --location '$unitycatalog_endpoint/catalogs' \
--header 'Accept: application/json'"

docker run --rm --network unitycatalog_network alpine/curl sh -c "$list_catalog_request_body" | jq .
```

#### 4.4. Create a Schema

This example creates a new schema named "Schema_A" within the "MyCatalog" catalog:

```bash
create_schema_a_request_body='{
  "name": "Schema_A",
  "catalog_name": "MyCatalog",
  "comment": "Schema A in catalog MyCatalog",
  "properties": {
    "Project": "Unity Catalog Demo",
    "Environment": "Development",
    "Access": "Public",
    "Type": "Schema"
  }
}'

create_schema_a_request=$(printf "curl -s \
--location '%s/schemas' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data '%s'" "$unitycatalog_endpoint" "$create_schema_a_request_body")

docker run --rm --network unitycatalog_network alpine/curl sh -c "$create_schema_a_request" | jq .
```

#### 4.5. List Schemas

This example lists all schemas within the "MyCatalog" catalog:

```bash
list_schemas_request="curl -s --location '$unitycatalog_endpoint/schemas?catalog_name=MyCatalog' \
--header 'Accept: application/json'"

echo $list_schemas_request

docker run --rm --network unitycatalog_network alpine/curl sh -c "$list_schemas_request" | jq .
```

#### 4.6. Create a Managed Table

This example demonstrates creating a managed table named "Table_A" within the "Schema_A" schema of the "MyCatalog" catalog. A managed table lets Unity Catalog manage the data location.

```bash
create_table_a_request_body='{
    "name": "Table_A",
    "catalog_name": "MyCatalog",
    "schema_name": "Schema_A",
    "table_type": "MANAGED",
    "data_source_format": "DELTA",
    "columns": [
        {
            "name": "ID",
            "type_name": "LONG",
            "comment": "The unique ID of the person",
            "nullable": "false"
        },
        {
            "name": "FirstName",
            "type_name": "STRING",
            "comment": "The persons official first name",
            "nullable": "false"
        },
        {
            "name": "LastName",
            "type_name": "STRING",
            "comment": "The persons official last name",
            "nullable": "false"
        }
    ],
    "comment": "A managed table. Leaving it to Unity Catalog to pick the location.",
    "properties": {
      "Project": "Unity Catalog Demo",
	    "Environment": "Development",
	    "Access": "Public",
	    "Type": "Table",
	    "Stage": "Gold"
    }
}'

create_table_a_request=$(printf "curl -s \
--location '%s/tables' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data '%s'" "$unitycatalog_endpoint" "$create_table_a_request_body")

docker run --rm --network unitycatalog_network alpine/curl sh -c "$create_table_a_request" | jq .
```

#### 4.7. Create an External Table

This example demonstrates creating an external table named "Table_B" within the "Schema_A" schema of the "MyCatalog" catalog. An external table points to existing data stored outside of Unity Catalog.

##### 4.7.1. Prepare the Data

First, you'll need to create a CSV file containing the data for the table. This example creates a file named `test.csv` within the container's `/opt/unitycatalog/external_data` directory:

```bash
docker exec -it  unitycatalog /bin/bash
cd /opt/unitycatalog
mkdir external_data
cd external_data
printf '"ID", "FirstName", "LastName"
"1", "Jean", "Boutros"
"2", "Eddy", "Stone"
"3", "Paul", "Clark"\n' > test.csv
```

##### 4.7.2. Define the Table

Now, define the external table using a `curl` command. The `storage_location` property points to the CSV file location within the container:

```bash
create_table_b_request_body='{
    "name": "Table_B",
    "catalog_name": "MyCatalog",
    "schema_name": "Schema_A",
    "table_type": "EXTERNAL",
    "data_source_format": "CSV",
    "columns": [
        {
            "name": "ID",
            "type_name": "LONG",
            "comment": "The unique ID of the person",
            "nullable": "false"
        },
        {
            "name": "FirstName",
            "type_name": "STRING",
            "comment": "The persons official first name",
            "nullable": "false"
        },
        {
            "name": "LastName",
            "type_name": "STRING",
            "comment": "The persons official last name",
            "nullable": "false"
        }
    ],
    "storage_location": "/opt/unitycatalog/external_data/test.csv",
    "comment": "No comments",
    "properties": {
      "Project": "Unity Catalog Demo",
	    "Environment": "Development",
	    "Access": "Public",
	    "Type": "External Table",
	    "Stage": "RAW"
    }
}'

create_table_b_request=$(printf "curl -s \
--location '%s/tables' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data '%s'" "$unitycatalog_endpoint" "$create_table_b_request_body")

docker run --rm --network unitycatalog_network alpine/curl sh -c "$create_table_b_request" | jq .
```

### 5. Additional Notes**

This document provides a basic overview of how to run unity catalog inside a docker and interact with its API.


