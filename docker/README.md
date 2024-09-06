# Unity Catalog Docker Images

This project provides the Docker images for running Unity Catalog Server and CLI.

## Prerequisites

Before you can build the Docker image, install the following tools:

* [Docker](https://www.docker.com/)
* [jq](https://jqlang.github.io/jq/)

## Building Unity Catalog Server Image

> [!NOTE]
>
> On [Mac computers with Apple silicon](https://support.apple.com/en-us/116943), you may want to change [DOCKER_DEFAULT_PLATFORM](https://docs.docker.com/reference/cli/docker/#environment-variables) environment variable before building the images as follows:
>
> ```bash
> export DOCKER_DEFAULT_PLATFORM=linux/amd64
> ```

[build-uc-server-docker](./bin/build-uc-server-docker) builds the Docker image of Unity Catalog Localhost Reference Server.

```bash
./docker/bin/build-uc-server-docker
```

> [!NOTE]
>
> `build-uc-server-docker` runs `sbt server/package` build while creating the Docker image.
> That gives all the required jars files part of the image.

`build-uc-server-docker` creates an image named `unitycatalog` with the version from [version.sbt](../version.sbt).

```bash
docker images unitycatalog
```

```text
REPOSITORY     TAG              IMAGE ID       CREATED         SIZE
unitycatalog   0.2.0-SNAPSHOT   5771da356693   7 minutes ago   2.58GB
```

## Running Unity Catalog Server Container

Once the Docker image of Unity Catalog's Localhost Reference Server is built, you can start it up using [start-uc-server-docker](./bin/start-uc-server-docker) script.

```bash
./docker/bin/start-uc-server-docker
```

```text
❤️ Starting unitycatalog:0.2.0-SNAPSHOT.
💡 Use Ctrl+C to stop and remove the container.
###################################################################
#  _    _       _ _            _____      _        _              #
# | |  | |     (_) |          / ____|    | |      | |             #
# | |  | |_ __  _| |_ _   _  | |     __ _| |_ __ _| | ___   __ _  #
# | |  | | '_ \| | __| | | | | |    / _` | __/ _` | |/ _ \ / _` | #
# | |__| | | | | | |_| |_| | | |___| (_| | || (_| | | (_) | (_| | #
#  \____/|_| |_|_|\__|\__, |  \_____\__,_|\__\__,_|_|\___/ \__, | #
#                      __/ |                                __/ | #
#                     |___/               v0.2.0-SNAPSHOT  |___/  #
###################################################################
```

`start-uc-server-docker` starts the Unity Catalog server.

In another terminal, execute the following command to learn more about the container.

```bash
docker container ls --filter name=unitycatalog --no-trunc --format 'table {{.ID}}\t{{.Image}}\t{{.Command}}\t{{.Ports}}'
```

```text
CONTAINER ID                                                       IMAGE                         COMMAND                           PORTS
1e15b648d6c2669eb83ac12991323ca288e27d9201c4ecbc8dc15ea2bfa6c389   unitycatalog:0.2.0-SNAPSHOT   "/bin/bash bin/start-uc-server"   0.0.0.0:8080-8081->8080-8081/tcp
```

Use the regular non-dockerized Unity Catalog CLI to access the server and list the catalogs.

```bash
./bin/uc catalog list
```

```text
┌─────┬────────────┬──────────┬─────────────┬──────────┬────────────────────────────────────┐
│NAME │  COMMENT   │PROPERTIES│ CREATED_AT  │UPDATED_AT│                 ID                 │
├─────┼────────────┼──────────┼─────────────┼──────────┼────────────────────────────────────┤
│unity│Main catalog│{}        │1721241605334│null      │f029b870-9468-4f10-badc-630b41e5690d│
└─────┴────────────┴──────────┴─────────────┴──────────┴────────────────────────────────────┘
```

## Building Unity Catalog CLI Image

[build-uc-cli-docker](./bin/build-uc-cli-docker) builds the Docker image of Unity Catalog CLI.

```bash
./docker/bin/build-uc-cli-docker
```

`build-uc-cli-docker` creates an image named `unitycatalog-cli` with the version from [version.sbt](../version.sbt).

```bash
docker images unitycatalog-cli
```

```text
REPOSITORY         TAG              IMAGE ID       CREATED          SIZE
unitycatalog-cli   0.2.0-SNAPSHOT   c13a37c4e91e   13 seconds ago   1.62GB
```

## Access Unity Catalog Server

[start-uc-cli-docker](./bin/start-uc-cli-docker) uses the `unitycatalog-cli` image to run Unity Catalog CLI in a Docker container.

> [!NOTE]
> You've already started the Unity Catalog server in a Docker container in [Running Unity Catalog Server Container](#running-unity-catalog-server-container) step.

> [!NOTE]
>
> `localhost` inside a Docker container is different from the local machine's `localhost`.

```bash
./docker/bin/start-uc-cli-docker catalog list --server http://host.docker.internal:8081
```

```text
┌─────┬────────────┬──────────┬─────────────┬──────────┬────────────────────────────────────┐
│NAME │  COMMENT   │PROPERTIES│ CREATED_AT  │UPDATED_AT│                 ID                 │
├─────┼────────────┼──────────┼─────────────┼──────────┼────────────────────────────────────┤
│unity│Main catalog│{}        │1721241605334│null      │f029b870-9468-4f10-badc-630b41e5690d│
└─────┴────────────┴──────────┴─────────────┴──────────┴────────────────────────────────────┘
```

## Testing Unity Catalog

This project provides several examples using `curl` commands to interact with the Unity Catalog API and demonstrate basic functionalities.

> [!NOTE]
> If you wish to use the CLI to test the catalog, run a docker CLI instance and follow the instructions
> from the repositories main readme.

### Store the Catalog Endpoint

The examples use a variable `unitycatalog_endpoint` to store the catalog's URL. Update this variable with the actual address based on your network configuration.

```bash
unitycatalog_endpoint="http://unitycatalog:8080/api/2.1/unity-catalog"
```

> \[!CAUTION\]
> Make sure you run the above command in your bash session before proceeding to the examples.
> Otherwise your examples will not run correctly.

> \[!IMPORTANT\]
> Because the default run script runs the script on a network called `unitycatalog_network`,
> if you want to test the catalog you need to make sure you application is running in the same network.
> For instance, you will not be able to test the API from your machine directly using Postman,
> wget or cUrl unless you run them with the catalog on the same network, or you change the catalog's
> network to run on the Host network.
> This is OS specific, therefore we opted for the most generic option, which is to create
> a docker network, but feel free to change those configurations to suit your needs.

### Create Catalog

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

### List Catalogs

This example lists all available catalogs:

```bash
list_catalog_request_body="curl -s --location '$unitycatalog_endpoint/catalogs' \
--header 'Accept: application/json'"

docker run --rm --network unitycatalog_network alpine/curl sh -c "$list_catalog_request_body" | jq .
```

### Create a Schema

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

### List Schemas

This example lists all schemas within the "MyCatalog" catalog:

```bash
list_schemas_request="curl -s --location '$unitycatalog_endpoint/schemas?catalog_name=MyCatalog' \
--header 'Accept: application/json'"

echo $list_schemas_request

docker run --rm --network unitycatalog_network alpine/curl sh -c "$list_schemas_request" | jq .
```

### Create an External Table

This example demonstrates creating an external table named "Table_B" within the "Schema_A" schema of the "MyCatalog" catalog. An external table points to existing data stored outside of Unity Catalog.

#### Prepare Data

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

#### Define Table

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
