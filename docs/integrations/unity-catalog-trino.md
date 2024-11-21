# Unity Catalog Trino Integration

This guide explains how to use Unity Catalog with Trino.

[Trino](https://trino.io/) is a distributed SQL engine designed to query data at scale, spread across one or more heterogeneous data sources.

Using Trino with Unity Catalog offers significant advantages:

- Neatly organizing data in tables and volumes in the Unity Catalog hierarchy makes it a lot easier to write clean, reproducible code.
- You can decouple business logic from file paths.
- You can get easy access to different file formats without end users needing to know how the data is stored.

## Prerequisites

To use Unity Catalog with Trino, you will need the following:

1. A running Unity Catalog instance.
2. A running instance of Trinno. You can use Trino’s Docker container or deploy it on a Kubernetes cluster. For setup instructions, refer to the [Trino documentation](https://trino.io/docs/current/installation.html). Use Trino 462 or higher.

We will demo using a local Unity Catalog server (running at localhost:8080) and Trino running locally inside a Docker container. You will need [Docker](https://www.docker.com/) installed on your machine to follow along.

# Launch Unity Catalog

Launch a local instance of Unity Catalog server using `bin/start-uc-server` from your local `unitycatalog` repository.

```
> bin/start-uc-server

###################################################################
#  _    _       _ _            _____      _        _              #
# | |  | |     (_) |          / ____|    | |      | |             #
# | |  | |_ __  _| |_ _   _  | |     __ _| |_ __ _| | ___   __ _  #
# | |  | | '_ \| | __| | | | | |    / _` | __/ _` | |/ _ \ / _` | #
# | |__| | | | | | |_| |_| | | |___| (_| | || (_| | | (_) | (_| | #
#  \____/|_| |_|_|\__|\__, |  \_____\__,_|\__\__,_|_|\___/ \__, | #
#                      __/ |                                __/ | #
#                     |___/               v0.3.0-SNAPSHOT  |___/  #
###################################################################
```

The local Unity Catalog server will run at `http://127.0.0.1:8080`.

Since we will be running Trino inside a Docker container, we need to forward the activity on this local port to a publicly accessible URL. You can use tools like `serveo` or `ngrok` to do this.

With Serveo, you can do this with:

```
ssh -R 80:localhost:8080 serveo.net
```

This will output a public URL like:

```
Forwarding HTTP traffic from https://abc123.serveo.net
```

Note this URL. You will need it in the next step.

# Configure Trino

Before you launch Trino, you will have to configure some settings for Trino to be able to access your Unity Catalog instance.

Create a `etc/catalog/iceberg.properties` file and store it in your local working directory. Add the following properties to the file:

```
    connector.name=iceberg
    iceberg.catalog.type=rest
    iceberg.rest-catalog.uri=https:<<your-public-url>>/api/2.1/unity-catalog/iceberg
    iceberg.rest-catalog.security=OAUTH2
    iceberg.rest-catalog.oauth2.token=not_used
    iceberg.rest-catalog.warehouse=unity
```

Substitute `<&lt;your-public-url>>` with the public URL you generated above.

Set `iceberg.rest-catalog.warehouse` to the catalog name you want to access. We will use the default `unity` catalog which comes pre-loaded by default with the local Unity Catalog server.

# Launch Trino

With the correct properties defined, you can now launch your Trino instance with the `iceberg.properties` file attached as a Volume, by running the code below in your terminal:

```
docker run --name trino -d -p 9000:8080 --volume $PWD/etc/catalog/iceberg.properties:/etc/trino/catalog/iceberg.properties trinodb/trino
```

Unity Catalog and Trino both use port `8080` by default when run locally. For proper functioning of Unity Catalog, launch Unity Catalog **_first_** and then make sure to map Trino to another available port, e.g. `9000`, on your machine. The `-p 9000:8080` above maps the default Trino port (8080) to your local 9000 port.

If you launch Trino first, then UC will automatically switch to a different port number and some of your code may not work.

# Using Trino to query Unity Catalog Assets

With Trino running and correctly configured, you can now use the Trino CLI to run queries on your data stored in Unity Catalog.

Launch the Trino CLI by running the code below in your terminal:

```
docker exec -it trino trino
```

This will open a Trino CLI. You can then write SQL queries to access your data.

## List Catalogs

You can list all available catalogs:

```
    > trino> SHOW CATALOGS;
     Catalog
    ---------
     iceberg
     system
     tpch
```

`iceberg` is your Unity Catalog catalog, accessed via the Iceberg REST API. It is the same as the `unity` catalog that comes pre-loaded by default with the local Unity Catalog server.

## List Schemas

You can list all schemas in a catalog:

```
    > trino> SHOW SCHEMAS FROM iceberg;
           Schema
    --------------------
     default
     information_schema
```

The standard catalog that comes with the local Unity

## List Tables

You can list all tables in a schema:

```
    > trino> SHOW TABLES FROM iceberg.default;
           Table
    -------------------
     marksheet_uniform
```

The Iceberg REST catalog works with Iceberg and UniForm tables. Read the [UniForm section](https://docs.unitycatalog.io/usage/tables/uniform/) for more information.
