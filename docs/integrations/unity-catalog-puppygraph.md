# Unity Catalog PuppyGraph Integration

This document walks through how to use [PuppyGraph](https://www.puppygraph.com) to query data from Delta tables registered in Unity Catalog as a graph.

## Prerequisites

- JDK 17 to build and run Unity Catalog and Spark
- Docker
- This repository `unitycatalog` cloned
- Spark downloaded

## Build the Unity Server and Spark support

Run the command From the cloned repository root directory

```sh
build/sbt clean package publishLocal spark/publishLocal
```

## Run the Unity Catalog Server

Run the command to start a Unity Server.

```sh
./bin/start-uc-server
```

For the remaining steps, continue in a different terminal.

## Create Tables under the Unity Catalog

Create a catalog `puppygraph` and several Delta tables under the schema `modern` in that catalog.

```sh
./bin/uc catalog create --name puppygraph
./bin/uc schema create --name modern --catalog puppygraph
./bin/uc table create --full_name puppygraph.modern.person --columns "id STRING, name STRING, age INT" --storage_location /tmp/puppygraph/person/ --format DELTA
./bin/uc table create --full_name puppygraph.modern.knows --columns "id STRING, from_id STRING, to_id STRING, weight DOUBLE" --storage_location /tmp/puppygraph/knowns/ --format DELTA
./bin/uc table create --full_name puppygraph.modern.software --columns "id STRING, name STRING, lang STRING" --storage_location /tmp/puppygraph/software/ --format DELTA
./bin/uc table create --full_name puppygraph.modern.created --columns "id STRING, from_id STRING, to_id STRING, weight DOUBLE" --storage_location /tmp/puppygraph/created/ --format DELTA
```

## Load Data into the Tables

Run the command from the Spark folder to start a Spark SQL shell .

```sh
./bin/spark-sql \
  --packages \
    io.delta:delta-spark_2.12:3.2.0,io.unitycatalog:unitycatalog-spark:0.2.0-SNAPSHOT \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  --conf spark.sql.catalog.puppygraph=io.unitycatalog.spark.UCSingleCatalog \
  --conf spark.sql.catalog.puppygraph.uri=http://localhost:8080
```

Run the following SQL to insert data into the Delta tables.

```sql
insert into puppygraph.modern.person VALUES
                                         ('v1', 'marko', 29),
                                         ('v2', 'vadas', 27),
                                         ('v4', 'josh', 32),
                                         ('v6', 'peter', 35);
INSERT INTO puppygraph.modern.software VALUES
                                           ('v3', 'lop', 'java'),
                                           ('v5', 'ripple', 'java');
INSERT INTO puppygraph.modern.created VALUES
                                          ('e9', 'v1', 'v3', 0.4),
                                          ('e10', 'v4', 'v5', 1.0),
                                          ('e11', 'v4', 'v3', 0.4),
                                          ('e12', 'v6', 'v3', 0.2);
INSERT INTO puppygraph.modern.knows VALUES
                                        ('e7', 'v1', 'v2', 0.5),
                                        ('e8', 'v1', 'v4', 1.0);
```

Exit the Spark SQL shell after data insertion is done.

## Querying the Tables as a Graph

Start PuppyGraph using Docker. Here we map the PuppyGraph port `8081` to `9081` on the host.

```sh
docker run -p 9081:8081 -p 8182:8182 -p 7687:7687 \
-v /tmp/puppygraph:/tmp/puppygraph \
--name puppy --rm -itd puppygraph/puppygraph:stable
```

Create the schema.json and replace `<host-name>` with your host ip address.

```schema.json
{
    "catalogs": [
        {
            "name": "puppygraph", 
            "type": "deltalake", 
            "metastore": {
                "type": "unity", 
                "host": "http://<host-name>:8081", 
                "token": "no-use", 
                "databricksCatalogName": "puppygraph"
            }
        }
    ], 
    "vertices": [
        {
            "label": "person", 
            "attributes": [
                { "type": "String", "name": "name" }, 
                { "type": "Int"   , "name": "age"  }
            ], 
            "mappedTableSource": {
                "catalog": "puppygraph", 
                "schema": "modern", 
                "table": "person", 
                "metaFields": {"id": "id"}
            }
        }, 
        {
            "label": "software", 
            "attributes": [
                { "type": "String", "name": "name" }, 
                { "type": "String", "name": "lang" }
            ], 
            "mappedTableSource": {
                "catalog": "puppygraph", 
                "schema": "modern", 
                "table": "software", 
                "metaFields": {"id": "id"}
            }
        }
    ], 
    "edges": [
        {
            "label": "knows",
            "from": "person", 
            "to": "person", 
            "attributes": [ {"type": "Double", "name": "weight"} ], 
            "mappedTableSource": {
                "catalog": "puppygraph", 
                "schema": "modern", 
                "table": "knows",
                "metaFields": {"from": "from_id", "id": "id", "to": "to_id"}
            }
        }, 
        {
            "label": "created", 
            "from": "person", 
            "to": "software", 
            "attributes": [ {"type": "Double", "name": "weight"} ], 
            "mappedTableSource": {
                "catalog": "puppygraph", 
                "schema": "modern", 
                "table": "created", 
                "metaFields": {"from": "from_id", "id": "id", "to": "to_id"}
            }
        }
    ]
}
```

Upload the schema to PuppyGraph. Note here port is 9081 as 8081 is used by Unity Catalog.

```sh
curl -XPOST -H "content-type: application/json" --data-binary @./schema.json --user "puppygraph:puppygraph123" localhost:9081/schema
```

Start a PuppyGraph Gremlin Console to query the graph.

```sh
docker exec -it puppygraph ./bin/console
```

Input the following query string to get all the software created by people that marko knows.

```text
g.V().has("name", "marko").out("knows").out("created").valueMap()
```

The output should be like this:

```text
puppy-gremlin> g.V().has("name", "marko").out("knows").out("created").valueMap()
Done! Elapsed time: 0.080s, rows: 2
==>map[lang:java name:lop]
==>map[lang:java name:ripple]
```

You can also use [Web UI](https://docs.puppygraph.com/user-interface/puppygraph-web-ui) for more features like graph visualization and a notebook-style query interface.

## See Also

- [Querying Unity Catalog Data as a Graph](https://docs.puppygraph.com/getting-started/querying-unity-catalog-data-as-a-graph)
- [The Internals of Unity Catalog | Spark Connector](https://books.japila.pl/unity-catalog-internals/spark-connector/)
