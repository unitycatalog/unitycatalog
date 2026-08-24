# Unity Catalog XTable Integration

This document walks through the steps to register an Apache XTable™ (Incubating) synced Delta table in Unity Catalog.

[Apache XTable](https://xtable.apache.org) provides cross-table omni-directional interoperability between Apache Hudi,
Apache Iceberg, and Delta Lake.

## Pre-Requisites

1. Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS/ADLS or local. In this
    guide, we will use a S3 example.
1. Follow the XTable installation guide [here](https://xtable.apache.org/docs/setup)
1. Clone the Unity Catalog repository from [here](https://github.com/unitycatalog/unitycatalog) and build the project
    by following the steps outlined [here](https://github.com/unitycatalog/unitycatalog?tab=readme-ov-file#prerequisites)

To sync a source Hudi/Iceberg table using XTable use the following:

```sh
sourceFormat: HUDI|ICEBERG # choose only one
targetFormats:
  - DELTA
datasets:
    tableBasePath: s3://path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE 
```

Now, from your terminal under the cloned Apache XTable™ (Incubating) directory, run the sync process using the below
command. This will generate the Delta Lake metadata.

```sh
java -jar xtable-utilities/target/incubator-xtable-utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml
```

> **Note:** At this point, if you check your bucket path, you will be able to see _delta_log directory with the JSON log.

## Configure Server Property for using S3

The server config file is at the location `etc/conf/server.properties`
For enabling server to vend AWS temporary credentials to access S3 buckets, the following parameters need to be set:

- `s3.bucketPath.i`: The S3 path of the bucket where the data is stored. Should be in the format `s3://<bucket-name>`.
- `s3.accessKey.i`: The AWS access key, an identifier of temp credentials.
- `s3.secretKey.i`: The AWS secret key used to sign API requests to AWS.
- `s3.sessionToken.i`: THE AWS session token, used to verify that the request is coming from a trusted source.

## Run the Unity Server

```sh
bin/start-uc-server
```

## Register the XTable-synced table in the Unity Catalog

In a separate terminal, run the following commands to register the target table in Unity Catalog.

```sh
bin/uc table create --full_name unity.default.people --columns "id INT, name STRING, age INT, city STRING, create_ts STRING" --storage_location s3://path/to/source/data
```

## Validating the results

You can now read the table registered in Unity Catalog using the below command.

```sh
bin/uc table read --full_name unity.default.people
```

## More References

- [Unity Catalog OSS with Hudi, Delta, Iceberg, and EMR + DuckDB](https://medium.com/@kywe665/unity-catalog-oss-with-hudi-delta-iceberg-and-emr-duckdb-710ab8f8a7dc)
- [Getting Started with X-Table and Unity Catalog | Universal Datalakes | Hands on Labs](https://www.linkedin.com/pulse/getting-started-x-table-unity-catalog-universal-datalakes-soumil-shah-l3rpe/?trackingId=LfVbu4PjS5awh%2FRkjfeViA%3D%3D)
- [Video: Getting Started with XTable and Unity Catalof](https://www.youtube.com/watch?v=1SKQRrenBj4&t=3s)
