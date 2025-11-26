# CelerData Integration

This document walks through the steps to use [CelerData Cloud BYOC](https://cloud.celerdata.com) to query data
governed by Unity Catalog OSS. [CelerData](https://celerdata.com) is a lakehouse query engine that delivers data
warehouse performance on open data lakes.

## Pre-requisites

- CelerData Cloud BYOC Environment: You can follow this [link](https://cloud.celerdata.com) to deploy one with the
    30-day free trial.

## Deploying Unity Catalog

In this example, for simplicity, we query the data that comes with the  UC quickstart, which is stored on local disk.
For this to work, you would need to deploy a UC server on every FE and BE/CN in your CelerData environment, under the
same path.

SSH into each FE and BE/CN node, install JDK 17, and under the same path, Clone, build, start Unity Catalog:

```sh
sudo apt install openjdk-17-jdk

git clone https://github.com/unitycatalog/unitycatalog.git

cd unitycatalog

build/sbt package

bin/start-uc-server
```

## Connecting CelerData Cloud BYOC to Unity Catalog

Now we connect CelerData Cloud BYOC to Unity Catalog through the CelerData Unity external catalog feature.

```SQL
create external catalog uc properties (
"type"="deltalake",
"hive.metastore.type" = "unity",
"databricks.host"= "http://127.0.0.1:8080",
"databricks.token" = "not-used",
"databricks.catalog.name" = "unity",
"aws.s3.region"= "us-west-2");
```

Check whether the connection is successful and query the data.

```SQL
-- show databases from the catalog
show databases from uc;

-- show tables from the `default` database
show tables from uc.`default`;

-- query
select * from uc.`default`.marksheet;
```
