# Delta Lake

Unity Catalog stores your tables as UniForm tables. UniForm tables are [Delta Lake](https://delta.io/) tables with both Delta Lake and [Apache Iceberg](https://iceberg.apache.org/) metadata. They can be read easily by Delta or Iceberg clients.

This page explains how Delta Lake works so you can get the most out of your Unity Catalog tables. It will also show some common ways of working with Delta tables.

## Data Catalog Architecture

Let’s start with a quick overview of the Data Catalog architecture.

Unity Catalog is a universal catalog for all of your data and AI assets. It is built as a hierarchical structure with the following architecture:

1. **Catalog**: your top-level directory for organizing your data and AI assets
2. **Schema**: your databases that organize data and AI assets into more granular logical categories
3. **Asset**: your data and AI assets

![](assets/images/uc-3-level.png)

Your assets can be [Tables](./usage/tables/uniform.md), [Volumes](./usage/volumes.md), [Functions](./usage/functions.md) or Models.

This page discusses Tables, which are stored as Delta Lake tables with both Delta Lake and Iceberg metadata.

## How Delta Lake Works

Delta Lake is an open table format for fast and reliable data storage.

A Delta Lake table consists of two things:

1. **Your data** stored in Parquet files
2. **A transaction log** that stores important metadata

Storing your data in Delta Lake tables gives you many great performance and reliability features. Let’s look at each one in detail.

### Optimized Queries

Delta Lake makes your table queries faster by storing all the metadata in a separate transaction log.

Regular Parquet files store metadata (like min/max values of columns) per row group in the footer of each file. This can become a problem when you have lots of Parquet files. Your query engine will have to read the metadata from each individual Parquet file. File-listing and access is slow, especially from cloud object stores.

Delta Lake stores metadata at the file-level in a separate transaction log. This means that you can quickly read all metadata by accessing the transaction log with a single operation. Your query engine can then use this information to know which files it can skip. This is much more efficient.

### Reliability

Delta Lake makes your table operations more secure and reliable by supporting transactions. Transactions prevent data corruption and broken pipelines by guaranteeing that your data is always in a valid state.

Specifically, ACID (Atomicity, Consistency, Isolation and Durability) transactions guarantee that:

1. Every data transaction is treated as an indivisible unit. Either the entire transaction gets completed or it fails entirely and no data gets changed.
2. Your database is in a valid state before and after every transaction. If a transaction is going to break any of the pre-defined constraints, the entire transaction is rejected and will not complete.
3. Concurrent processes happen in isolation from each other and cannot access each other's intermediate states, preventing accidental data corruption.
4. Once a transaction is completed, the changes are guaranteed to never be lost, even in the event of system failures or power outages. Changes are stored in non-volatile storage.

Suppose you’re writing a large amount of data to an existing data lake of Parquet files. If your cluster dies during the operation, you may be left with partially written Parquet files in your data lake. These partial files will break downstream reads. To fix this, you will need to identify all of the corrupted files and delete them. You will then need to re-run the operation and hope that your cluster won’t die again. Not fun.

This kind of situation is simply not possible with formats that support ACID transactions.

### File skipping

Delta Lake optimizes your query performance by identifying data that is not relevant to your query. This way your query engine can skip entire files, avoiding reading data unnecessarily.

Delta Lake does this by storing metadata at the file-level in the transaction log. This means that you can quickly read all metadata by accessing the transaction log. Your query engine can then use this information to know which files it can skip.

Clustering techniques like [Z-ordering](https://delta.io/blog/2023-06-03-delta-lake-z-order/) and [Liquid Clustering](https://docs.delta.io/latest/delta-clustering.html) can further improve your query speed by co-locating similar files.

### Data versioning / time travel

Delta Lake supports data versioning via its transaction log. All changes that are ever made to your data are recorded in this log. This means you have all the information about data transformations available for easy reference.

Data versioning means that Delta Lake also supports [time travel](https://delta.io/blog/2023-02-01-delta-lake-time-travel/) functionality. You can easily switch between different versions of your Delta table. This way you can go back to revert any accidental changes or to remember a previous state.

### Schema enforcement and evolution

Delta Lakes have built-in [schema enforcement](https://delta.io/blog/2022-11-16-delta-lake-schema-enforcement/) by default. New data is guaranteed to match the table schema of your existing data. This saves you from accidentally corrupting your data.

When your data structure evolves and you need more flexibility in your schema, Delta Lake also supports [Schema Evolution](https://delta.io/blog/2023-02-08-delta-lake-schema-evolution/).

## How to with Unity Catalog

Here is an example of how you can use Delta Lake’s powerful features from Unity Catalog using [Daft](www.getdaft.io)

### Delta Lake with Daft

You need to have a Unity Catalog server running to connect to.

For testing purposes, you can spin up a local server by running the code below in a terminal:

```sh
bin/start-uc-server
```

Then import Daft and the UnityCatalog abstraction:

```python
import daft
from daft.unity_catalog import UnityCatalog
```

Next, point Daft to your UC server

```python
unity = UnityCatalog(
    endpoint="http://127.0.0.1:8080",
    token="not-used",
)
```

And now you can access your Delta tables stored in Unity Catalog:

```python
> print(unity.list_tables("unity.default"))

['unity.default.numbers', 'unity.default.marksheet_uniform', 'unity.default.marksheet']

> unity_table = unity.load_table("unity.default.numbers")
> df = daft.read_delta_lake(unity_table)
> df.show()

as_int  as_double
564     188.755356
755     883.610563
644     203.439559
75      277.880219
42      403.857969
680     797.691220
821     767.799854
484     344.003740
477     380.678561
131     35.443732
294     209.322436
150     329.197303
539     425.661029
247     477.742227
958     509.371273
```

Take a look at the [Integrations](./integrations/unity-catalog-duckdb.md) page for more examples of working with Delta tables stored in Unity Catalog.
