# Unity Catalog Metric Views

This page shows you how to create, query, and manage **metric views** in Unity Catalog using
Apache Spark™.

A metric view is a view-like object that defines reusable **dimensions** and **measures** (named
aggregations) over a source table, expressed in YAML. Consumers query the measures with the
`measure(...)` function and group by the dimensions, so the aggregation logic lives in one governed
place in Unity Catalog instead of being copy-pasted across queries.

!!! warning "Prerequisites"
    Metric views require **Apache Spark 4.2 or later** (the `CREATE VIEW ... WITH METRICS` SQL DDL
    was introduced in Spark 4.2 via [SPARK-56920](https://issues.apache.org/jira/browse/SPARK-56920))
    and **Unity Catalog 0.6.0 or later**. Use the Spark 4.2 connector artifact:

    | Spark version | UC Spark connector artifact |
    | --- | --- |
    | Apache Spark 4.2.x | `io.unitycatalog:unitycatalog-spark_4.2_2.13` |

    Metric views are not available on Spark 4.0.x / 4.1.x. For general Spark setup (downloading
    Spark, configuring cloud storage, and the shell launch commands), see the
    [Apache Spark integration](../integrations/unity-catalog-spark.md) page.

## Launch Spark SQL against Unity Catalog

Start a Spark SQL shell wired to your local Unity Catalog server with the Spark 4.2 connector:

```sh
export CATALOG_NAME=unity
export UC_URI=http://localhost:8080
export UC_TOKEN=

bin/spark-sql --name "metric-view-test" \
    --master "local[*]" \
    --packages "io.unitycatalog:unitycatalog-spark_4.2_2.13:0.6.0" \
    --conf "spark.sql.catalog.$CATALOG_NAME=io.unitycatalog.spark.UCSingleCatalog" \
    --conf "spark.sql.catalog.$CATALOG_NAME.uri=$UC_URI" \
    --conf "spark.sql.catalog.$CATALOG_NAME.token=$UC_TOKEN" \
    --conf "spark.sql.defaultCatalog=$CATALOG_NAME"
```

## Create a source table

A metric view reads from an existing table. The metric-view definition resolves its `source:`
relation eagerly, so create the source first:

```sql
CREATE TABLE unity.default.events (region STRING, cnt INT)
USING parquet
LOCATION '/tmp/tables/events';

INSERT INTO unity.default.events VALUES ('us', 1), ('us', 2), ('eu', 3);
```

## Create a metric view

Use `CREATE VIEW ... WITH METRICS LANGUAGE YAML` to define the metric view. The YAML body declares
the `source`, the `dimensions` (grouping columns), and the `measures` (named aggregations):

```sql
CREATE VIEW unity.default.events_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: "0.1"
source: unity.default.events
dimensions:
  - name: region
    expr: region
measures:
  - name: cnt_sum
    expr: sum(cnt)
$$;
```

## List metric views

A metric view appears on the **view** surface via `SHOW VIEWS`:

```sql
SHOW VIEWS IN unity.default;
```

```console
+---------+--------------+-----------+
|namespace|      viewName|isTemporary|
+---------+--------------+-----------+
|  default|events_metrics|      false|
+---------+--------------+-----------+
```

It also appears on the **table** surface via `SHOW TABLES` (Spark 4.2 lists tables and views
together), alongside the source table:

```sql
SHOW TABLES IN unity.default;
```

```console
+---------+--------------+-----------+
|namespace|     tableName|isTemporary|
+---------+--------------+-----------+
|  default|        events|      false|
|  default|events_metrics|      false|
+---------+--------------+-----------+
```

## Inspect a metric view

`DESCRIBE` returns the metric view's dimension and measure columns:

```sql
DESCRIBE unity.default.events_metrics;
```

```console
+--------+---------+-------+
|col_name|data_type|comment|
+--------+---------+-------+
|  region|   string|   NULL|
| cnt_sum|   bigint|   NULL|
+--------+---------+-------+
```

`DESCRIBE EXTENDED` additionally reports the detailed view information, including that the object
is a `METRIC_VIEW`:

```sql
DESCRIBE EXTENDED unity.default.events_metrics;
```

## Query a metric view

Query a measure with the `measure(...)` function and `GROUP BY` on the dimensions. Spark rewrites
this into the aggregation defined by the measure (here, `sum(cnt)`) over the source table:

```sql
SELECT region, measure(cnt_sum) AS cnt_sum
FROM unity.default.events_metrics
GROUP BY region
ORDER BY region;
```

```console
+------+-------+
|region|cnt_sum|
+------+-------+
|    eu|      3|
|    us|      3|
+------+-------+
```

!!! note "Reading a measure requires `measure(...)`"
    Measures must be read through the `measure(...)` function; selecting a measure column directly
    (for example `SELECT cnt_sum FROM ...`) is not supported.

## Drop a metric view

```sql
DROP VIEW unity.default.events_metrics;
```

Use `DROP VIEW IF EXISTS` to make the drop a no-op when the view does not exist.

!!! note "Rename is not supported yet"
    Renaming a metric view (`ALTER VIEW ... RENAME TO`) is not supported yet and fails with an
    `UnsupportedOperationException`.
