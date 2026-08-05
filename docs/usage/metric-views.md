# Unity Catalog Metric Views

This page shows you how to create, query, and manage **metric views** in Unity Catalog using
Apache Spark™.

A metric view is a view-like object that defines reusable **dimensions** and **measures** (named
aggregations) over a source — either a table/view or a SQL query — expressed in YAML. Consumers
query the measures with the `measure(...)` function and group by the dimensions, so the aggregation
logic lives in one governed place in Unity Catalog instead of being copy-pasted across queries.

!!! warning "Prerequisites"
    Metric views require **Apache Spark 4.2 or later** (the `CREATE VIEW ... WITH METRICS` SQL DDL
    was introduced in Spark 4.2) and the **Unity Catalog 0.6.0** Spark 4.2 connector. Use the
    Spark 4.2 connector artifact:

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

!!! note "No Delta package here"
    Unlike the examples on the [Apache Spark integration](../integrations/unity-catalog-spark.md)
    page, this command deliberately omits the `delta-spark` package and the
    `spark.sql.catalog.spark_catalog=...DeltaCatalog` conf, because the walkthrough below uses a
    parquet source table. The connector logs a benign `WARN ... DeltaCatalog is not available in
    the classpath`, which is expected. A Delta source table is not an option on Spark 4.2
    yet, since no `delta-spark_4.2_2.13` artifact is published.

## Create a source table

A metric view computes over an existing source. Create a parquet external table to use as the
source (this needs no Delta package and works with the launch command above):

```sql
CREATE TABLE unity.default.events (region STRING, cnt INT)
USING parquet LOCATION '/tmp/uc_events_src';

INSERT INTO unity.default.events VALUES ('us', 1), ('us', 2), ('eu', 3);
```

!!! note
    A `CREATE TABLE` without a `LOCATION` (a managed table) fails with `Unity Catalog does not
    support non-Delta managed table.` — use an external (`LOCATION`) parquet table as shown, or a
    managed Delta table on a Spark version where `delta-spark` is available.

## Create a metric view

Use `CREATE VIEW ... WITH METRICS LANGUAGE YAML` to define the metric view. The YAML body declares:

- `source` — the data the metrics are computed over. This can be a **table or view**
  (`unity.default.events`) or a **SQL query** (`SELECT ... FROM ...`).
- `filter` *(optional)* — a row filter applied to the source, like a `WHERE` clause.
- `dimensions` — the grouping columns.
- `measures` — the named aggregations.

The example below computes metrics over the `events` table created above:

```sql
CREATE VIEW unity.default.events_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: "0.1"
source: unity.default.events
filter: cnt > 0
dimensions:
  - name: region
    expr: region
measures:
  - name: cnt_sum
    expr: sum(cnt)
$$;
```

To compute metrics over a query instead of a table, set `source` to a SQL statement, for example
`source: SELECT region, cnt FROM unity.default.events WHERE region <> 'unknown'`.

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

It also appears on the **table** surface via `SHOW TABLES`, alongside the source table:

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

```console
+---------------------------+-----------------------------------------------------+-------+
|col_name                   |data_type                                            |comment|
+---------------------------+-----------------------------------------------------+-------+
|region                     |string                                               |NULL   |
|cnt_sum                    |bigint                                               |NULL   |
|                           |                                                     |       |
|# Detailed View Information|                                                     |       |
|Catalog                    |unity                                                |       |
|Database                   |default                                              |       |
|Table                      |events_metrics                                       |       |
|View                       |events_metrics                                       |       |
|Type                       |METRIC_VIEW                                          |       |
|View Query Output Columns  |[region, cnt_sum]                                    |       |
|Properties                 |[metric_view.from.name=unity.default.events,         |       |
|                           | metric_view.where=cnt > 0]                          |       |
+---------------------------+-----------------------------------------------------+-------+
```

!!! note
    The output above is illustrative and trimmed for readability; the exact rows and column widths
    vary by Spark version.

## Query a metric view

Query a measure with the `measure(...)` function and `GROUP BY` on the dimensions. Spark rewrites
this into the aggregation defined by the measure (here, `sum(cnt)`) over the source:

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
