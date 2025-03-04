# Unity Catalog Kuzu Integration

[Kuzu](https://kuzudb.com/) is an embedded graph database built for query speed and scalability. It
supports the property graph data model and the Cypher query language.

Download and install the precompiled binary of Kuzu
for your OS as per the instructions shown [here](https://docs.kuzudb.com/installation/).
The `unity_catalog` functionality in Kuzu is made available via an extension that allows you to attach to a Unity Catalog and perform scan/copy operations on Delta tables.

## Prerequisites

To run the examples shown below, you need the following:

- A running Unity Catalog server
- Kuzu's [CLI](https://docs.kuzudb.com/installation/#command-line-shell) installed

Follow the steps in the Unity Catalog [quickstart](https://docs.unitycatalog.io/quickstart/) docs to create a Unity Catalog server.

```bash
# Clone the repo
> git clone git@github.com:unitycatalog/unitycatalog.git

# Ensure you have Java 18+ installed
> java -version
openjdk version "23.0.2" 2025-01-21
OpenJDK Runtime Environment Homebrew (build 23.0.2)
OpenJDK 64-Bit Server VM Homebrew (build 23.0.2, mixed mode, sharing)

# Run a local Unity Catalog server
> cd unitycatalog
> bin/start-uc-server
```

Once the Unity Catalog server is running, you can proceed with the rest of this tutorial by leveraging
the `unity` numbers table that is pre-defined in the Unity Catalog.

## Using the Unity Catalog extension in Kuzu

Kuzu's Unity Catalog extension can be installed and loaded by running the following commands using the Kuzu CLI
or your preferred language client API:

```sql
INSTALL unity_catalog;
LOAD EXTENSION unity_catalog;
```

In the following example, we will attach to the `default` schema under the `unity` catalog and scan the `numbers` Delta table.
Note that the table, schema and catalog are pre-defined in the running Unity Catalog server, so you don't have to create them yourself.

#### 1. Attach to Unity Catalog

```sql
ATTACH [CATALOG_NAME] AS [alias] (dbtype UC_CATALOG)
```

- `CATALOG_NAME`: The catalog name to attach to in the Unity Catalog
- `alias`: Database alias to use in Kuzu - If not provided, the catalog name will be used.
  When attaching to multiple databases, it's recommended to use aliasing.

For the example shown below, we will use the following command that attaches to the `default` schema under the `unity` catalog:

```sql
ATTACH 'unity' AS unity (dbtype UC_CATALOG);
```

**Note:** Kuzu attaches to the `default` schema under the given catalog name. Specifying a custom schema to attach to is not currently supported.


#### 2. Data type mapping from Unity Catalog to Kuzu

The table below shows the mapping from Unity Catalog's type to Kuzu's type:
| Data type in Unity Catalog         | Corresponding data type in Kuzu |
|-----------------------------|----------------------------------|
| BOOLEAN                     | BOOLEAN                           |
| BYTE                        | UNSUPPORTED                          |
| SHORT                       | INT16                                 |
| INT                    | INT32                                 |
| LONG                       | INT64                                 |
| DOUBLE                     | DOUBLE                                 |
| FLOAT                      | FLOAT                                 |
| DATE                    | DATE                                 |
| TIMESTAMP                    | TIMESTAMP                                 |
| TIMESTAMP_NTZ                   | UNSUPPORTED                                 |
| STRING                   | STRING                                 |
| BINARY                       | UNSUPPORTED                      |
| DECIMAL   | DECIMAL                                 |

If the type is marked as "unsupported", we do not support scanning it in Kuzu.
#### 3. Scan data from table

Finally, we can utilize the `LOAD FROM` statement to scan the `numbers` table. Note that you need to prefix the 
external `numbers` table with the database alias (in our example `unity`). See the `USE` statement which allows you to
skip this prefixing for a specific default database.

```sql
LOAD FROM unity.numbers
RETURN *
```

Result:

```
┌────────┬────────────┐
│ as_int │ as_double  │
│ INT32  │ DOUBLE     │
├────────┼────────────┤
│ 564    │ 188.755356 │
│ 755    │ 883.610563 │
│ 644    │ 203.439559 │
│ 75     │ 277.880219 │
│ 42     │ 403.857969 │
│ 680    │ 797.691220 │
│ 821    │ 767.799854 │
│ 484    │ 344.003740 │
│ 477    │ 380.678561 │
│ 131    │ 35.443732  │
│ 294    │ 209.322436 │
│ 150    │ 329.197303 │
│ 539    │ 425.661029 │
│ 247    │ 477.742227 │
│ 958    │ 509.371273 │
└────────┴────────────┘
```

Currently, Kuzu only supports scanning from Delta Lake tables registered in the Unity Catalog.


#### 4. `USE` statement

If you want to refer to Delta tables without aliasing, e.g., referring to `unity.numbers` simply as `numbers`, you can also use the `USE` command to set a default namespace.

Consider the same attached Unity Catalog as above:

```sql
ATTACH 'unity' AS unity (dbtype UC_CATALOG);
```

Instead of defining the catalog name via `unity.numbers` for each subsequent clause, you can do:

```sql
USE unity;
LOAD FROM numbers
RETURN *
```

#### 5. Copy data from a Delta table

The main purpose of the Unity Catalog extension is to facilitate seamless data transfer from tables in Unity Catalog to Kuzu.
In this example, we continue using the `unity` database, but this time,
we copy the data and persist it to Kuzu database. This is done with the `COPY FROM` statement. Here is an example:

We first create a `numbers` table in Kuzu. In this example we will make `numbers` have the same schema as the one defined in the Unity Catalog.

```sql
CREATE NODE TABLE numbers (id INT32, score DOUBLE , PRIMARY KEY(id));
```

When the schemas are the same, we can copy the data from the external Unity Catalog table to the Kuzu table simply as follows.

```sql
COPY numbers FROM unity.numbers;
```
In some cases, you may want to copy data into a subset of the properties in the Kuzu table. The following example shows how to copy a `unity.score` Delta table into the `score` property of the `numbers(id, score)` node table in Kuzu (setting the IDs values to their default null).
```sql
COPY numbers(score) FROM (LOAD FROM unity.score RETURN as_double);
`id` and `score`, we can still use `COPY FROM` but with a subquery that transforms the scanned tuples from `unity.numbers` into the schema of Kuzu table.
```sql
COPY numbers FROM (LOAD FROM unity.numbers RETURN score);
```

#### 6. Query the data in Kuzu

Finally, we can verify the data in the `numbers` table in Kuzu.

```cypher
MATCH (n:numbers) RETURN n.*;
```

Result:
```
┌───────┬────────────┐
│ n.id  │ n.score    │
│ INT32 │ DOUBLE     │
├───────┼────────────┤
│ 564   │ 188.755356 │
│ 755   │ 883.610563 │
│ 644   │ 203.439559 │
│ 75    │ 277.880219 │
│ 42    │ 403.857969 │
│ 680   │ 797.691220 │
│ 821   │ 767.799854 │
│ 484   │ 344.003740 │
│ 477   │ 380.678561 │
│ 131   │ 35.443732  │
│ 294   │ 209.322436 │
│ 150   │ 329.197303 │
│ 539   │ 425.661029 │
│ 247   │ 477.742227 │
│ 958   │ 509.371273 │
└───────┴────────────┘
```

#### 7. Detach Unity Catalog

To detach a Unity Catalog, use `DETACH [ALIAS]` as follows:

```
DETACH unity
```

## Contact

If you face any issues while using Kuzu's Unity Catalog extension,
please reach out to the Kuzu team
on [Discord](https://kuzudb.com/chat). Alternatively,
you can open an issue or discussion in our [GitHub](https://github.com/kuzudb/kuzu) repo.
