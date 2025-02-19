# Unity Catalog Kùzu Integration

[Kùzu](https://kuzudb.com/) is an embedded graph database built for query speed and scalability. You can download and install the precompiled binary [there](https://docs.kuzudb.com/installation/).
The `unity_catalog` extension in Kùzu allows users to attach to a unity catalog and perform scan/copy operations on delta lake tables.

## Usage

The Unity Catalog extension can be installed and loaded by running the following commands using the Kùzu CLI
or your preferred language client API:

```sql
INSTALL unity_catalog;
LOAD EXTENSION unity_catalog;
```

In the following example, we will attach to the `default` schema under the `unity` catalog and scan the `numbers` delta table.
The table, schema and catalog are pre-defined in the Unity Catalog, so you don't have to create them.
#### 1. Attach to Unity Catalog

```sql
ATTACH [CATALOG_NAME] AS [alias] (dbtype UC_CATALOG)
```

- `CATALOG_NAME`: The catalog name to attach to in the Unity Catalog
- `alias`: Database alias to use in Kùzu - If not provided, the catalog name will be used.
  When attaching multiple databases, it's recommended to use aliasing.

:::note[Note]
Kùzu attaches to the `default` schema under the given catalog name. Specifying the schema to attach is not supported right now.
:::

#### 2. Data type mapping from Unity Catalog to Kùzu

The table below shows the mapping from Unity Catalog's type to Kùzu's type:
| Data type in Unity Catalog         | Corresponding data type in Kùzu |
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

:::caution[Note]
Currently, Kùzu only supports scanning from Delta Lake tables registered in the Unity Catalog.
:::

#### 4. `USE` statement

You can use the `USE` statement for attached Unity Catalog to use a default Unity Catalog name (without an alias)
for future operations.
This can be used when reading from an attached Unity Catalog to avoid specifying the full catalog name
as a prefix to the table name.

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

#### 5. Copy data from table

One important use case of the Unity Catalog extension is to facilitate seamless data transfer from tables in Unity Catalog to Kùzu.
In this example, we continue using the `unity` database, but this time,
we copy the data and persist it to Kùzu. This is done with the `COPY FROM` statement. Here is an example:

We first create a `numbers` table in Kùzu. In this example we will make `numbers` have the same schema as the one defined in the Unity Catalog.

```cypher
CREATE NODE TABLE numbers (id INT32, score DOUBLE , PRIMARY KEY(id));
```

When the schemas are the same, we can copy the data from the external Unity Catalog table to the Kùzu table simply as follows.

```sql
 copy numbers from unity.numbers;
```
If the schemas are not the same, e.g., `numbers` contains only `score` property while the external `unity.numbers` contains
`id` and `score`, we can still use `COPY FROM` but with a subquery as follows:
```sql
COPY numbers FROM (LOAD FROM unity.numbers RETURN score);
```

#### 6. Query the data in Kùzu

Finally, we can verify the data in the `numbers` table in Kùzu.

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

:::note[Note]
Kùzu's unity catalog extension is a starting point towards a larger integration with the lakehouse ecosystem. It may have unresolved issues from upstream. To address these
issues or to discuss your use case further, please reach out to us on [Discord](https://kuzudb.com/chat).
:::
