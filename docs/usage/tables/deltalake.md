# Delta Lake

This page explains how Delta Lake works so you can get the most out of your Unity Catalog tables. It will also show some common ways of working with Delta tables.

## Get

bin/uc table get

## Create

bin/uc table create

## Read

bin/uc table read

## Write

bin/uc table write --full_name <catalog>.<schema>.<table>

## Delete

bin/uc table delete

### Delta Lake with Daft

Here is an example of how you can use Delta Lake’s powerful features from Unity Catalog using [Daft](https://getdaft.io)

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
