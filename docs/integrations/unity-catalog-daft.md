# Unity Catalog Daft Integration

This page shows you how to use Unity Catalog with Daft.

[Daft](https://getdaft.io/) is a library for parallel and distributed processing of multimodal data.

## Set up

To start, install Daft with the extra Unity Catalog dependencies using:

```sh
pip install -U "getdaft[unity,deltalake]"
```

Then import Daft and the `UnityCatalog` abstraction:

```python
import daft
from daft.unity_catalog import UnityCatalog
```

You need to have a Unity Catalog server running to connect to.

For testing purposes, you can spin up a local server by running the code below in a terminal:

```sh
bin/start-uc-server
```

## Connect Daft to Unity Catalog

Use the `UnityCatalog` abstraction to point Daft to your UC server.

This object requires an `endpoint` and a `token`. If you launched the UC server locally using the command above then
you can use the values below. Otherwise, substitute the `endpoint` and `token` values with the corresponding values
for your UC server.

```python
# point Daft to your UC server
unity = UnityCatalog(
    endpoint="http://127.0.0.1:8080",
    token="not-used",
)
```

You can also connect to a Unity Catalog in your Databricks workspace by using the following setting:

```python
endpoint = "https://<databricks_workspace_id>.cloud.databricks.com"
```

Once you're connected, you can list all your available catalogs using:

```console
> print(unity.list_catalogs())
['unity']
```

You can list all available schemas in a given catalog:

```console
> print(unity.list_schemas("unity"))
['unity.default']
```

And you can list all the available tables in a given schema:

```console
print(unity.list_tables("unity.default"))
['unity.default.numbers', 'unity.default.marksheet_uniform', 'unity.default.marksheet']
```

## Load Unity Tables into Daft DataFrame

You can use Daft to read Delta Lake tables in a Unity Catalog.

First, point Daft to your Delta table stored in your Unity Catalog:

```python
unity_table = unity.load_table("unity.default.numbers")
```

Unity Catalog tables are stored in the Delta Lake format.

Simply read your table using the Daft `read_deltalake` method:

```python
> df = daft.read_deltalake(unity_table)
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

Any subsequent filter operations on the Daft `df` DataFrame object will be correctly optimized to take advantage of
Delta Lake features.

```python
> df = df.where(df["as_int"] > 500)
> df.show()

as_int   as_double
564      188.755356
755      883.610563
644      203.439559
680      797.691220
821      767.799854
539      425.661029
958      509.371273
```

Daft support for Unity Catalog is under rapid development. Refer to the
[Daft documentation](https://www.getdaft.io/projects/docs/en/stable/integrations/unity_catalog/) for
more information.
