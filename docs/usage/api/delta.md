# UC Delta API

Unity Catalog 0.5.0 introduced a dedicated REST API for catalog-managed Delta tables at
`/api/2.1/unity-catalog/delta/v1/...`.

The UC Delta API lets Delta clients create, load, list, alter, rename, and delete catalog-managed Delta tables through
a single REST surface. It also supports temporary credential vending and commit operations for managed tables.

## When to use it

Use the UC Delta API when you are building or integrating a Delta engine that needs to:

- Read and write catalog-managed Delta tables through Unity Catalog
- Vend scoped storage credentials for reads and writes
- Post and validate commits server-side before they land in table history

For general catalog metadata operations such as creating schemas, volumes, or functions, use the
[Unity Catalog REST API](index.md) instead.

## Quickstart

Start a local Unity Catalog server:

```sh
bin/start-uc-server
```

Set the API base path and verify the server is up:

```sh
export UC="http://localhost:8080/api/2.1/unity-catalog/delta/v1"

curl -s "$UC/config?catalog=unity&protocol-versions=1.0" | python3 -m json.tool
```

Load metadata for a preloaded sample table:

```sh
curl -s "$UC/catalogs/unity/schemas/default/tables/numbers" | python3 -m json.tool
```

Vend read credentials for that table:

```sh
curl -s "$UC/catalogs/unity/schemas/default/tables/numbers/credentials?operation=READ" | python3 -m json.tool
```

## API reference

The generated OpenAPI documentation lives in the repository:

- [UC Delta API reference](https://github.com/unitycatalog/unitycatalog/tree/main/api/delta-docs)
- [OpenAPI specification](https://github.com/unitycatalog/unitycatalog/tree/main/api)

## Spark integration

If you are using Apache Spark, the [Spark integration guide](../../integrations/unity-catalog-spark.md) shows how to
configure `UCSingleCatalog` with the version-matched `unitycatalog-spark` connector artifact.
