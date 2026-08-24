# Unity Catalog Python Client SDK

Welcome to the official Python Client SDK for Unity Catalog!

Unity Catalog is the industry's only universal catalog for data and AI.

- **Multimodal interface supports any format, engine, and asset**
    - Multi-format support: It is extensible and supports Delta Lake, Apache Iceberg and Apache Hudi via UniForm, Apache Parquet, JSON, CSV, and many others.
    - Multi-engine support: With its open APIs, data cataloged in Unity can be read by many leading compute engines.
    - Multimodal: It supports all your data and AI assets, including tables, files, functions, AI models.
- **Open source API and implementation** - OpenAPI spec and OSS implementation (Apache 2.0 license). It is also compatible with Apache Hive's metastore API and Apache Iceberg's REST catalog API. Unity Catalog is currently a sandbox project with LF AI and Data Foundation (part of the Linux Foundation).
- **Unified governance** for data and AI - Govern and secure tabular data, unstructured assets, and AI assets with a single interface.

## Python Client SDK

The Unity Catalog Python SDK provides a convenient Python-native interface to all of the functionality of the Unity Catalog
REST APIs. The library includes interfaces for all supported public modules.

This library is generated using the [OpenAPI Generator](https://openapi-generator.tech/docs/generators/python) toolkit, providing client interfaces using the `aiohttp` request handling library.

## Installation

The Python Client SDK and associated shared namespace package for Unity Catalog use [hatch](https://hatch.pypa.io/latest/) as their supported build backend.

To ensure that you can install the package, install `hatch` via any of the listed options [here](https://hatch.pypa.io/latest/install/).

To use the `unitycatalog-client` SDK, you can install directly from PyPI:

```sh
pip install unitycatalog-client
```

To install from source, you will need to fork and clone the [unitycatalog repository](https://github.com/unitycatalog/unitycatalog) locally.

To build the Python source locally, you will need to have `JDK17` installed and activated.

Once your configuration supports the execution of [sbt](https://www.scala-sbt.org/), you can run the following within the root
of the repository to generate the Python Client SDK source:

```sh
build/sbt pythonClient/generate
```

The source code will be generated at `unitycatalog/clients/python/target`. 

You can then install the package in editable mode from the repository root:

```sh
pip install -e clients/python/target
```

## Usage

To get started with using the Python Client SDK, first ensure that you have a running Unity Catalog server to connect to.
You can follow instructions [here](https://docs.unitycatalog.io/quickstart/) to quickly get started with setting up
a local Unity Catalog server if needed.

Once the server is running, you can set up the Python client to make async requests to the Unity Catalog server.

For the examples listed here, we will be using a local unauthenticated server for simplicity's sake.

```python
from unitycatalog.client import Configuration


config = Configuration()
config.host = "http://localhost:8080/api/2.1/unity-catalog"
```

Once we have our configuration, we can set our client that we will be using for each request:

```python
from unitycatalog.client import ApiClient


client = ApiClient(configuration=config)
```

With our client configured and instantiated, we can use any of the Unity Catalog APIs by importing from the
`client` namespace directly and send requests to the server.

```python
from unitycatalog.client import CatalogsApi


catalogs_api = CatalogsApi(api_client=client)
my_catalogs = await catalogs_api.list_catalogs()
```

>Note: APIs that support pagination (such as `list_catalogs`) should have continutation token logic for assembling the paginated
return values into a single collection.

A simple example of consuming a paginated response is:

```python
async def list_all_catalogs(catalog_api):
  token = None
  catalogs = []
  while True:
    results = await catalog_api.list_catalogs(page_token=token)
    catalogs += results.catalogs
    if next_token := results.next_page_token:
      token = next_token
    else:
      break
  return catalogs

my_catalogs = await list_all_catalogs(catalogs_api)
```

Creating a new catalog with the Python SDK is straight-forward:

```python
from unitycatalog.client.models import CreateCatalog, CatalogInfo


async def create_catalog(catalog_name, catalog_api, comment=None):
    new_catalog = CreateCatalog(
        name=catalog_name,
        comment=comment or ""
    )
    return await catalog_api.create_catalog(create_catalog=new_catalog)
        
await create_catalog("MyNewCatalog", catalog_api=catalogs_api, comment="This is a new catalog.")
```

Adding a new Schema to our created Catalog is similarly simple:

```python
from unitycatalog.client import SchemasApi
from unitycatalog.client import CreateSchema, SchemaInfo


schemas_api = SchemasApi(api_client=client)

async def create_schema(schema_name, catalog_name, schema_api, comment=None):
    new_schema = CreateSchema(
        name=schema_name,
        catalog_name=catalog_name,
        comment=comment or ""
    )
    return await schema_api.create_schema(create_schema=new_schema)

await create_schema(schema_name="MyNewSchema", catalog_name="MyNewCatalog", schema_api=schemas_api, comment="This is a new schema.")
```

And listing the schemas within our newly created Catalog (note that if you expect paginated responses, ensure that you are passing
continuation tokens as shown above in `list_all_catalogs`):

```python
await schemas_api.list_schemas(catalog_name="MyNewCatalog")
```

## Feedback

Have requests for the Unity Catalog project? Interested in getting involved in the Open Source project?

See the [repository on GitHub](https://github.com/unitycatalog/unitycatalog)

Read [the documentation](https://www.unitycatalog.io/) for more guidance and examples!
