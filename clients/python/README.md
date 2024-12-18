# Unity Catalog Python Client SDK

The Python SDK for Unity Catalog that is published to https://pypi.org/project/unitycatalog-client/.

## Generate Client Library

To generate the client library code from the API specification in `api/all.yaml`, run:

```sh
build/sbt pythonClient/generate
```

The generated library will be located in `clients/python/target`.

## Package Build Process

The sbt build configuration that runs to build a deployable package for the SDK
replaces several generic auto-generated files and performs some required cleanup actions
on the generated directory structure to ensure that this package conforms to the
namespace package configuration required by [this guidance](https://packaging.python.org/en/latest/guides/packaging-namespace-packages/).

This package shares the root `unitycatalog.*` namespace with the other suite of
unitycatalog official packages (`unitycatalog-ai`, `unitycatalog-openai`, etc.).

The sbt generation process for this SDK will:

1. Copy over the ignore rules for the OpenAPI Generator to the `target` directory from the [build location](build/).
2. Exclude the creation of the root `__init__.py` file from the shared namespace root (`unitycatalog`).
3. Exclude the creation of package building files (`pyproject.toml` and `setup.py`) in favor of the distribution versions of these files.
4. Remove additional irrelevant files (for details, see the definitions within the [.openapi-generator-ignore](build/.openapi-generator-ignore) file).
5. Copy over the release versions of `pyproject.toml`, `setup.py`, and the release package `README.md` file to the correct locations within
the generated code directories.
6. Place the generated source code into a hatch-compatible `src` directory for packaging of a shared namespace package.

For details on what operations are performed in the build process, see the [processing script](../../project/PythonPostBuild.scala) to learn more.

### Packaging the Client SDK into distribution formats

If you would like to generate the distributable artifacts (required for deploying `unitycatalog-client` to PyPI), simply execute the
packaging script, located [here](./build/build-python-package.sh). This will create both a `.whl` artifact and a `bdist` archive for deploying
to PyPI.

If modifying the `pyproject.toml` file, please verify that the generated `.whl` file contains all required modules by running (from repo root):

```sh
unzip -l clients/python/target/dist/unitycatalog_client-0.3.0.dev0-py3-none-any.whl
```

The above example is for development version 0.3.0.dev0. Update accordingly with the version that you are generating. The output of this command
should provide a full listing of the namespace directories:

```sh
➜ unzip -l clients/python/target/dist/unitycatalog_client-0.3.0.dev0-py3-none-any.whl
Archive:  clients/python/target/dist/unitycatalog_client-0.3.0.dev0-py3-none-any.whl
  Length      Date    Time    Name
---------  ---------- -----   ----
     6485  02-02-2020 00:00   unitycatalog/client/__init__.py
    26492  02-02-2020 00:00   unitycatalog/client/api_client.py
      652  02-02-2020 00:00   unitycatalog/client/api_response.py
...
      846  02-02-2020 00:00   unitycatalog/client/models/volume_operation.py
      769  02-02-2020 00:00   unitycatalog/client/models/volume_type.py
     7823  02-02-2020 00:00   unitycatalog_client-0.3.0.dev0.dist-info/METADATA
       87  02-02-2020 00:00   unitycatalog_client-0.3.0.dev0.dist-info/WHEEL
     8651  02-02-2020 00:00   unitycatalog_client-0.3.0.dev0.dist-info/RECORD
---------                     -------
   752681                     84 files
```

**Important: Ensure that there is no `__init__.py` residing in `unitycatalog/` root directory**. This will break the shared namespace behavior
with other `unitycatalog-x` packages on PyPI, rendering the usage of all installed libraries broken.

If there is an issue with the build pathing resolution, this archive will only contain the `METADATA`, `WHEEL`, and `RECORD` files, making
the library unusable.

## Updating deployment build files

If you are updating requirements, modifying the release version, or are providing additional guidance within the distributed package's PyPI
README.md, ensure that you make updates to:

1. The [pyproject.toml](build/pyproject.toml) file. If you are releasing a new version of the SDK, the `version` field is updated properly to the appropriate version of Unity Catalog's release when generating the distributable package code (it will match the release version specified within the [version.sbt](../../version.sbt) file). This does not need to be manually updated.
2. The `setup.py` file. This file's version is updated automatically during the build process with the version specified in the [version.sbt](../../version.sbt).
3. The [README.md](build/README.md) file. This file is not updated with releases, but is the source of the landing page at [PyPI](https://pypi.org/project/unitycatalog-client/).

### Build definition updates

To modify the packaging definition, ensure that changes are made to [the pyproject file](build/pyproject.toml) and [the setup.py file](build/setup.py). If making changes to this file other than simple version updates, ensure that the namespace package behavior still functions by installing `unitycatalog-ai` in the same python environment and that the results of running the following code shows **both** the `unitycatalog-client` **and** the `unitycatalog-ai` namespaces listed in the result.

```python
import unitycatalog
print(unitycatalog.__path__)
```

Running this should show a result with at least 2 entries, such as:

```sh
_NamespacePath(['/Users/me/.pyenv/versions/anaconda3-2022.10/envs/py-311/lib/python3.11/site-packages/unitycatalog', '/Users/me/repos/unitycatalog/ai/core/src/unitycatalog', '/Users/me/repos/unitycatalog/ai/integrations/anthropic/src/unitycatalog', '/Users/me/repos/unitycatalog/clients/python/target/unitycatalog', '/Users/me/repos/unitycatalog/ai/integrations/langchain/src/unitycatalog', '/Users/me/repos/unitycatalog/ai/integrations/llama_index/src/unitycatalog', '/Users/me/repos/unitycatalog/ai/integrations/openai/src/unitycatalog'])
```

## Run Tests

Client tests use the `pytest` library. To run them:

1. Install dependencies

    ```sh
    pip install requests pytest
    ```

2. Generate the client library, install it, and run the tests

    ```sh
    ./run-tests.sh
    ```

## Building docs for release

To generate the API documentation for the Unity Catalog Client SDK, simply execute from repository root:

> Note: you will need to ensure that the client package code is generated before building the docs.

```sh
clients/python/build/build-docs.sh
```

The generated documentation will be in the `target` directory.
