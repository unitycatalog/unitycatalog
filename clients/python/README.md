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

1. Exclude the creation of the root `__init__.py` file from the shared namespace root (`unitycatalog`).
2. Exclude the creation of package building files (`pyproject.toml` and `setup.py`) in favor of the distribution versions of these files.
3. Remove additional irrelevant files (for details, see the definitions within the [.openapi-generator-ignore](.openapi-generator-ignore) file).
4. Copy over the release versions of `pyproject.toml`, `setup.py`, and the release package `README.md` file to the correct locations within
the generated code directories.

For details on what operations are performed in the build process, see the [processing script](../../project/PythonPostBuild.scala) to learn more.

## Updating deployment build files

If you are updating requirements, modifying the release version, or are providing additional
guidance within the distributed package's PyPI README.md, ensure that you make
updates to:

1. The [pyproject.toml](build/pyproject.toml) file. If you are releasing a new version of the SDK, the `version` field is updated properly to the appropriate version of Unity Catalog's release when generating the distributable package code (it will match the release version specified within the [version.sbt](../../version.sbt) file).
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
