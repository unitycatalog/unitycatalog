# Unity Catalog Python Client SDK

The Python SDK for Unity Catalog that is published to [PyPI](https://pypi.org/project/unitycatalog-client/).

## How to Release the Unity Catalog Python Client SDK

### Generate Client Library

```sh
build/sbt pythonClient/generate
```

### Prepare Release Version

```sh
clients/python/build/prepare-release.sh <release version>
```

Example: releasing a release candidate version for Unity Catalog version 1.2.3 would use `1.2.3rc0`

### Build Package for Distribution

```sh
clients/python/build/build-python-package.sh
```

### Validate Package Contents

```sh
clients/python/build/validate-python-build.sh
```

### Run Tests

```sh
./run-tests.sh
```

### Publish to Test PyPI (optional)

```sh
twine upload --repository-url https://test.pypi.org/legacy/  clients/python/target/dist/*
```

> Note: A maintainer token for Test PyPI is required to upload to this repository.

### Publish to PyPI

```sh
twine upload clients/python/target/dist/*
```

> Note: A maintainer token for PyPI is required to upload to this repository.

### Build Documentation Locally

```sh
clients/python/build/build-docs.sh
```

The docs will be generated at `clients/python/target/docs/`.

-----------

## Detailed Instructions and Guidance for Maintainers

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
7. Remove irrelevant files that are artifacts of the generated code process. These files would otherwise be included in the archives within PyPI and serve no purpose.

For details on what operations are performed in the build process, see the [processing script](../../project/PythonPostBuild.scala) to learn more.

### Updating Release Version

If you are preparing for a release to either [Test PyPI](https://test.pypi.org/project/unitycatalog-client/) or [Main PyPI](https://pypi.org/project/unitycatalog-client/) you will need to run the version update script prior to building the distribution artifacts.

Version conventions are as follows:

**Development Build**: `<major>.<minor>.<micro>.dev0` (this should not be pushed to PyPI!)

**Release Candidate**: `<major>.<minor>.<micro>rc<n>` (increment `n` but always start with `0`. RC builds should be pushed to both TestPyPI and Main PyPI to ensure adequate e2e testing prior to releasing a final version.). **Note** that an `rc` build does not have a period (`.`) between the `<micro>` version and the `rc<n>` definition!

**Full Release**: `<major>.<minor>.<micro>`

> Note: To update for a release, prior to running the packaging script (shown in the next section), simply run from the repo root (as an example, this is preparing the 0.3.1 first release candidate build):

```sh
clients/python/build/prepare-release.sh 0.3.1rc0
```

### Packaging Client SDK

If you would like to generate the distributable artifacts (required for deploying `unitycatalog-client` to PyPI), simply execute the
packaging script, located [here](./build/build-python-package.sh).

```sh
clients/python/build/build-python-package.sh
```

This will create both a `.whl` artifact and a `bdist` archive for deploying
to PyPI.

If modifying the `pyproject.toml` file, please verify that the generated `.whl` file and the archived source are not empty:

```sh
clients/python/build/validate-python-build.sh
```

**Important: Ensure that there is no `__init__.py` residing in `unitycatalog/` root directory**. This will break the shared namespace behavior
with other `unitycatalog-x` packages on PyPI, rendering the usage of all installed libraries broken.

If there is an issue with the build pathing resolution, this archive will only contain the `METADATA`, `WHEEL`, and `RECORD` files, making
the library unusable.

## Updating Deployment Build Files

If you are updating requirements, modifying the release version, or are providing additional guidance within the distributed package's PyPI
README.md, ensure that you make updates to:

1. The [pyproject.toml](build/pyproject.toml) file. If you are releasing a new version of the SDK, the `version` field is updated properly to the appropriate version of Unity Catalog's release when generating the distributable package code (it will match the release version specified within the [version.sbt](../../version.sbt) file). This will only work for development builds. See the section above (`Updating the final release version`) for guidance on release version updates.
2. The `setup.py` file. This file's version is updated automatically during the build process with the version specified in the [version.sbt](../../version.sbt). See the section above (`Updating the final release version`) for guidance on release version updates.
3. The [README.md](build/README.md) file. This file is not updated with releases, but is the source of the landing page at [PyPI](https://pypi.org/project/unitycatalog-client/).

### Build Definition Updates

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

## Building Docs

To generate the API documentation for the Unity Catalog Client SDK, simply execute from repository root:

> Note: you will need to ensure that the client package code is generated before building the docs.

```sh
clients/python/build/build-docs.sh
```

The generated documentation will be in the `target` directory under `docs`.
