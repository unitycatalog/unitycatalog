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

The sbt assembly process for this SDK will:

1. Remove the generated `__init__.py` file from within the root `unitycatalog` directory. Having any `__init__.py` file in any of the shared namespace packages
at this level will invalidate the inferred shared namespace logic when Python
resolves common namespaces, effectively breaking all installed packages.
2. Remove the `git_push.sh` script as it is not needed and should not be used for
identification of a release (these packages are deployed to PyPI, not GitHub).
3. Replace the `pyproject.toml` generated file with the deployment `pyproject.toml` file that resides within the `unitycatalog/clients/python/build` directory.
4. Replace the `README.md` generated file with the deployment `README.md` files.
This file is attached to the PyPI deployment and should be kept up to date with
important information regarding the Python SDK.
5. Replace the `setup.py` generated file with the deployment `setup.py` file.
This is to ensure that if a script-based installation from the generated package
structure will build a valid namespace installation for local validation.

## Updating deployment build files

If you are updating requirements, modifying the release version, or are providing additional
guidance within the distributed package's PyPI README.md, ensure that you make
updates to:

1. The `pyproject.toml` file located at: `unitycatalog/clients/python/build/pyproject.toml`. If you are releasing a new version of the SDK, ensure that the `version` field is updated properly to the appropriate version of Unity Catalog's release.
2. The `setup.py` file location at: `unitycatalog/client/python/build/setup.py`. This file **must** be updated with a new version specifier when releasing a new version of the `unitycatalog-client` package so that local script-based installations of the source code will reflect the correct version.
3. The `README.md` file located at: `unitycatalog/clients/python/build/README.md`. This file is not updated with releases, but is the source of the landing page at [PyPI](https://pypi.org/project/unitycatalog-client/).

### Build definition updates

To modify the packaging definition, ensure that changes are made to `unitycatalog/client/python/build/pyproject.toml` and **not to the generated `pyproject.toml`** that is created when running the sbt build command. If making changes to this file other than simple version updates, ensure that the namespace package behavior still functions by installing `unitycatalog-ai` in the same python environment and that the results of running the following code shows **both** the `unitycatalog-client` **and** the `unitycatalog-ai` namespaces listed in the result.

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
