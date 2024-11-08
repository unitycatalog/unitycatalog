# Unity Catalog Python Client

The Python SDK for Unity Catalog that is published to https://pypi.org/project/unitycatalog/.

## Generate Client Library

To generate the client library code from the API specification in `api/all.yaml`, run:
```sh
build/sbt pythonClient/generate
```

The generated library will be located in `clients/python/target`.

## Run Tests

Client tests use the `pytest` library. To run them:
1. Install dependencies
    ```sh
    pip install requests pytest
    ```
2. Generate the client library, install it, and run the tests
    ```
    ./run-tests.sh
    ```
