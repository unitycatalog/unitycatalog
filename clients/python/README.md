# Unity Catalog Python Client

The Python SDK for Unity Catalog that is published to https://pypi.org/project/unitycatalog/.

## Generate Client Library

To generate the client library code from the API specification in `api/all.yaml`, run:
```sh
build/sbt generate
```

The generated library will be located in `clients/python/target`.

## Run Tests

Client tests use the `pytest` library. To run them:
1. Install dependencies
    ```sh
    pip install requests pytest
    ```
2. Install unitycatalog package
    ```sh
    pip install clients/python/target/
    ```
3. Run tests
    ```sh
    pytest clients/python/tests
    ```