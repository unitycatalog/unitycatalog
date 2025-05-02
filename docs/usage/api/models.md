# How to work with Models using the REST API

Use the `/models` endpoint to work with models.

| Method | Description                           | Example                 |
| ------ | ------------------------------------- | ----------------------- |
| GET    | Retrieve a list of models             | GET /models             |
| GET    | Retrieve metadata of a specific model | GET /models/my_model    |
| POST   | Create a new model                    | POST /models            |
| DELETE | Remove a model                        | DELETE /models/my_model |

The following sections show how to use each of these methods. Examples are demonstrated using Python and the `requests` library.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list models

Use the `GET` command at the `/models` endpoint to retrieve a list of all available volumes on the server. You will need to specify the catalog and schema names.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/models"
URL = f"{BASE_URL}{ENDPOINT}"

params = {
    "catalog_name": "unity",
    "schema_name": "default"
}

response = requests.get(URL, params=params)
response.json()
```

## How to get model metadata

To retrieve metadata about a specific model, use the GET command at the `/models/<full-model-name>` endpoint. The full name means the 3-level namespace reference in the standard Unity Catalog format: `catalog.schema.model`.

For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/models/unity.default.model_name"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
data = response.json()
```

## How to delete a model

Use the `DELETE` command at the `/models/<full-model-name>` endpoint to delete a specific model. The full name is the 3-level namespace reference to your model: `catalog.schema.model`. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/functions/unity.default.lowercase"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.delete(URL)
```
