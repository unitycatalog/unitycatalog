# How to work with Functions using the REST API

Use the `/functions` endpoint to work with functions.

| Method | Description                              | Endpoint                           |
| ------ | ---------------------------------------- | ---------------------------------- |
| GET    | Retrieve a list of functions             | /functions                         |
| GET    | Retrieve metadata of a specific function | /functions/catalog.schema.function |
| POST   | Create a new function                    | /functions                         |
| DELETE | Remove a function                        | /functions/catalog.schema.function |

The following sections show how to use each of these methods. Examples are demonstrated using Python and the `requests` library.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list functions

Use the `GET` command and the `/functions` endpoint to get a list of all registered functions. You will need to supply the catalog and schema names.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/functions"
URL = f"{BASE_URL}{ENDPOINT}"

params = {
    "catalog_name": "unity",
    "schema_name": "default"
}

response = requests.get(URL, params=params)
response.json()
```

## How to get function metadata

To retrieve metadata about a specific function, use the GET command at the `/functions/<full-function-name>` endpoint. The full name means the 3-level namespace reference in the standard Unity Catalog format: `catalog.schema.function`.

For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/functions/unity.default.lowercase"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
response.json()
```

This will return all available metadata for the specified function:

```
{'name': 'lowercase',
 'catalog_name': 'unity',
 'schema_name': 'default',
 'input_params': {'parameters': [{'name': 's',
    'type_text': 'string',
    'type_json': '{"name":"s","type":"string","nullable":false,"metadata":{}}',
    'type_name': 'STRING',
    'type_precision': None,
    'type_scale': None,
    'type_interval_type': None,
    'position': 0,
    'parameter_mode': 'IN',
    'parameter_type': 'PARAM',
    'parameter_default': None,
    'comment': None}]},
 'data_type': 'STRING',
 'full_data_type': 'STRING',
 'return_params': None,
 'routine_body': 'EXTERNAL',
 'routine_definition': 'g = s.lower()\\nreturn g',
 'routine_dependencies': None,
 'parameter_style': 'S',
 'is_deterministic': True,
 'sql_data_access': 'NO_SQL',
 'is_null_call': False,
 'security_type': 'DEFINER',
 'specific_name': 'lowercase',
 'comment': 'Converts a string to lowercase.',
 'properties': None,
 'full_name': 'unity.default.lowercase',
 'owner': None,
 'created_at': 1721234405640,
 'created_by': None,
 'updated_at': None,
 'updated_by': None,
 'function_id': 'ebb706d6-7ba0-4b2e-8d9c-716d13fd57f8',
 'external_language': 'python'}
```

## How to delete a function

Use the DELETE command at the `/functions/<full-function-name>` endpoint to delete a specific function. The full name is the 3-level namespace reference to your function: `catalog.schema.function`. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/functions/unity.default.lowercase"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.delete(URL)
```
