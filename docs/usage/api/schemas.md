# How to work with Schemas using the REST API

Use the `/schemas` endpoint to work with schemas.

| Method | Description                                               | Example                   |
| ------ | --------------------------------------------------------- | ------------------------- |
| GET    | Retrieve a list of schemas                                | GET /schemas              |
| GET    | Retrieve metadata of a specific schema                    | GET /schemas/my_schema    |
| POST   | Create a new schema                                       | POST /schemas             |
| PATCH  | Partially update an existing schema, e.g. the description | PATCH /schemas/my_schema  |
| DELETE | Remove a schema                                           | DELETE /schemas/my_schema |

The following sections show how to use each of these methods. Examples are demonstrated using Python and the `requests` library.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list schemas

Use the `GET` command at the `/schemas` endpoint to retrieve a list of all available schemas on the server.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/schemas"
URL = f"{BASE_URL}{ENDPOINT}"

params = {"catalog_name": "unity"}

response = requests.get(URL, headers=headers, params=params)
response.json()
```

This will return the 1 pre-loaded schema `unity` when run against the default local Unity Catalog server.

## How to retrieve schema metadata

To retrieve metadata about a specific schema, use the `GET` command at the `/schemas/<full-schema-name>` endpoint. The full name refers to the standard hierarchical Unity Catalog namespace: `catalog.schema`. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/schemas/unity.default"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
response.json()
```

This will return all available metadata for the specified schema:

```
{'name': 'default',
 'catalog_name': 'unity',
 'comment': 'Default schema',
 'properties': {},
 'full_name': 'unity.default',
 'owner': None,
 'created_at': 1721238005571,
 'created_by': None,
 'updated_at': None,
 'updated_by': None,
 'schema_id': 'b08dfd57-a939-46cf-b102-9b906b884fae'}
```

## How to create a schema

Use the `POST` command at the `/schemas` endpoint to create a new catalog. Provide the schema name, catalog name and an optional comment along with the request:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/schemas"
URL = f"{BASE_URL}{ENDPOINT}"

data = {
    "name": "my_schema",
    "catalog_name": "unity",
    "comment": "My new schema"
}

response = requests.post(URL, headers=headers, json=data)
response.json()
```

This will return:

```
{'name': 'my_schema',
 'catalog_name': 'unity',
 'comment': 'My new schema',
 'properties': {},
 'full_name': 'unity.my_schema',
 'owner': None,
 'created_at': 1746192903542,
 'created_by': None,
 'updated_at': 1746192903542,
 'updated_by': None,
 'schema_id': '673993a0-c617-4bee-8d6e-03290151cb89'}
```

## How to update a schema

Use the `PATCH` command at the `/schemas/<full-schema-name>` endpoint to partially update an existing catalog. Include the fields to update in a dictionary and send this with the PATCH request. For example, to update the comment field of the schema we just created above:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/schemas/unity.my_schema"
URL = f"{BASE_URL}{ENDPOINT}"

data = {
    "comment": "Updated schema description"
}

response = requests.patch(URL, headers=headers, json=data)
response.json()
```

This has successfully updated only the comment field without having to rewrite the entire schema.

## How to delete a schema

Use the `DELETE` command at the `/schemas/<full-schema-name>` endpoint to delete a specific schema.

<!-- prettier-ignore -->
!!! tip "Force Delete"
    You can add the `force` parameter to force a delete even if the catalog is not empty.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/schemas/unity.my_schema"
URL = f"{BASE_URL}{ENDPOINT}"

params = {"force": "true"}  # Optional

response = requests.delete(URL, headers=headers, params=params)
```

This will delete the schema.
