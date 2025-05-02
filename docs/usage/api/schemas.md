# How to work with Schemas using the REST API

Use the `/schemas` endpoint to work with schemas.

| Method | Description                                               | Example                   |
| ------ | --------------------------------------------------------- | ------------------------- |
| GET    | Retrieve a list of schemas                                | GET /schemas              |
| GET    | Retrieve metadata of a specific schema                    | GET /schemas/my_schema    |
| POST   | Create a new schema                                       | POST /schemas             |
| PATCH  | Partially update an existing schema, e.g. the description | PATCH /schemas/my_schema  |
| DELETE | Remove a schema                                           | DELETE /schemas/my_schema |

The following sections show how to use each of these methods.

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

## How to retrieve catalog metadata

To retrieve metadata about a specific catalog, use the `GET` command at the `/schemas/<catalog-name>` endpoint. For example:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/schemas/unity"
> URL = f"{BASE_URL}{ENDPOINT}"

> response = requests.get(URL, headers=headers)
> data = response.json()
> data
```

This will return all available metadata for the specified catalog:

```
{'name': 'unity',
 'comment': 'Main catalog',
 'properties': {},
 'owner': None,
 'created_at': 1721238005334,
 'created_by': None,
 'updated_at': None,
 'updated_by': None,
 'id': 'f029b870-9468-4f10-badc-630b41e5690d'}
```

## How to create a catalog

Use the `POST` command at the `/schemas` endpoint to create a new catalog:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/schemas"
> URL = f"{BASE_URL}{ENDPOINT}"

> data = {
>     "name": "my_new_catalog",
>     "comment": "Created via REST API"
> }

> response = requests.post(URL, headers=headers, json=data)
> response.json()
```

This will return:

```
{'name': 'my_new_catalog',
 'comment': 'Created via REST API',
 'properties': {},
 'owner': None,
 'created_at': 1745157750562,
 'created_by': None,
 'updated_at': 1745157750562,
 'updated_by': None,
 'id': '08601e1a-d940-419c-8bfe-7667ea9bcab8'}
```

## How to update a catalog

Use the `PATCH` command at the `/schemas/<catalog-name>` endpoint to partially update an existing catalog. Include the fields to update in a dictionary and send this with the PATCH request. For example, to update the comment field of the catalog we just created above:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/schemas/my_new_catalog"
> URL = f"{BASE_URL}{ENDPOINT}"

> data = {
>     "comment": "This is the updated catalog description"
> }

> response = requests.patch(URL, json=data, headers=headers)
> response.json()
```

This should return:

```
{'name': 'my_new_catalog',
 'comment': 'This is the updated catalog description',
 'properties': {},
 'owner': None,
 'created_at': 1745158805225,
 'created_by': None,
 'updated_at': 1745158807128,
 'updated_by': None,
 'id': 'aeb72f8b-9d4b-4128-be97-639e914e785c'}
```

This has successfully updated only the comment field without having to rewrite the entire catalog.

## How to delete a catalog

Use the `DELETE` command at the `/schemas/<catalog-name>` endpoint to delete a specific catalog.

<!-- prettier-ignore -->
!!! tip "Force Delete"
    You can add the `force` parameter to force a delete even if the catalog is not empty.

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/schemas/my_new_catalog"
> URL = f"{BASE_URL}{ENDPOINT}"

> params = {"force": "true"}
> response = requests.delete(URL, headers=headers, params=params)

> if response.status_code == 200:
>     print("✅ The catalog was successfully deleted.")
> else:
>     print("❌ Failed to delete catalog:", response.text)
```

This should return:

```
✅ The catalog was successfully deleted.
```
