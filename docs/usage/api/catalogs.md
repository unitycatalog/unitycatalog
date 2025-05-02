# How to work with Catalogs using the REST API

Use the `/catalogs` endpoint to work with catalogs.

| Method | Description                                                | Endpoint          |
| ------ | ---------------------------------------------------------- | ----------------- |
| GET    | Retrieve a list of catalogs                                | /catalogs         |
| GET    | Retrieve metadata of a specific catalog                    | /catalogs/catalog |
| POST   | Create a new catalog                                       | /catalogs         |
| PATCH  | Partially update an existing catalog, e.g. the description | /catalogs/catalog |
| DELETE | Remove a catalog                                           | /catalogs/catalog |

The following sections show how to use each of these methods. Examples are demonstrated using Python and the `requests` library.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list catalogs

Use the `GET` command at the `/catalogs` endpoint to retrieve a list of all available catalogs on the server.

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/catalogs"
> URL = f"{BASE_URL}{ENDPOINT}"

> response = requests.get(URL, headers=headers)
> data = response.json()
> catalogs = data.get("catalogs", [])
> for catalog in catalogs:
>    print(f"• {catalog['name']}")

• unity
```

Here we use a second GET command and a `for` loop to fetch only the catalog names from the return data JSON response. Note that there is no guarantee of a specific ordering of the elements in the list.

## How to get catalog metadata

To retrieve metadata about a specific catalog, use the `GET` command at the `/catalogs/<catalog-name>` endpoint. For example:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/catalogs/unity"
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

Use the `POST` command at the `/catalogs` endpoint to create a new catalog:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/catalogs"
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

Use the `PATCH` command at the `/catalogs/<catalog-name>` endpoint to partially update an existing catalog. Include the fields to update in a dictionary and send this with the PATCH request. For example, to update the comment field of the catalog we just created above:

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/catalogs/my_new_catalog"
> URL = f"{BASE_URL}{ENDPOINT}"

> data = {
>     "comment": "This is the updated catalog description"
> }

> response = requests.patch(URL, json=data, headers=headers)
> response.json()
```

This will return:

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

Use the `DELETE` command at the `/catalogs/<catalog-name>` endpoint to delete a specific catalog.

<!-- prettier-ignore -->
!!! tip "Force Delete"
    You can add the `force` parameter to force a delete even if the catalog is not empty.

```python
> BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
> ENDPOINT = "/catalogs/my_new_catalog"
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
