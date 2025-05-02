# How to work with Volumes using the REST API

Use the `/volumes` endpoint to work with volumes.

| Method | Description                             | Example                   |
| ------ | --------------------------------------- | ------------------------- |
| GET    | Retrieve a list of volumes              | GET /volumes              |
| GET    | Retrieve metadata of a specific catalog | GET /volumes/my_volume    |
| POST   | Create a new catalog                    | POST /volumes             |
| DELETE | Remove a catalog                        | DELETE /volumes/my_volume |

The following sections show how to use each of these methods.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list volumes

Use the `GET` command at the `/volumes` endpoint to retrieve a list of all available volumes on the server. You will need to specify the catalog and schema names using the params keyword argument.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/volumes"
URL = f"{BASE_URL}{ENDPOINT}"

params = {
    "catalog_name": "unity",
    "schema_name": "default"
}

response = requests.get(URL, headers=headers, params=params)
data = response.json()
```

This will return the 2 pre-loaded volumes when running on the default Unity Catalog local server:
• json_files
• txt_files

## How to retrieve volume metadata

To retrieve metadata about a specific volume, use the GET command at the `/volumes/<full-volume-name>` endpoint. The full name means the 3-level namespace reference in the standard Unity Catalog format: `catalog.schema.volume`. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/volumes/unity.default.json_files"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
data = response.json()
```

This will return all available metadata for the specified volume:

```
{'catalog_name': 'unity',
 'schema_name': 'default',
 'name': 'json_files',
 'comment': None,
 'owner': None,
 'created_at': 1721238005627,
 'created_by': None,
 'updated_at': 1721238005627,
 'updated_by': None,
 'volume_id': 'd3f18882-eb1f-4cbb-bbc4-0347091224e8',
 'volume_type': 'EXTERNAL',
 'storage_location': 'file:///Users/.../unitycatalog-fork/etc/data/external/unity/default/volumes/json_files/',
 'full_name': 'unity.default.json_files'}
```

## How to create a volume

Use the POST command at the `/volumes` endpoint to create a new volume. You will need to supply a dictionary containing the volume, catalog and schema names as well as the Volume Type and Storage Location.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/volumes"
URL = f"{BASE_URL}{ENDPOINT}"

data = {
    "name": "my_new_volume",
    "catalog_name": "unity",
    "schema_name": "default",
    "volume_type": "EXTERNAL",
    "storage_location": "file:///Users/user/tmp", # (1)
    "comment": "External volume pointing to local directory"
}

headers = {"Content-Type": "application/json"}

response = requests.post(URL, json=data, headers=headers)
data = response.json()
data
```

1. Use "s3://my-bucket/..." for s3 tables

This will return:

```
{'catalog_name': 'unity',
 'schema_name': 'default',
 'name': 'my_new_volume',
 'comment': 'External volume pointing to local directory',
 'owner': None,
 'created_at': 1746191265987,
 'created_by': None,
 'updated_at': 1746191265987,
 'updated_by': None,
 'volume_id': 'c989db68-8064-43c0-9946-b1ec34da22bd',
 'volume_type': 'EXTERNAL',
 'storage_location': 'file:///Users/user/tmp',
 'full_name': 'unity.default.my_new_volume'}
```

## How to delete a volume

Use the DELETE command at the `/volumes/<full-volume-name>` endpoint to delete a specific volume. The full name is the 3-level namespace reference to your volume: `catalog.schema.volume`. For example, to delete the volume that was just created:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/volumes/unity.default.my_new_volume"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.delete(URL)
```
