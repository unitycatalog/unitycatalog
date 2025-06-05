# How to work with Tables using the REST API

Use the `/tables` endpoint to work with tables.

| Method | Description                           | Endpoint                     |
| ------ | ------------------------------------- | ---------------------------- |
| GET    | Retrieve a list of tables             | /tables                      |
| GET    | Retrieve metadata of a specific table | /tables/catalog.schema.table |
| POST   | Create a new table                    | /tables                      |
| DELETE | Remove a table                        | /tables/catalog.schema.table |

The following sections show how to use each of these methods. Examples are demonstrated using Python and the `requests` library.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list tables

Use the `GET` command at the `/tables` endpoint to retrieve a list of all available tables on the server. You will need to specify the catalog and schema names.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables"
URL = f"{BASE_URL}{ENDPOINT}"

params = {
    "catalog_name": "unity",
    "schema_name": "default"
}

response = requests.get(URL, headers=headers, params=params)
response.json()
```

This will return the 4 pre-loaded tables when running on the default Unity Catalog local server:
• marksheet
• marksheet_uniform
• numbers
• user_countries

## How to get table metadata

To retrieve metadata about a specific table, use the GET command at the `/tables/<full-table-name>` endpoint. The full name means the 3-level namespace reference in the standard Unity Catalog format: `catalog.schema.table`. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables/unity.default.marksheet"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
response.json()
```

This will return all available metadata for the specified table:

```
{'name': 'marksheet',
 'catalog_name': 'unity',
 'schema_name': 'default',
 'table_type': 'MANAGED',
 'data_source_format': 'DELTA',
 'columns': [{'name': 'id',
   'type_text': 'int',
   'type_json': '{"name":"id","type":"integer","nullable":false,"metadata":{}}',
   'type_name': 'INT',
   'type_precision': 0,
   'type_scale': 0,
   'type_interval_type': None,
   'position': 0,
   'comment': 'ID primary key',
   'nullable': False,
   'partition_index': None},
  {'name': 'name',
   'type_text': 'string',
   'type_json': '{"name":"name","type":"string","nullable":false,"metadata":{}}',
   'type_name': 'STRING',
   'type_precision': 0,
   'type_scale': 0,
   'type_interval_type': None,
   'position': 1,
   'comment': 'Name of the entity',
   'nullable': False,
   'partition_index': None},
  {'name': 'marks',
   'type_text': 'int',
   'type_json': '{"name":"marks","type":"integer","nullable":true,"metadata":{}}',
   ...
```

## How to create a table

Use the POST command at the `/tables` endpoint to create a new table. You will need to supply a dictionary containing the table, catalog and schema names as well as the Table Type, Data Source Format, and Storage Location.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables"
URL = f"{BASE_URL}{ENDPOINT}"

data = {
    "name": "my_new_table",
    "catalog_name": "unity",
    "schema_name": "default",
    "table_type": "EXTERNAL",
    "data_source_format": "DELTA",
    "storage_location": "file:///Users/user/tmp", # (1)
    "comment": "External table pointing to local directory"
}

headers = {"Content-Type": "application/json"}

response = requests.post(URL, json=data, headers=headers)
response.json()
```

1. Change the path to "s3://my-bucket/..." for s3 tables

This will return:

```
{'name': 'my_new_table',
 'catalog_name': 'unity',
 'schema_name': 'default',
 'table_type': 'EXTERNAL',
 'data_source_format': 'DELTA',
 'columns': [],
 'storage_location': 'file:///Users/user/tmp',
 'comment': 'External sales table pointing to local directory',
 'properties': {},
 'owner': None,
 'created_at': 1745235186765,
 'created_by': None,
 'updated_at': 1745235186765,
 'updated_by': None,
 'table_id': '53b8c66f-01c8-41bc-a762-edbe20aa7813'}
```

## How to delete a table

Use the DELETE command at the `/tables/<full-table-name>` endpoint to delete a specific table. The full name is the 3-level namespace reference to your table: `catalog.schema.table`. For example, to delete the table that was just created:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables/unity.default.my_new_table"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.delete(URL)
```
