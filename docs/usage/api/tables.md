# How to work with Tables using the REST API

Use the `/tables` endpoint to work with tables.

| Method | Description                             | Example                 |
| ------ | --------------------------------------- | ----------------------- |
| GET    | Retrieve a list of tables               | GET /tables             |
| GET    | Retrieve metadata of a specific catalog | GET /tables/my_table    |
| POST   | Create a new catalog                    | POST /tables            |
| DELETE | Remove a catalog                        | DELETE /tables/my_table |

The following sections show how to use each of these methods.

<!-- prettier-ignore -->
!!! note "Unity Catalog server"
    The examples assume a local Unity Catalog server running at the default location `http://localhost:8080`. Update the `BASE_URL` if your server is running somewhere else.

## How to list tables

Use the `GET` command at the `/tables` endpoint to retrieve a list of all available tables on the server.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables"
URL = f"{BASE_URL}{ENDPOINT}"

params = {
    "catalog_name": "unity",
    "schema_name": "default"
}

response = requests.get(URL, headers=headers, params=params)
data = response.json()

tables = data.get("tables", [])
for table in tables:
    print(f"• {table['name']}")
```

This will output the names of the 4 pre-loaded tables when running on the default Unity Catalog local server:

```
• marksheet
• marksheet_uniform
• numbers
• user_countries
```

Here we use a second GET command and a `for` loop to fetch only the table names from the returned data JSON response. Note that there is no guarantee of a specific ordering of the elements in the list.

## How to retrieve table metadata

To retrieve metadata about a specific table, use the `GET` command at the `/tables/<table-name>` endpoint. For example:

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables/unity.default.marksheet"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.get(URL, headers=headers)
data = response.json()
data
```

This will return all available metadata for the specified table (truncated for legibility):

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

Use the `POST` command at the `/tables` endpoint to create a new table:

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
    "storage_location": "file:///Users/rpelgrim/tmp", # or use "s3://my-bucket/..." for s3 tables
    "comment": "External table pointing to local directory"
}

headers = {"Content-Type": "application/json"}

response = requests.post(URL, json=data, headers=headers)
data = response.json()
data
```

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

Use the `DELETE` command at the `/tables/<table-name>` endpoint to delete a specific table.

```python
BASE_URL = "http://localhost:8080/api/2.1/unity-catalog"
ENDPOINT = "/tables/unity.default.my_new_table"
URL = f"{BASE_URL}{ENDPOINT}"

response = requests.delete(URL)

if response.status_code == 200:
    print("✅ The table was successfully deleted.")
else:
    print("❌ Failed to delete table:", response.text)
```

```
✅ The table was successfully deleted.
```
