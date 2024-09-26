# Quickstart

This quickstart shows how to run Unity Catalog on localhost which is great for experimentation and testing.

## How to start the Unity Catalog server

Start by cloning the open source Unity Catalog GitHub repository:

```bash
git clone git@github.com:unitycatalog/unitycatalog.git
```

To run Unity Catalog, you need **Java 17** installed on your machine.  You can always run the `java --version` command to verify that you have the right version of Java installed such as the following example output.

```bash
% java --version
openjdk 17.0.12 2024-07-16
OpenJDK Runtime Environment Homebrew (build 17.0.12+0)
OpenJDK 64-Bit Server VM Homebrew (build 17.0.12+0, mixed mode, sharing)
```


Change into the `unitycatalog` directory and run `bin/start-uc-server` to instantiate the server.  Here is what you should see:

```console
################################################################### 
#  _    _       _ _            _____      _        _              #
# | |  | |     (_) |          / ____|    | |      | |             #
# | |  | |_ __  _| |_ _   _  | |     __ _| |_ __ _| | ___   __ _  #
# | |  | | '_ \| | __| | | | | |    / _` | __/ _` | |/ _ \ / _` | #
# | |__| | | | | | |_| |_| | | |___| (_| | || (_| | | (_) | (_| | #
#  \____/|_| |_|_|\__|\__, |  \_____\__,_|\__\__,_|_|\___/ \__, | #
#                      __/ |                                __/ | #
#                     |___/               v0.2.0           |___/  #
###################################################################
```


Well, that was pretty easy!

## Verify Unity Catalog server is running

Let’s create a new Terminal window and verify that the Unity Catalog server is running.

Unity Catalog has a few built-in tables that are great for quick experimentation.  Let’s look at all the tables that have a catalog name of “unity” and a schema name of “default” with the Unity Catalog CLI.

```bash
bin/uc table list --catalog unity --schema default
```

```console
┌─────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────┬────────────────────────────────────┐
│      NAME       │  CATALOG_NAME  │  SCHEMA_NAME   │   TABLE_TYPE   │DATA_SOURCE_FORM│    COLUMNS     │STORAGE_LOCATION│    COMMENT     │   PROPERTIES   │     OWNER      │   CREATED_AT   │   CREATED_BY   │   UPDATED_AT   │   UPDATED_BY   │              TABLE_ID              │
│                 │                │                │                │       AT       │                │                │                │                │                │                │                │                │                │                                    │
├─────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────────────────────────┤
│marksheet        │unity           │default         │MANAGED         │DELTA           │[{"name":"id"...│file:///Users...│Managed table   │{"key1":"valu...│null            │1721266805595   │null            │1721266805595   │null            │c389adfa-5c8f-497b-8f70-26c2cca4976d│
├─────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────────────────────────┤
│marksheet_uniform│unity           │default         │EXTERNAL        │DELTA           │[{"name":"id"...│file:///tmp/m...│Uniform table   │{"key1":"valu...│null            │1721266805611   │null            │1721266805611   │null            │9a73eb46-adf0-4457-9bd8-9ab491865e0d│
├─────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────────────────────────┤
│numbers          │unity           │default         │EXTERNAL        │DELTA           │[{"name":"as_...│file:///Users...│External table  │{"key1":"valu...│null            │1721266805617   │null            │1721266805617   │null            │32025924-be53-4d67-ac39-501a86046c01│
├─────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────┼────────────────────────────────────┤
│user_countries   │unity           │default         │EXTERNAL        │DELTA           │[{"name":"fir...│file:///Users...│Partitioned t...│{}              │null            │1721266805622   │null            │1721266805622   │null            │26ed93b5-9a18-4726-8ae8-c89dfcfea069│
└─────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────┴────────────────────────────────────┘
```


Let’s read the content of the `unity.default.numbers` table with the Unity Catalog CLI.
```bash
bin/uc table read --full_name unity.default.numbers
```

```console
┌───────────────────────────────────────┬──────────────────────────────────────┐
│as_int(integer)                        │as_double(double)                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│564                                    │188.75535598441473                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│755                                    │883.6105633023361                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│644                                    │203.4395591086936                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│75                                     │277.8802190765611                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│42                                     │403.857969425109                      │
├───────────────────────────────────────┼──────────────────────────────────────┤
│680                                    │797.6912200731077                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│821                                    │767.7998537403159                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│484                                    │344.00373976089304                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│477                                    │380.6785614543262                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│131                                    │35.44373222835895                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│294                                    │209.32243623208947                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│150                                    │329.19730274053694                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│539                                    │425.66102859000944                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│247                                    │477.742227230588                      │
├───────────────────────────────────────┼──────────────────────────────────────┤
│958                                    │509.3712727285101                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```


We can see it’s straightforward to make queries with the Unity Catalog CLI.

## Unity Catalog structure

Unity Catalog stores all assets in a 3-level namespace:

1. catalog
2. schema
3. assets like tables, volumes, functions, etc.

![UC 3 Level](./assets/images/uc-3-level.png)

Here's an example Unity Catalog instance:

![UC Example Catalog](./assets/images/uc_example_catalog.png)

This Unity Catalog instance contains a single catalog named `cool_stuff`.

The `cool_stuff` catalog contains two schema: `thing_a` and `thing_b`.

`thing_a` contains a Delta table, a function, and a Lance volume.  `thing_b` contains two Delta tables.

Unity Catalog provides a nice organizational structure for various datasets.

## List the catalogs and schemas with the CLI

The UC server is pre-populated with a few sample catalogs, schemas, Delta tables, etc.

Let's start by listing the catalogs using the CLI.

```bash
bin/uc catalog list
```

You should see a catalog named `unity`. Let's see what's in this `unity` catalog (pun intended).

```bash
bin/uc schema list --catalog unity
```

```console
┌───────┬────────────┬──────────────┬──────────┬─────────────┬─────┬─────────────┬──────────┬──────────┬──────────┬────────────────────────────────────┐
│ NAME  │CATALOG_NAME│   COMMENT    │PROPERTIES│  FULL_NAME  │OWNER│ CREATED_AT  │CREATED_BY│UPDATED_AT│UPDATED_BY│             SCHEMA_ID              │
├───────┼────────────┼──────────────┼──────────┼─────────────┼─────┼─────────────┼──────────┼──────────┼──────────┼────────────────────────────────────┤
│default│unity       │Default schema│{}        │unity.default│null │1721266805571│null      │null      │null      │b08dfd57-a939-46cf-b102-9b906b884fae│
└───────┴────────────┴──────────────┴──────────┴─────────────┴─────┴─────────────┴──────────┴──────────┴──────────┴────────────────────────────────────┘
```

You should see that there is a schema named `default`.  To go deeper into the contents of this schema,
you have to list different asset types separately. Let's start with tables.

## Operate on Delta tables with the CLI

Let's list the tables.

```bash
bin/uc table list --catalog unity --schema default
```

```console
┌─────────────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬─────────┬────────────────────────────────────┐
│      NAME       │CATALOG_N│SCHEMA_NA│TABLE_TYP│DATA_SOUR│ COLUMNS │STORAGE_L│ COMMENT │PROPERTIE│  OWNER  │CREATED_A│CREATED_B│UPDATED_A│UPDATED_B│              TABLE_ID              │
│                 │   AME   │   ME    │    E    │CE_FORMAT│         │ OCATION │         │    S    │         │    T    │    Y    │    T    │    Y    │                                    │
├─────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼────────────────────────────────────┤
│marksheet        │unity    │default  │MANAGED  │DELTA    │[{"nam...│file:/...│Manage...│{"key1...│null     │172126...│null     │172126...│null     │c389adfa-5c8f-497b-8f70-26c2cca4976d│
├─────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼────────────────────────────────────┤
│marksheet_uniform│unity    │default  │EXTERNAL │DELTA    │[{"nam...│file:/...│Unifor...│{"key1...│null     │172126...│null     │172126...│null     │9a73eb46-adf0-4457-9bd8-9ab491865e0d│
├─────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼────────────────────────────────────┤
│numbers          │unity    │default  │EXTERNAL │DELTA    │[{"nam...│file:/...│Extern...│{"key1...│null     │172126...│null     │172126...│null     │32025924-be53-4d67-ac39-501a86046c01│
├─────────────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼─────────┼────────────────────────────────────┤
│user_countries   │unity    │default  │EXTERNAL │DELTA    │[{"nam...│file:/...│Partit...│{}       │null     │172126...│null     │172126...│null     │26ed93b5-9a18-4726-8ae8-c89dfcfea069│
└─────────────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴─────────┴────────────────────────────────────┘
```

You should see a few tables. Some details are truncated because of the nested nature of the data.

> To see all the content, you can add `--output jsonPretty` to any command.

Next, let's get the metadata of one those tables.

```bash
bin/uc table get --full_name unity.default.numbers
```

```console
┌───────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│              KEY              │                                                                    VALUE                                                                    │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                           │numbers                                                                                                                                      │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME                   │unity                                                                                                                                        │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME                    │default                                                                                                                                      │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE                     │EXTERNAL                                                                                                                                     │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT             │DELTA                                                                                                                                        │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS                        │{"name":"as_int","type_text":"int","type_json":"{\"name\":\"as_int\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}}","type_name":"I│
│                               │NT","type_precision":0,"type_scale":0,"type_interval_type":null,"position":0,"comment":"Int column","nullable":false,"partition_index":null} │
│                               │{"name":"as_double","type_text":"double","type_json":"{\"name\":\"as_double\",\"type\":\"double\",\"nullable\":false,\"metadata\":{}}","type_│
│                               │name":"DOUBLE","type_precision":0,"type_scale":0,"type_interval_type":null,"position":1,"comment":"Double                                    │
│                               │column","nullable":false,"partition_index":null}                                                                                             │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION               │file:///Users/denny.lee/GitHub/dennyglee/unitycatalog/etc/data/external/unity/default/tables/numbers/                                        │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT                        │External table                                                                                                                               │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES                     │{"key1":"value1","key2":"value2"}                                                                                                            │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│OWNER                          │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT                     │1721266805617                                                                                                                                │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_BY                     │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT                     │1721266805617                                                                                                                                │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_BY                     │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID                       │32025924-be53-4d67-ac39-501a86046c01                                                                                                         │
└───────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

You can see this is a Delta table from the `DATA_SOURCE_FORMAT` metadata.

Here's how to print a snippet of a Delta table (powered by the [Delta Kernel Java](https://delta.io/blog/delta-kernel/) project).

```bash
bin/uc table read --full_name unity.default.numbers
```

```console
┌───────────────────────────────────────┬──────────────────────────────────────┐
│as_int(integer)                        │as_double(double)                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│564                                    │188.75535598441473                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│755                                    │883.6105633023361                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│644                                    │203.4395591086936                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│75                                     │277.8802190765611                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│42                                     │403.857969425109                      │
├───────────────────────────────────────┼──────────────────────────────────────┤
│680                                    │797.6912200731077                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│821                                    │767.7998537403159                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│484                                    │344.00373976089304                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│477                                    │380.6785614543262                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│131                                    │35.44373222835895                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│294                                    │209.32243623208947                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│150                                    │329.19730274053694                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│539                                    │425.66102859000944                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│247                                    │477.742227230588                      │
├───────────────────────────────────────┼──────────────────────────────────────┤
│958                                    │509.3712727285101                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

Let's try creating a new table.

```bash
bin/uc table create --full_name unity.default.my_table \
--columns "col1 int, col2 double" --storage_location /tmp/uc/my_table
```

```console
┌───────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│              KEY              │                                                                    VALUE                                                                    │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                           │my_table                                                                                                                                     │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CATALOG_NAME                   │unity                                                                                                                                        │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME                    │default                                                                                                                                      │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_TYPE                     │EXTERNAL                                                                                                                                     │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│DATA_SOURCE_FORMAT             │DELTA                                                                                                                                        │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COLUMNS                        │{"name":"col1","type_text":"int","type_json":"{\"name\":\"col1\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}","type_name":"INT","│
│                               │type_precision":0,"type_scale":0,"type_interval_type":null,"position":0,"comment":null,"nullable":true,"partition_index":null}               │
│                               │{"name":"col2","type_text":"double","type_json":"{\"name\":\"col2\",\"type\":\"double\",\"nullable\":true,\"metadata\":{}}","type_name":"DOUB│
│                               │LE","type_precision":0,"type_scale":0,"type_interval_type":null,"position":1,"comment":null,"nullable":true,"partition_index":null}          │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION               │file:///tmp/uc/my_table/                                                                                                                     │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│COMMENT                        │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│PROPERTIES                     │{}                                                                                                                                           │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│OWNER                          │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_AT                     │1727374971317                                                                                                                                │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│CREATED_BY                     │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_AT                     │1727374971317                                                                                                                                │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│UPDATED_BY                     │null                                                                                                                                         │
├───────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│TABLE_ID                       │263a234a-7a29-44c8-8e41-8c46aa30650c                                                                                                         │
└───────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

If you list the tables (e.g., `bin/uc table list --catalog unity --schema default`) again, you should see this new table.

> Note, at this point, `unity.default.my_table` is an empty table; e.g. if you run `bin/uc table read --full_name unity.default.my_table` there will be no rows.

Next, append some randomly generated data to the table using `write`.

```bash
bin/uc table write --full_name unity.default.my_table
```

Read the table to confirm the random data was appended:

```bash
bin/uc table read --full_name unity.default.my_table
```

```console
┌───────────────────────────────────────┬──────────────────────────────────────┐
│col1(integer)                          │col2(double)                          │
├───────────────────────────────────────┼──────────────────────────────────────┤
│358                                    │289.04381385477393                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│571                                    │22.709993302915787                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│611                                    │764.2649352165037                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│54                                     │630.1608533928434                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│115                                    │693.1841737595549                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│596                                    │413.80063917354624                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│26                                     │292.77910499212913                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│958                                    │911.4001368368549                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│443                                    │577.1734071275029                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│900                                    │958.5189161369797                     │
├───────────────────────────────────────┼──────────────────────────────────────┤
│555                                    │372.55648414616195                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│408                                    │27.775882385931094                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│216                                    │60.152459742584426                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│357                                    │491.59433846198317                    │
├───────────────────────────────────────┼──────────────────────────────────────┤
│221                                    │917.8549104485671                     │
└───────────────────────────────────────┴──────────────────────────────────────┘
```

Delete the table to clean up:

```bash
bin/uc table delete --full_name unity.default.my_table
```
> Note, while you have deleted the table from Unity Catalog, the underlying file system may still have the files (i.e., check the /tmp/uc/my_table/ folder).  

## Manage models in Unity Catalog using MLflow

Unity Catalog supports the management and governance of ML models as securable assets.  Starting with 
[MLflow 2.16.1](https://mlflow.org/releases/2.16.1), MLflow offers integrated support for using Unity Catalog as the 
backing resource for the MLflow model registry.  What this means is that with the MLflow client, you will be able to 
interact directly with your Unity Catalog service for the creation and access of registered models.

## Setup MLflow for usage with Unity Catalog

In your desired development environment, install MLflow 2.16.1 or higher:

```bash
$ pip install mlflow
```

The installation of MLflow includes the MLflow CLI tool, so you can start a local MLflow server with UI by running the command below in your terminal:

```bash
$ mlflow ui
```

It will generate logs with the IP address, for example:

```console
[2023-10-25 19:39:12 -0700] [50239] [INFO] Starting gunicorn 20.1.0
[2023-10-25 19:39:12 -0700] [50239] [INFO] Listening at: http://127.0.0.1:5000 (50239)
```

Next, from within a python script or shell, import MLflow and set the tracking URI and the registry URI.

```python
import mlflow

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_registry_uri("uc:http://127.0.0.1:8080")
```

At this point, your MLflow environment is ready for use with the newly started MLflow tracking server and the Unity Catalog server acting as your model registry.

You can quickly train a test model and validate that the MLflow/Unity catalog integration is fully working.

```python
import os
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import pandas as pd

X, y = datasets.load_iris(return_X_y=True, as_frame=True)
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

with mlflow.start_run():
    # Train a sklearn model on the iris dataset
    clf = RandomForestClassifier(max_depth=7)
    clf.fit(X_train, y_train)
    # Take the first row of the training dataset as the model input example.
    input_example = X_train.iloc[[0]]
    # Log the model and register it as a new version in UC.
    mlflow.sklearn.log_model(
        sk_model=clf,
        artifact_path="model",
        # The signature is automatically inferred from the input example and its predicted output.
        input_example=input_example,
        registered_model_name="unity.default.iris",
    )

loaded_model = mlflow.pyfunc.load_model(f"models:/unity.default.iris/1")
predictions = loaded_model.predict(X_test)
iris_feature_names = datasets.load_iris().feature_names
result = pd.DataFrame(X_test, columns=iris_feature_names)
result["actual_class"] = y_test
result["predicted_class"] = predictions
result[:4]
```

This code snippet will create a registered model `default.unity.iris` and log the trained model as model version 1.  It then loads the model from the Unity Catalog server, and performs batch inference on the test set using the loaded model.

The results can be seen in the Unity Catalog UI at [http://localhost:3000,](http://localhost:3000) per the instructions in the [Interact with the Unity Catalog tutorial](https://github.com/unitycatalog/unitycatalog?tab=readme-ov-file#interact-with-the-unity-catalog-ui).  

![](./assets/images/uc_ui_models.png)

### Continue with the Rapidstart
Do you want to try more Unity Catalog features including Apache Spark integration, MLflow integration, and Google Authentication, continue with the [Rapidstart](./rapidstart.md).

## APIs and Compatibility

- Open API specification: See the [Unity Catalog Rest API](https://docs.unitycatalog.io/swagger-docs/).
- Compatibility and stability: The APIs are currently evolving and should not be assumed to be stable.
