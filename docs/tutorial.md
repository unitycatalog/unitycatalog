# Tutorial

Let's take Unity Catalog for spin. In this tutorial, we are going to do the following:

- In one terminal, run the UC server.
- In another terminal, we will explore the contents of the UC server using the UC CLI,
  which is an example UC connector provided to demonstrate how to use the UC SDK for various assets,
  as well as provide a convenient way to explore the content of any UC server implementation.

## Prerequisites

You have to ensure that your local environment has the following:

- Clone this repository.
- Ensure the `JAVA_HOME` environment variable your terminal is configured to point to JDK11+.
- Compile the project running `build/sbt package` in the repository root directory.

## Run the UC Server

In a terminal, in the cloned repository root directory, start the UC server.

```sh
bin/start-uc-server
```

For the rest of the steps, continue in a different terminal.

## List the catalogs and schemas with the CLI

Unity Catalog stores all assets in a 3-level namespace:

1. catalog
2. schema
3. assets like tables, volumes, functions, etc.

The UC server is pre-populated with a few sample catalogs, schemas, Delta tables, etc.

Let's start by listing the catalogs using the CLI.

```sh
bin/uc catalog list
```

You should see a catalog named `unity`. Let's see what's in this `unity` catalog (pun intended).

```sh
bin/uc schema list --catalog unity
```

You should see that there is a schema named `default`. To go deeper into the contents of this schema,
you have to list different asset types separately. Let's start with tables.

### Operate on Delta tables with the CLI

Let's list the tables.

```sh
bin/uc table list --catalog unity --schema default
```

You should see a few tables. Some details are truncated because of the nested nature of the data.
To see all the content, you can add `--output jsonPretty` to any command.

Next, let's get the metadata of one those tables.

```sh
bin/uc table get --full_name unity.default.numbers
```

You can see that it is a Delta table. Now, specifically for Delta tables, this CLI can
print a snippet of the contents of a Delta table (powered by the [Delta Kernel Java](https://delta.io/blog/delta-kernel/) project).
Let's try that.

```sh
bin/uc table read --full_name unity.default.numbers
```

Let's try creating a new table.

```sh
bin/uc table create --full_name unity.default.myTable --columns "col1 int, col2 double" --storage_location /tmp/uc/myTable
```

If you list the tables again, you should see this new table. Next, let's write to the table with
some randomly generated data (again, powered by [Delta Kernel Java](https://delta.io/blog/delta-kernel/)] and read it back.

```sh
bin/uc table write --full_name unity.default.myTable
bin/uc table read --full_name unity.default.myTable
```

## APIs and Compatibility

- Open API specification: The Unity Catalog Rest API is documented [here](../api).
- Compatibility and stability: The APIs are currently evolving and should not be assumed to be stable.
