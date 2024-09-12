# Managed vs External Volumes

Unity Catalog supports both managed and external volumes.

A **managed volume** is a volume for which Unity Catalog manages both the metadata and the data itself, incl. creation, storage, and deletion. Data is managed at a pre-configured storage location to which Unity Catalog has full read and write access.

An **external volume** is a volume for which Unity Catalog manages _only_ the metadata and not the lifecycle of the underlying data itself. The user is responsible for data creation, storage and deletion at the external location.

![Managed vs External volumes](../../assets/images/uc_managed_external_volumes_bg.png)

## How to Create an External Volume

Use the `uc/bin volume create` command with a `storage_location` to create an external volume.

For example:

```
bin/uc volume create \
  --full_name unity.default.new_files \
  --storage_location ~/tmp/
```

Which will output:

```
┌─────────────────────────────────┬──────────────────────────────────────────────────────────────────┐
│               KEY               │                              VALUE                               │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│CATALOG_NAME                     │unity                                                             │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│SCHEMA_NAME                      │default                                                           │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│NAME                             │new_files                                                         │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│COMMENT                          │null                                                              │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│CREATED_AT                       │1726135930624                                                     │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│UPDATED_AT                       │1726135930624                                                     │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│VOLUME_ID                        │369419a0-7f8b-496c-94e2-6bc363c139cc                              │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│VOLUME_TYPE                      │EXTERNAL                                                          │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│STORAGE_LOCATION                 │file:///Users/user/tmp/                                       │
├─────────────────────────────────┼──────────────────────────────────────────────────────────────────┤
│FULL_NAME                        │unity.default.new_files                                           │
└─────────────────────────────────┴──────────────────────────────────────────────────────────────────┘

```

The `VOLUME_TYPE` field confirms that you have created an `EXTERNAL` table.

The storage location can be a local path (absolute path) or an S3 path. When an S3 path is provided, the server will vend temporary credentials to access the S3 bucket. The server properties must be set up accordingly, see the [server configuration documentation](../../usage/server.md)

## Dropping Managed vs External Volumes

When you drop a managed volume from Unity Catalog, the underlying data is also removed.

When you drop an external volume from Unity Catalog, the underlying data is not touched. If you want the data to be deleted, you will have to do so manually.

![Dropping Managed vs External volumes](../../assets/images/uc_managed_external_volumes_drop_bg.png)

## When to use which volume type

Unity Catalog gives you the freedom to use both managed and external volumes, depending on your needs.

**You may want to use managed volumes when:**

- You prefer simplicity and easy data management.
- Your data lifecycle is tightly coupled with the volume definition.
- You don't want to worry about the details of data storage.

**You may want to use external volumes when:**

- You need to manage data storage locations explicitly.
- You require data persistence independent of volume definitions.
- You need external read or write access to your data.
