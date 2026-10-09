# Managed vs External Volumes

Unity Catalog supports two kinds of volumes: **managed** and **external**. The volume type
determines who owns the underlying storage and what happens to your files when the volume
is deleted. Choose the type with the `--volume_type` flag when creating a volume
(`MANAGED` or `EXTERNAL`); if you omit it, the CLI creates an `EXTERNAL` volume.

## Quick comparison

|                          | Managed volume                                              | External volume                                      |
|--------------------------|-------------------------------------------------------------|------------------------------------------------------|
| Storage location         | Assigned automatically by Unity Catalog                     | You provide it with `--storage_location`             |
| Who manages the files    | Unity Catalog                                               | You, outside of Unity Catalog                         |
| On `volume delete`       | Catalog entry removed; files queued for background cleanup  | Catalog entry removed; files left untouched          |
| Typical use              | Scratch space, pipeline outputs Unity Catalog should own    | Registering existing data without moving it          |

## External volumes

Use an external volume when your data already lives somewhere — an ADLS Gen2 container,
an S3 bucket, or a local directory — and you want Unity Catalog to govern access to it
without moving or copying the files.

```sh
bin/uc volume create --full_name unity.default.my_volume \
  --storage_location abfss://data@myaccount.dfs.core.windows.net/raw \
  --volume_type EXTERNAL
```

A few things to know:

- `--storage_location` is **required** for external volumes; the request fails without it.
- The storage location must not overlap any existing table, volume, or registered model.
- If the location falls inside a registered external location, you need `OWNER` or
  `CREATE_EXTERNAL_VOLUME` permission on that external location.
- Deleting an external volume only removes the catalog entry. Your files stay exactly
  where they are, so other systems reading the same path are unaffected.

## Managed volumes

Use a managed volume when you want Unity Catalog to own the full lifecycle of the data,
for example intermediate outputs of a pipeline or temporary working files.

```sh
bin/uc volume create --full_name unity.default.scratch --volume_type MANAGED
```

A few things to know:

- Do **not** pass `--storage_location` for a managed volume; Unity Catalog assigns a
  location automatically under the catalog/schema managed storage location, and the
  request fails if you provide one.
- Deleting a managed volume removes the catalog entry and queues the underlying files
  for background cleanup. Cleanup is asynchronous — the worker waits for the
  `server.storage-cleanup.initial-delay` interval before deleting files, and failed
  attempts are retried. There is no undelete.
- Creating managed volumes under a catalog or schema only requires the usual
  catalog/schema permissions once the catalog or schema is allowed to create managed
  storage.

## Which should I choose?

- Your data already exists and other tools or teams use the same path? → **External**.
  Unity Catalog governs access; you keep owning the files.
- The data is produced and consumed inside Unity Catalog-governed workflows and should
  disappear with the volume? → **Managed**. Unity Catalog handles placement and cleanup.

For the basic volume operations (list, get, read, write, delete), see
[Unity Catalog Volumes](volumes.md).
