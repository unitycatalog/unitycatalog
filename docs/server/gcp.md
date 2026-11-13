# Deploy for GCP

This page explains how to configure Unity Catalog server with Google Cloud Storage (GCS) storage and credential.
Here's a quick overview:

* Unity Catalog server does not need any GCP configuration of its own.
* To add GCS storage on Unity Catalog server, a user needs a ***service account*** that has access to the bucket, and
  a key of it. Then a ***credential*** needs to be created with that key.
* An ***external location*** pointing to the `gs://` path needs to be created.
* When a client needs access, the server signs in as the service account and vends an OAuth token
  [downscoped](https://cloud.google.com/iam/docs/downscoping-short-lived-credentials) to the requested path and the
  client's privileges.

# Prerequisites

* Unity Catalog server version \>= 0.8.0
* Ownership of running Unity Catalog server instance
* GCP IAM permissions: create a service account and a key for it
* GCS bucket permissions: grant roles on the bucket

# Create a service account with access to the bucket

1. [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create) in your project.
2. Grant it a role on the bucket:
    * **Storage Object Admin** (`roles/storage.objectAdmin`) for locations that clients write to.
    * **Storage Object Viewer** (`roles/storage.objectViewer`) for read-only locations.

    A downscoped token never grants more than the service account itself has.
3. [Create a JSON key](https://cloud.google.com/iam/docs/keys-create-delete) for the service account and download it.
   The file holds the service account email, the key ID and the private key.

# Create the credential and external location

## Assign user permissions to create external location and credential

If using a non-admin user to create an external location and credential, the user needs to have
`CREATE EXTERNAL LOCATION` and `CREATE STORAGE CREDENTIAL` permissions:

```sh
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE EXTERNAL LOCATION"
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE STORAGE CREDENTIAL"
```

## Create storage credential with the service account key

```sh
bin/uc credential create --name my_gcs_cred --gcp_service_account_key_file /path/to/my-project-0123456789ab.json
┌───────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│          KEY          │                                          VALUE                                           │
├───────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                   │my_gcs_cred                                                                               │
├───────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│GCP_SERVICE_ACCOUNT_KEY│{"email":"uc@my-project.iam.gserviceaccount.com","private_key_id":"0123456789abcdef0123456│
│                       │789abcdef01234567"}                                                                       │
├───────────────────────┼────────────────────────────────────────────────...
```

The CLI reads `client_email`, `private_key_id` and `private_key` from the file; the REST API takes them as `email`,
`private_key_id` and `private_key` of `gcp_service_account_key`. The server stores the private key and never returns
it: `credential get` and `credential list` show the email and key ID only. Delete the downloaded file once the
credential is created.

To rotate the key, create a new key for the service account and update the credential with its file:

```sh
bin/uc credential update --name my_gcs_cred --gcp_service_account_key_file /path/to/new-key.json
```

A credential keeps the cloud it was created with: a GCP credential cannot be updated with an AWS IAM role, and an
external location on `gs://` only accepts a GCP credential.

## Create external location

```sh
bin/uc external_location create --name my_loc --url gs://my-bucket/path --credential_name my_gcs_cred
```

This path is now ready for use. It can be used as storage for external tables, volumes, or storage location of
catalog or schema, in the same way as [an S3 location](aws.md#create-catalog-or-schema-with-storage-location).

# Migration of existing per-bucket credential configuration

A server with service account key files configured in `server.properties` for buckets directly, without a storage
credential, keeps working: that configuration is used for a bucket that no external location covers. It is legacy and
should be migrated. The old configuration looks like this:

```ini
## GCS Storage Config (Multiple configs can be added by incrementing the index)
gcs.bucketPath.0=gs://my-bucket
# Optional (If blank, it will use Default Application chain to find credentials)
gcs.jsonKeyFilePath.0=/path/to/key.json
```

The following steps are recommended to migrate the old configuration:

1. Create a credential with the same service account key, and an external location that **covers all of the existing
   data** in the bucket, as described above. No server restart is needed.
2. Verify that data access is working properly. Then remove the `gcs.*` keys from `server.properties` and restart the
   server.

# Limitations

* Only service account keys are supported. Vending through the server's own GCP identity, by impersonating a service
  account that the bucket owner has shared with it, is not supported yet.
