# Deploy for Azure

This page explains how to configure Unity Catalog server with Azure Data Lake Storage Gen2 (ADLS) storage and
credential. Here's a quick overview:

* Unity Catalog server does not need any Azure configuration of its own.
* To add ADLS storage on Unity Catalog server, a user needs a Microsoft Entra ID ***service principal*** that has access
  to the storage account. Then a ***credential*** needs to be created with the service principal.
* An ***external location*** pointing to the `abfss://` path needs to be created.
* When a client needs access, the server signs in as the service principal and vends a
  [user delegation SAS](https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas) token
  scoped to the requested path and the client's privileges, which is valid for one hour.

# Prerequisites

* Unity Catalog server version \>= 0.8.0
* Ownership of running Unity Catalog server instance. A database created by an older release needs
  `bin/start-uc-server --upgrade-schema` first (see [Upgrading the database schema](upgrades.md)).
* Microsoft Entra ID permissions: create an app registration and a client secret
* Azure storage account permissions: assign roles on the storage account
* A storage account with a hierarchical namespace (ADLS Gen2)

# Create a service principal with access to the storage account

Follow [Create Unity Catalog managed storage using a service principal](https://learn.microsoft.com/en-us/azure/databricks/archive/unity-catalog/service-principals)
from Databricks to register an application in Microsoft Entra ID and create a client secret for it. Record its
**Directory (tenant) ID**, **Application (client) ID** and the **secret value**.

Then assign the service principal a role on the storage account (or on a container):

* **Storage Blob Data Contributor** for locations that clients write to.
* **Storage Blob Data Reader** for read-only locations.

Both roles let the server request a user delegation key, which it needs to sign SAS tokens. A SAS token never grants
more than the service principal itself has.

# Create the credential and external location

## Assign user permissions to create external location and credential

If using a non-admin user to create an external location and credential, the user needs to have
`CREATE EXTERNAL LOCATION` and `CREATE STORAGE CREDENTIAL` permissions:

```sh
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE EXTERNAL LOCATION"
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE STORAGE CREDENTIAL"
```

## Create storage credential with the service principal

```sh
bin/uc credential create --name my_azure_cred \
  --azure_directory_id 11111111-1111-1111-1111-111111111111 \
  --azure_application_id 22222222-2222-2222-2222-222222222222 \
  --azure_client_secret "$AZURE_CLIENT_SECRET"
┌───────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│          KEY          │                                          VALUE                                           │
├───────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                   │my_azure_cred                                                                             │
├───────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│AZURE_SERVICE_PRINCIPAL│{"directory_id":"11111111-1111-1111-1111-111111111111","application_id":"22222222-2222-222│
│                       │2-2222-222222222222"}                                                                     │
├───────────────────────┼────────────────────────────────────────────────...
```

The server stores the client secret and never returns it: `credential get` and `credential list` show the directory
and application IDs only. A command-line argument is visible to other processes on the machine and is kept in the
shell history, so read the secret into a variable first (`read -s AZURE_CLIENT_SECRET`) as above.

To rotate the secret, update the credential with the same directory and application IDs and the new secret:

```sh
bin/uc credential update --name my_azure_cred \
  --azure_directory_id 11111111-1111-1111-1111-111111111111 \
  --azure_application_id 22222222-2222-2222-2222-222222222222 \
  --azure_client_secret "$AZURE_CLIENT_SECRET"
```

A credential keeps the cloud it was created with: an Azure credential cannot be updated with an AWS IAM role, and an
external location on `abfss://` only accepts an Azure credential.

## Create external location

```sh
bin/uc external_location create --name my_loc --url abfss://my-container@myaccount.dfs.core.windows.net/path --credential_name my_azure_cred
```

This path is now ready for use. It can be used as storage for external tables, volumes, or storage location of
catalog or schema, in the same way as [an S3 location](aws.md#create-catalog-or-schema-with-storage-location).

# Migration of existing per-account credential configuration

A server with service principals configured in `server.properties` for storage accounts directly, without a storage
credential, keeps working: that configuration is used for a storage account that no external location covers. It is
legacy and should be migrated. The old configuration looks like this:

```ini
## ADLS Storage Config (Multiple configs can be added by incrementing the index)
adls.storageAccountName.0=myaccount
adls.tenantId.0=11111111-1111-1111-1111-111111111111
adls.clientId.0=22222222-2222-2222-2222-222222222222
adls.clientSecret.0=...
```

A client secret in `server.properties` is deprecated; until you migrate, set it in the `UC_ADLS_CLIENTSECRET_<n>`
environment variable instead (see [Secrets](configuration.md#secrets)).

The following steps are recommended to migrate the old configuration:

1. Create a credential with the same service principal, and an external location that **covers all of the existing
   data** in the storage account, as described above. No server restart is needed.
2. Verify that data access is working properly. Then remove the `adls.*` keys from `server.properties` and restart
   the server.

# Limitations

* Only service principals with a client secret are supported. Azure managed identities need a different workflow,
  since the server is not a first-party Azure service and cannot verify ownership of a managed identity; see
  [issue #47](https://github.com/unitycatalog/unitycatalog/issues/47).
