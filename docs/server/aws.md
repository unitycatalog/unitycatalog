# Deploy for AWS

This page explains how to configure Unity Catalog server with AWS storage and credential. Here's a quick overview:

* Unity Catalog server needs to hold credentials of a ***master role*** (or user) which is usually the IAM role the server runs as.
* To add S3 storage on Unity Catalog server, a user needs to have a separate IAM role dedicated to this storage. Then a ***credential*** needs to be created using this storage IAM role.
* An ***external location*** pointing to the S3 path needs to be created.
* The user needs to configure the S3 bucket and the storage IAM role properly in AWS.

# Prerequisites

* Unity Catalog server version \>= 0.4.0
* Ownership of running Unity Catalog server instance
* AWS account IAM permissions: create and configure roles
* AWS S3 bucket permissions: s3:PutBucketPolicy to configure bucket policy

# Configure Unity Catalog server to run on AWS

## Create and use a master role

It’s recommended to run Unity Catalog server with a master IAM role. Create an IAM role with your chosen name to be used as UC master role, for example *`arn:aws:iam::1234567:role/UCMasterRole-EXAMPLE`*. Grant it the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowAssumeAllRoles",
            "Effect": "Allow",
            "Action": "sts:AssumeRole",
            "Resource": "*"
        }
    ]
}
```

Attach this IAM role to the AWS hosting environment where the Unity Catalog server runs.

## Configure in *server.properties*

This configuration tells the Unity Catalog server which role it should use as the master role. Note that when it’s running in an AWS environment, there’s no need to explicitly provide any token or secret as the server can get them via [`DefaultCredentialsProvider`](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html).

```ini
# server.properties (AWS-hosted)
aws.s3.masterRoleArn=arn:aws:iam::1234567:role/UCMasterRole-EXAMPLE
aws.region=us-west-2
```

# Configure Unity Catalog server to run on-premises using IAM role (preferred method for on-premises)

Using an IAM role in a service outside of AWS requires a setup with “[*AWS IAM Roles Anywhere*](https://aws.amazon.com/iam/roles-anywhere/)”. After that, the process would be exactly the same as [running a Unity Catalog server on AWS](#configure-unity-catalog-server-to-run-on-aws). This is the preferred approach.

# Configure Unity Catalog server to run on-premises using IAM user

## Create and use a master user

This approach is only applicable when the above *AWS IAM Roles Anywhere* method is not possible or desirable.

The Unity Catalog server can also take an **IAM user** in place of the UC master role. To do this, just create an **IAM user** with your chosen name to be used as UC master role, for example *`arn:aws:iam::1234567:user/UCMasterRole-EXAMPLE`*. Then grant it the [same permission as the master role](#create-and-use-a-master-role).

## Configure in *server.properties* or environment variables

When running on-premises and without *AWS IAM Roles Anywhere* deployment, Unity Catalog server has to be configured manually to provide the credential of the master user. In the `server.properties` file the IAM **user** arn can be specified for the key `aws.s3.masterRoleArn` despite the fact that the key is called `masterRoleArn`:

```ini
# server.properties (on‑prem with IAM user — only if Roles Anywhere isn’t possible)
aws.s3.masterRoleArn=arn:aws:iam::1234567:user/UCMasterRole-EXAMPLE
aws.s3.accessKey=  # Leave it blank to delegate to DefaultCredentialsProvider
aws.s3.secretKey=  # Leave it blank to delegate to DefaultCredentialsProvider
aws.region=us-west-2
```
It's recommended to leave `aws.s3.accessKey` and `aws.s3.secretKey` unset so that 
[`DefaultCredentialsProvider`](https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/auth/credentials/DefaultCredentialsProvider.html) is still used if it’s properly configured. Otherwise these keys can be also configured through environment variables instead.

# Create and configure both the S3 storage and storage credential

This process largely follows [Create a storage credential and external location for S3 using Catalog Explorer or SQL](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual) from Databricks, except that the storage role should trust the UC master role configured in prior steps, not the Databricks UC master role.

## Assign user permissions to create external location and credential

If using a non-admin user to create an external location and credential, the user needs to have `CREATE EXTERNAL LOCATION` and `CREATE STORAGE CREDENTIAL` permissions:

```sh
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE EXTERNAL LOCATION"
bin/uc permission create --securable_type metastore --name metastore --principal some_user@some-host --privilege "CREATE STORAGE CREDENTIAL"
```

## Create storage credential with storage IAM role

This IAM role is different from the master role. It is not assigned to the Unity Catalog server hosting environment but it will have access to the S3 bucket.
Follow the same process described on [this page](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual) to create the IAM role, except that do not trust Databricks UC master role. Trust the master role (or user) configured on the OSS Unity Catalog server in the prior steps instead. Then create a credential securable on Unity Catalog server:

```sh
bin/uc credential create --name my_aws_cred --aws_iam_role_arn arn:aws:iam::987654321:role/UCDbRole-EXAMPLE
┌────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────┐
│        KEY         │                                          VALUE                                           │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│NAME                │my_aws_cred                                                                               │
├────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────┤
│AWS_IAM_ROLE        │{"role_arn":"arn:aws:iam::987654321:role/UCDbRole-EXAMPLE","unity_catalog_iam_arn":"arn:aw│
│                    │s:iam::1234567:role/UCMasterRole-EXAMPLE","external_id":"1862cf4e-5e90-4f96-9125-8ea8928cc│
│                    │405"}                                                                                     │
├────────────────────┼────────────────────────────────────────────────...
```

Note that master role and external ID are returned in the response. Follow [the instruction](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual) to update the trust relationship of the storage role to include the external ID. The updated trust relationship should look like this:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": ["arn:aws:iam::1234567:user/UCMasterRole-EXAMPLE"]
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "1862cf4e-5e90-4f96-9125-8ea8928cc405"  // The same external ID as returned by server
        }
      }
    }
  ]
}
```

## Create external location

Configure the S3 bucket to grant permissions to the storage role created in the previous step, as described in [the instruction](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/s3/s3-external-location-manual). Then create an external location using command:

```sh
bin/uc external_location create --name my_loc --url s3://my-bucket/path --credential_name my_aws_cred
```

This bucket path is now ready for use. It can be used as storage for external tables, volumes, or storage location of catalog or schema.

## Create catalog or schema with storage location

To use the above external location as storage location when creating catalog or schema, just set an URL like this:

```sh
bin/uc catalog create --name my_cat --storage_root s3://my-bucket/path
```

With this new catalog *my\_cat*, all managed tables, volumes etc will have storage location 
allocated under the storage location of this catalog, for example: *s3://my-bucket/path/\_\_unitystorage/catalogs/{catalog-uuid}/tables/{table-uuid}*.

Similarly, schemas can be created with managed storage just like catalogs:
```sh
bin/uc schema create --catalog my_cat2 --name my_schema --storage_root s3://my-bucket/path
```

# Static access keys for S3-compatible storage (no STS)

Some S3-compatible platforms (for example NetApp ONTAP Object Storage) implement the S3 data API
but not STS AssumeRole. Configure access key secrets on the Unity Catalog server and register a
storage credential that references the access key **id** only (the secret is never stored in the
catalog database):

```ini
# server.properties — secrets stay on the server, keyed by access key id
s3.static.secretKey.AKIAEXAMPLE0=...
s3.static.secretKey.AKIAEXAMPLE1=...
# Optional, only for stores that issue a session token:
# s3.static.sessionToken.AKIAEXAMPLE0=...
# Soft TTL stamped on vended credentials so clients refresh (default 3600):
# s3.static.credentialTtlSeconds=3600
```

```sh
bin/uc credential create --name my_static_cred --aws_s3_access_key_id AKIAEXAMPLE1
bin/uc external_location create --name my_loc --url s3://my-bucket/path --credential_name my_static_cred
```

Temporary credential APIs then return the matching access key and secret (with a stamped
expiration, and typically no session token). The Hadoop/Spark connector applies access and secret
only in that case (`AwsBasicCredentials` / `fs.s3a.access.key` + `fs.s3a.secret.key`) so S3A can
talk to STS-less stores. IAM-role credentials still vend and use an STS session token as before.
The underlying key does not expire, and clients pick up a rotation after the stamped TTL. To rotate, add the new pair alongside the old one and point the credential at it:

```sh
# server.properties: s3.static.secretKey.AKIAEXAMPLE2=...
bin/uc credential update --name my_static_cred --aws_s3_access_key_id AKIAEXAMPLE2
```

Then remove the retired `s3.static.secretKey.<old-id>` entry. The key is as broad as the storage
account behind it — prefer one account (or key) per trust boundary.

This path coexists with normal IAM-role credentials on the same server: a credential carries either
`aws_iam_role` (vended through STS) or `aws_s3_access_key` (vended from server configuration).

## Endpoint URLs

S3-compatible stores often expose separate routes for STS (AssumeRole) and S3 data access. Unity
Catalog supports both through distinct configuration keys. The legacy `aws.endpointUrl` property
remains as a fallback when the explicit keys are unset.

### Global configuration (recommended)

Use this path when credentials are vended through storage credentials and external locations:

```ini
# Region used for signing and STS calls
aws.region=us-east-1

# STS endpoint for server-side AssumeRole calls
aws.stsEndpointUrl=https://mcg.example.com/sts

# S3 data endpoint returned as endpoint_url with vended temporary credentials
aws.s3EndpointUrl=https://mcg.example.com/s3

# Deprecated fallback applied to both STS and S3 when the explicit keys above are unset
# aws.endpointUrl=https://legacy.example.com
```

Precedence:

- **STS client:** `aws.stsEndpointUrl` → `aws.endpointUrl` → AWS SDK default
- **S3 endpoint returned to clients:** `aws.s3EndpointUrl` → `aws.endpointUrl` → absent

When static access keys and IAM-role credentials coexist on one server, leave global endpoints
unset unless every store shares the same routes; otherwise configure endpoints on the client
(`fs.s3a.endpoint` for the Hadoop connector).

### Legacy per-bucket configuration

Indexed `s3.*` entries still support separate STS and S3 endpoints when different legacy buckets
target different backends:

```ini
s3.bucketPath.0=s3://some-bucket
s3.region.0=us-east-1
s3.awsRoleArn.0=arn:aws:iam::123456789012:role/storage-role
s3.endpointUrl.0=https://mcg.example.com/s3
s3.stsEndpointUrl.0=https://mcg.example.com/sts
```

For each index, STS uses `s3.stsEndpointUrl.N` → `s3.endpointUrl.N`. The S3 data endpoint is
`s3.endpointUrl.N`.

### Static credential endpoint behaviour

A static credential carries no endpoint of its own, so the `endpoint_url` returned alongside vended
credentials comes from the general S3 configuration: first the per-bucket entry matching the storage
base (`s3.bucketPath.N` / `s3.endpointUrl.N`), otherwise `aws.s3EndpointUrl`, otherwise
`aws.endpointUrl`.

Both per-bucket caveats still apply. A per-bucket entry is only registered when it supplies
`s3.bucketPath.N` plus either `s3.region.N` and `s3.awsRoleArn.N`, or `s3.accessKey.N`,
`s3.secretKey.N` and `s3.sessionToken.N` — so it cannot declare an endpoint on its own without also
putting a key back into `server.properties`.

### Session policy S3 actions

Vended STS session policies use explicit S3 actions (`s3:GetObject`, `s3:PutObject`,
`s3:DeleteObject`, multipart helpers, and `s3:ListBucket` with `s3:prefix` conditions). Partial
action wildcards such as `s3:GetO*` are not emitted.

KMS permissions (`kms:Decrypt` / `kms:GenerateDataKey*`) scoped by `kms:ViaService` are included for
AWS STS endpoints so SSE-KMS buckets work on AWS. Encryption-context ARNs are omitted to stay within
STS packed-policy limits on long paths. The KMS statement is omitted entirely when the configured STS
endpoint is not an AWS STS host (MinIO reports `invalid condition key` for `kms:ViaService`).

## Security considerations

### What a vended static credential is

The STS path downscopes each vended credential to the requested location and privileges (see
`AwsPolicyGenerator`) and returns a session credential that AWS itself invalidates after an hour.
Static vending has neither property: it returns the configured key pair unchanged.

- **Not scoped to the external location.** The vended key carries whatever the store grants it —
  typically the whole bucket or account, not the path prefix the client asked for. Unity Catalog
  checks privileges when vending, and that is the only enforcement point.
- **Not actually expiring.** `s3.static.credentialTtlSeconds` is stamped as `expiration_time` so
  well-behaved clients come back for a fresh credential; the key itself keeps working. Revoking a
  grant, deleting the credential, or dropping the external location does not invalidate a key a
  client already holds — only rotating the key does (see above).

So treat a vended static credential as the store credential itself, use one key per trust boundary,
and prefer IAM roles wherever STS is available.

### Cross-user access

Clients (for example the Unity Catalog Hadoop connector) may cache vended cloud credentials in the
JVM, keyed by storage path/operation and a *credential context* derived from the Unity Catalog
server URI, storage scheme, and UC authentication configuration (`TokenProvider` configs).

A natural concern: user A is granted access to an external location and receives vended
credentials; user B, who is not granted that location, later accesses the same path on the same
client JVM and might reuse user A’s cached credentials without calling Unity Catalog again.

With that cache key, when each caller authenticates to Unity Catalog as a **different user**
(distinct static tokens or otherwise distinct `TokenProvider` configs), the callers produce
different credential context ids. The cache does not hit across users: user B’s request misses,
credential vending runs again, and Unity Catalog authorization denies access.

Two limits on that argument. It is a property of the cache key in the client, not something the
server enforces — a different or older client may key its cache more coarsely, and the server cannot
tell. And it depends on distinct UC auth identities, not on Unity Catalog catalog names: if several
application users share one UC service principal (same token or OAuth client), they share a
credential context and therefore share the cache. Avoid that pattern when per-user isolation is
required, and given the scope of a static key, do not rely on client-side caching behaviour as the
only thing standing between an unauthorized user and the key.

# Migration of existing per-bucket credential configuration

For a server with old credentials configured in the `server.properties` file that are used for accessing S3 buckets directly, without creating a storage credential according to this doc, they are recommended to be migrated. These old configurations may look like this:

```ini
## S3 Storage Config (Multiple configs can be added by incrementing the index)
s3.bucketPath.0=s3://some-bucket/
s3.region.0=us-west-2
s3.awsRoleArn.0=<some ARN to be assumed> 
# Optional (If blank, it will use DefaultCredentialsProviderChain)
s3.accessKey.0=...
s3.secretKey.0=...
```

The following steps are recommended to migrate the old credential configs:

1. Comment out the old configs in the `server.properties` file
2. Start from the beginning of this doc, follow the entire procedure to configure everything, including master role, storage role, storage credential, and external location. This process may involve restarting the server after modifying the `server.properties` file. Make sure the external location is created to **cover all of the existing data**.
3. Verify that data access is working properly. Then completely remove old configs from the `server.properties` file
