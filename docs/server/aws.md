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
