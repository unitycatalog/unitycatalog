# Credential Vending with External Locations

This example demonstrates Unity Catalog's **credential vending** capability
using **external locations** and **storage credentials** backed by AWS S3. It
uses **AWS IAM Roles Anywhere** with self-signed X.509 certificates so the UC
server can obtain temporary AWS credentials without long-lived access keys.

## Architecture

```
                    Local Machine                          AWS
               ┌─────────────────────┐      ┌──────────────────────────────┐
               │                     │      │                              │
               │  Self-signed CA ────┼──────▶  Trust Anchor               │
               │       │             │      │  (IAM Roles Anywhere)       │
               │       ▼             │      │       │                     │
               │  Server Cert        │      │       ▼                     │
               │       │             │      │  UC Master Role             │
               │       ▼             │      │       │ sts:AssumeRole      │
               │  aws_signing_helper │      │       ▼                     │
               │       │ credential  │      │  Data Access Role           │
               │       │ _process    │      │       │                     │
               │       ▼             │      │       ▼                     │
               │  UC Server ─────────┼──────▶  S3 Bucket                  │
               │                     │      │                              │
               └─────────────────────┘      └──────────────────────────────┘
```

### How it works

1. **Certificate generation** -- A self-signed CA issues a server certificate
   for the UC server identity.

2. **IAM Roles Anywhere** -- The CA certificate is registered as a **Trust
   Anchor**. A **Profile** maps certificate holders to the UC Master IAM Role.
   The `aws_signing_helper` tool exchanges the server certificate for short-lived
   AWS credentials via the `credential_process` mechanism in `~/.aws/config`.

3. **Credential vending** -- When a client creates a **storage credential**
   (`POST /credentials`) with a customer-owned Data Access Role ARN, the UC
   server records it along with a server-generated `external_id`. The customer
   adds the UC master role and the `external_id` to the Data Access Role's trust
   policy.

4. **External location** -- A client creates an **external location**
   (`POST /external-locations`) that maps an S3 path prefix to the storage
   credential.

5. **Temporary credentials** -- When a client requests temporary credentials
   (`POST /temporary-path-credentials`), the UC server:
   - Resolves the matching external location for the requested path
   - Uses its master role identity to call `sts:AssumeRole` on the Data Access
     Role (with the `external_id`)
   - Generates a scoped IAM policy limiting access to the requested path and
     operation
   - Returns short-lived credentials (1 hour)

## Prerequisites

| Tool | Purpose | Install |
|------|---------|---------|
| AWS CLI v2 | Provision AWS resources | [Install guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) |
| OpenSSL | Generate certificates | Included on macOS / most Linux distros |
| jq | Parse JSON responses | `brew install jq` or `apt install jq` |
| aws_signing_helper | Credential process for IAM Roles Anywhere | [Download](https://docs.aws.amazon.com/rolesanywhere/latest/userguide/credential-helper.html) |
| curl | Interact with the UC REST API | Included on macOS / most Linux distros |

You also need an AWS account with permissions to create IAM roles, S3 buckets,
and IAM Roles Anywhere resources.

## Quick Start

### 1. Generate certificates

```bash
./generate-certs.sh
```

Creates a self-signed CA and a server certificate in `.certs/`.

### 2. Provision AWS resources

```bash
# Optional: set a region (default: us-east-1)
export AWS_REGION=us-east-1

./setup-aws.sh
```

This creates:

- An S3 bucket (`uc-demo-data-<account-id>`)
- A UC Master IAM Role trusted by IAM Roles Anywhere
- A Data Access IAM Role with S3 permissions (trust policy uses a placeholder
  `external_id` that is updated later by `demo.sh`)
- An IAM Roles Anywhere Trust Anchor (using the CA certificate)
- An IAM Roles Anywhere Profile mapped to the master role

All resource identifiers are saved to `.env`.

### 3. Configure the UC server

```bash
./configure-uc.sh
```

Generates `server.properties` with the correct `aws.masterRoleArn` and prints
the `~/.aws/config` snippet you need to add for `credential_process`. For
example:

```ini
[profile uc-credential-vending]
credential_process = aws_signing_helper credential-process \
  --certificate       /path/to/.certs/server.crt \
  --private-key       /path/to/.certs/server.key \
  --trust-anchor-arn  arn:aws:rolesanywhere:us-east-1:123456789012:trust-anchor/abc \
  --profile-arn       arn:aws:rolesanywhere:us-east-1:123456789012:profile/def \
  --role-arn          arn:aws:iam::123456789012:role/uc-demo-master-role \
  --region            us-east-1
```

### 4. Start the UC server

Copy (or symlink) the generated `server.properties` into the UC server's config
directory, then start the server:

```bash
cp server.properties ../../etc/conf/server.properties
export AWS_PROFILE=uc-credential-vending
cd ../..
bin/start-uc-server
```

### 5. Run the demo

```bash
./demo.sh
```

The demo script performs the full credential vending flow:

1. Creates a catalog and schema
2. Creates a storage credential pointing to the Data Access Role
3. Reads the server-generated `external_id` and updates the Data Access Role's
   trust policy
4. Creates an external location mapping the S3 bucket to the credential
5. Requests temporary path credentials from the UC server
6. Uses the vended credentials to list and read objects in S3

## Manual API Walkthrough

If you prefer to run the steps individually, here are the curl commands.
Replace `$DATA_ROLE_ARN` and `$BUCKET_NAME` with values from `.env`.

### Create a storage credential

```bash
curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/credentials \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "s3-data-access",
    "purpose": "STORAGE",
    "aws_iam_role": {
      "role_arn": "'$DATA_ROLE_ARN'"
    }
  }' | jq .
```

The response includes:

- `aws_iam_role.unity_catalog_iam_arn` -- the UC master role ARN. Add this to
  the Data Access Role's trust policy.
- `aws_iam_role.external_id` -- add this as an `sts:ExternalId` condition in
  the trust policy.

### Create an external location

```bash
curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/external-locations \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo-external-location",
    "url": "s3://'$BUCKET_NAME'/demo",
    "credential_name": "s3-data-access"
  }' | jq .
```

### Request temporary path credentials

```bash
curl -s -X POST http://localhost:8080/api/2.1/unity-catalog/temporary-path-credentials \
  -H 'Content-Type: application/json' \
  -d '{
    "url": "s3://'$BUCKET_NAME'/demo",
    "operation": "PATH_READ"
  }' | jq .
```

The response contains `aws_temp_credentials` with `access_key_id`,
`secret_access_key`, and `session_token` valid for one hour, scoped to the
requested path and operation.

### Verify with the AWS CLI

```bash
export AWS_ACCESS_KEY_ID=<access_key_id from above>
export AWS_SECRET_ACCESS_KEY=<secret_access_key from above>
export AWS_SESSION_TOKEN=<session_token from above>

aws s3 ls s3://$BUCKET_NAME/demo/
aws s3 cp s3://$BUCKET_NAME/demo/hello.txt -
```

## Cleanup

```bash
./teardown-aws.sh
```

This deletes all AWS resources (S3 bucket, IAM roles, Roles Anywhere Trust
Anchor and Profile) and removes the `.env` file. You should also:

- Remove the `[profile uc-credential-vending]` section from `~/.aws/config`
- Delete the `.certs/` directory if you no longer need the certificates

## Customization

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `AWS_REGION` | `us-east-1` | AWS region for all resources |
| `RESOURCE_PREFIX` | `uc-demo` | Prefix for resource names |
| `UC_BASE_URL` | `http://localhost:8080` | UC server base URL (used by `demo.sh`) |
| `CERT_CN` | `unity-catalog-server` | Common Name for the server certificate |
| `CA_DAYS` | `3650` | CA certificate validity (days) |
| `CERT_DAYS` | `365` | Server certificate validity (days) |

## Troubleshooting

### "S3 bucket configuration not found"

The UC server could not match the requested path to an external location or
per-bucket config. Verify the external location URL matches the path you are
requesting credentials for.

### "Unable to assume role"

- Ensure the Data Access Role's trust policy includes the correct
  `unity_catalog_iam_arn` as principal and the correct `external_id` in the
  `sts:ExternalId` condition.
- The `demo.sh` script updates the trust policy automatically. If you are
  running the steps manually, you need to update it yourself after creating
  the storage credential.

### Credential process errors

- Verify `aws_signing_helper` is on your PATH.
- Check that the certificate and key paths in `~/.aws/config` are absolute
  and point to the correct files.
- Ensure the IAM Roles Anywhere Trust Anchor and Profile are enabled.
