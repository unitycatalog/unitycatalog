# Unity Catalog Helm Chart Examples

This directory contains example values files for deploying Unity Catalog with various S3-compatible storage services.

## Available Examples

### MinIO (`values-minio.yaml`)
Configuration for using Unity Catalog with MinIO, a popular open-source S3-compatible object storage service.

**Key features:**
- Path-style access required
- Custom endpoint configuration
- Internal cluster communication without SSL

**Use case:** Self-hosted object storage, development environments, air-gapped deployments

### Ceph Object Storage (`values-ceph.yaml`)
Configuration for using Unity Catalog with Ceph RADOS Gateway (RGW).

**Key features:**
- Enterprise-grade distributed storage
- Optional STS endpoint support
- Multiple bucket configurations

**Use case:** Large-scale production deployments, on-premises infrastructure

### AWS-Compatible Services (`values-aws-compatible.yaml`)
Examples for various S3-compatible cloud storage services:
- Wasabi Cloud Storage
- DigitalOcean Spaces
- Backblaze B2
- Custom enterprise S3 services

**Key features:**
- Different endpoint configurations
- Mix of path-style and virtual-hosted-style access
- STS support for enterprise services

**Use case:** Multi-cloud deployments, cost-optimized storage solutions

### LocalStack (`values-localstack.yaml`)
Configuration for local development with LocalStack.

**Key features:**
- Local AWS services emulation
- Dummy credentials support
- File-based database for simplicity

**Use case:** Local development, CI/CD pipelines, testing

## Usage

1. Choose the appropriate example file based on your storage backend
2. Copy and customize the values file:
   ```bash
   cp examples/values-minio.yaml my-values.yaml
   ```
3. Update the configuration with your specific settings:
   - Storage endpoints
   - Credentials and secrets
   - Database configuration
   - Network settings

4. Create necessary secrets:
   ```bash
   # Example for MinIO
   kubectl create secret generic minio-credentials \
     --from-literal=accessKey=YOUR_ACCESS_KEY \
     --from-literal=secretKey=YOUR_SECRET_KEY
   ```

5. Deploy Unity Catalog:
   ```bash
   helm install unity-catalog ./helm -f my-values.yaml
   ```

## S3 Configuration Parameters

### Required Parameters
- `bucketPath`: S3 bucket path (e.g., `s3://my-bucket`)
- `region`: AWS region or region-like identifier
- `credentialsSecretName`: Name of Kubernetes secret containing credentials

### Optional Parameters for S3-Compatible Services
- `endpoint`: Custom S3 endpoint URL (e.g., `https://minio.example.com`)
- `stsEndpoint`: Custom STS endpoint for credential vending
- `pathStyleAccess`: Use path-style access instead of virtual-hosted-style (default: `false`)
- `sslEnabled`: Enable/disable SSL for endpoint communication (default: `true`)
- `awsRoleArn`: IAM role for AssumeRole operations (not used by all services)

## Creating Credential Secrets

All S3 configurations require a Kubernetes secret with the following keys:
- `accessKey`: S3 access key ID
- `secretKey`: S3 secret access key

Example:
```bash
kubectl create secret generic my-s3-credentials \
  --from-literal=accessKey=AKIAIOSFODNN7EXAMPLE \
  --from-literal=secretKey=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

## Notes

1. **Path-style vs Virtual-hosted-style Access:**
   - Path-style: `http://s3.amazonaws.com/bucket/key`
   - Virtual-hosted-style: `http://bucket.s3.amazonaws.com/key`
   - Services like MinIO and LocalStack typically require path-style access

2. **SSL Configuration:**
   - Enable SSL for production deployments
   - Disable SSL for local development or internal cluster communication

3. **STS Endpoints:**
   - Only required if using credential vending with AssumeRole
   - Not all S3-compatible services support STS

4. **Database Selection:**
   - Use PostgreSQL for production deployments with multiple replicas
   - File-based H2 database is suitable for development and single-replica deployments

## Troubleshooting

### Connection Issues
- Verify endpoint URLs are accessible from within the cluster
- Check network policies and firewall rules
- Ensure SSL certificates are valid if `sslEnabled: true`

### Authentication Failures
- Verify secret exists and contains correct keys
- Check access key and secret key values
- Ensure IAM roles have necessary permissions (if using AWS)

### Path-style Access Errors
- Toggle `pathStyleAccess` setting
- MinIO and some services require `pathStyleAccess: true`
- AWS S3 generally works with `pathStyleAccess: false`