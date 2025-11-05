# Quick Start: S3-Compatible Storage (MinIO)

**Feature**: 001-s3-compatibility

## Overview

This guide shows how to configure Unity Catalog to work with MinIO or other S3-compatible storage services.

## Prerequisites

- Unity Catalog server installed (version >= 0.1.0 with S3-compatible support)
- MinIO server running and accessible
- MinIO bucket created
- MinIO access credentials (access key + secret key)

## Configuration Methods

### Method 1: Direct Configuration (server.properties)

**File**: `etc/conf/server.properties`

```properties
# MinIO configuration
s3.bucketPath.0=s3://my-minio-bucket
s3.region.0=us-east-1
s3.accessKey.0=minioadmin
s3.secretKey.0=minioadmin123
s3.serviceEndpoint.0=https://minio.example.com:9000
```

**Start Server**:
```bash
bin/start-uc-server
```

---

### Method 2: Environment Variables

**Set Variables**:
```bash
export S3_BUCKET_PATH_0=s3://my-minio-bucket
export S3_REGION_0=us-east-1
export S3_ACCESS_KEY_0=minioadmin
export S3_SECRET_KEY_0=minioadmin123
export S3_SERVICE_ENDPOINT_0=https://minio.example.com:9000
```

**Start Server**:
```bash
bin/start-uc-server
```

---

### Method 3: Helm Chart (Kubernetes)

**File**: `values.yaml`

```yaml
storage:
  credentials:
    s3:
      - bucketPath: s3://my-minio-bucket
        region: us-east-1
        credentialsSecretName: minio-creds
        serviceEndpoint: https://minio.example.com:9000
```

**Create Secret**:
```bash
kubectl create secret generic minio-creds \
  --from-literal=accessKey=minioadmin \
  --from-literal=secretKey=minioadmin123
```

**Deploy**:
```bash
helm install unity-catalog ./helm -f values.yaml
```

---

## Testing with Unity Catalog CLI

### 1. Create Catalog and Schema

```bash
bin/uc catalog create --name minio_catalog --comment "MinIO catalog"
bin/uc schema create --catalog minio_catalog --name default --comment "Default schema"
```

### 2. Register External Location

```bash
bin/uc location create \
  --name minio_location \
  --url s3://my-minio-bucket/data \
  --comment "MinIO external location"
```

### 3. Create External Table

```bash
bin/uc table create \
  --full_name minio_catalog.default.test_table \
  --columns "id INT, name STRING" \
  --storage_location s3://my-minio-bucket/data/test_table \
  --format DELTA
```

### 4. Request Temporary Credentials

```bash
bin/uc credential create --location s3://my-minio-bucket/data/test_table
```

**Expected Output**:
```json
{
  "awsCredentials": {
    "accessKeyId": "minioadmin",
    "secretAccessKey": "minioadmin123",
    "sessionToken": "..."
  }
}
```

---

## Spark Integration

### PySpark Example

```python
from pyspark.sql import SparkSession

# Create Spark session with Unity Catalog
spark = SparkSession.builder \
    .appName("MinIO Unity Catalog") \
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
    .config("spark.sql.catalog.unity.token", "<your-token>") \
    .config("spark.sql.defaultCatalog", "unity") \
    .getOrCreate()

# Read from MinIO-backed table
# (Spark automatically configures fs.s3a.endpoint from Unity Catalog)
df = spark.table("minio_catalog.default.test_table")
df.show()

# Write to MinIO-backed table
new_data = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
new_data.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("minio_catalog.default.test_table")
```

### Scala Example

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("MinIO Unity Catalog")
  .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
  .config("spark.sql.catalog.unity.uri", "http://localhost:8080")
  .config("spark.sql.catalog.unity.token", "<your-token>")
  .config("spark.sql.defaultCatalog", "unity")
  .getOrCreate()

val df = spark.table("minio_catalog.default.test_table")
df.show()
```

**Note**: You do NOT need to manually configure `fs.s3a.endpoint` in Spark. Unity Catalog automatically injects this property when vending credentials.

---

## Multiple S3-Compatible Services

Unity Catalog supports multiple S3-compatible storage backends simultaneously:

```properties
# AWS S3 (no serviceEndpoint = default AWS)
s3.bucketPath.0=s3://my-aws-bucket
s3.region.0=us-west-2
s3.awsRoleArn.0=arn:aws:iam::123456789012:role/UCRole
s3.accessKey.0=<aws-key>
s3.secretKey.0=<aws-secret>

# MinIO
s3.bucketPath.1=s3://my-minio-bucket
s3.region.1=us-east-1
s3.accessKey.1=minioadmin
s3.secretKey.1=minioadmin123
s3.serviceEndpoint.1=https://minio.example.com:9000

# Wasabi
s3.bucketPath.2=s3://my-wasabi-bucket
s3.region.2=us-west-1
s3.accessKey.2=<wasabi-key>
s3.secretKey.2=<wasabi-secret>
s3.serviceEndpoint.2=https://s3.us-west-1.wasabisys.com

# Backblaze B2
s3.bucketPath.3=s3://my-b2-bucket
s3.region.3=us-west-004
s3.accessKey.3=<b2-key>
s3.secretKey.3=<b2-secret>
s3.serviceEndpoint.3=https://s3.us-west-004.backblazeb2.com
```

Unity Catalog routes credentials based on bucket path prefix matching.

---

## MinIO-Specific Notes

### STS Support

MinIO supports AWS STS API but with limitations:
- `AssumeRoleWithWebIdentity` supported
- Full IAM policy downscoping NOT supported in all MinIO versions
- Static credentials (accessKey + secretKey) always work

**Recommendation**: Use static credentials for MinIO unless you have MinIO >= 2024.01.01 with full STS support.

### Path Style Access

MinIO requires path-style S3 access. Unity Catalog automatically enables this when `fs.s3a.endpoint` is set:

```properties
fs.s3a.endpoint=https://minio.example.com:9000
fs.s3a.path.style.access=true
```

### SSL/TLS Configuration

**Production**: Use HTTPS endpoints
```properties
s3.serviceEndpoint.0=https://minio.example.com:9000
```

**Development**: HTTP is supported but not recommended
```properties
s3.serviceEndpoint.0=http://localhost:9000
```

**Self-signed certificates**: Configure Java truststore
```bash
export JAVA_OPTS="-Djavax.net.ssl.trustStore=/path/to/truststore.jks"
bin/start-uc-server
```

---

## Troubleshooting

### Problem: Connection Refused

**Symptom**: `java.net.ConnectException: Connection refused`

**Solutions**:
1. Verify MinIO is running: `curl https://minio.example.com:9000/minio/health/live`
2. Check firewall rules allow port 9000
3. Verify serviceEndpoint URL is correct (include port if non-standard)

---

### Problem: Access Denied

**Symptom**: `S3Exception: Access Denied (Service: S3, Status Code: 403)`

**Solutions**:
1. Verify MinIO credentials: `mc admin info myminio`
2. Check bucket exists: `mc ls myminio/my-minio-bucket`
3. Verify bucket policy allows access
4. Ensure credentials have not expired

---

### Problem: UnknownHostException

**Symptom**: `java.net.UnknownHostException: minio.example.com`

**Solutions**:
1. Verify DNS resolution: `nslookup minio.example.com`
2. Add entry to `/etc/hosts` if using local MinIO
3. Check Kubernetes service names match (e.g., `minio.minio-ns.svc.cluster.local`)

---

### Problem: Endpoint Not Applied

**Symptom**: Spark attempts to connect to AWS S3 instead of MinIO

**Solutions**:
1. Verify serviceEndpoint configured in server.properties
2. Check Unity Catalog server logs for "serviceEndpoint" in S3StorageConfig
3. Ensure bucket path matches configuration exactly
4. Verify Unity Catalog version supports S3-compatible storage (>= 0.1.0)

---

## Migration from AWS S3 to MinIO

### Step 1: Data Migration

Use MinIO Client (mc) or AWS CLI with endpoint override:

```bash
# Configure mc
mc alias set aws https://s3.amazonaws.com AWS_KEY AWS_SECRET
mc alias set minio https://minio.example.com:9000 minioadmin minioadmin123

# Mirror data
mc mirror --preserve aws/my-aws-bucket/ minio/my-minio-bucket/
```

### Step 2: Add MinIO Configuration

Keep existing AWS S3 config, add MinIO as new index:

```properties
# Existing AWS S3 (index 0)
s3.bucketPath.0=s3://my-aws-bucket
s3.region.0=us-west-2
s3.awsRoleArn.0=arn:aws:iam::123456789012:role/UCRole
s3.accessKey.0=<aws-key>
s3.secretKey.0=<aws-secret>

# New MinIO (index 1)
s3.bucketPath.1=s3://my-minio-bucket
s3.region.1=us-east-1
s3.accessKey.1=minioadmin
s3.secretKey.1=minioadmin123
s3.serviceEndpoint.1=https://minio.example.com:9000
```

### Step 3: Update Table Locations

Use Unity Catalog CLI to update table storage locations:

```bash
bin/uc table update \
  --full_name catalog.schema.table \
  --storage_location s3://my-minio-bucket/data/table
```

### Step 4: Validate

Test read/write with Spark:

```python
df = spark.table("catalog.schema.table")
df.count()  # Should succeed with MinIO endpoint
```

---

## Best Practices

1. **Use HTTPS**: Always use HTTPS endpoints in production
2. **Secrets Management**: Use Kubernetes secrets or environment variables, never commit credentials
3. **Region Configuration**: Even with custom endpoints, configure region for consistency
4. **Backup Configuration**: Document serviceEndpoint URLs in runbooks
5. **Monitor Logs**: Unity Catalog logs show which endpoint is used for each request
6. **Test Failover**: Verify behavior when MinIO is temporarily unavailable

---

## Additional Resources

- [MinIO STS Documentation](https://min.io/docs/minio/linux/developers/security-token-service.html)
- [Unity Catalog Documentation](../../docs/README.md)
- [Spark S3A Configuration](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)
- [AWS SDK Endpoint Override](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html)

---

## Support

For issues specific to S3-compatible storage support, please file a GitHub issue with:
- MinIO version
- Unity Catalog version
- Configuration (sanitized, no secrets)
- Error logs
- Steps to reproduce
