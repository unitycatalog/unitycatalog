# MinIO Integration with Unity Catalog

This guide explains how to configure Unity Catalog to work with MinIO, an S3-compatible object storage service that supports AWS STS (Security Token Service) for temporary credential generation.

## Prerequisites

1. MinIO server with STS enabled
2. Unity Catalog server
3. Spark cluster with Unity Catalog connector

## MinIO Setup

### 1. Enable STS in MinIO

MinIO supports STS AssumeRole operations. Ensure your MinIO server is configured with:

```bash
# Set MinIO root credentials
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin

# Start MinIO with STS enabled
minio server /data --console-address ":9001"
```

### 2. Create IAM Policies and Roles in MinIO

Use the MinIO client (`mc`) to set up IAM policies and roles:

```bash
# Configure MinIO client
mc alias set myminio http://localhost:9000 minioadmin minioadmin

# Create a policy for data access
cat > /tmp/data-access-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::unity-catalog-data/*",
        "arn:aws:s3:::unity-catalog-data"
      ]
    }
  ]
}
EOF

# Create the policy
mc admin policy create myminio data-access-policy /tmp/data-access-policy.json

# Create a user that will assume roles
mc admin user add myminio unity-catalog-user unity-catalog-password

# Attach policy to user
mc admin policy attach myminio data-access-policy --user unity-catalog-user
```

## Unity Catalog Configuration

### 1. Server Configuration (server.properties)

Add the following configuration to your `etc/conf/server.properties` file:

```properties
# MinIO S3 Configuration
s3.bucketPath.0=s3://unity-catalog-data
s3.region.0=us-east-1
s3.awsRoleArn.0=arn:minio:iam::account:role/data-access-role
s3.accessKey.0=unity-catalog-user
s3.secretKey.0=unity-catalog-password
# MinIO specific settings
s3.endpoint.0=http://localhost:9000
s3.stsEndpoint.0=http://localhost:9000
s3.pathStyleAccess.0=true
s3.sslEnabled.0=false

# If you have multiple MinIO instances or buckets
s3.bucketPath.1=s3://another-bucket
s3.region.1=us-east-1
s3.awsRoleArn.1=arn:minio:iam::account:role/another-role
s3.accessKey.1=another-user
s3.secretKey.1=another-password
s3.endpoint.1=http://minio2:9000
s3.stsEndpoint.1=http://minio2:9000
s3.pathStyleAccess.1=true
s3.sslEnabled.1=false
```

### 2. SSL/TLS Configuration (Production)

For production environments, enable SSL:

```properties
# MinIO with SSL
s3.endpoint.0=https://minio.example.com:9000
s3.stsEndpoint.0=https://minio.example.com:9000
s3.sslEnabled.0=true
```

If using self-signed certificates, you may need to configure the JVM trust store:

```bash
# Add MinIO's certificate to Java trust store
keytool -import -alias minio -file minio-cert.pem -keystore $JAVA_HOME/lib/security/cacerts
```

## Spark Configuration

When using Spark with Unity Catalog and MinIO, configure your Spark session:

```scala
val spark = SparkSession.builder()
  .appName("Unity Catalog with MinIO")
  .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
  .config("spark.sql.catalog.unity.uri", "http://localhost:8080")
  .config("spark.sql.catalog.unity.token", "<if-auth-enabled>")
  // MinIO endpoint configuration for Spark
  .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
  .config("spark.hadoop.fs.s3a.path.style.access", "true")
  .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
  .getOrCreate()
```

## Creating Tables with MinIO Storage

### 1. Create a Schema

```sql
CREATE SCHEMA IF NOT EXISTS unity.minio_schema;
```

### 2. Create an External Table

```sql
CREATE TABLE unity.minio_schema.sample_table (
    id BIGINT,
    name STRING,
    value DOUBLE
)
USING DELTA
LOCATION 's3://unity-catalog-data/tables/sample_table';
```

### 3. Create a Managed Table

```sql
CREATE TABLE unity.minio_schema.managed_table (
    id BIGINT,
    data STRING
)
USING PARQUET;
```

## Credential Vending Flow with MinIO

1. **Client Request**: Spark client requests access to a table stored in MinIO
2. **Unity Catalog Authorization**: Unity Catalog checks user permissions
3. **STS AssumeRole**: Unity Catalog calls MinIO's STS endpoint to assume the configured role
4. **Temporary Credentials**: MinIO returns temporary credentials with scoped permissions
5. **Credential Injection**: Unity Catalog injects credentials into Spark's Hadoop configuration
6. **Data Access**: Spark uses the temporary credentials to access data in MinIO

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Verify MinIO is running and accessible
   - Check firewall rules
   - Ensure endpoints are correctly configured

2. **Invalid Credentials**
   - Verify access key and secret key
   - Check if the user has the necessary policies attached
   - Ensure the role ARN format is correct for MinIO

3. **Path Style Access Issues**
   - MinIO requires path-style access (not virtual-hosted style)
   - Ensure `s3.pathStyleAccess.i=true` is set

4. **SSL Certificate Issues**
   - For self-signed certificates, import them into Java trust store
   - Or disable SSL verification (not recommended for production)

### Debug Logging

Enable debug logging in Unity Catalog:

```properties
# In log4j2.properties
logger.aws.name = io.unitycatalog.server.service.credential.aws
logger.aws.level = DEBUG
```

## Security Considerations

1. **Use SSL/TLS in Production**: Always use HTTPS endpoints in production
2. **Rotate Credentials**: Regularly rotate MinIO access credentials
3. **Limit IAM Policies**: Use the principle of least privilege for IAM policies
4. **Network Security**: Use private networks or VPNs between Unity Catalog and MinIO
5. **Audit Logging**: Enable audit logging in both MinIO and Unity Catalog

## Performance Optimization

1. **Connection Pooling**: The STS client connection is reused (TODO: implement caching)
2. **Credential Duration**: Adjust credential duration based on workload (currently 1 hour)
3. **Network Latency**: Place Unity Catalog server close to MinIO for lower latency
4. **Bucket Configuration**: Use separate buckets for different access patterns

## Example Use Case

Here's a complete example of setting up a data pipeline with MinIO:

```python
from pyspark.sql import SparkSession

# Initialize Spark with Unity Catalog
spark = SparkSession.builder \
    .appName("MinIO Data Pipeline") \
    .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog") \
    .config("spark.sql.catalog.unity.uri", "http://localhost:8080") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read data from MinIO through Unity Catalog
df = spark.table("unity.minio_schema.sample_table")

# Process data
result = df.groupBy("name").sum("value")

# Write results back to MinIO
result.write \
    .mode("overwrite") \
    .saveAsTable("unity.minio_schema.aggregated_results")
```

## Limitations and Future Work

1. **Endpoint Configuration per Table**: Currently, S3 endpoint configuration is global per bucket path
2. **Dynamic Credential Refresh**: Automatic credential refresh before expiration
3. **Multi-Region Support**: Support for MinIO clusters in different regions
4. **STS Token Caching**: Implement caching of STS tokens to reduce API calls