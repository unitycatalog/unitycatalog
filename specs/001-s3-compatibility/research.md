# Research: S3-Compatible Storage Support

**Feature**: 001-s3-compatibility  
**Date**: 2025-11-04  
**Status**: Complete

## Overview

This document captures research findings for implementing S3-compatible storage support in Unity Catalog, enabling MinIO, Wasabi, and other S3-compatible services.

## Key Research Areas

### 1. MinIO STS Support

**Decision**: Use MinIO's STS-compatible API with AssumeRoleWithWebIdentity when available; fall back to static credentials when STS is not configured.

**Rationale**: 
- MinIO implements a subset of AWS STS API including AssumeRole and AssumeRoleWithWebIdentity
- MinIO does not support full policy-based permission downscoping like AWS STS
- MinIO STS returns temporary credentials (access key, secret key, session token) with configurable expiration
- Static credentials are acceptable fallback for simpler MinIO deployments

**Alternatives Considered**:
1. **Require full AWS STS compatibility**: Rejected - would exclude MinIO and most S3-compatible services
2. **Static credentials only**: Rejected - loses security benefits of temporary credentials and automatic expiration
3. **Custom credential provider per service**: Rejected - increases complexity and maintenance burden

**References**:
- MinIO STS API: https://min.io/docs/minio/linux/developers/security-token-service.html
- AWS STS AssumeRole: https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html

---

### 2. AWS SDK STS Client Custom Endpoint Configuration

**Decision**: Configure AWS STS client with custom endpoint URL via `StsClient.builder().endpointOverride(URI)`.

**Rationale**:
- AWS SDK for Java 2.x provides `endpointOverride()` method on all client builders
- This allows pointing STS client to MinIO or other S3-compatible endpoints
- Maintains same AWS SDK API patterns and credential structures
- No need for custom HTTP clients or credential providers

**Implementation Pattern**:
```java
StsClient stsClient = StsClient.builder()
    .region(Region.of(region))
    .endpointOverride(URI.create("https://minio.example.com:9000"))
    .credentialsProvider(...)
    .build();
```

**Alternatives Considered**:
1. **Custom HTTP client**: Rejected - unnecessary complexity, loses SDK benefits
2. **Multiple credential vendor implementations**: Rejected - violates DRY, harder to maintain
3. **Service-specific credential classes**: Rejected - breaks existing API contracts

**References**:
- AWS SDK Endpoint Override: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html
- StsClient Builder: https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/sts/StsClient.Builder.html

---

### 3. Spark S3A Filesystem Endpoint Configuration

**Decision**: Set `spark.hadoop.fs.s3a.endpoint` property in credential properties returned by CredPropsUtil.

**Rationale**:
- Spark's S3A filesystem connector supports `fs.s3a.endpoint` configuration
- This property overrides the default AWS S3 endpoint for all S3 operations
- Already integrated into credential properties flow via `CredPropsUtil.createTableCredProps()` and `createPathCredProps()`
- Property is set per storage location, enabling mixed AWS S3 + MinIO in same session

**Implementation Pattern**:
```java
// In CredPropsUtil.s3TempCredPropsBuilder()
if (serviceEndpoint != null) {
    builder.set("fs.s3a.endpoint", serviceEndpoint);
}
```

**Alternatives Considered**:
1. **Global Spark configuration**: Rejected - cannot support mixed storage backends
2. **Custom S3A filesystem implementation**: Rejected - unnecessary complexity, maintenance burden
3. **Dynamic endpoint resolution in FileSystem**: Rejected - requires Spark core changes

**References**:
- Hadoop S3A Endpoint Configuration: https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_endpoints
- Unity Catalog CredPropsUtil: `connectors/spark/src/main/scala/io/unitycatalog/spark/auth/CredPropsUtil.java`

---

### 4. Configuration Property Naming Convention

**Decision**: Use `s3.serviceEndpoint.[index]` following existing S3 configuration index pattern.

**Rationale**:
- Consistent with existing pattern: `s3.bucketPath.0`, `s3.region.0`, `s3.awsRoleArn.0`
- Index-based configuration supports multiple S3-compatible storage backends
- Clear semantic meaning - "serviceEndpoint" indicates the S3-compatible service's API endpoint
- Works with existing ServerProperties parsing logic

**Pattern**:
```properties
# AWS S3 (no serviceEndpoint = default AWS)
s3.bucketPath.0=s3://my-aws-bucket
s3.region.0=us-west-2
s3.awsRoleArn.0=arn:aws:iam::123456789012:role/UCRole

# MinIO
s3.bucketPath.1=s3://my-minio-bucket
s3.region.1=us-east-1
s3.awsRoleArn.1=arn:minio:iam::minioadmin:role/UCRole
s3.serviceEndpoint.1=https://minio.example.com:9000
s3.accessKey.1=minioadmin
s3.secretKey.1=minioadmin123
```

**Alternatives Considered**:
1. **s3.stsEndpoint.[index]**: Rejected - less clear that it applies to data access too
2. **s3.customEndpoint.[index]**: Rejected - "custom" is vague
3. **s3.endpoint.[index]**: Rejected - ambiguous (STS vs. S3 API vs. both)
4. **Separate minio.* namespace**: Rejected - violates abstraction, hard to extend to other providers

---

### 5. Credential Vending Flow Architecture

**Decision**: Extend existing `StsCredentialsGenerator` to accept optional service endpoint; pass through credential vending chain.

**Current Flow**:
```
CloudCredentialVendor.vendCredential()
  → AwsCredentialVendor.vendAwsCredentials()
    → CredentialsGenerator.generate()
      → StsCredentialsGenerator (uses AWS STS)
      → StaticCredentialsGenerator (returns static creds)
```

**Enhanced Flow**:
```
ServerProperties.getS3Configurations()
  → S3StorageConfig (includes optional serviceEndpoint)
    → AwsCredentialVendor (passes endpoint to generator)
      → StsCredentialsGenerator (uses custom endpoint if provided)
        → StsClient with endpointOverride
          → MinIO/S3-compatible STS API
```

**Rationale**:
- Minimal changes to existing architecture
- Maintains backward compatibility (null endpoint = AWS STS)
- Extends rather than replaces existing generators
- No new interfaces or abstract classes needed

**Key Changes**:
1. **S3StorageConfig**: Add `serviceEndpoint` field with Lombok `@Builder.Default(null)`
2. **ServerProperties.getS3Configurations()**: Parse `s3.serviceEndpoint.[index]` property
3. **StsCredentialsGenerator constructors**: Accept optional `endpointOverride` parameter
4. **CredPropsUtil**: Include endpoint in Spark configuration properties

**Alternatives Considered**:
1. **New MinioCredentialVendor class**: Rejected - duplicates code, harder to maintain
2. **Strategy pattern with provider-specific generators**: Rejected - over-engineering for current needs
3. **Configuration-driven credential provider**: Rejected - increases complexity without clear benefit

---

### 6. Helm Chart Environment Variable Mapping

**Decision**: Map `serviceEndpoint` from Helm values to environment variables following existing S3 credential pattern.

**Pattern**:
```yaml
# values.yaml
storage:
  credentials:
    s3:
      - bucketPath: s3://my-minio-bucket
        region: us-east-1
        awsRoleArn: arn:minio:iam::minioadmin:role/UCRole
        serviceEndpoint: https://minio.example.com:9000  # NEW
        credentialsSecretName: minio-creds

# deployment.yaml (init container env)
- name: S3_SERVICE_ENDPOINT_{{ $index }}
  value: {{ $config.serviceEndpoint | quote }}
```

**server.properties.template** (processed by envsubst in init container):
```properties
s3.serviceEndpoint.0=${S3_SERVICE_ENDPOINT_0}
```

**Rationale**:
- Consistent with existing S3 credential injection (`S3_ACCESS_KEY_0`, `S3_SECRET_KEY_0`)
- Environment variables processed by envsubst in init container
- Supports multiple S3-compatible backends with indexed naming
- Clear separation between template and actual values

**Alternatives Considered**:
1. **ConfigMap with full server.properties**: Rejected - loses secret management benefits
2. **Helm chart generates full config**: Rejected - harder to customize, less flexible
3. **Kubernetes Service for endpoint discovery**: Rejected - assumes in-cluster deployment

---

### 7. Testing Strategy

**Decision**: Multi-level testing approach matching existing Unity Catalog test patterns.

**Test Levels**:

1. **Unit Tests** (JUnit 5 + Mockito):
   - `S3StorageConfigTest`: Verify builder with serviceEndpoint
   - `CredentialsGeneratorTest`: Mock STS client with custom endpoint
   - `ServerPropertiesTest`: Parse serviceEndpoint configuration
   - `CredPropsUtilTest`: Verify fs.s3a.endpoint property inclusion

2. **Integration Tests** (with mock STS):
   - `CloudCredentialVendorTest`: End-to-end credential vending with custom endpoint
   - `SdkTemporaryTableCredentialTest`: Verify TemporaryCredentials API works with MinIO configs

3. **End-to-End Tests** (optional, requires real MinIO):
   - Spin up MinIO container (testcontainers)
   - Configure Unity Catalog with MinIO endpoint
   - Create table, request credentials, verify Spark can read data

**Rationale**:
- Follows existing test structure in `server/src/test/`
- Unit tests cover logic without external dependencies
- Integration tests validate credential flow
- E2E tests validate full stack (can be run separately/manually)

**Alternatives Considered**:
1. **E2E tests only**: Rejected - slow, fragile, hard to debug
2. **Mock-heavy approach**: Rejected - doesn't validate AWS SDK integration
3. **Manual testing only**: Rejected - doesn't prevent regressions

---

### 8. Documentation Requirements

**Decision**: Update configuration documentation, add examples, document limitations.

**Documentation Updates**:

1. **server.properties** (inline comments):
   ```properties
   ## S3 Storage Config (Multiple configs can be added by incrementing the index)
   s3.bucketPath.0=
   s3.region.0=
   s3.awsRoleArn.0=
   # Optional: S3-compatible service endpoint (MinIO, Wasabi, etc.)
   # Leave empty for AWS S3. For MinIO: https://minio.example.com:9000
   s3.serviceEndpoint.0=
   ```

2. **Helm values.yaml** (documentation comments):
   ```yaml
   # -- (string) Optional S3-compatible service endpoint
   # @default -- not set (uses AWS S3)
   # @raw
   #
   # Example for MinIO: `https://minio.example.com:9000`
   # Example for Wasabi: `https://s3.wasabisys.com`
   #
   serviceEndpoint:
   ```

3. **README.md** or **docs/server/configuration.md**:
   - New section: "S3-Compatible Storage Configuration"
   - Example: Configuring MinIO
   - Known limitations: MinIO STS policy scoping
   - Troubleshooting: Common endpoint configuration errors

**Rationale**:
- Users need clear guidance on new configuration options
- Examples reduce support burden
- Limitations prevent incorrect expectations
- Follows existing Unity Catalog documentation patterns

---

## Implementation Checklist

- [ ] Extend S3StorageConfig with serviceEndpoint field
- [ ] Update ServerProperties to parse s3.serviceEndpoint.[index]
- [ ] Modify StsCredentialsGenerator to accept endpoint override
- [ ] Update CredPropsUtil to set fs.s3a.endpoint
- [ ] Update Helm values.yaml with serviceEndpoint field
- [ ] Update Helm deployment.yaml with S3_SERVICE_ENDPOINT_* env vars
- [ ] Update server.properties.template with serviceEndpoint config
- [ ] Add unit tests for all modified classes
- [ ] Add integration tests for credential vending
- [ ] Update documentation (server.properties, Helm chart, README)
- [ ] Run `build/sbt compile` to verify build
- [ ] Run `build/sbt test` to verify all tests pass
- [ ] Run `build/sbt javafmtAll` to format code

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| MinIO STS doesn't support policy downscoping | Medium | Document limitation; use static credentials if needed; scope at MinIO policy level |
| Endpoint URL validation errors | Low | Add URL format validation in ServerProperties with clear error messages |
| Spark S3A endpoint conflicts in mixed environments | Medium | Test with both AWS S3 and MinIO tables in same session; verify per-table endpoint isolation |
| Helm chart upgrade breaks existing deployments | High | Extensive backward compatibility testing; make serviceEndpoint truly optional |
| Performance degradation from endpoint lookup | Low | Cache STS clients in AwsCredentialVendor (already implemented) |

## Open Questions

**None remaining** - All research questions resolved with concrete decisions.
