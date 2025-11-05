# API Contracts

**Feature**: 001-s3-compatibility

## Overview

This feature does NOT introduce any API specification changes. All modifications are internal configuration and implementation changes.

## Why No API Contracts?

Per **Unity Catalog Constitution Principle I: API Specification First**, this feature was designed specifically to avoid API changes by:

1. **Configuration-based approach**: Service endpoint is a configuration property, not part of the API contract
2. **Spark internal mechanism**: Endpoint information flows through Spark configuration properties (`fs.s3a.endpoint`), not API responses
3. **Backward compatibility**: Existing `TemporaryCredentials` API remains unchanged
4. **Internal extension**: Changes limited to ServerProperties, S3StorageConfig, and CredentialsGenerator internals

## Contracts Validated

While no API specification changes are needed, the following internal contracts were validated:

### S3StorageConfig Contract

**Type**: Java class contract  
**Location**: `server/src/main/java/io/unitycatalog/server/service/credential/aws/S3StorageConfig.java`

**Contract**:
- MUST remain immutable (Lombok @Builder pattern)
- MUST support null serviceEndpoint (backward compatibility)
- MUST validate URL format when serviceEndpoint provided
- MUST NOT break existing builder usage

**Validation**: Covered by unit tests in `S3StorageConfigTest.java`

---

### CredentialsGenerator Contract

**Type**: Java interface contract  
**Location**: `server/src/main/java/io/unitycatalog/server/service/credential/aws/CredentialsGenerator.java`

**Contract**:
- `generate(CredentialContext)` signature MUST remain unchanged
- Implementations MUST handle null/empty serviceEndpoint gracefully
- MUST maintain existing behavior when serviceEndpoint is null

**Validation**: Covered by unit tests in `StsCredentialsGeneratorTest.java`

---

### Spark Configuration Properties Contract

**Type**: Hadoop configuration contract  
**Location**: `connectors/spark/src/main/scala/io/unitycatalog/spark/auth/CredPropsUtil.java`

**Contract**:
- MUST set `fs.s3a.endpoint` only when serviceEndpoint is non-null
- MUST NOT set `fs.s3a.endpoint` for AWS S3 (undefined = AWS S3 default)
- MUST preserve all existing S3A properties (access.key, secret.key, session.token)

**Validation**: Covered by unit tests in `CredPropsUtilTest.java`

---

## Constitution Compliance

✅ **Principle I: API Specification First** - No API changes required  
✅ **Principle II: Backward Compatibility** - All changes additive and optional  
✅ **Principle III: No Schema Modifications** - No database schema changes  
✅ **Principle IV: Zero Secrets** - Secrets managed via environment variables  
✅ **Principle V: Code Quality** - Google Java Style Guide enforced  
✅ **Principle VI: Build Integrity** - sbt compile/test gates enforced  
✅ **Principle VII: Helm Synchronization** - values.yaml updated with serviceEndpoint

---

## Testing Strategy

Since no API contracts changed, testing focuses on:

1. **Unit Tests**: S3StorageConfig, ServerProperties, CredentialsGenerator, CredPropsUtil
2. **Integration Tests**: End-to-end credential vending with MinIO testcontainer
3. **Spark Integration Tests**: Verify fs.s3a.endpoint property flow and data access
4. **Backward Compatibility Tests**: Verify AWS S3 behavior unchanged when serviceEndpoint absent

See `specs/001-s3-compatibility/spec.md` Section 8 (Testing) for full strategy.
