# Feature Specification: S3-Compatible Storage Support

**Feature Branch**: `001-s3-compatibility`  
**Created**: 2025-11-04  
**Status**: Draft  
**Input**: User description: "3rd-party s3 compatibility with MinIO and other S3-compatible storage engines"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Configure MinIO Storage Backend (Priority: P1) 🎯 MVP

Data engineers need to configure Unity Catalog to use MinIO or other S3-compatible storage services instead of AWS S3 for cost optimization, on-premises deployment, or regulatory compliance.

**Why this priority**: This is the foundational capability that enables all other S3-compatible storage functionality. Without this, Unity Catalog is locked into AWS S3.

**Independent Test**: Can be fully tested by configuring a MinIO endpoint in server.properties, creating a table with MinIO storage location, and verifying the table metadata is stored correctly.

**Acceptance Scenarios**:

1. **Given** a Unity Catalog server with MinIO configuration in `etc/conf/server.properties`, **When** an administrator starts the server, **Then** the server successfully initializes and validates the MinIO endpoint
2. **Given** a properly configured MinIO storage backend, **When** an administrator creates a catalog or table with a MinIO storage location, **Then** the metadata is stored successfully in Unity Catalog
3. **Given** an existing S3 configuration, **When** an administrator adds a MinIO configuration, **Then** both storage backends are available simultaneously without conflict

---

### User Story 2 - Retrieve Temporary Credentials for MinIO (Priority: P1) 🎯 MVP

Spark applications need to retrieve temporary credentials from Unity Catalog to access data stored in MinIO, following the same pattern as AWS S3 temporary credentials.

**Why this priority**: Credential vending is core to Unity Catalog's security model. Without working credentials, tables cannot be read or written.

**Independent Test**: Can be tested by requesting temporary table credentials via the API for a MinIO-based table and verifying credentials are returned with appropriate scoped permissions.

**Acceptance Scenarios**:

1. **Given** a table stored in MinIO, **When** a Spark application requests temporary credentials for READ operation, **Then** Unity Catalog returns scoped temporary credentials valid for the table location
2. **Given** a MinIO storage location, **When** requesting temporary credentials for WRITE operation, **Then** Unity Catalog returns credentials with write permissions scoped to the specific path
3. **Given** MinIO does not support full STS downscoping, **When** credentials are vended, **Then** Unity Catalog returns functional credentials with documented permission scope limitations

---

### User Story 3 - Access MinIO Tables from Spark (Priority: P1) 🎯 MVP

Data analysts using Spark need to read and write tables stored in MinIO through the Unity Catalog Spark connector, with the S3A filesystem properly configured for non-AWS S3-compatible endpoints.

**Why this priority**: This validates end-to-end integration and delivers actual value to users who want to query MinIO-based tables.

**Independent Test**: Can be tested by configuring UCSingleCatalog in a Spark session, querying a MinIO-backed table, and verifying data is read successfully with the correct endpoint configured.

**Acceptance Scenarios**:

1. **Given** a Spark session with UCSingleCatalog configured, **When** querying a table stored in MinIO, **Then** Spark successfully sets `spark.hadoop.fs.s3a.endpoint` to the MinIO service endpoint and reads the data
2. **Given** a MinIO-backed table, **When** writing data via Spark, **Then** temporary credentials are automatically renewed if expired and data is written successfully
3. **Given** both AWS S3 and MinIO tables in the same Spark session, **When** querying tables from different storage backends, **Then** Spark correctly applies the appropriate endpoint configuration for each storage location

---

### User Story 4 - Deploy with Helm Chart (Priority: P2)

DevOps teams need to deploy Unity Catalog in Kubernetes with MinIO configuration managed through Helm values, ensuring deployment parity with local/direct deployment methods.

**Why this priority**: Kubernetes deployments are common in production. Without Helm support, adopters cannot use S3-compatible storage in containerized environments.

**Independent Test**: Can be tested by deploying Unity Catalog via Helm with MinIO credentials in values.yaml, verifying the server starts correctly and MinIO endpoints are accessible.

**Acceptance Scenarios**:

1. **Given** Helm values with MinIO storage configuration, **When** deploying Unity Catalog, **Then** the server pod starts with correct MinIO service endpoint environment variables
2. **Given** MinIO credentials stored in Kubernetes secrets, **When** the Helm chart is applied, **Then** credentials are mounted securely and not exposed in logs or config maps
3. **Given** an existing Helm deployment using only AWS S3, **When** adding MinIO configuration to values.yaml, **Then** the upgrade succeeds and both storage backends are available

---

### User Story 5 - Support Multiple S3-Compatible Providers (Priority: P3)

Organizations need to use multiple S3-compatible storage services (MinIO, Wasabi, DigitalOcean Spaces, Cloudflare R2) simultaneously in the same Unity Catalog deployment.

**Why this priority**: Demonstrates extensibility and supports diverse infrastructure needs, but builds on established single-provider patterns.

**Independent Test**: Can be tested by configuring multiple S3-compatible endpoints (e.g., MinIO + Wasabi) with different service endpoints, creating tables on each, and verifying all are accessible.

**Acceptance Scenarios**:

1. **Given** multiple S3-compatible storage configurations with different service endpoints, **When** creating tables on different providers, **Then** each table correctly uses its provider's endpoint
2. **Given** tables distributed across multiple S3-compatible providers, **When** querying in Spark, **Then** Spark automatically applies the correct endpoint for each provider's storage location
3. **Given** different credential renewal intervals across providers, **When** accessing tables, **Then** credentials are renewed independently per provider

---

### Edge Cases

- **What happens when** MinIO service endpoint is unreachable? Unity Catalog should fail gracefully with clear error messages and not affect access to other storage backends.
- **What happens when** service_endpoint is specified but the storage location uses standard AWS S3? The AWS STS endpoint should take precedence, or the custom endpoint should be ignored for s3.amazonaws.com domains.
- **What happens when** both AWS S3 and MinIO tables exist in the same Spark session? Spark must handle multiple endpoint configurations concurrently without interference.
- **What happens when** MinIO credentials expire? The credential renewal flow should work identically to AWS S3 credential renewal.
- **What happens during** Helm chart upgrade from a non-MinIO version? Existing S3 configurations must remain functional, with MinIO support added additively.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST accept an optional `s3.serviceEndpoint.[index]` configuration attribute for each S3 storage configuration in `etc/conf/server.properties`
- **FR-002**: System MUST accept an optional `s3.serviceEndpoint.[index]` configuration attribute for each S3 storage configuration in Helm chart `values.yaml`
- **FR-003**: System MUST use the service endpoint as the STS endpoint when generating temporary credentials for S3-compatible storage
- **FR-004**: System MUST pass the service endpoint to Spark via `spark.hadoop.fs.s3a.endpoint` property when vending credentials for S3-compatible storage locations
- **FR-005**: System MUST maintain backward compatibility by treating missing `serviceEndpoint` attribute as standard AWS S3 (using default AWS STS endpoint)
- **FR-006**: System MUST support multiple concurrent S3 storage configurations, each with its own optional service endpoint
- **FR-007**: Spark connector MUST correctly apply the service endpoint when accessing tables stored in S3-compatible storage
- **FR-008**: Temporary credential generation MUST follow the existing credential vending flow architecture without breaking changes
- **FR-009**: System MUST validate service endpoint URL format and fail with clear error messages for invalid endpoints
- **FR-010**: System MUST allow service endpoint override via environment variables following existing pattern (`S3_SERVICE_ENDPOINT_0`, `S3_SERVICE_ENDPOINT_1`, etc.)

### API Specification Requirements (Unity Catalog Specific)

- **API-001**: TemporaryCredentials API response MUST remain backward compatible (no breaking changes to response schema)
- **API-002**: S3 credential vending logic MUST be extended internally without modifying public API contracts
- **API-003**: Any new attributes added to TemporaryCredentials MUST be optional with sensible defaults

### Key Entities

- **S3StorageConfig**: Extended to include optional `serviceEndpoint` field representing the S3-compatible service's STS/API endpoint URL
- **TemporaryCredentials**: Extended to optionally include service endpoint information for S3A filesystem configuration
- **CredentialsGenerator**: Modified to support custom STS endpoints when generating credentials

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Unity Catalog successfully connects to MinIO storage and performs table create/read/write operations
- **SC-002**: Spark applications can query tables stored in MinIO through Unity Catalog Spark connector without manual endpoint configuration
- **SC-003**: Temporary credentials generated for MinIO storage locations work identically to AWS S3 credentials
- **SC-004**: System supports at least 3 different S3-compatible storage providers (AWS S3, MinIO, and one other like Wasabi) concurrently in the same deployment
- **SC-005**: Configuration via `server.properties` and Helm chart `values.yaml` remain synchronized for S3-compatible storage attributes
- **SC-006**: Existing AWS S3 deployments upgrade to the new version without configuration changes or functionality regression
- **SC-007**: Credential renewal works correctly for S3-compatible storage following the same patterns as AWS S3
- **SC-008**: System handles mixed storage configurations (AWS S3 + MinIO) in a single Spark session without endpoint conflicts

## Assumptions

- MinIO and other S3-compatible storage services implement S3-compatible APIs at the object storage level (GET/PUT/DELETE objects)
- MinIO may not support full AWS STS AssumeRole with policy-based downscoping; Unity Catalog will document limitations and potentially use static credentials or service account credentials when STS is not available
- The existing `AwsCredentialVendor` and `StsCredentialsGenerator` can be extended to support custom STS endpoints
- Spark's S3A filesystem connector supports custom endpoint configuration via `fs.s3a.endpoint` property
- Environment variable naming pattern (`S3_SERVICE_ENDPOINT_[index]`) follows existing conventions for S3 configuration
- Service endpoint URLs will be in the format `http://hostname:port` or `https://hostname:port`
- The feature does not require changes to the Unity Catalog API specification (`api/all.yaml`) as endpoint configuration is internal

## Non-Functional Requirements

- **NFR-001**: Configuration validation MUST occur at server startup with clear error messages for misconfigured service endpoints
- **NFR-002**: Service endpoint configuration MUST NOT expose sensitive credentials in logs or error messages
- **NFR-003**: Performance of credential vending for S3-compatible storage MUST be comparable to AWS S3 (within 10% latency)
- **NFR-004**: Documentation MUST clearly explain supported S3-compatible storage providers and any known limitations (e.g., STS policy scoping)
- **NFR-005**: Helm chart upgrades MUST not require downtime for services only using AWS S3

## Out of Scope

- Support for S3-compatible storage providers that do not implement S3 API semantics
- Automatic detection of S3-compatible storage vs. AWS S3 (must be explicitly configured)
- Migration tools to move existing tables between AWS S3 and S3-compatible storage
- Custom credential providers beyond STS and static credentials
- Changes to the public Unity Catalog REST API specification
