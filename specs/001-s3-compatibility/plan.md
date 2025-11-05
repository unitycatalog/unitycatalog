# Implementation Plan: S3-Compatible Storage Support

**Branch**: `001-s3-compatibility` | **Date**: 2025-11-04 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-s3-compatibility/spec.md`

**Note**: This plan extends Unity Catalog's credential vending system to support S3-compatible storage services like MinIO.

## Summary

Enable Unity Catalog to work with S3-compatible storage services (MinIO, Wasabi, DigitalOcean Spaces, Cloudflare R2) by adding optional `serviceEndpoint` configuration to the existing S3 storage configuration. This endpoint will be used for STS credential generation and passed to Spark via `spark.hadoop.fs.s3a.endpoint` for data access. The implementation extends the existing credential vending flow (`AwsCredentialVendor` → `StsCredentialsGenerator`) without breaking changes, maintaining backward compatibility with AWS S3 by treating missing endpoints as standard AWS S3.

## Technical Context

**Language/Version**: Java 17 (server/core), Java 11 (Spark connector), Scala 2.13 (Spark connector)  
**Primary Dependencies**: 
- AWS SDK for Java 2.x (STS client, S3 client)
- Spark 4.0.0 with Delta Lake 4.0.0
- Apache Hadoop S3A filesystem connector
- Micronaut framework (server)
- Lombok (builders and annotations)

**Storage**: H2 database (default), PostgreSQL (production option) - Unity Catalog metadata only; feature adds configuration for S3-compatible object storage  
**Testing**: JUnit 5, Mockito, sbt test framework  
**Target Platform**: Linux/macOS server, Kubernetes (via Helm), Spark compute clusters  
**Project Type**: Multi-module Java/Scala project (server, clients, connectors)  
**Performance Goals**: 
- Credential vending latency <200ms (comparable to AWS S3)
- Support 100+ concurrent credential requests
- No performance degradation for existing AWS S3 users

**Constraints**: 
- Must maintain backward compatibility with all existing S3 configurations
- No changes to Unity Catalog public REST API specification
- Must work with MinIO's limited STS support (may not support policy-based downscoping)
- Spark S3A filesystem must support custom endpoint configuration
- Configuration must be synchronized between server.properties and Helm charts

**Scale/Scope**: 
- Configuration: 10-20 new lines across 3 files (ServerProperties.java, S3StorageConfig.java, CredentialsGenerator.java)
- Spark integration: 10-15 lines in CredPropsUtil.java
- Helm charts: 30-40 lines in values.yaml and deployment templates
- Tests: 50-100 lines of new test coverage
- Documentation: Updates to server.properties examples, Helm values documentation

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*  
**Status**: ✅ Passed initial check (2025-11-04), ✅ Re-validated post-design (2025-11-04)

**Unity Catalog Constitution Compliance:**

- [x] **API Specification First**: Does this feature require API changes? **NO** - This is internal configuration only. No changes to `api/all.yaml` required. The `TemporaryCredentials` API response remains unchanged; endpoint information is passed via internal Spark properties.
- [x] **Backward Compatibility**: Are all API changes backward compatible? **YES** - No API changes. New `serviceEndpoint` configuration attribute is optional with default behavior (AWS S3) when omitted.
- [x] **Avoid Schema Modifications**: Does this feature avoid modifying existing table/model schemas where possible? **YES** - No database schema changes. Only extends `S3StorageConfig` Java class with new optional field.
- [x] **Zero Secrets in Version Control**: Will this feature handle secrets correctly (using `.local` files)? **YES** - Follows existing pattern. Service endpoint is a URL, not a secret. Credentials continue to use existing `.local` file pattern.
- [x] **Code Quality Standards**: Will implementation follow Google Java Style Guide and pass Checkstyle? **YES** - All code will be formatted with `build/sbt javafmtAll` and pass Checkstyle validation.
- [x] **Build Integrity**: Has plan included verification step with `build/sbt compile` after implementation? **YES** - Included in Phase 2 polish tasks and quality gates.
- [x] **Helm Chart Sync**: If config files are modified, is Helm chart update included in tasks? **YES** - Updates to both `server.properties` (s3.serviceEndpoint.[index]) and Helm chart (`values.yaml` + `deployment.yaml` templates) are included in foundational tasks.

**Justifications for any NON-NEGOTIABLE principle exceptions:**

None. This feature fully complies with all Unity Catalog constitution principles:
- **API Specification First**: Internal implementation only, no API spec changes needed
- **Backward Compatibility**: Additive changes only, existing configs continue to work
- **Avoid Schema Modifications**: No database schema changes
- **Zero Secrets**: Uses existing secret management patterns

## Project Structure

### Documentation (this feature)

```text
specs/[###-feature]/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

This is a multi-module Java/Scala project. The S3-compatibility feature touches:

```text
server/
└── src/
    ├── main/java/io/unitycatalog/server/
    │   ├── service/credential/aws/
    │   │   ├── S3StorageConfig.java              # Add serviceEndpoint field
    │   │   ├── CredentialsGenerator.java         # Support custom STS endpoint
    │   │   └── AwsCredentialVendor.java          # Pass endpoint to generator
    │   └── utils/
    │       └── ServerProperties.java             # Parse serviceEndpoint config
    └── test/java/io/unitycatalog/server/
        ├── service/credential/
        │   ├── CloudCredentialVendorTest.java    # Test with custom endpoints
        │   └── aws/
        │       └── CredentialsGeneratorTest.java # New tests for endpoint config
        └── sdk/tempcredential/
            └── SdkTemporaryTableCredentialTest.java # Integration tests

connectors/spark/
└── src/
    ├── main/scala/io/unitycatalog/spark/
    │   └── auth/
    │       └── CredPropsUtil.java                # Add fs.s3a.endpoint property
    └── test/java/io/unitycatalog/spark/
        └── auth/
            └── AwsVendedTokenProviderTest.java   # Test endpoint propagation

etc/conf/
├── server.properties                             # Example: s3.serviceEndpoint.0=
└── server.properties.local                       # Real MinIO endpoints (gitignored)

helm/
├── values.yaml                                   # Add serviceEndpoint field to S3 config
└── templates/
    └── server/
        ├── deployment.yaml                       # Map serviceEndpoint to env vars
        └── configmap.yaml                        # Template with S3_SERVICE_ENDPOINT_*

docs/
└── server/
    └── configuration.md                          # Document serviceEndpoint attribute
```

**Structure Decision**: Unity Catalog uses a multi-module sbt project structure. This feature primarily modifies the **server** module (credential vending) and **connectors/spark** module (S3A endpoint configuration). Changes are localized to:
1. **Configuration layer**: ServerProperties, S3StorageConfig
2. **Credential vending layer**: CredentialsGenerator, AwsCredentialVendor
3. **Spark integration layer**: CredPropsUtil
4. **Deployment layer**: Helm charts and config templates

## Complexity Tracking

**No constitution violations** - This section intentionally left empty as all constitution gates passed.

---

## Phase Tracking

### Phase 0: Research ✅ COMPLETE

**Objective**: Resolve all technical unknowns from Technical Context

**Output**: `research.md` with 8 research areas resolved:
1. ✅ MinIO STS support approach (AssumeRoleWithWebIdentity with static fallback)
2. ✅ AWS SDK STS client custom endpoint configuration (endpointOverride())
3. ✅ Spark S3A filesystem endpoint configuration (fs.s3a.endpoint property)
4. ✅ Configuration property naming (s3.serviceEndpoint.[index] indexed pattern)
5. ✅ Credential vending flow architecture (extend StsCredentialsGenerator)
6. ✅ Helm chart environment variable mapping (S3_SERVICE_ENDPOINT_* pattern)
7. ✅ Testing strategy (unit, integration, E2E with testcontainers)
8. ✅ Documentation requirements (server.properties, Helm values, README)

**Status**: Complete (2025-11-04)

---

### Phase 1: Design & Contracts ✅ COMPLETE

**Objective**: Define data models, API contracts, and quick-start guide

**Output**:
- ✅ `data-model.md` - S3StorageConfig extension with serviceEndpoint field
- ✅ `contracts/README.md` - Validated no API specification changes required
- ✅ `quickstart.md` - MinIO configuration examples and troubleshooting guide
- ✅ Agent context updated via `update-agent-context.sh copilot`
- ✅ Constitution Check re-validated (all gates passed)

**Key Decisions**:
- No API specification changes (internal configuration only)
- S3StorageConfig.serviceEndpoint is optional (null = AWS S3)
- Endpoint passed to Spark via fs.s3a.endpoint property
- Helm chart uses S3_SERVICE_ENDPOINT_* environment variables

**Status**: Complete (2025-11-04)

---

### Phase 2: Task Generation ✅ COMPLETE

**Objective**: Generate `tasks.md` with implementation tasks organized by user story

**Command**: `/speckit.tasks`

**Output**: `tasks.md` with:
- ✅ Phase 1: Setup tasks (4 tasks - build verification)
- ✅ Phase 2: Foundational tasks (18 tasks - S3StorageConfig, ServerProperties, CredentialsGenerator)
- ✅ Phase 3: US1 tasks (7 tasks - MinIO configuration support)
- ✅ Phase 4: US2 tasks (8 tasks - Credential vending with custom endpoint)
- ✅ Phase 5: US3 tasks (10 tasks - Spark S3A endpoint propagation)
- ✅ Phase 6: US4 tasks (9 tasks - Helm chart deployment)
- ✅ Phase 7: US5 tasks (8 tasks - Multi-provider support)
- ✅ Phase 8: Polish tasks (27 tasks - documentation, quality gates, E2E validation)

**Task Summary**:
- Total: 91 tasks
- Parallel opportunities: 49 tasks (54%)
- MVP scope: ~60 tasks (Phases 1-5 + essential polish)
- Critical path: Foundational phase (18 tasks) blocks all user stories

**Status**: Complete (2025-11-04)

---

### Implementation Phases (Post-Planning)

These phases occur AFTER `/speckit.tasks` generates the task list:

**Phase 3**: Foundational Changes (Core Configuration)  
**Phase 4**: User Story Implementation (US1-US5)  
**Phase 5**: Testing & Quality Gates  
**Phase 6**: Documentation & Polish
