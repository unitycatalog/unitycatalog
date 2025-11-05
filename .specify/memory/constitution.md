<!--
Sync Impact Report:
- Version change: none → 1.0.0 (Initial constitution)
- Modified principles: N/A (Initial version)
- Added sections: All sections are new (7 Core Principles + 3 Supporting Sections)
- Removed sections: N/A
- Templates requiring updates:
  ✅ plan-template.md - Added Constitution Check section with Unity Catalog-specific compliance gates
  ✅ spec-template.md - Added API Specification Requirements section for Unity Catalog
  ✅ tasks-template.md - Added Unity Catalog Constitution Compliance Tasks in foundational phase and quality gates in polish phase
  ✅ checklist-template.md - Added Unity Catalog Constitution Compliance checklist items
  ✅ agent-file-template.md - Added Java code style standards and build commands
  ✅ .gitignore - Added *.local and etc/conf/server.properties.local patterns
- Follow-up TODOs: None
-->

# Unity Catalog Constitution

## Core Principles

### I. API Specification First (NON-NEGOTIABLE)

Unity Catalog is fundamentally an **OpenAPI specification project**. All implementations (server, clients, connectors) are realizations of the API specification defined in `api/all.yaml`.

**Rules:**
- The API specification (`api/all.yaml`) is the **single source of truth** for all Unity Catalog functionality
- All new features MUST begin with API specification updates in `api/all.yaml`
- API changes MUST be generated into server models and client SDKs via `build/sbt generate` before implementation
- No feature implementation may deviate from the API specification
- All API endpoints, models, and behaviors MUST be documented in the OpenAPI spec

**Rationale:** Unity Catalog's value proposition is its open, standardized API that enables multi-engine, multi-format data catalog interoperability. The specification ensures consistency across all implementations and enables the vibrant ecosystem of integrations.

### II. Backward Compatibility (NON-NEGOTIABLE)

Unity Catalog is a foundational data infrastructure component used by enterprise production systems. Breaking changes disrupt the entire ecosystem.

**Rules:**
- API changes MUST maintain backward compatibility with existing clients
- New attributes in API models MUST be optional with sensible defaults
- API changes MUST be additive wherever possible (new endpoints, new optional fields)
- Deprecation of APIs requires advance notice, migration path documentation, and extended support period
- Version compatibility MUST be maintained according to semantic versioning principles
- Existing functionality MUST continue to work unchanged when new features are added

**Rationale:** Enterprise data systems depend on Unity Catalog for critical operations. Breaking changes create cascading failures across dozens of integrated systems and tools. The community-driven nature of the project means changes affect a large, diverse user base.

### III. Avoid Table Schema Modifications for New Features

Table schemas and data models represent persistent state. Schema changes are expensive, risky, and difficult to roll back.

**Rules:**
- New functionality SHOULD NOT require modifications to existing table/model properties
- Prefer new optional attributes over modifying existing ones
- Prefer new tables/models over expanding existing schemas when adding distinct capabilities
- Schema changes MUST include migration scripts and rollback procedures
- Properties added to core entities (Catalog, Schema, Table, Volume, Function, Model) require architectural review

**Rationale:** Schema migrations in production catalog systems are high-risk operations. They require downtime, coordination across environments, and cannot be easily reversed. Avoiding unnecessary schema changes reduces operational risk and maintains system stability.

### IV. Security: Zero Secrets in Version Control

Configuration files containing authentication credentials MUST NEVER be committed to version control.

**Rules:**
- Files `etc/conf/server.properties` and `ui/.env` MUST NOT be committed with real secrets
- Real secrets MUST be stored in `.local` files: `etc/conf/server.properties.local` or `ui/.env.local`
- New `.local` files MUST be added to `.gitignore` when created
- Template/example config files MAY contain placeholder or empty values
- Pre-commit hooks or CI checks SHOULD validate that secret files are not committed
- Documentation MUST clearly indicate which files should contain real secrets and which are templates

**Rationale:** Public repositories with committed secrets create severe security vulnerabilities. Credential exposure leads to unauthorized access, data breaches, and compromised systems. The `.local` pattern provides a clear separation between templates and actual credentials.

### V. Code Quality and Standards Compliance

Unity Catalog follows established Java coding standards to ensure consistency, readability, and maintainability across the large codebase.

**Rules:**
- All Java code MUST comply with Google Java Style Guide as enforced by Checkstyle (`dev/checkstyle-config.xml`)
- Code formatting MUST pass `build/sbt javafmtCheckAll` before commit
- Use `build/sbt javafmtAll` to automatically fix formatting issues
- Checkstyle violations MUST be resolved or explicitly suppressed with documented justification
- Code reviews MUST verify style compliance
- IDE configurations SHOULD be set up to use google-java-format plugin

**Rationale:** Consistent code style improves collaboration in the large, multi-contributor open source project. Automated style enforcement reduces review friction and maintains code quality as the project scales.

### VI. Build Integrity

The project MUST always build successfully on the main branch. Broken builds block all contributors.

**Rules:**
- After completing any implementation task that modifies code, MUST verify successful build with `build/sbt compile`
- All tests MUST pass before merging: `build/sbt test`
- Pull requests MUST include build verification in CI pipeline
- Broken builds on main branch are **P0 incidents** requiring immediate resolution
- Contributors MUST test locally before pushing changes
- Build failures MUST be investigated and resolved, not bypassed

**Rationale:** A broken build halts all development work and prevents releases. Ensuring build integrity is fundamental to project velocity and reliability. With many contributors, disciplined build hygiene is essential.

### VII. Helm Chart Synchronization

The Helm chart deployment configuration MUST stay synchronized with application configuration files to ensure deployment parity.

**Rules:**
- Any attribute added to `etc/conf/server.properties` MUST be reflected in `helm/values.yaml` and `helm/templates/`
- Any environment variable added to `ui/.env` MUST be reflected in `helm/templates/ui/deployment.yaml`
- Helm chart updates MUST maintain the same structure and naming conventions as application configs
- Configuration changes MUST include both application config and Helm chart updates in the same PR
- Documentation MUST indicate how to configure values in both direct deployment and Helm deployment

**Rationale:** Kubernetes/Helm deployments are increasingly common in enterprise environments. Configuration drift between local/direct deployment and Helm deployment creates deployment failures and operational issues. Maintaining parity ensures all deployment methods work correctly.

## Development Workflow

### Pull Request Requirements

All code contributions MUST:
1. Reference an issue number (created via GitHub Issues)
2. Include description of changes and rationale
3. Pass all automated checks (build, tests, style, licenses)
4. Maintain or improve test coverage for modified code
5. Update relevant documentation (API docs, README, usage guides)
6. Include Helm chart updates if configuration changes are made
7. Be reviewed and approved by at least one maintainer

### Major Feature Process

Major features (>100 LOC or user-facing behavior changes) MUST:
1. Discuss proposal in GitHub Issues or Slack before implementation
2. Reach consensus on design approach with maintainers
3. Include design document when architectural changes are involved
4. Consider impact on backward compatibility
5. Update API specification first, then implement

### Testing Requirements

- Unit tests for business logic and utilities
- Integration tests for API endpoints and multi-component workflows
- Contract tests when API specifications change
- Test naming MUST clearly indicate what is being tested
- Edge cases and error conditions MUST be tested
- Tests MUST be independent and deterministic

### API Specification Updates

When modifying `api/all.yaml`:
1. Follow OpenAPI 3.0 specification standards
2. Document all new endpoints, models, and fields with descriptions
3. Run `build/sbt generate` to regenerate models and client code
4. Verify generated code compiles without errors
5. Update API documentation in `api/README.md` and `api/Apis/` or `api/Models/` as needed
6. Test with at least one client (CLI, Python client, or Spark connector)

## Quality Gates

Before merging any pull request:
- ✅ `build/sbt compile` succeeds
- ✅ `build/sbt test` succeeds
- ✅ `build/sbt javafmtCheckAll` passes
- ✅ `build/sbt checkstyle` passes (if applicable)
- ✅ No secrets committed in config files
- ✅ Helm charts updated if config changed
- ✅ API specification valid if modified
- ✅ Documentation updated for user-facing changes
- ✅ Backward compatibility maintained

## Technology Constraints

- **Java Version**: JDK 17 required for server and most modules; JDK 11 for clients and Spark connector
- **Build System**: sbt (Scala Build Tool)
- **API Standard**: OpenAPI 3.0
- **Code Style**: Google Java Style Guide
- **License**: Apache 2.0 (all contributions must be compatible)
- **Supported Storage**: S3, ADLS, GCS, local filesystem
- **Supported Formats**: Delta Lake, Apache Iceberg (via UniForm), Apache Hudi (via UniForm), Parquet, JSON, CSV

## Governance

### Constitution Authority

This constitution defines the **non-negotiable principles and required practices** for Unity Catalog development. All contributors, maintainers, and reviewers MUST follow these principles.

### Amendment Process

Amendments to this constitution require:
1. Proposal documented in GitHub Issue with rationale
2. Discussion period (minimum 2 weeks) for community feedback
3. Approval from project maintainers
4. Version increment following semantic versioning:
   - MAJOR: Breaking governance changes (removed/redefined principles)
   - MINOR: New principles or material expansions
   - PATCH: Clarifications, typo fixes, non-semantic refinements
5. Update to dependent templates and documentation
6. Announcement to community via Slack and mailing lists

### Compliance Verification

- Pull request reviewers MUST verify compliance with constitution principles
- CI/CD pipelines SHOULD automate verification where possible
- Constitution violations MAY be raised during code review and MUST be addressed before merge
- Repeated violations MAY result in restrictions on commit access

### Version Control

Constitution amendments MUST update:
- Version number (semantic versioning)
- Last Amended date (ISO 8601 format: YYYY-MM-DD)
- Sync Impact Report (at top of file)

### Exemptions

In rare cases, exemptions may be granted for:
- Security fixes requiring immediate deployment
- Critical production incidents
- Experimental features clearly marked as such

Exemptions require documented justification and approval from project maintainers.

**Version**: 1.0.0 | **Ratified**: 2025-11-04 | **Last Amended**: 2025-11-04
