# Unity Catalog Roadmap

This document outlines the roadmap for the Unity Catalog project. As always,
features may move in/out of milestones pending available resources, priorities, and community discussions.

## Release priorities

### 0.8: S3-compatible storage, identity, and authorization

We plan to add S3-compatible external locations with scoped, short-lived credential vending,
GCP storage, and modular OAuth/OIDC integration.
Identity and authorization work includes groups and group grants, ownership transfer,
BROWSE, MANAGE, READ METADATA, WRITE VOLUME, CREATE MODEL VERSION, and consistent
authorization checks across supported operations. We also plan to add pluggable
authorization with OPA and refresh the catalog browsing and editing UI.

### Beyond 0.8: Catalog interoperability, production operation, and migration

We plan to add Iceberg REST view lifecycle support, service principals, tags on catalogs,
schemas and tables, Open Sharing server integration, and lineage integration with OpenLineage.
We also plan to add Hive Metastore bulk migration tooling, register/unregister APIs for tables,
and Azure storage. Production work includes
deployment and upgrade guidance, backend database and upgrade testing, and validation of
multi-instance operation, recovery and scale.

> **Note:** The projected roadmap is tentative and may change. For details of what shipped in each release, see the [Unity Catalog release notes](https://github.com/unitycatalog/unitycatalog/releases).

## Full roadmap

The matrix is cumulative. Checkmarks in future columns indicate planned scope.
Releases through 0.5 are consolidated; 0.6, 0.7, and 0.8 remain separate.
Beyond 0.8 combines later plans and unassigned features without assigning a release.

| Feature | Area | Through v0.5 | v0.6 | v0.7 | v0.8 (planned) | Beyond 0.8 |
| --- | --- | :---: | :---: | :---: | :---: | :---: |
| **Core** |  |  |  |  |  |  |
| Catalog | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Schema | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed location in catalog | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed location in schema | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Credential | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| External Location | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| AWS storage | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| GCP storage | API + Server | _ | _ | _ | ✓ | ✓ |
| Azure storage | API + Server | _ | _ | _ | _ | ✓ |
| Multi-tenancy | API + Server | _ | _ | _ | _ | ✓ |
| S3-compatible storage | API + Server | _ | _ | _ | ✓ | ✓ |
| Refreshed catalog UI | UI | _ | _ | _ | ✓ | ✓ |
| **Identity & Authentication** |  |  |  |  |  |  |
| Local identity management (user) | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Group management | API + Server | _ | _ | _ | ✓ | ✓ |
| Support for Machine identities (SPs) | API + Server | _ | _ | _ | _ | ✓ |
| SCIM user provisioning | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| SCIM group provisioning | API + Server | _ | _ | _ | _ | _ |
| OAuth/OIDC for Users | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| OAuth/OIDC for Services | API + Server | _ | _ | _ | _ | ✓ |
| OAuth client-side support | Spark integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| SAML authentication support | API + Server | _ | _ | _ | _ | _ |
| Pluggable authorization / OPA | API + Server | _ | _ | _ | ✓ | ✓ |
| **Access Control & Governance** |  |  |  |  |  |  |
| Support for change of ownership | API + Server | _ | _ | _ | ✓ | ✓ |
| MODIFY and core CREATE_\* privileges | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| BROWSE | API + Server | _ | _ | _ | ✓ | ✓ |
| Expanded privileges | API + Server | _ | _ | _ | ✓ | ✓ |
| Temporary credential vending for tables | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Temporary credential vending for volumes | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Temporary credential vending for models | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Basic grants | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Auditing | API + Server | _ | _ | _ | _ | ✓ |
| SQL DCL changes | Spark Integration | _ | _ | _ | _ | _ |
| RBAC | API + Server | _ | _ | _ | _ | _ |
| Row level filters | API + Server | _ | _ | _ | _ | _ |
| Column level masks | API + Server | _ | _ | _ | _ | _ |
| ABAC | API + Server | _ | _ | _ | _ | _ |
| Lineage integration with OpenLineage | API + Server | _ | _ | _ | _ | ✓ |
| **Production operation and migration** |  |  |  |  |  |  |
| Monitoring and Telemetry | API + Server | _ | _ | ✓ | ✓ | ✓ |
| Database schema upgrades | API + Server | _ | _ | _ | _ | ✓ |
| Change events | API + Server | _ | _ | _ | _ | _ |
| Server-side storage credential and FileIO caching | API + Server | _ | _ | _ | _ | ✓ |
| Driver-side credential caching across queries | Hadoop / Spark integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed tables and volumes lifecycle | API + Server | _ | _ | ✓ | ✓ | ✓ |
| Production operation and upgrade validation | Server / deployment | _ | _ | _ | _ | ✓ |
| Hive Metastore bulk migration tooling | Migration tooling | _ | _ | _ | _ | ✓ |
| **Tables** |  |  |  |  |  |  |
| External table reads & writes | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| External table reads & writes | Spark integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| External table reads & writes | Delta integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Register/unregister APIs for tables | API + Server | _ | _ | _ | _ | ✓ |
| Managed Delta table reads | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed Delta table reads | Delta+Spark integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed Delta tables creates+writes with catalog-managed commits | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed Delta tables creates+writes with catalog-managed commits | Delta-Spark integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed Delta tables creates+writes with catalog-managed commits | Delta Kernel integration | _ | _ | _ | _ | ✓ |
| Delta Uniform tables with read as Iceberg via Iceberg REST API | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Delta Uniform tables with read as Iceberg via Iceberg REST API | Delta integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Iceberg tables with create+read+write | API + Server | _ | _ | ✓ | ✓ | ✓ |
| Multi-engine data types for column definitions | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Streaming tables | API + Server | _ | _ | _ | _ | _ |
| Shallow clones | API + Server | _ | _ | _ | _ | ✓ |
| **Views** |  |  |  |  |  |  |
| Basic Spark SQL flavor views | API + Server | _ | ✓ | ✓ | ✓ | ✓ |
| Multi-dialect views | API + Server | _ | _ | _ | _ | ✓ |
| Iceberg view support | API + Server | _ | _ | _ | _ | ✓ |
| Materialized views | API + Server | _ | _ | _ | _ | _ |
| Metric views | API + Server | _ | ✓ | ✓ | ✓ | ✓ |
| **Non-tabular and AI assets** |  |  |  |  |  |  |
| Skills | API + Server | _ | _ | _ | _ | ✓ |
| Object tags | API + Server | _ | _ | _ | _ | ✓ |
| Functions (SQL UDFs, Python UDFs) | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Functions (SQL UDFs, Python UDFs) | ML integrations with advanced python SDK | ✓ | ✓ | ✓ | ✓ | ✓ |
| Functions (SQL UDFs, Python UDFs) | Spark integration | _ | _ | _ | _ | ✓ |
| Multi-engine functions (SQL) | API + Server | _ | _ | _ | _ | _ |
| Remote functions | API + Server | _ | _ | _ | _ | _ |
| External volumes | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| External volumes | Spark integration | _ | _ | _ | _ | ✓ |
| Managed volumes | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Managed volumes | Spark integration | _ | _ | _ | _ | ✓ |
| Models and model versions | API + Server | ✓ | ✓ | ✓ | ✓ | ✓ |
| Models and model versions | MLflow integration | ✓ | ✓ | ✓ | ✓ | ✓ |
| Models and model versions | Spark integration | _ | _ | _ | _ | ✓ |
| Features tables | API + Server | _ | _ | _ | _ | _ |
| Data monitors | API + Server | _ | _ | _ | _ | _ |
| **Sharing** |  |  |  |  |  |  |
| Open Sharing | API + Server | _ | _ | _ | _ | ✓ |
| Shares | API + Server | _ | _ | _ | _ | ✓ |
| Recipients | API + Server | _ | _ | _ | _ | ✓ |
| Providers | API + Server | _ | _ | _ | _ | ✓ |
| **Federation** |  |  |  |  |  |  |
| Connections | API + Server | _ | _ | _ | _ | _ |
| Foreign objects (catalogs, schemas, tables) | API + Server | _ | _ | _ | _ | _ |
| Support for different data sources: JDBC, Iceberg REST, HMS | API + Server | _ | _ | _ | _ | ✓ |
