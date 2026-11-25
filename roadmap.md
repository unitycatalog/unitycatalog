# Unity Catalog Roadmap

This document outlines the roadmap for the Unity Catalog project. As always,
features may move in/out of milestones pending available resources, priorities, and community discussions.

## Release priorities

### 0.8: S3-compatible storage, identity, and authorization

We plan to add S3-compatible external locations with scoped, short-lived credential vending,
GCP storage credentials and external locations, and work toward modular OAuth/OIDC integration.
Identity and authorization work includes groups and group grants, ownership transfer,
BROWSE, MANAGE, READ METADATA, WRITE VOLUME, CREATE MODEL VERSION, and consistent
authorization checks across supported operations. We also plan to add pluggable
authorization with OPA and refresh the catalog browsing and editing UI. Production
work includes database schema upgrades and server-side storage credential caching.

### Beyond 0.8: Catalog interoperability, production operation, and migration

We plan to add Iceberg REST view lifecycle support, service principals, tags on catalogs,
schemas and tables, Open Sharing server integration, and lineage integration with OpenLineage.
We also plan to add Hive Metastore bulk migration tooling, register/unregister APIs for tables,
and Azure storage credentials and external locations. Production work includes
deployment and upgrade guidance, backend database and upgrade testing, and validation of
multi-instance operation, recovery and scale.

> **Note:** The projected roadmap is tentative and may change. For details of what shipped in each release, see the [Unity Catalog release notes](https://github.com/unitycatalog/unitycatalog/releases).

## Full roadmap

The matrix is cumulative. ✅ marks included scope, — unavailable scope, and
❓ items under consideration. Future columns indicate planned scope.
Releases through 0.5 are consolidated; 0.6, 0.7, and 0.8 remain separate.
Beyond 0.8 combines later plans and unassigned features without assigning a release.

<table>
  <tr>
    <td><b>Feature</b></td>
    <td><b>Area</b></td>
    <td><b>Through v0.5</b></td>
    <td><b>v0.6</b></td>
    <td><b>v0.7</b></td>
    <td><b>v0.8 (planned)</b></td>
    <td><b>Beyond 0.8</b></td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Core</td>
  </tr>
  <tr>
    <td>Catalog</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Schema</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Managed location in catalog</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Managed location in schema</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Credential</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>External Location</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>AWS storage</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>GCP storage credentials and external locations</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Azure storage credentials and external locations</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Multi-tenancy</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>S3-compatible storage</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Refreshed catalog UI</td>
    <td>UI</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Identity &amp; Authentication</td>
  </tr>
  <tr>
    <td>Local identity management (user)</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Group management</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Support for Machine identities (SPs)</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>SCIM user provisioning</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>SCIM group provisioning</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>OAuth/OIDC for Users</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Pluggable OIDC providers (Okta, Entra ID, proxy passthrough)</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>OAuth/OIDC for Services</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>OAuth client-side support</td>
    <td>Spark integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>SAML authentication support</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Access Control &amp; Governance</td>
  </tr>
  <tr>
    <td>Support for change of ownership</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>MODIFY and core CREATE_* privileges</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>BROWSE</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Expanded privileges</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Temporary credential vending for tables</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Temporary credential vending for volumes</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Temporary credential vending for models</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Basic grants</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Pluggable authorization / OPA</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Object tags</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Auditing</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>SQL DCL changes</td>
    <td>Spark Integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>RBAC</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Row level filters</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Column level masks</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>ABAC</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Lineage integration with OpenLineage</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Production operation and migration</td>
  </tr>
  <tr>
    <td>Monitoring and Telemetry</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center" bgcolor="green">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Database schema upgrades</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Change events</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Server-side storage credential caching</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Server-side FileIO caching</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Driver-side credential caching across queries</td>
    <td>Hadoop / Spark integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Managed tables and volumes lifecycle</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center" bgcolor="green">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Production operation and upgrade validation</td>
    <td>Server / deployment</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Hive Metastore bulk migration tooling</td>
    <td>Migration tooling</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Tables</td>
  </tr>
  <tr>
    <td rowspan="3">External table reads &amp; writes</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Register/unregister APIs for tables</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="2">Managed Delta table reads</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Delta+Spark integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="3">Managed Delta tables creates+writes with catalog-managed commits</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Delta-Spark integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Delta Kernel integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="2">Delta Uniform tables with read as Iceberg via Iceberg REST API</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Iceberg tables with create+read+write</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center" bgcolor="green">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Multi-engine data types for column definitions</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Streaming tables</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Shallow clones</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Views</td>
  </tr>
  <tr>
    <td>Basic Spark SQL flavor views</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center" bgcolor="green">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Multi-dialect views</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Iceberg view support</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Materialized views</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Metric views</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center" bgcolor="green">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Non-tabular and AI assets</td>
  </tr>
  <tr>
    <td>Skills</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="3">Functions (SQL UDFs, Python UDFs)</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>ML integrations with advanced python SDK</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Multi-engine functions (SQL)</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Remote functions</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td rowspan="2">External volumes</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="2">Managed volumes</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td rowspan="3">Models and model versions</td>
    <td>API + Server</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>MLflow integration</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Features tables</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Data monitors</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Sharing</td>
  </tr>
  <tr>
    <td>Open Sharing</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Shares</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Recipients</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td>Providers</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">✅</td>
  </tr>
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Federation</td>
  </tr>
  <tr>
    <td>Connections</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Foreign objects (catalogs, schemas, tables)</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Support for different data sources: JDBC, Iceberg REST, HMS</td>
    <td>API + Server</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">—</td>
    <td align="center">❓</td>
  </tr>
</table>
