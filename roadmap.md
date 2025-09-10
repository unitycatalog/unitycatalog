# Unity Catalog Roadmap

This document outlines the roadmap for the Unity Catalog open source project. As always,
features may move in/out of milestones pending available resources and priorities.

## 0.4 Release priorities

### Storage management

By more tightly integrating the already released credential and external locations API with the rest of the server
internals, the next release will allow for more fine-grained, dynamic, and online management of storage locations and their
credentials. Furthermore operators can delegate some storage management to the catalog via the managed locations
for catalogs and schemas features.

### Catalog managed commits

Catalog managed commits are the basis for many new and powerful client (Delta) and server side features.
Supporting the table scan and commit APIs is a key priority for the upcoming release.

### End to end OAuth support

OAuth support is important for cloud users and RBAC, and unity client plans to support common OAuth flows
for authentication.

## Full roadmap

<table>
  <tr>
    <td><b>Feature</b></td>
    <td><b>Area</b></td>
    <td><b>v0.1</b></td>
    <td><b>v0.2</b></td>
    <td><b>v0.3</b></td>
    <td><b>v0.4</b></td>
    <td><b>v0.5+</b></td>
  </tr>
  
  <!-- Core Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Core</td>
  </tr>

  <tr>
    <td>Catalog</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Schema</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Managed location in catalog</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Managed location in schema</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Credential</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td align="center">🛠️</td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>External Location</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td align="center">🛠️</td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Multi-tenancy</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>

  <!-- Identity & Authentication Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Identity & Authentication</td>
  </tr>

  <tr>
    <td>Local identity management (user)</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Group management</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Support for Machine identities (SPs)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>SCIM to support identity sync from IdP  (users and groups)</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>OAuth/OIDC for Users</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>OAuth/OIDC for Services</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>OAuth client-side support</td>
    <td>Spark integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>SAML authentication support</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>

  <!-- Access Control & Governance Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Access Control & Governance</td>
  </tr>

  <tr>
    <td>Support for change of ownership</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Add permission/privilege support for MODIFY, CREATE_X, BROWSE</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Add remaining permissions/privileges (MANAGE etc)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Permission parity with Databricks UC</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for tables</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for volumes</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for models</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Basic grants</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Auditing</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>SQL DCL changes</td>
    <td>Spark Integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>RBAC</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Row level filters</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Column level masks</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>ABAC</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Lineage</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <!-- Server production-readiness (support running as a HMS replacement) Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Server production-readiness (support running as a HMS replacement)</td>
  </tr>

  <tr>
    <td>Monitoring and Telemetry</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Database schema upgrades</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Change events</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <!-- Tables Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Tables</td>
  </tr>
  
  <tr>
    <td rowspan="3">External table reads & writes</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="2">Managed Delta table reads</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta+Spark integration</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="3">Managed Delta tables creates+writes with catalog-managed commits</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta-Spark integration</td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta Kernel integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="2">Delta Uniform tables with read as Iceberg via Iceberg REST API</td>
    <td>API + Server</td>
    <td align="center">🛠️</td>
    <td align="center">🛠️</td>
    <td align="center">🛠️</td>
    <td align="center">🛠️</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Iceberg tables with create+read+write</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Multi-engine data types for column definitions</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <!-- Views Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Views</td>
  </tr>
  
  <tr>
    <td rowspan="1">Basic Spark SQL flavor views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Multi-dialect views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Iceberg view support</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Materialized views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">🛠️</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Streaming tables</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">🛠️</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Shallow clones</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <!-- Non-tabular and AI assets Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Non-tabular and AI assets</td>
  </tr>
  
  <tr>
    <td rowspan="3">Functions (SQL UDFs, Python UDFs)</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>ML integrations with advanced python SDK</td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Multi-engine functions (SQL)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Remote functions</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="2">External volumes</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="2">Managed volumes</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="3">Models and model versions</td>
    <td>API + Server</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>MLflow integration</td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Spark integration</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  
  <tr>
    <td rowspan="1">Features tables</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Data monitors</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <!-- Sharing Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Sharing</td>
  </tr>
  
  <tr>
    <td rowspan="1">Delta Sharing integration</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Shares</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Recipients</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Providers</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <!-- Federation Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">Federation</td>
  </tr>
  
  <tr>
    <td rowspan="1">Connections</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Foreign objects (catalogs, schemas, tables)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <tr>
    <td rowspan="1">Support for different data sources: JDBC, Iceberg REST, HMS</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  
  <!-- UI Section -->
  <tr>
    <td colspan="7" bgcolor="grey" align="center">UI (needs to be completed)</td>
  </tr>
</table>