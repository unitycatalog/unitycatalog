# Unity Catalog Roadmap

This document outlines the roadmap for the Unity Catalog open source project. As always,
features may move in/out of milestones pending available resources, priorities, and community discussions.

## 0.6+ Release priorities

### View support

Views provide governed, reusable definitions over tables.
We will start with supporting basic Spark SQL views and then progress toward broader engine interoperability.

### Full Iceberg REST Catalog support

The Iceberg REST Catalog provides an open interface for engines and tools to discover and access Iceberg tables. We will continue to strengthen interoperability through this interface, including Delta Uniform table access and Iceberg table lifecycle support, so more engines can access data registered in Unity Catalog through open standards.

### S3-compatible storage

S3-compatible storage support will allow Unity Catalog deployments to use object stores that expose an S3-compatible API. This allows Unity Catalog to be deployed across a broader range of on-prem and cloud environments.

### Production hardening

Production hardening focuses on the reliability and operational maturity needed for broader production deployments.
The roadmap includes monitoring and telemetry, database schema upgrades, server-side storage credential and FileIO caching, and managed table and volume lifecycle support.


> **Note:** The projected roadmap is tentative and may change. For details of what shipped in each release, see the [Unity Catalog release notes](https://github.com/unitycatalog/unitycatalog/releases).

## Full roadmap

<table>
  <tr>
    <td><b>Feature</b></td>
    <td><b>Area</b></td>
    <td><b>v0.3</b></td>
    <td><b>v0.4</b></td>
    <td><b>v0.5</b></td>
    <td><b>v0.6</b></td>
    <td><b>v0.7</b></td>
    <td><b>v0.8+</b></td>
  </tr>
  <!-- Core Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Core</td>
  </tr>
  <tr>
    <td>Catalog</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Managed location in catalog</td>
    <td>API + Server</td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Managed location in schema</td>
    <td>API + Server</td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Credential</td>
    <td>API + Server</td>
    <td align="center">🛠️</td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>External Location</td>
    <td>API + Server</td>
    <td align="center">🛠️</td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Multi-tenancy</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
<tr>
    <td>S3-compatible storage</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <!-- Identity & Authentication Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Identity & Authentication</td>
  </tr>
  <tr>
    <td>Local identity management (user)</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>SCIM to support identity sync from IdP  (users and groups)</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>OAuth/OIDC for Users</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>OAuth client-side support</td>
    <td>Spark integration</td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>SAML authentication support</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <!-- Access Control & Governance Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Access Control & Governance</td>
  </tr>
    <tr>
    <td>Support for change of ownership</td>
    <td>API + Server</td>
    <td></td>
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
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for tables</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for volumes</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Temporary credential vending for models</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Basic grants</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>RBAC</td>
    <td>API + Server</td>
    <td></td>
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
    <td></td>
    <td align="center">❓</td>
  </tr>
  <!-- Server production-readiness (support running as a HMS replacement) Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Server production-readiness (support running as a HMS replacement)</td>
  </tr>
    <tr><td>Monitoring and Telemetry</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr><td>Database schema upgrades</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Change events</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Server-side storage credential and FileIO caching</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Managed tables and volumes lifecycle</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <!-- Tables Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Tables</td>
  </tr>
  <tr>
    <td rowspan="3">External table reads & writes</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
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
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta+Spark integration</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr><td rowspan="3">Managed Delta tables creates+writes with catalog-managed commits</td>
    <td>API + Server</td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta-Spark integration</td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta Kernel integration</td>
    <td></td>
    <td align="center" bgcolor="green"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td rowspan="2">Delta Uniform tables with read as Iceberg via Iceberg REST API</td>
    <td>API + Server</td>
    <td align="center">🛠️</td>
    <td align="center">🛠️</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Delta integration</td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr><td>Iceberg tables with create+read+write</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Multi-engine data types for column definitions</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <!-- Views Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Views</td>
  </tr>
    <tr>
    <td>Basic Spark SQL flavor views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Multi-dialect views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Iceberg view support</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Materialized views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
    <tr><td>Metric views</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Streaming tables</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
    <tr>
    <td>Shallow clones</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <!-- Non-tabular and AI assets Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Non-tabular and AI assets</td>
  </tr>
  <tr>
    <td rowspan="3">Functions (SQL UDFs, Python UDFs)</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>ML integrations with advanced python SDK</td>
    <td></td>
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
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Multi-engine functions (SQL)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Remote functions</td>
    <td>API + Server</td>
    <td></td>
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
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
    <tr>
    <td>Spark integration</td>
    <td></td>
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
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td rowspan="3">Models and model versions</td>
    <td>API + Server</td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>MLflow integration</td>
    <td></td>
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
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <tr>
    <td>Features tables</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Data monitors</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <!-- Sharing Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Sharing</td>
  </tr>
    <tr><td>Open Sharing</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
      <tr>
    <td>Shares</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
      <tr>
    <td>Recipients</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
      <tr>
    <td>Providers</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
  <!-- Federation Section -->
  <tr>
    <td colspan="8" bgcolor="grey" align="center">Federation</td>
  </tr>
  <tr>
    <td>Connections</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
  <tr>
    <td>Foreign objects (catalogs, schemas, tables)</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center">❓</td>
  </tr>
    <tr>
    <td>Support for different data sources: JDBC, Iceberg REST, HMS</td>
    <td>API + Server</td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td></td>
    <td align="center"><img src="https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/ApprovedChanges.svg" alt="done"/></td>
  </tr>
</table>
