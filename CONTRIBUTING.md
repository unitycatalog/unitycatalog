We happily welcome contributions to Unity Catalog. We use [GitHub Issues](https://github.com/unitycatalog/unitycatalog/issues) to track community reported issues and [GitHub Pull Requests ](https://github.com/unitycatalog/unitycatalog/pulls) for accepting changes.

# Governance

Unity Catalog is an independent open-source project and is also a project of the [LF AI and Data Foundation](https://lfaidata.foundation/). The project under the Linux Foundation follows [open governance](https://github.com/opengovernance/opengovernance.dev), which means that there is no one company or individual in control of a project.

Unity Catalog is built on OpenAPI spec and an open source server implementation under [Apache 2.0 license](https://github.com/unitycatalog/unitycatalog/blob/main/LICENSE). It is also compatible with Apache Hive's metastore API and Apache Iceberg's REST catalog API.
This is a community effort and is supported by Amazon Web Services (AWS), Microsoft Azure, Google Cloud, Nvidia, Salesforce, DuckDB, LangChain, dbt Labs, Fivetran, Confluent, Unstructured, Onehouse, Immuta, Informatica and many more.

We are excited to work with the open source communities in the many years to come to realize this vision. You can join the Unity Catalog open source community at unitycatalog.io and the [Unity Catalog Community Slack](https://go.unitycatalog.io/slack).

# Communication

- Before starting work on a major feature, please reach out to us via [GitHub](https://github.com/unitycatalog/unitycatalog/issues), [Slack](https://unitycatalog.slack.com/), etc. We will make sure no one else is already working on it and ask you to open a GitHub issue.
- A "major feature" is defined as any change that is > 100 LOC altered (not including tests), or changes any user-facing behavior.
- We will use the GitHub issue to discuss the feature and come to agreement.
- This is to prevent your time being wasted, as well as ours.
- The GitHub review process for major features is also important so that organizations with commit access can come to agreement on design.
- If it is appropriate to write a design document, the document must be hosted either in the GitHub tracking issue, or linked to from the issue and hosted in a world-readable location.
- Specifically, if the goal is to add a new extension, please read the extension policy.
- Small patches and bug fixes don't need prior communication. If you have identified a bug and have ways to solve it, please create an [issue](https://github.com/unitycatalog/unitycatalog/issues) or create a [pull request](https://github.com/unitycatalog/unitycatalog/pulls).

# Auto-Assigning Issues
We have implemented a feature that allows users to comment "**take**" on an issue to be auto-assigned the issue without having to depend on or wait for the maintainers to assign the issue before working on it.

# Coding style

We generally follow the [Apache Spark Scala Style Guide](https://spark.apache.org/contributing.html).

# Sign your work

The sign-off is a simple line at the end of the explanation for the patch. Your signature certifies that you wrote the patch or otherwise have the right to pass it on as an open-source patch.

```
Signed-off-by: Jane Smith <jane.smith@email.com>
Use your real name (sorry, no pseudonyms or anonymous contributions.)
```
