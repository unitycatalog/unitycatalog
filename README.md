<img src="./docs/assets/images/uc-logo.png" width="600px" />

# Unity Catalog: Open, Multimodal Catalog for Data & AI

Unity Catalog is the industry’s only universal catalog for data and AI.

- **Multimodal interface supports any format, engine, and asset**
  - Multi-format support: It is extensible and supports Delta Lake, Apache Iceberg and Apache Hudi via UniForm, Apache Parquet, JSON, CSV, and many others.
  - Multi-engine support: With its open APIs, data cataloged in Unity can be read by many leading compute engines.
  - Multimodal: It supports all your data and AI assets, including tables, files, functions, AI models.
- **Open source API and implementation** - OpenAPI spec and OSS implementation (Apache 2.0 license). It is also compatible with Apache Hive's metastore API and Apache Iceberg's REST catalog API. Unity Catalog is currently a sandbox project with LF AI and Data Foundation (part of the Linux Foundation).
- **Unified governance** for data and AI - Govern and secure tabular data, unstructured assets, and AI assets with a single interface.

The first release of Unity Catalog focuses on a core set of APIs for tables, unstructured data, and AI assets - with more to come soon on governance, access, and client interoperability. This is just the beginning!

![UC Hero Image](./docs/assets/images/uc.png)

### Vibrant ecosystem

This is a community effort. Unity Catalog is supported by

- [Amazon Web Services](https://aws.amazon.com/)
- [Confluent](https://www.confluent.io/)
- [Daft (Eventual)](https://github.com/Eventual-Inc/Daft)
- [dbt Labs](https://www.getdbt.com/)
- [DuckDB](https://duckdblabs.com/)
- [Fivetran](https://www.fivetran.com/)
- [Google Cloud](https://cloud.google.com/)
- [Granica](https://granica.ai/)
- [Immuta](https://www.immuta.com/)
- [Informatica](https://www.informatica.com/)
- [LanceDB](https://lancedb.com/)
- [LangChain](https://www.langchain.com/)
- [LlamaIndex](https://www.llamaindex.ai/)
- [Microsoft Azure](https://azure.microsoft.com)
- [NVIDIA](https://www.nvidia.com/)
- [Onehouse](https://www.onehouse.ai/)
- [PuppyGraph](https://www.puppygraph.com/)
- [Salesforce](https://www.salesforce.com/)
- [StarRocks (CelerData)](https://celerdata.com/)
- [Spice AI](https://github.com/spiceai/spiceai)
- [Tecton](https://www.tecton.ai/)
- [Unstructured](https://unstructured.io/)

Unity Catalog is proud to be hosted by the LF AI & Data Foundation.

<a href="https://lfaidata.foundation/projects">
  <img src="./docs/assets/images/lfaidata-project-badge-sandbox-color.png" width="200px" />
</a>

## Quickstart - Hello UC!

Let's take Unity Catalog for spin. In this guide, we are going to do the following:

- In one terminal, run the UC server.
- In another terminal, we will explore the contents of the UC server using a CLI.
  An example project is provided to demonstrate how to use the UC SDK for various assets
  as well as provide a convenient way to explore the content of any UC server implementation.

> If you prefer to run Unity Catalog in Docker use `docker
> compose up`. See the [Docker Compose docs](./docs/docker_compose.md) for more details.

### Prerequisites

You have to ensure that your local environment has the following:

- Clone this repository.
- Ensure the `JAVA_HOME` environment variable your terminal is configured to point to JDK17.
- Compile the project using `build/sbt package`


### Run the UC Server

In a terminal, in the cloned repository root directory, start the UC server.

```sh
bin/start-uc-server
```

For the remaining steps, continue in a different terminal.

### Operate on Delta tables with the CLI

Let's list the tables.

```sh
bin/uc table list --catalog unity --schema default
```

You should see a few tables. Some details are truncated because of the nested nature of the data.
To see all the content, you can add `--output jsonPretty` to any command.

Next, let's get the metadata of one of those tables.

```sh
bin/uc table get --full_name unity.default.numbers
```

You can see that it is a Delta table. Now, specifically for Delta tables, this CLI can
print a snippet of the contents of a Delta table (powered by the [Delta Kernel Java](https://delta.io/blog/delta-kernel/) project).
Let's try that.

```sh
bin/uc table read --full_name unity.default.numbers
```

### Operate on Delta tables with DuckDB

For operating on tables with DuckDB, you will have to [install it](https://duckdb.org/docs/installation/) (version 1.0).
Let's start DuckDB and install a couple of extensions. To start DuckDB, run the command `duckdb` in the terminal.
Then, in the DuckDB shell, run the following commands:

```sql
install uc_catalog from core_nightly;
load uc_catalog;
install delta;
load delta;
```

If you have installed these extensions before, you may have to run `update extensions` and restart DuckDB
for the following steps to work.

Now that we have DuckDB all set up, let's try connecting to UC by specifying a secret.

```sql
CREATE SECRET (
      TYPE UC,
      TOKEN 'not-used',
      ENDPOINT 'http://127.0.0.1:8080',
      AWS_REGION 'us-east-2'
 );
```

You should see it print a short table saying `Success` = `true`. Then we attach the `unity` catalog to DuckDB.

```sql
ATTACH 'unity' AS unity (TYPE UC_CATALOG);
```

Now we are ready to query. Try the following:

```sql
SHOW ALL TABLES;
SELECT * from unity.default.numbers;
```

You should see the tables listed and the contents of the `numbers` table printed.
To quit DuckDB, press `Ctrl`+`D` (if your platform supports it), press `Ctrl`+`C`, or use the `.exit` command in the DuckDB shell.

### Interact with the Unity Catalog UI

![UC UI](./docs/assets/images/uc-ui.png)

To use the Unity Catalog UI, start a new terminal and ensure you have already started the UC server (e.g., `./bin/start-uc-server`)

**Prerequisites**
* Node: https://nodejs.org/en/download/package-manager
* Yarn: https://classic.yarnpkg.com/lang/en/docs/install

**How to start the UI through yarn**
```
cd /ui
yarn install
yarn start
```


## CLI tutorial

You can interact with a Unity Catalog server to create and manage catalogs, schemas and tables,
operate on volumes and functions from the CLI, and much more.
See the [cli usage](docs/usage/cli.md) for more details.

## APIs and Compatibility

- Open API specification: See the [Unity Catalog Rest API](https://docs.unitycatalog.io/swagger-docs/).
- Compatibility and stability: The APIs are currently evolving and should not be assumed to be stable.

## Building Unity Catalog

Unity Catalog can be built using [sbt](https://www.scala-sbt.org/).

To build UC (incl. [Spark Integration](./connectors/spark) module), run the following command:

```sh
build/sbt clean package publishLocal spark/publishLocal
```

Refer to [sbt docs](https://www.scala-sbt.org/1.x/docs/) for more commands.

## Deployment

- To create a tarball that can be used to deploy the UC server or run the CLI, run the following:
  ```sh
  build/sbt createTarball
  ```
  This will create a tarball in the `target` directory. See the full [deployment guide](docs/deployment.md) for more details.

## Compiling and testing

- Install JDK 17 by whatever mechanism is appropriate for your system, and
  set that version to be the default Java version (e.g. via the env variable `JAVA_HOME`)
- To compile all the code without running tests, run the following:
  ```sh
  build/sbt clean compile
  ```
- To compile and execute tests, run the following:
  ```sh
  build/sbt -J-Xmx2G clean test
  ```
- To execute tests with coverage, run the following:
  ```sh
  build/sbt -J-Xmx2G jacoco
  ```
- To update the API specification, just update the `api/all.yaml` and then run the following:
  ```sh
  build/sbt generate
  ```
  This will regenerate the OpenAPI data models in the UC server and data models + APIs in the client SDK.
- To format the code, run the following:
  ```sh
  build/sbt javafmtAll
  ```

## Setting up IDE

IntelliJ is the recommended IDE to use when developing Unity Catalog. The below steps outline how to add the project to IntelliJ:

1. Clone Unity Catalog into a local folder, such as `~/unitycatalog`.
2. Select `File` > `New Project` > `Project from Existing Sources...` and select `~/unitycatalog`.
3. Under `Import project from external model` select `sbt`. Click `Next`.
4. Click `Finish`.

Java code adheres to the [Google style](https://google.github.io/styleguide/javaguide.html), which is verified via `build/sbt javafmtCheckAll` during builds.
In order to automatically fix Java code style issues, please use `build/sbt javafmtAll`.

### Configuring Code Formatter for Eclipse/IntelliJ

Follow the instructions for [Eclipse](https://github.com/google/google-java-format#eclipse) or
[IntelliJ](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides) to install the **google-java-format** plugin (note the required manual actions for IntelliJ).

### Using more recent JDKs

The build script [checks for a lower bound on the JDK](./build.sbt#L14) but the [current SBT version](./project/build.properties)
imposes an upper bound. Please check the [JDK compatibility](https://docs.scala-lang.org/overviews/jdk-compatibility/overview.html) documentation for more information

### Serving the documentation with mkdocs

For an overview of how to contribute to the documentation, please see our introduction [here](./docs/README.md).
For the official documentation, please take a look at [https://docs.unitycatalog.io/](https://docs.unitycatalog.io/).
