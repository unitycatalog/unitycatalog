# Deployment

This guide outlines how to deploy the Unity Catalog server.

## Deploying using tarball

### Prerequisites

- To generate the tarball, run the following command in the source code:

    ```sh
    build/sbt createTarball
    ```

### Unpacking the tarball

- The tarball generated in the `target` directory can be unpacked using the following command:

    ```sh
    tar -xvf unitycatalog-<version>.tar.gz
    ```

- Unpacking the tarball will create the following directory structure:

    ```console
    unitycatalog-<version>
    ├── bin
    │   ├── start-uc-server
    │   └── uc
    ├── etc
    │   ├── conf
    │   ├── data
    │   ├── db
    │   └── logs
    └── jars
    ```

- The `bin` directory contains the scripts that you can use to start the UC server and run the CLI.
- The `etc` directory contains the configuration, data, database, and logs directories.
- The `jars` directory contains the jar files required to run the UC server.

### Configuring the UC server

- The UC server can be configured by modifying the files in `etc/conf/`. This includes properties related to logging,
    server environment and the s3 configuration.
- Setting the server environment to `dev` will use properties located in `etc/conf/hibernate.properties` to configure
    the backend database whereas `test` will spin up an in-memory database.
- The `etc/data/` directory contains the data files that are used by the UC server. This includes the tables and volumes
    that are created.
- If you configure local storage, the server reads and writes it with its own identity; see
    [Local file system storage](./configuration.md#local-file-system-storage) before letting other users write to it.
- The `etc/db/` directory contains the backend database that is used by the UC server. By default
    this is an H2 database file, which supports one server per file; to run more than one server, use
    PostgreSQL or MySQL.

### Configuring the database

- The backend database can be configured by modifying the `etc/conf/hibernate.properties` file.
- You need to provide the connection details to connect to your database server.
- Hibernate and Casbin use one HikariCP pool built from those connection properties, so total
    database connections are capped by `hibernate.hikari.maximumPoolSize` (not Hibernate's pool
    plus a separate Casbin connection). Optional pool settings use `hibernate.hikari.*` keys.
    Autocommit defaults to false so Hibernate can roll back JDBC work. The Casbin JDBC adapter still
    holds one pooled connection for the process lifetime; Casbin enables autocommit on that
    checkout so policy reads do not sit idle-in-transaction.

### Example MySQL Connection

#### Prerequisites

- Install docker.
- Download JDBC driver for [MySQL](https://dev.mysql.com/downloads/connector/j/).

#### Start MySQL server

- In a terminal, navigate to the cloned repository root directory.
- Modify `etc/db/mysql-example.yml` to configure MySQL server. Then start MySQL using Docker:

    ```sh
    docker-compose -f etc/db/mysql-example.yml up -d
    ```

- Unity Catalog treats storage paths and names as case-sensitive. PostgreSQL and H2 compare text
  case-sensitively by default, but MySQL's default collation is case-insensitive. Create the
  database with a binary collation. The example file already does this for `ucdb` when it creates a
  new data volume; for your own server run:

    ```sql
    CREATE DATABASE ucdb CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;
    ```

  The server logs a warning at startup if the database or its tables use a case-insensitive collation.

- Modify the `etc/conf/hibernate.properties` file with your MySQL connection details:

    ```properties
    hibernate.connection.driver_class=com.mysql.cj.jdbc.Driver
    hibernate.connection.url=jdbc:mysql://localhost:3306/ucdb
    hibernate.connection.user=uc_default_user
    hibernate.connection.password=uc_default_password
    hibernate.hikari.maximumPoolSize=20
    hibernate.hikari.minimumIdle=2
    ```

- Modify the `jars/classpath` file and add path to your JDBC driver.

### Example PostgreSQL Connection

#### Prerequisites

- Install docker.
- The server ships with the PostgreSQL JDBC driver. Do not add another one to `jars/classpath`.

#### Start PostgreSQL server

- In a terminal, navigate to the cloned repository root directory.
- Modify `etc/db/postgres-example.yml` to configure PostgreSQL server. Then start PostgreSQL using Docker:

    ```sh
    docker-compose -f etc/db/postgres-example.yml up -d
    ```

- Modify the `etc/conf/hibernate.properties` file with your PostgreSQL connection details:

    ```properties
    hibernate.connection.driver_class=org.postgresql.Driver
    hibernate.connection.url=jdbc:postgresql://localhost:5432/ucdb
    hibernate.connection.user=uc_default_user
    hibernate.connection.password=uc_default_password
    hibernate.hikari.maximumPoolSize=20
    hibernate.hikari.minimumIdle=2
    ```

### Existing deployments and column type changes

Flyway manages the database schema. See [Upgrading the database schema](upgrades.md).

`uc_properties.property_value` used to be created as `varchar(255)`. Table and view properties
(user `TBLPROPERTIES`, Spark `view.sqlConfig.*`, and any other REST-sent property) can exceed
that. New installs get a wider column from the schema steps. **Databases created before 0.7.0 keep
`varchar(255)` until a 0.7.0 server starts on them (its schema update widens the column on H2,
PostgreSQL and MySQL) or you run one of the statements below**; the schema steps of later releases
leave it unchanged.

PostgreSQL:

```sql
ALTER TABLE uc_properties ALTER COLUMN property_value TYPE text;
```

MySQL:

```sql
ALTER TABLE uc_properties MODIFY property_value MEDIUMTEXT NOT NULL;
```

H2:

```sql
ALTER TABLE uc_properties ALTER COLUMN property_value SET DATA TYPE VARCHAR(16777215);
```
