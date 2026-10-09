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
- The `etc/db/` directory contains the backend database that is used by the UC server.

### Configuring the database

- The backend database can be configured by modifying the `etc/conf/hibernate.properties` file.
- You need to provide the connection details to connect to your database server.
- Hibernate and Casbin use one HikariCP pool built from those connection properties, so total
    database connections are capped by `hibernate.hikari.maximumPoolSize` (not Hibernate's pool
    plus a separate Casbin connection). Optional pool settings use `hibernate.hikari.*` keys.
    Autocommit defaults to false so Hibernate can roll back JDBC work. jdbc-adapter 2.7.0 still
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
- Download JDBC driver for [PostgreSQL](https://jdbc.postgresql.org/download/).

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

- Modify the `jars/classpath` file and add path to your jdbc driver.

### Existing deployments and column type changes

The server uses `hibernate.hbm2ddl.auto=update`. Hibernate can create missing tables and
columns, but **it does not change the type or length of a column that already exists**. The
exception is the PostgreSQL large object conversion below, which the server runs itself.

#### PostgreSQL: large object columns converted to `text`

Earlier versions stored these columns as PostgreSQL large objects (`oid`):
`uc_tables.view_definition`, `uc_columns.type_text`, `uc_functions.routine_definition` and
`uc_credentials.credential`. **On its first start against such a database, the server converts
them to `text`** before the schema update, in a single transaction. Later starts find `text`
columns and skip it. H2 and MySQL are not affected.

Before upgrading:

- **Back up the database.** Servers from before the conversion cannot read `text` columns, so
  rolling back the server means restoring that backup.
- **Stop the old servers.** They fail on the converted columns, and their open transactions hold
  locks the conversion must wait for. Avoid a rolling upgrade.
- **Plan for downtime.** The conversion waits at most 30 seconds (`lock_timeout`) for each
  table lock; if a lock doesn't come in time, the start fails and the next start retries. The
  timeout only bounds waiting, though. Once the conversion has a lock, it keeps the table under
  an `ACCESS EXCLUSIVE` lock, blocking all reads and writes, until every column is converted.
  That takes time in proportion to the data, mostly `uc_columns`. A `statement_timeout` shorter
  than that aborts the conversion on every start.

After the conversion, the large objects remain in `pg_largeobject`, unreferenced. Once you no
longer need to roll back, remove them with
[`vacuumlo`](https://www.postgresql.org/docs/current/vacuumlo.html). `vacuumlo` removes every
large object that no `oid` or `lo` column references, so run it with `-n` first to see what it
would remove, especially if other applications share the database:

```sh
vacuumlo -n -v -h <host> -U <user> <database>
vacuumlo -v -h <host> -U <user> <database>
```

#### `uc_properties.property_value`

`uc_properties.property_value` used to be created as `varchar(255)`. Table and view properties
(user `TBLPROPERTIES`, Spark `view.sqlConfig.*`, and any other REST-sent property) can exceed
that. New installs get a wider column from Hibernate. **Existing databases keep `varchar(255)`
until you run one of the statements below**; upgrading the server JAR alone has no effect.

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
