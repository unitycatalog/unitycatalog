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
    ```

- Modify the `jars/classpath` file and add path to your jdbc driver.

### Existing deployments and column type changes

The server uses `hibernate.hbm2ddl.auto=update`. Hibernate can create missing tables and
columns, but **it does not change the type or length of a column that already exists**.

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

### Pre-populating `uc_tables.delta_latest_backfilled_version` (optional)

Managed Delta tables track the highest published (backfilled) commit version in
`uc_tables.delta_latest_backfilled_version`. Hibernate adds the column on upgrade but leaves it
null on existing rows; the server reconstructs the value from `uc_delta_commits` the first time
it touches a table and persists it, so **no action is required** and the statements below can be
run before, during, or after the upgrade.

Running them up front populates every table in one pass, which makes the values inspectable
immediately rather than appearing table by table as traffic arrives. They only touch rows that
are still null, so they are safe to re-run and safe to run against an already-upgraded server.

PostgreSQL:

```sql
UPDATE uc_tables t
SET delta_latest_backfilled_version = c.backfilled_through
FROM (
  SELECT table_id,
         COALESCE(
           MAX(commit_version) FILTER (WHERE is_backfilled_latest_commit),
           MIN(commit_version) - 1
         ) AS backfilled_through
  FROM uc_delta_commits
  GROUP BY table_id
) c
WHERE t.id = c.table_id
  AND t.delta_latest_backfilled_version IS NULL;
```

MySQL:

```sql
UPDATE uc_tables t
JOIN (
  SELECT table_id,
         COALESCE(
           MAX(CASE WHEN is_backfilled_latest_commit THEN commit_version END),
           MIN(commit_version) - 1
         ) AS backfilled_through
  FROM uc_delta_commits
  GROUP BY table_id
) c ON c.table_id = t.id
SET t.delta_latest_backfilled_version = c.backfilled_through
WHERE t.delta_latest_backfilled_version IS NULL;
```

H2:

```sql
UPDATE uc_tables t
SET delta_latest_backfilled_version = (
  SELECT COALESCE(
           MAX(CASE WHEN c.is_backfilled_latest_commit THEN c.commit_version END),
           MIN(c.commit_version) - 1
         )
  FROM uc_delta_commits c
  WHERE c.table_id = t.id
)
WHERE t.delta_latest_backfilled_version IS NULL
  AND EXISTS (SELECT 1 FROM uc_delta_commits c WHERE c.table_id = t.id);
```
