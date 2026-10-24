# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`flyway_schema_history` table.

Hibernate no longer changes the schema. If your `etc/conf/hibernate.properties` still sets
`hibernate.hbm2ddl.auto=update`, the server ignores it and logs a warning. You can delete the line.

Every server start brings the database to the schema version of its release before the server
serves requests. It sets up an empty database, adopts a database without schema history (one from
the baseline release) and applies the steps that are not applied yet. It refuses to start on a
database it cannot upgrade and tells you what to do (see [Messages](#messages)).

## Schema versions

| Version | Change |
| ------- | ------ |
| 1 | The schema of the baseline release. A database without schema history from that release already has it. |
| 2 | Widens `uc_function_params.type_json`, so function parameter types can be longer. |
| 3 | MySQL only: makes text comparisons case-sensitive (see [MySQL](#mysql-case-sensitive-text)). Changes nothing on PostgreSQL and H2. |

## Upgrading to a new release

Use these steps for every database (H2, PostgreSQL, MySQL).

1. Stop all servers.
2. Back up the database.
3. Start the new release:

    ```sh
    bin/start-uc-server
    ```

    The server applies the pending steps, logs what it applied, then starts:

    ```text
    Upgraded the database schema from version 1 to 3 (2 steps)
    ```

On PostgreSQL and MySQL, when several servers start at the same time, one applies the steps and
the others wait for it, then start.

## First start on an empty database

A server that starts on an empty database sets up the schema itself, the same way.

## MySQL: case-sensitive text

Starting with this release, text comparisons in MySQL are case-sensitive, like PostgreSQL and H2.
MySQL databases created with the default collation ignored case, so names and storage paths that
differed only in case counted as the same value. After step 3, they are distinct values.

The step converts the database and each Unity Catalog table to the `utf8mb4_0900_bin` collation.
MySQL rebuilds each table, and writes to a table are blocked while it is rebuilt. The time it takes
grows with the size of your tables, and the server starts serving requests after it ends.

## Going back to the baseline release

The baseline release can run on the upgraded schema: steps 1 and 3 change nothing it cannot read,
and step 2 only widens a column. Stop the new release's servers and start the baseline release on
the same database.

## Messages

Messages that the server prints when it refuses to start, and what to do.

`The schema is newer than this release (version <n>, this release ends at <m>). Run the newer release.`
: The database was upgraded by a newer release than the one you ran. Run the newer release again.
  To go back to an older release, restore a backup taken before the upgrade.

`The database has tables but none of Unity Catalog's: not a Unity Catalog database. Point the server at an empty database or a Unity Catalog one.`
: `hibernate.connection.url` points at a database that other applications use. Check the URL, or
  use an empty database for Unity Catalog.

`A migration step failed in an earlier run. See docs/server/upgrades.md.`
: A step failed and its failure is recorded in the history. Read the error that the failing start
  printed and fix its cause (for example a full disk or missing privileges for the database user).
  Then restore the backup you took before the upgrade and start the server again. Do not edit
  `flyway_schema_history` by hand.

`Could not take the schema migration lock: <reason>`
: The server could not get the lock that keeps two migration runs apart. Check that the database
  is reachable and that its user can run the lock call (`pg_advisory_lock` on PostgreSQL,
  `GET_LOCK` on MySQL).

`MySQL did not grant the schema migration lock`
: MySQL refused the lock. Check the database and start the server again.

`Could not release the schema migration lock: <reason>`
: The migration finished, but the lock call failed at the end. The database releases the lock when
  that connection closes, at the latest when the process exits.

`Could not list the database's tables: <reason>`
: The server could not read the database. Check the connection settings and the user's
  privileges.
