# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`uc_schema_history` table.

Hibernate no longer changes the schema. If your `etc/conf/hibernate.properties` still sets
`hibernate.hbm2ddl.auto=update`, the server ignores it and logs a warning. You can delete the line.

Every server start brings the database to the schema version of its release before the server
serves requests. It sets up an empty database, adopts a database without schema history (one from
the baseline release) and applies the steps that are not applied yet. It refuses to start on a
database it cannot upgrade and tells you what to do (see [Messages](#messages)).

## The baseline release

The first baseline release is 0.7.0: schema version 1 is its schema, and a 0.7.0 database without
schema history is adopted as version 1. A database from an earlier release must be upgraded to
0.7.0 first: start a 0.7.0 server on it once, stop it, then start this release. This holds until a
later release migrates older schemas too.

## Schema versions

| Version | Change |
| ------- | ------ |
| 1 | The schema of the baseline release. A database without schema history from that release already has it. |
| 2 | Widens `uc_function_params.type_json`, so function parameter types can be longer. |

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
    Upgraded the database schema from version 1 to 2 (1 steps)
    ```

On PostgreSQL and MySQL, when several servers start at the same time, one applies the steps and
the others wait for it, then start.

## First start on an empty database

A server that starts on an empty database sets up the schema itself, the same way.

## Going back to the baseline release

To go back, stop the new release's servers and restore the backup you took before the upgrade.

Do not start the baseline release on the upgraded database as it is: its Hibernate setting
`hibernate.hbm2ddl.auto=update` changes column types back (it narrows the column that step 2
widened) while the schema history still records the step. If you must run 0.7.0 on the upgraded
database, first set `hibernate.hbm2ddl.auto=none` in its `etc/conf/hibernate.properties`.

## Messages

Messages that the server prints when it refuses to start, and what to do.

`The database is from a release before 0.7.0. Start a 0.7.0 server on it once, then start this release.`
: The database has no schema history and lacks a table the baseline release added. Follow
  [The baseline release](#the-baseline-release).

`The schema is newer than this release (version <n>, this release ends at <m>). Run the newer release.`
: The database was upgraded by a newer release than the one you ran. Run the newer release again.
  To go back to an older release, restore a backup taken before the upgrade.

`The database has tables but none of Unity Catalog's: not a Unity Catalog database. Point the server at an empty database or a Unity Catalog one.`
: `hibernate.connection.url` points at a database that other applications use. Check the URL, or
  use an empty database for Unity Catalog.

`Validate failed: Migrations have failed validation. Detected failed migration to version <n> (<description>).`
: Step `<n>` failed and its failure is recorded in the history. Read the error that the failing
  start printed and fix its cause (for example a full disk or missing privileges for the database
  user). Then restore the backup you took before the upgrade and start the server again. Do not edit
  `uc_schema_history` by hand.

`Could not take the schema migration lock: <reason>`
: The server could not get the lock that keeps two migration runs apart. Check that the database
  is reachable and that its user can run the lock call (`pg_advisory_lock` on PostgreSQL,
  `GET_LOCK` on MySQL).

`MySQL did not grant the schema migration lock`
: MySQL refused the lock. Check the database and start the server again.

`Could not release the schema migration lock: <reason>`
: The migration finished, but the lock call failed at the end. The database releases the lock when
  that connection closes, at the latest when the process exits. Start the server again.

`Could not list the database's tables: <reason>`
: The server could not read the database. Check the connection settings and the user's
  privileges.

`The schema migration needs 3 database connections (its lock plus 2 for Flyway), but the pool allows <n>. Set hibernate.hikari.maximumPoolSize to 3 or more.`
: PostgreSQL and MySQL only. Raise `hibernate.hikari.maximumPoolSize` in
  `etc/conf/hibernate.properties`.
