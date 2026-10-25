# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`uc_schema_history` table.

Hibernate no longer changes the schema. If your `etc/conf/hibernate.properties` still sets
`hibernate.hbm2ddl.auto=update`, the server ignores it and logs a warning. You can delete the line.

Every server start brings the database to the schema version of its release before the server
serves requests. It sets up an empty database (the first start of a new install), adopts a
database without schema history (one from the baseline release) and applies the steps that are not
applied yet. It refuses to start on a
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
    Upgraded the database schema from version 1 to 2 (1 step(s))
    ```

On PostgreSQL and MySQL, when several servers start at the same time, one applies the steps and
the others wait for it, then start.

The server opens its API port only after the steps finish, so a long step on a large table can
outlast a startup or liveness probe. During an upgrade, lengthen or turn off those probes, or
start one server first.

## Going back to the baseline release

To go back, stop the new release's servers and restore the backup you took before the upgrade.

Do not start the baseline release on the upgraded database as it is: its Hibernate setting
`hibernate.hbm2ddl.auto=update` changes column types back (it narrows the column that step 2
widened) while the schema history still records the step. If you must run 0.7.0 on the upgraded
database, first set `hibernate.hbm2ddl.auto=none` in its `etc/conf/hibernate.properties`.

## Messages

Messages that the server prints when it refuses to start, and what to do.

`The database is from a release before 0.7.0 (missing tables: <tables>). Start a 0.7.0 server on it once, then start this release.`
: The database has no schema history and lacks tables of the baseline release. Follow
  [The baseline release](#the-baseline-release).

`The schema is newer than this release (version <n>, this release ends at <m>). Run the newer release.`
: The database was upgraded by a newer release than the one you ran. Run the newer release again.
  To go back to an older release, restore a backup taken before the upgrade.

`The database has tables but none of Unity Catalog's: not a Unity Catalog database. Point the server at an empty database or a Unity Catalog one.`
: `hibernate.connection.url` points at a database that other applications use. Check the URL, or
  use an empty database for Unity Catalog.

`Validate failed: Migrations have failed validation. Detected failed migration to version <n> (<description>).`
: On H2 and MySQL, step `<n>` failed and its failure is recorded in the history (PostgreSQL rolls a
  failed step back and records nothing). Flyway's advice to run `repair` does not apply: Unity
  Catalog has no repair command. Read the error that the failing start printed and fix its cause
  (for example a full disk or missing privileges for the database user). Then restore the backup you
  took before the upgrade and start the server again. Do not edit `uc_schema_history` by hand.

`Could not take the schema migration lock: <reason>`
: The server could not get the lock that keeps two migration runs apart, so it changed nothing.
  Check that the database is reachable and that its user can run the lock call
  (`pg_advisory_lock` on PostgreSQL, `GET_LOCK` on MySQL), then start the server again. On MySQL,
  `the lock call returned NULL` means the call failed or was interrupted, for example by
  `KILL QUERY`.

`Could not release the schema migration lock: <reason>. The database releases it when the connection closes; start the server again.`
: Releasing the lock failed at the end of the run. The server closed the lock's database
  connection, which releases the lock. Start the server again.

`Unsupported database jdbc:<name>: use jdbc:h2:, jdbc:postgresql: or jdbc:mysql:`
: `hibernate.connection.url` points at a database that Unity Catalog does not support. Use H2,
  PostgreSQL or MySQL.

`Could not connect to the database: <reason>` or `Could not list the database's tables: <reason>`
: The server could not read the database. Check the connection settings and the user's
  privileges.
