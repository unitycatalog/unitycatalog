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

A step whose change is already in the database (a start stopped before it recorded the step, or
someone applied the change by hand) is recorded without running it, and the server logs
`Step <n> (<description>) is already in the database; recorded it without running it.`

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

## Locks held by other sessions

Each statement of the upgrade waits at most 60 seconds for a lock that another session holds, for
example a client's open transaction on a table that the step changes. Then the step fails and the
server refuses to start (see [Messages](#messages)). Stop the other sessions on the database and
start the server again: it runs the step again.

## A step that failed

When a step fails, the next start picks up from it:

- PostgreSQL keeps no record of the failed step, and the next start runs it again. A failed schema
  change was rolled back; a step that changes rows in batches keeps the batches it committed and
  goes on from there.
- H2 and MySQL can't roll a schema change back, so the history records the step as failed. The
  next start checks what the step left. A step that adds or changes one thing (a table, column,
  index, constraint or column type) is recorded without running when its change is there, and runs
  when it is absent. A step that changes rows runs again and changes only the rows it has not
  changed yet. The server logs `Step <n> (<description>) failed in an earlier run; running it
  again.`

For any other failed step, for example one that makes several changes and stopped between them,
the server can't tell what is in the database and refuses to start. Read the error that the
failing start printed and fix its cause (for example a full disk or missing privileges for the
database user). Then either restore the backup you took before the upgrade, or undo the step's
change by hand and remove its failed record:

```sql
-- PostgreSQL and MySQL
DELETE FROM uc_schema_history WHERE success = false;
-- H2
DELETE FROM "uc_schema_history" WHERE "success" = FALSE;
```

Then start the server again: it runs the step.

## Stopping a server during the upgrade

`bin/start-uc-server` runs the server in its own process, so the server gets the SIGTERM that
`docker stop`, Kubernetes or `kill` sends. A server that gets SIGTERM during the upgrade stops at
the next safe point: before its next step, or after the current batch of a step that changes rows
in batches. A statement that is running finishes first; the server waits for it up to 60 seconds
(Kubernetes stops waiting after the pod's grace period, 30 seconds by default), and after that the
database finishes or rolls back the statement on its own. The server prints `Stopped the schema
migration at <point> on SIGTERM; start the server again to finish it.` and exits with a non-zero
code. The next start finishes the upgrade.

On H2, the database closes as the process stops, so the running step fails instead of stopping;
the next start handles it as [a step that failed](#a-step-that-failed).

## Rows that block an upgrade

A step can add a rule that existing rows break, for example that names are unique. Such a step
ships a check that lists the rows that break the rule, and the upgrade waits until there are none.

Before you stop the old servers, run the new release's check against the database:

```sh
bin/start-uc-server --check-schema
```

It changes nothing and does not start the server. It prints the schema version, the pending steps
and the result:

- Exit code 0: the upgrade will go through.
- Exit code 1: the upgrade would be refused. The output says why, for example the rows that break
  a rule. The release notes say how to fix them. Fix them, then run the check again until it exits
  with 0, and upgrade.

The server runs the same checks when it starts and refuses to start the same way, before any step
runs.

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

`Step <n> (<description>) failed in an earlier run, and the server can't tell how much of its change is in the database. Restore the backup you took before the upgrade, or undo the step's change by hand and remove its failed record as docs/server/upgrades.md shows (section "A step that failed"); then start the server again.`
: Step `<n>` failed in an earlier start, and it is not a step that the server can check and run
  again. See [A step that failed](#a-step-that-failed).

`Step <n> (<description>) waited more than 60 seconds for a lock on <table>; another session holds it. Stop the other sessions on the database, then start the server again.`
: See [Locks held by other sessions](#locks-held-by-other-sessions). The message names the table
  when the step's statement names one.

`The upgrade is stopped before any change: <n> check found rows that break a rule of this release.`
: The message lists the rows. See [Rows that block an upgrade](#rows-that-block-an-upgrade).

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

`hibernate.default_schema is not supported: the schema migration uses the connection's schema. Remove it and set the schema in the JDBC URL instead (PostgreSQL: currentSchema=...).`
: The same holds for `hibernate.default_catalog`. Put the schema in `hibernate.connection.url`, for
  example `jdbc:postgresql://localhost:5432/ucdb?currentSchema=uc`.
