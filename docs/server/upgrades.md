# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`flyway_schema_history` table.

Hibernate no longer changes the schema. If your `etc/conf/hibernate.properties` still sets
`hibernate.hbm2ddl.auto=update`, the server ignores it and logs a warning. You can delete the line.

A server starts only when the database is on the schema version of its release, or is empty (it
then sets up the schema itself). Otherwise it refuses to start and tells you what to do (see
[Messages](#messages)).

## Schema versions

| Version | Change |
| ------- | ------ |
| 1 | The 0.7 schema. A 0.7 database already has it. |
| 2 | Widens `uc_function_params.type_json`, so function parameter types can be longer. |
| 3 | MySQL only: makes text comparisons case-sensitive (see [MySQL](#mysql-case-sensitive-text)). Changes nothing on PostgreSQL and H2. |

## Upgrading to a new release

Use these steps for every database (H2, PostgreSQL, MySQL).

1. Look at the state of the database with the new release's tool:

    ```sh
    bin/uc-migrate info
    ```

    On a 0.7 database it prints:

    ```text
    State: UNVERSIONED_UC
    Current version: none
    Latest version: 3
    Applied steps: none
    Pending steps:
      2  widen function param type json
      3  case sensitive text
    ```

2. Stop all servers.
3. Back up the database.
4. Apply the pending steps:

    ```sh
    bin/uc-migrate migrate
    ```

    ```text
    Schema version 1 -> 3 (2 steps)
    ```

5. Start the new release.

`bin/uc-migrate` reads the database settings from `etc/conf/hibernate.properties`, like the
server. Run it again at any time: when nothing is pending, it prints `(0 steps)` and changes
nothing. On PostgreSQL and MySQL, only one `migrate` runs at a time per database; a second one
waits for the first.

`State` in the `info` output is one of:

| State | Meaning |
| ----- | ------- |
| `EMPTY` | No tables yet. `migrate` (or the first server start) sets up the schema. |
| `UNVERSIONED_UC` | A 0.7 database. `migrate` adopts it as version 1 and applies the later steps. |
| `PENDING` | Some steps of this release are not applied yet. |
| `CURRENT` | Every step of this release is applied. |
| `AHEAD` | The database belongs to a newer release. |
| `NOT_UC` | The database has tables, but none of Unity Catalog's. |
| `FAILED` | An earlier run recorded a failed step. |

## First start on an empty database

A server that starts on an empty database sets up the schema itself, so a new install needs no
`bin/uc-migrate` run. On PostgreSQL and MySQL, when several servers start at the same time, one
sets up the schema and the others wait for it, then start.

## MySQL: case-sensitive text

Starting with this release, text comparisons in MySQL are case-sensitive, like PostgreSQL and H2.
MySQL databases created with the default collation ignored case, so names and storage paths that
differed only in case counted as the same value. After step 3, they are distinct values.

The step converts the database and each Unity Catalog table to the `utf8mb4_0900_bin` collation.
MySQL rebuilds each table, and writes to a table are blocked while it is rebuilt. Run
`bin/uc-migrate migrate` with all servers stopped. The time it takes grows with the size of your
tables.

## Going back to 0.7

0.7 can run on the upgraded schema: steps 1 and 3 change nothing 0.7 cannot read, and step 2 only
widens a column. Stop the new release's servers and start 0.7 on the same database.

## Messages

Messages that `bin/uc-migrate` or the server print, and what to do.

`The database has no schema version (a database from 0.7); this release needs version 3. Stop the servers and run bin/uc-migrate migrate.`
: (printed by the server) The server found a 0.7 database. Follow
  [Upgrading to a new release](#upgrading-to-a-new-release).

`The database schema is at version <n>; this release needs version <m>. Stop the servers and run bin/uc-migrate migrate.`
: (printed by the server) The database is behind this release. Follow
  [Upgrading to a new release](#upgrading-to-a-new-release).

`The schema is newer than this release (version <n>, this release ends at <m>). Run the newer release.`
: (printed by `bin/uc-migrate` and the server) The database was upgraded by a newer release than
  the one you ran. Run the newer release again. To go back to an older release, restore a backup
  taken before the upgrade.

`The database has tables but none of Unity Catalog's: not a Unity Catalog database. Point the server at an empty database or a Unity Catalog one.`
: (printed by `bin/uc-migrate` and the server) `hibernate.connection.url` points at a database
  that other applications use. Check the URL, or use an empty database for Unity Catalog.

`A migration step failed in an earlier run. See docs/server/upgrades.md, then rerun bin/uc-migrate migrate.`
: (printed by `bin/uc-migrate` and the server) A step failed and its failure is recorded in the
  history. Read the error that the failing run printed and fix its cause (for example a full disk
  or missing privileges for the database user). Then restore the backup you took before the run,
  and run `bin/uc-migrate migrate` again. Do not edit `flyway_schema_history` by hand.

`Could not take the schema migration lock: <reason>`
: `bin/uc-migrate` or the server could not get the lock that keeps two runs apart. Check that the
  database is reachable and that its user can run the lock call (`pg_advisory_lock` on
  PostgreSQL, `GET_LOCK` on MySQL).

`MySQL did not grant the schema migration lock`
: MySQL refused the lock. Check the database and run the command again.

`Could not release the schema migration lock: <reason>`
: The run finished, but the lock call failed at the end. The database releases the lock when that
  connection closes, at the latest when the process exits. Run `bin/uc-migrate info` to see the
  state.

`Could not list the database's tables: <reason>`
: `bin/uc-migrate` or the server could not read the database. Check the connection settings and
  the user's privileges.

`Usage: bin/uc-migrate migrate | info`
: `bin/uc-migrate` takes exactly one argument, `migrate` or `info`.
