# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`uc_schema_history` table.

Hibernate no longer changes the schema. If your `etc/conf/hibernate.properties` still sets
`hibernate.hbm2ddl.auto=update`, the server ignores it and logs a warning. You can delete the line.

Every server start brings the database to the schema version of its release before the server
serves requests. It sets up an empty database, adopts a database without schema history (one from
the baseline release, see [Adopting an older database](#adopting-an-older-database)) and applies
the steps that are not applied yet. It refuses to start on a database it cannot upgrade and tells
you what to do (see [Messages](#messages)).

After the steps, and on every start, the server checks the tables that this release's steps
change (see [The schema differs](#the-schema-differs)).

A step whose change is already in the database (a start stopped before it recorded the step, or
someone applied the change by hand) is recorded without running it, and the server logs
`Step <n> (<description>) is already in the database; recorded it without running it.`

## The baseline release

The first baseline release is 0.7.0: schema version 1 is its schema, and a 0.7.0 database without
schema history is adopted as version 1. A database from an earlier release must be upgraded to
0.7.0 first: start a 0.7.0 server on it once, stop it, then start this release. This holds until a
later release migrates older schemas too.

## Adopting an older database

A database from a release before 0.7.0 still differs from a fresh 0.7.0 database after that one
0.7.0 start, because 0.7.0's Hibernate update neither changes an existing CHECK constraint nor
reports a constraint it fails to add. When the server adopts a database without schema history, it
compares every table with the 0.7.0 schema (columns, keys, indexes, foreign keys and CHECK
constraints) and repairs these differences before it records the baseline, after the
[backup](#backup-before-the-upgrade) if that is on:

| Difference | Releases | Repair |
| ---------- | -------- | ------ |
| The CHECKs on `uc_function_params.type_name` and `uc_functions.data_type` allow 0 to 20, not 0 to 21: creating a function with a `TABLE_TYPE` parameter or return type fails. | 0.4.0 and earlier | Re-creates both CHECKs to allow 0 to 21. |
| The unique constraint `uc_users_external_id_key` on `uc_users.external_id` is missing, because two users shared an external id when 0.7.0 first started. | Every release before 0.7.0 | Adds the constraint. |

The server logs each repair as `Repaired a difference from the 0.7.0 schema: <difference>.`, and
`bin/start-uc-server --check-schema` lists the repairs it would make. It refuses to start, before
any change, in the cases below.

### Users that share an external id

The constraint can't be added while users share an external id; the message lists them. The
external id maps an identity provider's user to the Unity Catalog user, so decide which user keeps
it, clear it on the others, then start the server again:

```sql
UPDATE uc_users SET external_id = NULL WHERE id = '<id>';
-- MySQL
UPDATE uc_users SET external_id = NULL WHERE id = UUID_TO_BIN('<id>');
```

### Function types of releases before 0.4.1

Release 0.4.1 added the function type `VARIANT`, which moved the types after it. A database with
the 0.4.0 CHECKs stores some function types as numbers that this release reads as other types:

| Stored | Written by 0.4.0 and earlier | Read by 0.4.1 and later |
| ------ | ---------------------------- | ----------------------- |
| 17 | `CHAR` | `VARIANT` |
| 18 | `NULL` | `CHAR` |
| 19 | `USER_DEFINED_TYPE` | `NULL` |
| 20 | `TABLE_TYPE` | `USER_DEFINED_TYPE` |

When it finds such a value, the server refuses and lists each function with the column and the
stored value. The database does not record which release wrote a row, so only you can tell which
meaning a value has:

- If every listed function was created before the database first ran 0.4.1 or later, shift the
  stored values. Widen the two CHECKs first, since 21 breaks them, then update the rows:

    ```sql
    -- PostgreSQL
    ALTER TABLE uc_function_params DROP CONSTRAINT uc_function_params_type_name_check,
      ADD CONSTRAINT uc_function_params_type_name_check CHECK (type_name BETWEEN 0 AND 21);
    ALTER TABLE uc_functions DROP CONSTRAINT uc_functions_data_type_check,
      ADD CONSTRAINT uc_functions_data_type_check CHECK (data_type BETWEEN 0 AND 21);
    -- H2 and MySQL: find the CHECKs' names first
    SELECT tc.table_name, tc.constraint_name FROM information_schema.table_constraints tc
      JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema
        AND cc.constraint_name = tc.constraint_name
      WHERE LOWER(tc.table_name) IN ('uc_function_params', 'uc_functions')
        AND UPPER(cc.check_clause) LIKE '%BETWEEN 0 AND 20%';
    ALTER TABLE uc_function_params DROP CONSTRAINT <name>;  -- MySQL: DROP CHECK <name>
    ALTER TABLE uc_function_params ADD CHECK (type_name BETWEEN 0 AND 21);
    ALTER TABLE uc_functions DROP CONSTRAINT <name>;        -- MySQL: DROP CHECK <name>
    ALTER TABLE uc_functions ADD CHECK (data_type BETWEEN 0 AND 21);
    -- every database
    UPDATE uc_function_params SET type_name = type_name + 1 WHERE type_name BETWEEN 17 AND 20;
    UPDATE uc_functions SET data_type = data_type + 1 WHERE data_type BETWEEN 17 AND 20;
    ```

- Otherwise, values written by 0.4.1 or later already mean the newer types, and shifting them
  would break those functions. Delete the listed functions, for example through a 0.7.0 server,
  and create them again after the upgrade.

Then start the server again.

### Other differences

Any other difference stops the server, which lists the first 20 differing lines:

```text
The database differs from the 0.7.0 schema in ways the server can't repair (-: the 0.7.0 schema, +: what the database has):
  +column uc_catalogs.note type=character varying size=64 nullable=YES default=null
Compare it with a fresh 0.7.0 database and fix the differences by hand, or ask on the project's issue tracker; see docs/server/upgrades.md (section "Adopting an older database").
```

Such a difference comes from a change made by hand or from a database setup that this release was
not tested with (for example MySQL before 8.0.16, which ignores CHECK constraints). Fix the
differences it lists, or ask on the project's issue tracker, then start the server again. A change
of one of this release's steps that is already in the database is no difference: the server
records that step without running it.

## Schema versions

| Version | Change |
| ------- | ------ |
| 1 | The schema of the baseline release. A database without schema history from that release already has it. |
| 2 | Widens `uc_function_params.type_json`, so function parameter types can be longer. |
| 3 | MySQL only: makes text comparisons case-sensitive (see [MySQL](#mysql-case-sensitive-text)). Changes nothing on PostgreSQL and H2. |

## Upgrading to a new release

Upgrade one release at a time: a release upgrades a database of the release before it, not one of
an older release. To go from 0.8.0 to 1.0.0 with 0.9.0 in between, upgrade to 0.9.0 first. A
release without steps of its own (for example most patch releases) can be skipped; the server
tells you when a release can't be (see [Skipping a release](#skipping-a-release)).

Use these steps for every database (H2, PostgreSQL, MySQL).

1. Stop all servers.
2. Back up the database, or let the server do it (see
   [Backup before the upgrade](#backup-before-the-upgrade)).
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

With the Helm chart, `helm upgrade` checks the database, stops the old servers and starts the new
release; see [Deploy with Helm](helm.md#upgrade-and-uninstall). Back up the database first.

## Backup before the upgrade

The server can back up the whole database before the upgrade changes anything, on the same
database server, and restore it if the upgrade fails. The backup is off by default. Turn it on in
`etc/conf/server.properties`:

```properties
server.migration.backup=true
```

It needs:

- All servers stopped, as for every upgrade: the backup is taken while no other server uses the
  database, and a restore erases what was written after it.
- A database user that can create and drop databases on the database server (PostgreSQL: `CREATEDB`
  and owner of the Unity Catalog database), and room there for a full copy of the database.
- On PostgreSQL, the database name in `hibernate.connection.url`.
- On H2, a file database (`jdbc:h2:file:...`). The server refuses to start on an in-memory one
  with the backup on.

The server takes the backup only when it has steps to apply or a database to adopt, after the
checks that can refuse the upgrade (a skipped release, a database it
[can't adopt](#adopting-an-older-database), [rows that block an upgrade](#rows-that-block-an-upgrade),
a step it can't run again), so an up-to-date or refused database gets none. It logs
`Backed up the database to <copy> before the upgrade.` and drops the copy after a successful
upgrade. To go back after a successful upgrade, use a backup of your own (see
[Going back to the baseline release](#going-back-to-the-baseline-release)).

| Database | The backup | The restore |
| -------- | ---------- | ----------- |
| PostgreSQL | The database `<db>_uc_backup`, a copy made with `CREATE DATABASE <db>_uc_backup TEMPLATE <db>` | `DROP DATABASE <db>`, then `ALTER DATABASE <db>_uc_backup RENAME TO <db>` |
| MySQL | The database `<db>_uc_backup`, with each table created from `SHOW CREATE TABLE` and its rows copied in one transaction | Drops the tables of `<db>` and moves each copied table back with `RENAME TABLE` |
| H2 | The file `<file>_uc_backup.mv.db` next to the database file, copied with the database closed | Moves the copy back in place of the database file |

On PostgreSQL, the copy and the drop need every session on `<db>` closed. The server closes its
own connections and runs these statements, and its migration lock, from a small database of its
own, `<db>_uc_upgrade`, which it creates. When another session is connected, for example a server
of the old release that is still running, the server refuses to start before any change:

```text
Could not back up the database before the upgrade, which changed nothing: other sessions are connected to the database (...). Stop the other servers on the database, then start the server again
```

### When the upgrade fails

When a step fails, the [schema check](#the-schema-differs) after the steps fails, or the server gets
SIGTERM during the upgrade, the server restores the backup and refuses to start, printing the error
and then:

```text
The database was restored from the backup taken before the upgrade; the previous release can start on it.
```

The copy is gone: it is the database again. Fix the cause and start the new release again; the
upgrade starts from the beginning, with a new backup.

A start that finds the database partway through this release's steps (a server killed with SIGKILL,
or an upgrade that failed with the backup off) keeps a copy that is already there, taken before the
upgrade started, and restores that one if the upgrade fails. A database still at the previous
release gets a new copy; an older copy is dropped first.

### Stopping the server during a restore

On SIGTERM the server stops at the next safe point (see
[Stopping a server during the upgrade](#stopping-a-server-during-the-upgrade)), then restores the
backup before it exits, however long that takes. Give it a grace period longer than 60 seconds (a
running statement) plus the restore: on PostgreSQL and MySQL the restore drops and renames, which
is quick; on H2 it moves a file. On Kubernetes, set the pod's `terminationGracePeriodSeconds`
(30 seconds by default); with `docker stop`, use `-t` (10 seconds by default).

If the server is killed during the restore, or the restore fails, the copy is still there, and the
server prints where. Restore it by hand with all servers stopped:

```sql
-- PostgreSQL, connected to another database such as postgres
DROP DATABASE <db>;
ALTER DATABASE <db>_uc_backup RENAME TO <db>;
-- MySQL
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE <db>.<table>, ...;            -- every table of <db>
RENAME TABLE <db>_uc_backup.<table> TO <db>.<table>, ...;  -- every table of the copy
DROP DATABASE <db>_uc_backup;
SET FOREIGN_KEY_CHECKS = 1;
```

On MySQL, a copy without tables means the restore had moved them back: drop `<db>_uc_backup`. On
H2, move `<file>_uc_backup.mv.db` to `<file>.mv.db`.

### The restored mark with Helm

When the environment variable `UC_CHART_REVISION` is set to the Helm chart revision the server runs
in, a restore also writes a mark naming the release and that revision: the table `uc_restored` in
the database `<db>_uc_upgrade` on PostgreSQL and MySQL, the file `<file>_uc_upgrade.properties` on
H2. A later start of the same release in the same revision refuses instead of upgrading again:

```text
The upgrade to <release> failed and the database was restored in chart revision <revision>; fix the cause, then run helm upgrade again (a new revision clears this).
```

The previous release starts on the restored database, so `helm rollback`, `helm upgrade --atomic`
or Flux remediation can bring it back. A start in another revision clears the mark and upgrades.
Without `UC_CHART_REVISION` no mark is written: fix the cause and start the server again.

## Skipping a release

The server refuses to start on a database of a release older than the one before it, for example
this release 1.0.0 on a 0.8.0 database:

```text
The database is at schema version 3, from release 0.8.0 or earlier; this release upgrades only from 0.9.0 (version 5 or later). Upgrade to 0.9.0 first, then to this release.
```

It changes nothing. Start the release that the message names on the database once, stop it, then
start this release. `bin/start-uc-server --check-schema` reports the same.

A database partway between the release before and this one (an upgrade that stopped midway) goes
on. So does an empty database. A database without schema history is one of the
[baseline release](#the-baseline-release), at version 1: a release goes on with it only when it
upgrades from the baseline release.

## The schema differs

The server compares the tables that this release's steps change (created, altered, dropped or
indexed) with the schema those steps build: their columns, keys, indexes, foreign keys and CHECK
constraints. It does so after the steps run and on every start, also when no step is pending. A
difference means someone changed one of those tables by hand, and the server refuses to start:

```text
The schema of uc_function_params differs from what this release's steps build (-: what they build, +: what the database has):
  +column uc_function_params.note type=character varying size=64 nullable=YES default=null
Undo the manual change, or restore the backup; see docs/server/upgrades.md (section "The schema differs").
```

The message lists the first 20 differing lines. Undo the change it lists (here, drop the column
`note`), or restore the backup you took before the upgrade, then start the server again.
`bin/start-uc-server --check-schema` runs the same comparison on a database that is up to date.

Not checked: tables that no step of this release changes, and MySQL collations. On MySQL, a text
column wider than the steps make it counts as a match: converting a latin1 or utf8mb3 table to
utf8mb4 (step 3) widens text columns so they keep their length in characters.

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
  changed yet. Step 3 on MySQL, which is safe to repeat, runs again too. The server logs
  `Step <n> (<description>) failed in an earlier run; running it again.`

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

With the [backup](#backup-before-the-upgrade) on, the server restores it before it exits instead
(see [Stopping the server during a restore](#stopping-the-server-during-a-restore)).

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
- Exit code 1: the upgrade would be refused, or the server would not start. The output says why,
  for example the rows that break a rule, a skipped release, or a schema that
  [differs](#the-schema-differs). The release notes say how to fix the rows. Fix the cause, then
  run the check again until it exits with 0, and upgrade.

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

## Cutting a release

For maintainers. When you cut a release, add a line for it to
`server/src/main/resources/db/migration/releases.properties`: the release and the version of the
last step it ships (the last step of the release before it when it ships none), for example
`0.8.0=3`. The next release upgrades only from that schema version on.

The release notes list the release's schema steps and checks:

```markdown
### Database schema

Upgrade from <previous release>: the server refuses a database of an older release. Back up the
database first, and run `bin/start-uc-server --check-schema` before you stop the old servers.

| Version | Change | Database |
| ------- | ------ | -------- |
| <n> | <what the step changes, and how long it takes on large tables> | <all, or the database> |

Checks: <the rule each check enforces, and how to fix the rows it lists>, or none.
```

## Messages

Messages that the server prints when it refuses to start, and what to do.

`The database is from a release before 0.7.0 (missing tables: <tables>). Start a 0.7.0 server on it once, then start this release.`
: The database has no schema history and lacks tables of the baseline release. Follow
  [The baseline release](#the-baseline-release).

`The database differs from the 0.7.0 schema in ways the server can't repair (-: the 0.7.0 schema, +: what the database has): ...`
: See [Other differences](#other-differences).

`The upgrade is stopped before any change: the database differs from the 0.7.0 schema, and these rows keep the server from repairing it.`
: The message lists users that share an external id or functions with types of a release before
  0.4.1. See [Adopting an older database](#adopting-an-older-database).

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

`The database is at schema version <v>, from release <r> or earlier; this release upgrades only from <p> (version <n> or later). Upgrade to <p> first, then to this release.`
: See [Skipping a release](#skipping-a-release).

`The schema of <tables> differs from what this release's steps build (-: what they build, +: what the database has): ...`
: See [The schema differs](#the-schema-differs).

`The upgrade is stopped before any change: <n> check found rows that break a rule of this release.`
: The message lists the rows. See [Rows that block an upgrade](#rows-that-block-an-upgrade).

`The database was restored from the backup taken before the upgrade; the previous release can start on it.`
: Printed after the error of an upgrade that failed with the backup on. See
  [When the upgrade fails](#when-the-upgrade-fails).

`The upgrade to <release> failed and the database was restored in chart revision <revision>; fix the cause, then run helm upgrade again (a new revision clears this).`
: See [The restored mark with Helm](#the-restored-mark-with-helm).

`Could not back up the database before the upgrade, which changed nothing: <reason>`
: The backup failed, so the server did not upgrade. On PostgreSQL, `other sessions are connected to
  the database` means another server or client is connected: stop it. Otherwise check that the
  database user can create and drop databases and that the database server has room for a copy.

`Could not restore the backup: <reason>. <where the copy is>`
: The upgrade failed and the restore failed too. Restore the copy by hand (see
  [Stopping the server during a restore](#stopping-the-server-during-a-restore)).

`The backup before the upgrade (server.migration.backup) needs an H2 file database (jdbc:h2:file:...), but this one is in memory. Turn the backup off, or use a file database.`
: See [Backup before the upgrade](#backup-before-the-upgrade).

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
