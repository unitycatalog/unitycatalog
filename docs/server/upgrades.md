# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`uc_schema_history` table. The steps also create the tables of embedded OpenSharing, whose names
start with `os_`, whether or not it is enabled.

For now the schema steps are optional, and you run them by hand. The server still lets Hibernate
update the schema when it starts (`hibernate.hbm2ddl.auto=update` in
`etc/conf/hibernate.properties`). The steps build the same schema, so on a database that the steps
set up, Hibernate changes nothing.

`bin/start-uc-server --upgrade-schema` brings the database to the schema version of its release,
then exits without starting the server: with 0 when it succeeds, and with 1 and the reason when it
refuses. It sets up an empty database (a new install) and applies the steps that are not applied
yet. It refuses a database that has Unity Catalog's tables but no schema history, such as one that
a server set up when it started.

## Upgrading to a new release

Run the commands with the new release.

1. Stop all servers.
2. Back up the database (see [Backing up the database](#backing-up-the-database)).
3. Upgrade the schema:

    ```sh
    bin/start-uc-server --upgrade-schema
    ```

    It applies the pending steps and logs what it applied.

4. Start the new release:

    ```sh
    bin/start-uc-server
    ```

On PostgreSQL and MySQL, `--upgrade-schema` takes a lock, so when several run at the same time, one
applies the steps and the others wait for it. It holds that lock on one database session for the
whole run, so connect to the database directly or through a pooler in session mode, not in
transaction mode (PgBouncer's `pool_mode = transaction`): there the lock can be released on
another session, and the command fails.
On MySQL the lock is a named lock (`GET_LOCK`), which Group Replication in multi-primary mode does
not share between primaries: there, run `--upgrade-schema` on one host only.

A step waits for locks that other sessions hold on its table, for example an open transaction of
a reporting tool. If `--upgrade-schema` stays at a step for long, look for such sessions and end
them: `SELECT pid, state, query FROM pg_stat_activity WHERE datname = current_database()` on
PostgreSQL, `SHOW PROCESSLIST` on MySQL.

## Backing up the database

Take the backup with all servers stopped, so that it holds no half-written change. The examples use
the database `ucdb`.

- PostgreSQL: `pg_dump -Fc -d ucdb -f uc-backup.dump`. To restore it, create an empty database and
  run `pg_restore -d ucdb uc-backup.dump`.
- MySQL: `mysqldump --single-transaction --databases ucdb > uc-backup.sql`. To restore it, drop the
  database and run `mysql < uc-backup.sql`.
- H2: copy the database file, `etc/db/h2db.mv.db` with the default
  `hibernate.connection.url`. To restore it, copy it back.
- A managed database (for example Amazon RDS or Cloud SQL): a snapshot of the instance works too.

## Messages

Messages that `--upgrade-schema` prints when it refuses, and what to do.

`The database's schema history does not match this release's schema steps: <reasons>`
: Flyway's check of `uc_schema_history` failed, for example because a recorded step differs from
  this release's step of the same version (its checksum), or this release lacks a recorded step.
  `<reasons>` may advise Flyway's `repair` command, which Unity Catalog doesn't have. Run the release
  that applied the steps, or restore the backup. Do not edit `uc_schema_history` by hand. On H2 and
  MySQL, a step that failed is reported here too: this release can't retry it, so restore the backup
  and fix the cause, then run the command on the restored database. On PostgreSQL, a failed step
  leaves no history row, so the command runs it again.

`Step <n> left Unity Catalog index <index> missing on <table>: <hint>`
: On PostgreSQL and H2, index names are unique within a schema, so the step skipped Unity Catalog's
  index because another object, such as another table's index, already has that name. The step is
  recorded as applied, so renaming the other index and running the command again does not create
  Unity Catalog's index.
  Restore the backup (on a new install, drop the `uc_*` tables and `uc_schema_history`), rename the
  other index or give Unity Catalog its own schema, then run the command again.

`Could not take the schema migration lock: <reason>`
: The command could not get the lock that keeps two migration runs apart, so it changed nothing.
  Check that the database is reachable and that its user can run the lock call
  (`pg_advisory_lock` on PostgreSQL, `GET_LOCK` on MySQL), then run the command again.
