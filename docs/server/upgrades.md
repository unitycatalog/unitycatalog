# Upgrading the database schema

Unity Catalog uses [Flyway](https://flywaydb.org/) to manage the schema of its database. Each
release ships the schema steps it needs, and each step is applied once and recorded in the
`uc_schema_history` table.

For now the schema steps are optional, and you run them by hand, except schema step V4 on PostgreSQL
and MySQL (see [Credentials as text](#credentials-as-text)). The server still lets Hibernate update
the schema when it starts (`hibernate.hbm2ddl.auto=update` in `etc/conf/hibernate.properties`). The
steps build the same schema, so on a database that the steps set up, Hibernate changes nothing.

`bin/start-uc-server --upgrade-schema` brings the database to the schema version of its release,
then exits without starting the server: with 0 when it succeeds, and with 1 and the reason when it
refuses. It sets up an empty database (a new install), adopts a database from the baseline
release, and applies the steps that are not applied yet.

## The baseline release

The first baseline release is 0.7.0: schema version 1 is its schema. A database without schema
history is adopted as version 1 when its schema matches version 1 exactly, and as the latest version
when it matches that exactly, such as a database that a server of this release set up when it
started; `--upgrade-schema` lists every difference. A database from an earlier release must be
upgraded to 0.7.0 first: start a 0.7.0 server on it once, stop it, then upgrade it to this release.
Such a database still differs from version 1 in two checks, so repair it first (see
[Repairing a database from before 0.7.0](#repairing-a-database-from-before-070)).

### Repairing a database from before 0.7.0

A database that a release before 0.7.0 set up keeps that release's checks on the function type
columns even after a 0.7.0 server ran on it: they allow types 0 to 20, while version 1 allows 0 to
21. `--upgrade-schema` then lists these differences (shown here for H2; the other databases print
the checks in their own syntax):

```console
missing check uc_function_params "TYPE_NAME" BETWEEN 0 AND 21
unexpected check uc_function_params "TYPE_NAME" BETWEEN 0 AND 20
missing check uc_functions "DATA_TYPE" BETWEEN 0 AND 21
unexpected check uc_functions "DATA_TYPE" BETWEEN 0 AND 20
```

To repair the database, stop all servers, back it up, and replace the two checks. The database
named the checks itself when it created them, so first find their names: each query below returns
one name. Put it in place of the `<...>` in the statements that follow, run them, then run
`bin/start-uc-server --upgrade-schema`.

- H2 (open the database file with the H2 shell while no server runs):

    ```sql
    -- <check of uc_functions.data_type>
    SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_name = 'UC_FUNCTIONS' AND cc.check_clause = '"DATA_TYPE" BETWEEN 0 AND 20';
    -- <check of uc_function_params.type_name>
    SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_name = 'UC_FUNCTION_PARAMS' AND cc.check_clause = '"TYPE_NAME" BETWEEN 0 AND 20';

    ALTER TABLE uc_functions DROP CONSTRAINT "<check of uc_functions.data_type>";
    ALTER TABLE uc_functions ADD CHECK (data_type BETWEEN 0 AND 21);
    ALTER TABLE uc_function_params DROP CONSTRAINT "<check of uc_function_params.type_name>";
    ALTER TABLE uc_function_params ADD CHECK (type_name BETWEEN 0 AND 21);
    ```

- PostgreSQL:

    ```sql
    -- <check of uc_functions.data_type>
    SELECT conname FROM pg_constraint WHERE conrelid = 'uc_functions'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) = 'CHECK (((data_type >= 0) AND (data_type <= 20)))';
    -- <check of uc_function_params.type_name>
    SELECT conname FROM pg_constraint WHERE conrelid = 'uc_function_params'::regclass AND contype = 'c' AND pg_get_constraintdef(oid) = 'CHECK (((type_name >= 0) AND (type_name <= 20)))';

    ALTER TABLE uc_functions DROP CONSTRAINT "<check of uc_functions.data_type>", ADD CHECK (data_type BETWEEN 0 AND 21);
    ALTER TABLE uc_function_params DROP CONSTRAINT "<check of uc_function_params.type_name>", ADD CHECK (type_name BETWEEN 0 AND 21);
    ```

- MySQL (in the Unity Catalog database, which `DATABASE()` names):

    ```sql
    -- <check of uc_functions.data_type>
    SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_schema = DATABASE() AND tc.table_name = 'uc_functions' AND cc.check_clause = '(`data_type` between 0 and 20)';
    -- <check of uc_function_params.type_name>
    SELECT tc.constraint_name FROM information_schema.table_constraints tc JOIN information_schema.check_constraints cc ON cc.constraint_schema = tc.constraint_schema AND cc.constraint_name = tc.constraint_name WHERE tc.table_schema = DATABASE() AND tc.table_name = 'uc_function_params' AND cc.check_clause = '(`type_name` between 0 and 20)';

    ALTER TABLE uc_functions DROP CHECK `<check of uc_functions.data_type>`, ADD CHECK (data_type BETWEEN 0 AND 21);
    ALTER TABLE uc_function_params DROP CHECK `<check of uc_function_params.type_name>`, ADD CHECK (type_name BETWEEN 0 AND 21);
    ```

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

### Credentials as text

Schema step V4 widens `uc_credentials.credential` from `tinytext` to `mediumtext` on MySQL, and changes
nothing on H2. On PostgreSQL it rewrites `uc_credentials`: it stores each credential as text instead of
a large object, read as UTF-8, so a database whose encoding cannot hold a stored credential's characters
fails the step. It reads every credential's large object, so run `--upgrade-schema` as a role that can
read them, such as their owner; if one cannot be read or is missing, the step fails and changes nothing;
restore that large object, or delete and recreate its credential, then run it again. On PostgreSQL and
MySQL, run schema step V4 before starting a server of this release: the server reads and writes the
column as text, so it cannot read credentials stored as large objects, and longer ones do not fit in
`tinytext`. The large objects stay in the database afterwards, holding the credentials as the earlier
release stored them, including earlier versions of updated ones: remove them with `vacuumlo -v ucdb`.
`vacuumlo` removes every large object that no column references, not only the credentials' ones, so run
it only when the database holds nothing but Unity Catalog, and check first with `vacuumlo -n -v ucdb`.
On PostgreSQL a release from before schema step V4 cannot read the converted column, so going back to it
needs the backup.

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

## Going back to the baseline release

To go back, stop the new release's servers and restore the backup you took before the upgrade.

Do not start the baseline release on the upgraded database as it is: its Hibernate setting
`hibernate.hbm2ddl.auto=update` changes column types back (it narrows the column that step 2
widened, and on MySQL the one step 4 widened; on PostgreSQL it expects large objects where step 4
stored text, see [Credentials as text](#credentials-as-text)) while the schema history still records
the steps.

## Messages

Messages that `--upgrade-schema` prints when it refuses, and what to do.

`The database has no schema history, and its schema is neither the schema of version 1 nor that of version <m>. It differs from version 1 in:`
: Followed by one line per difference (`missing ...` or `unexpected ...`). Repair the differences or
  restore the backup, then run `bin/start-uc-server --upgrade-schema` again. A database from before
  0.7.0 shows the differences that
  [Repairing a database from before 0.7.0](#repairing-a-database-from-before-070) repairs; the
  message then adds that it must be started once on a 0.7.0 server first. Other differences come
  from changes made outside Unity Catalog's steps: undo them.

`The database schema differs from the schema of version <m>:`
: Followed by the differences, as above. The schema history records this release's last version,
  but the schema is not the one this release's steps build: something changed it outside the steps.
  Undo those changes or restore the backup, then run the command again. A `missing index` line can
  end with a note that another table's index already has that name: on PostgreSQL and H2, index
  names are unique within a schema, so the step skipped Unity Catalog's index but is recorded as
  applied. Renaming the other index and running the command again does not create it; follow the
  note: restore the backup (on a new install, drop the `uc_*` tables and `uc_schema_history`),
  rename the other index or give Unity Catalog its own schema, then run the command again.

`The database's schema history does not match this release's schema steps: <reasons>`
: Flyway's check of `uc_schema_history` failed, for example because a recorded step differs from
  this release's step of the same version (its checksum), or this release lacks a recorded step.
  `<reasons>` may advise Flyway's `repair` command, which Unity Catalog doesn't have. Run the release
  that applied the steps, or restore the backup. Do not edit `uc_schema_history` by hand. On H2 and
  MySQL, a step that failed is reported here too: this release can't retry it, so restore the backup
  and fix the cause, then run the command on the restored database. On PostgreSQL, a failed step
  leaves no history row, so the command runs it again.

`Could not take the schema migration lock: <reason>`
: The command could not get the lock that keeps two migration runs apart, so it changed nothing.
  Check that the database is reachable and that its user can run the lock call
  (`pg_advisory_lock` on PostgreSQL, `GET_LOCK` on MySQL), then run the command again.
