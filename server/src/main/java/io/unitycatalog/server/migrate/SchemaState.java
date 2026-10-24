package io.unitycatalog.server.migrate;

/** What a database holds, compared with the schema steps of this build. */
public enum SchemaState {
  /** No tables, apart from Casbin's authorization rules table. */
  EMPTY,
  /** Every step of this build is applied. */
  CURRENT,
  /** Some steps of this build are not applied yet. */
  PENDING,
  /** A step newer than this build is applied: the database belongs to a newer release. */
  AHEAD,
  /** Unity Catalog's tables without Flyway's history: a database from 0.7. */
  UNVERSIONED_UC,
  /** Tables, but none of Unity Catalog's. */
  NOT_UC,
  /** An earlier run recorded a failed step. */
  FAILED
}
