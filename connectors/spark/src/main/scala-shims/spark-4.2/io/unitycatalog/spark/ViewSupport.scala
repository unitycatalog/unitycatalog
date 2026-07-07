package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark-4.2 placeholder for [[ViewSupport]]. Scaffolding only: this shim currently mirrors the
 * no-op 4.0/4.1 versions (the `rejectIfShimmedView` hook is a no-op and no view CRUD is wired in
 * yet). The real Spark-4.2 view implementation -- mixing in the native
 * `org.apache.spark.sql.connector.catalog.TableViewCatalog` and delegating `createView`/`loadView`
 * /`dropView`/... to the UC proxy -- is added in a follow-up change once the connector's view
 * glue lands.
 */
trait ViewSupport { self: UCSingleCatalog =>
  protected def rejectIfShimmedView(t: Table, ident: Identifier): Unit = ()
}
