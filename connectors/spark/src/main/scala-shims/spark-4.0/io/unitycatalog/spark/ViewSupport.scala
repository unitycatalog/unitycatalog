package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark 4.0 does not have the metric-view APIs (`TableViewCatalog`, `loadTableOrView`,
 * `createView`, `loadView`, ...), so this shim provides `rejectIfShimmedView` as a no-op. It
 * lets the shared [[UCSingleCatalog.loadTable]] body compile and run without referencing any
 * view-only types. The `with TableViewCatalog` mixin on `UCSingleCatalog` resolves to the
 * [[org.apache.spark.sql.connector.catalog.TableViewCatalog]] stub trait also defined in this
 * shim directory.
 */
trait ViewSupport { self: UCSingleCatalog =>
  protected def rejectIfShimmedView(t: Table, ident: Identifier): Unit = ()
}
