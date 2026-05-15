/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Empty Spark-4.0 shim for [[ViewSupport]] (the real implementation lives in
 * `scala-shims/spark-4.2/`). Provides the `rejectIfShimmedView` hook as a no-op so that
 * the shared [[UCSingleCatalog.loadTable]] body compiles and runs on Spark 4.0 without
 * any reference to Spark-4.2-only types (`MetadataTable`, `ViewInfo`).
 *
 * Spark 4.0 / 4.1 do not have the metric-view APIs (`TableViewCatalog`, `loadTableOrView`,
 * `createView`, `loadView`, ...). The `with TableViewCatalog` mixin on `UCSingleCatalog`
 * resolves to the [[org.apache.spark.sql.connector.catalog.TableViewCatalog]] stub trait
 * also defined in this shim directory.
 */
trait ViewSupport { self: UCSingleCatalog =>
  protected def rejectIfShimmedView(t: Table, ident: Identifier): Unit = ()
}
