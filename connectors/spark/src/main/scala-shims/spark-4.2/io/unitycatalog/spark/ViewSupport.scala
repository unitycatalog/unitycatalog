/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{
  Column,
  Identifier,
  MetadataTable,
  Table,
  TableInfo,
  TableViewCatalog,
  ViewInfo
}

/**
 * Spark-4.2-only mixin providing the view-side overrides that
 * [[org.apache.spark.sql.connector.catalog.TableViewCatalog]] adds. Mixed into
 * `UCSingleCatalog` and resolved per Spark version via the `scala-shims/spark-X.Y/`
 * directory mechanism (see `project/CrossSparkVersions.scala`):
 *
 *   - `scala-shims/spark-4.0/io/unitycatalog/spark/ViewSupport.scala`: empty trait
 *   - `scala-shims/spark-4.1/io/unitycatalog/spark/ViewSupport.scala`: empty trait
 *   - this file:                                                              real impl
 *
 * The trait is self-typed against [[UCSingleCatalog]] so it can read the catalog's
 * `delegate` and `ucProxy` state (declared `protected[spark]` for that purpose).
 *
 * The companion `rejectIfShimmedView` hook is the Spark-4.2-only half of the catalog's
 * `loadTable` orthogonality check: when Delta's `case o => o` fallthrough returns a
 * `MetadataTable + ViewInfo` for a view-like row, this hook converts it to the
 * `NoSuchTableException` the Spark resolver expects from a strict table-side load.
 */
trait ViewSupport extends TableViewCatalog { self: UCSingleCatalog =>

  /**
   * Spark 4.2 implementation of the view-rejection hook called by
   * [[UCSingleCatalog.loadTable]]. On 4.0/4.1 this is a no-op (no view types exist).
   */
  protected def rejectIfShimmedView(t: Table, ident: Identifier): Unit = t match {
    case mot: MetadataTable if mot.getTableInfo.isInstanceOf[ViewInfo] =>
      throw new NoSuchTableException(ident)
    case _ => ()
  }

  override def listViews(namespace: Array[String]): Array[Identifier] =
    ucProxy.listViews(namespace)

  override def loadView(ident: Identifier): ViewInfo =
    ucProxy.loadView(ident)

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo =
    ucProxy.createView(ident, info)

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo =
    throw new UnsupportedOperationException("Replacing a view is not supported yet")

  override def dropView(ident: Identifier): Boolean =
    ucProxy.dropView(ident)

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    ucProxy.renameView(oldIdent, newIdent)

  /**
   * Single-RPC entry for `TableViewCatalog`. Forwards to `delegate.loadTable(ident)`,
   * which (when `delegate` is Delta wrapping `UCProxy`) chains
   * `Delta.loadTable -> super.loadTable (DelegatingCatalogExtension) -> UCProxy.loadTable`.
   * `UCProxy.loadTable` issues exactly one UC `getTable` and returns either a `V1Table`
   * for normal tables or a `MetadataTable + ViewInfo` for views; Delta's `case o => o`
   * fallthrough returns the `MetadataTable` unchanged. Spark's resolver then
   * discriminates on the returned type to route through the table or view path.
   */
  override def loadTableOrView(ident: Identifier): Table = delegate.loadTable(ident)

  /**
   * Spark-4.2-only `createTable(Identifier, TableInfo)` overload. Spark 4.2 routes CREATE TABLE
   * through this entry point; reuse the shared 4-arg path so external-table validation and
   * managed-Delta staging stay centralized.
   */
  override def createTable(
      ident: Identifier,
      tableInfo: TableInfo): Table = {
    val columns = tableInfo.columns().map { field =>
      Column.create(field.name, field.dataType, field.comment, field.metadata)
    }.toArray
    createTable(ident, columns, tableInfo.partitions(), tableInfo.properties())
  }
}
