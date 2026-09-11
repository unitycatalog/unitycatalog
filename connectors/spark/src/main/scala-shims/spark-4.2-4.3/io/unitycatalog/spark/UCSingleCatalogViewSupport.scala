package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.{
  Identifier,
  Relation,
  RelationCatalog,
  TableSummary,
  View
}

/**
 * Spark-4.2-only mixin providing the relation and view overrides that
 * [[org.apache.spark.sql.connector.catalog.RelationCatalog]] adds. Mixed into
 * `UCSingleCatalog` and resolved per Spark version via the `scala-shims/spark-X.Y/`
 * directory mechanism (see `project/CrossSparkVersions.scala`):
 *
 *   - `scala-shims/spark-4.0-4.1/io/unitycatalog/spark/UCSingleCatalogViewSupport.scala`: empty trait
 *   - this file (`scala-shims/spark-4.2/...`): real impl
 *
 * The trait is self-typed against [[UCSingleCatalog]] so it can read the catalog's
 * `delegate` and `ucProxy` state (declared `protected[spark]` for that purpose).
 */
trait UCSingleCatalogViewSupport extends RelationCatalog { self: UCSingleCatalog =>

  override def listViews(namespace: Array[String]): Array[Identifier] =
    ucProxy.listViews(namespace)

  /**
   * Route `SHOW TABLES` summaries straight to `ucProxy` (bypassing the `delegate`/Delta chain,
   * like the `listViews` delegate above) so the listing uses the connector's credential-free
   * `listTableSummaries` override rather than the default that `loadTable`s every table. Spark's
   * default `listRelationSummaries` composes this with `listViews`; see
   * [[UCProxyViewSupport.listTableSummaries]] for the credential-vending bug this avoids.
   */
  override def listTableSummaries(namespace: Array[String]): Array[TableSummary] =
    ucProxy.listTableSummaries(namespace)

  override def loadView(ident: Identifier): View =
    ucProxy.loadView(ident)

  override def createView(ident: Identifier, view: View): View =
    ucProxy.createView(ident, view)

  override def replaceView(ident: Identifier, view: View): View =
    throw new UnsupportedOperationException("Replacing a view is not supported yet")

  override def dropView(ident: Identifier): Boolean =
    ucProxy.dropView(ident)

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    ucProxy.renameView(oldIdent, newIdent)

  /**
   * Keep normal table loading on the delegate path. If the UC table-only path finds a view, reuse
   * the view metadata carried by its `NoSuchTableException` instead of issuing another UC lookup.
   */
  override def loadRelation(ident: Identifier): Relation = {
    try {
      delegate.loadTable(ident)
    } catch {
      case viewFound: ViewFoundDuringTableLoadException =>
        val t = viewFound.tableInfo
        if (UCViewTypes.isViewCommandsSupportedTableType(t.getTableType)) {
          ucProxy.toView(t)
        } else {
          throw new UnsupportedOperationException(
            s"Loading a ${t.getTableType} view is not supported yet")
        }
    }
  }
}
