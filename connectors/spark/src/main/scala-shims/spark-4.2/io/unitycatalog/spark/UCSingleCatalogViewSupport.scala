package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, NoSuchViewException}
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  Relation,
  RelationCatalog,
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
   * Keep normal table loading on the delegate path. If the delegate rejects the identifier as
   * a table, try the UC view path with any view row from that load memoized.
   */
  override def loadRelation(ident: Identifier): Relation = {
    ucProxy.memoizeViewOnLoadTable(ident)
    try {
      try {
        delegate.loadTable(ident)
      } catch {
        case tableNotFound: NoSuchTableException =>
          try {
            ucProxy.loadView(ident)
          } catch {
            case _: NoSuchViewException => throw tableNotFound
          }
      }
    } finally {
      ucProxy.clearViewMemo()
    }
  }
}
