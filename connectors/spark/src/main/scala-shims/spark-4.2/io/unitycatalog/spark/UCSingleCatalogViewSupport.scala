package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  Relation,
  RelationCatalog,
  View
}

/**
 * Spark-4.2-only mixin providing the view-side overrides that the Spark 4.2
 * [[org.apache.spark.sql.connector.catalog.RelationCatalog]] adds. Mixed into
 * `UCSingleCatalog` and resolved per Spark version via the `scala-shims/spark-X.Y/`
 * directory mechanism (see `project/CrossSparkVersions.scala`):
 *
 *   - `scala-shims/spark-4.0-4.1/io/unitycatalog/spark/UCSingleCatalogViewSupport.scala`: empty trait
 *   - this file (`scala-shims/spark-4.2/...`): real impl
 *
 * The trait is self-typed against [[UCSingleCatalog]] so it can read the catalog's
 * `delegate` and `ucProxy` state (declared `protected[spark]` for that purpose).
 *
 * `loadRelation` is the Spark 4.2 load entry point Spark's resolver calls directly on this catalog
 * (see `RelationResolution`): a `RelationCatalog` is asked for `loadRelation(ident)` and only a
 * returned `Relation` or a `NoSuchTableException` is understood on that path. Table loads route
 * through `delegate` (Delta wrapping `UCProxy`) so Delta-specific read/write behavior is
 * preserved; views route through `ucProxy` directly since Delta has no role in view handling.
 */
trait UCSingleCatalogViewSupport extends RelationCatalog { self: UCSingleCatalog =>

  /**
   * Loads a table or a view in exactly ONE UC `getTable` RPC (Spark 4.2 cost model). Spark's
   * resolver calls `loadRelation` directly on this catalog (see `RelationResolution`) and only
   * understands a returned `Relation` or a `NoSuchTableException` on that path.
   *
   * We issue the single discrimination RPC via `ucProxy.getUCTableLike(ident)`, stash the result
   * in `ucProxy`'s per-thread consume-once memo, then dispatch through the SAME downstream paths
   * as before -- each of which re-enters `getUCTableLike` and hits the memo (0 extra RPC):
   *   - a view routes to `ucProxy.loadView` -> `getUCTableLike` (memo hit),
   *   - a table routes to `delegate.loadTable` (Delta wrapping `UCProxy`, or `UCProxy` itself)
   *     -> `ucProxy.loadTable` -> `getUCTableLike` (memo hit). Going through `delegate` preserves
   *     Delta's V1Table -> DeltaTableV2 decoration and server-side planning.
   * The whole sequence is synchronous on one thread; the `finally` clears the memo so nothing
   * leaks past this resolution (success, exception, 404, or an unmapped view kind).
   */
  override def loadRelation(ident: Identifier): Relation = {
    // One discrimination RPC. Stash and clear are both scoped by this try/finally so EVERY path
    // that can populate the memo is lexically bracketed by the `finally` -- no early return can
    // leave a stale entry on a pooled analyzer thread.
    val t = ucProxy.getUCTableLike(ident)
    try {
      ucProxy.stashGetUCTableLike(ident, t)
      t match {
        case None =>
          // Genuinely absent: the resolver maps `NoSuchTableException` to "relation not found".
          throw new NoSuchTableException(ident)
        case Some(info) if UCViewTypes.isViewLikeTableType(info.getTableType) =>
          if (UCViewTypes.isViewCommandsSupportedTableType(info.getTableType)) {
            // View route: `loadView` -> `getUCTableLike` consumes the memo (0 extra RPC).
            ucProxy.loadView(ident)
          } else {
            // A listed-but-unmapped view kind (e.g. MATERIALIZED_VIEW). On the resolver's load
            // path only a `Relation` or `NoSuchTableException` is understood, so surface
            // `NoSuchTableException` (matching the previous `loadRelation` semantics); the richer
            // "not supported yet" message is reserved for the explicit `loadView` override. The
            // memoized `Some(info)` is never consumed here but the `finally` clears it.
            throw new NoSuchTableException(ident)
          }
        case Some(_) =>
          // Table route: `delegate.loadTable` -> `ucProxy.loadTable` -> `getUCTableLike` consumes
          // the memo (0 extra RPC).
          delegate.loadTable(ident)
      }
    } finally {
      ucProxy.clearGetUCTableLike()
    }
  }

  override def listViews(namespace: Array[String]): Array[Identifier] =
    ucProxy.listViews(namespace)

  override def loadView(ident: Identifier): View =
    ucProxy.loadView(ident)

  override def createView(ident: Identifier, info: View): View =
    ucProxy.createView(ident, info)

  override def replaceView(ident: Identifier, info: View): View =
    throw new UnsupportedOperationException("Replacing a view is not supported yet")

  override def dropView(ident: Identifier): Boolean =
    ucProxy.dropView(ident)

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit =
    ucProxy.renameView(oldIdent, newIdent)
}
