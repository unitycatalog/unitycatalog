package io.unitycatalog.spark

import io.unitycatalog.client.model.{TableInfo => UCTableInfo}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Empty Spark-4.0 shim for [[UCProxyViewSupport]] (the real implementation lives
 * in `scala-shims/spark-4.2/`). Provides the `wrapAsView` hook that throws on Spark 4.0
 * because no view types exist there -- the shared `UCProxy.loadTable` calls into this
 * hook only when a UC row's `tableType` is in [[UCSingleCatalog.viewLikeUcTypes]], which
 * indicates the row is configured for view operations on Spark versions that support
 * them.
 */
trait UCProxyViewSupport { self: UCProxy =>
  protected def wrapAsView(t: UCTableInfo, ident: Identifier): Table =
    throw new NoSuchTableException(ident)
}
