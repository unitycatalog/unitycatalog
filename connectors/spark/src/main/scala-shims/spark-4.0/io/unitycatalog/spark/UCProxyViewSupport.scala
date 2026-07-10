package io.unitycatalog.spark

import io.unitycatalog.client.model.{TableInfo => UCTableInfo}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark 4.0 does not support the metric-view API, so this shim leaves views unsupported: the
 * `wrapAsView` hook throws because no view types exist on this Spark version. The shared
 * `UCProxy.loadTable` only calls this hook for a view-like UC `tableType`, which cannot occur on a
 * Spark version without view support.
 */
trait UCProxyViewSupport { self: UCProxy =>
  protected def wrapAsView(t: UCTableInfo, ident: Identifier): Table =
    throw new NoSuchTableException(ident)
}
