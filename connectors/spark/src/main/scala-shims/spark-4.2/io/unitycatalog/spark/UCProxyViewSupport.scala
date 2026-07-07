package io.unitycatalog.spark

import io.unitycatalog.client.model.{TableInfo => UCTableInfo}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark-4.2 placeholder for [[UCProxyViewSupport]]. Scaffolding only: this shim currently mirrors
 * the no-op 4.0/4.1 version (the `wrapAsView` hook throws `NoSuchTableException`). The real
 * Spark-4.2 implementation -- wrapping a view-like UC row as a `MetadataTable`+`ViewInfo` and
 * implementing the view CRUD surface -- is added in a follow-up change once the connector's view
 * glue lands.
 */
trait UCProxyViewSupport { self: UCProxy =>
  protected def wrapAsView(t: UCTableInfo, ident: Identifier): Table =
    throw new NoSuchTableException(ident)
}
