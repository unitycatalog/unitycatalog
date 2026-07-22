package io.unitycatalog.spark

import io.unitycatalog.client.model.{TableInfo => UCTableInfo, TableType}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table}

/**
 * Spark 4.0 / 4.1 lack the Spark 4.2 v2 view API (`RelationCatalog` / `ViewCatalog` / `View`), so
 * views cannot be created, listed as views, replaced, renamed, or dropped through the connector on
 * these versions. Plain SQL views are still readable: they are surfaced on the table listing and
 * resolved from their SQL text via a V1 VIEW `CatalogTable`. Metric and materialized views stay
 * inert.
 */
trait UCProxyViewSupport { self: UCProxy =>

  protected[spark] def loadViewLikeFromTableSurface(t: UCTableInfo, ident: Identifier): Table =
    if (t.getTableType == TableType.VIEW) self.buildV1ViewTable(t)
    else throw new NoSuchTableException(ident)

  protected[spark] def hideFromTableListing(tableType: TableType): Boolean =
    UCViewTypes.isViewLikeTableType(tableType) && tableType != TableType.VIEW
}
