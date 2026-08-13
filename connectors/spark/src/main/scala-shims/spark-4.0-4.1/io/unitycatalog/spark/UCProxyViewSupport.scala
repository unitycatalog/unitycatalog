package io.unitycatalog.spark

import scala.collection.JavaConverters._

import io.unitycatalog.client.model.{TableInfo => UCTableInfo, TableType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, SchemaCompensation}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.types.StructType

/**
 * Spark 4.0 / 4.1 lack the Spark 4.2 v2 view API (`RelationCatalog` / `ViewCatalog` / `View`), so
 * views cannot be created, listed as views, replaced, renamed, or dropped through the connector on
 * these versions. Plain SQL views are still readable: they are surfaced on the table listing and
 * resolved from their SQL text via a V1 VIEW `CatalogTable`. Metric and materialized views stay
 * inert.
 */
trait UCProxyViewSupport { self: UCProxy =>

  protected[spark] def loadViewLikeFromTableSurface(t: UCTableInfo, ident: Identifier): Table =
    if (t.getTableType == TableType.VIEW) buildV1ViewTable(t)
    else throw new NoSuchTableException(ident)

  protected[spark] def hideFromTableListing(tableType: TableType): Boolean =
    UCViewTypes.isViewLikeTableType(tableType) && tableType != TableType.VIEW

  // A UC view has no storage or data source format; Spark resolves it from its SQL text. Returning
  // a VIEW `CatalogTable` routes resolution through Spark's relation resolver, which parses
  // `viewText` against the view's default catalog/namespace.
  protected[spark] def buildV1ViewTable(t: UCTableInfo): Table = {
    val identifier = TableIdentifier(t.getName, Some(t.getSchemaName), Some(t.getCatalogName))
    val fields = Option(t.getColumns).map(_.asScala).getOrElse(Seq.empty)
      .map(self.toStructField).toArray
    val base = Option(t.getProperties).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    // Spark 4.2 surfaces these through the View API (withQueryColumnNames / withSchemaMode); on v1
    // they are read from properties (viewQueryColumnNames / viewSchemaModeFromProperties), so
    // populate them here for parity. The `view.sqlConfig.*` keys are already carried in `base`.
    // Query-output names can differ from declared names (e.g. `CREATE VIEW v(a,b) AS SELECT x,y`),
    // and Spark matches by them; prefer the persisted `view.query.out.*` in `base`, synthesizing
    // from declared columns only when absent (deriving would risk INCOMPATIBLE_VIEW_SCHEMA_CHANGE).
    val queryOut =
      if (base.contains(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS) || fields.isEmpty) {
        Map.empty[String, String]
      } else {
        Map(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS -> fields.length.toString) ++
          fields.zipWithIndex.map { case (f, i) =>
            s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$i" -> f.name
          }
      }
    val schemaModeDefault =
      if (base.contains(CatalogTable.VIEW_SCHEMA_MODE)) Map.empty[String, String]
      else Map(CatalogTable.VIEW_SCHEMA_MODE -> SchemaCompensation.toString)
    // Unqualified refs resolve against the creation-time catalog/namespace, which can differ from
    // the view's location; prefer the persisted `view.catalogAndNamespace.*` in `base`, falling
    // back to the view's location only when absent (`self.name()` is a same-namespace fallback).
    val viewNamespaceProps =
      if (base.contains(CatalogTable.VIEW_CATALOG_AND_NAMESPACE)) Map.empty[String, String]
      else CatalogTable.catalogAndNamespaceToProps(self.name(), Seq(t.getSchemaName))
    val viewTable = CatalogTable(
      identifier = identifier,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = StructType(fields),
      viewText = Option(t.getViewDefinition),
      comment = Option(t.getComment),
      properties = base ++ queryOut ++ schemaModeDefault ++ viewNamespaceProps,
      createTime = t.getCreatedAt,
      tracksPartitionsInCatalog = false
    )
    self.asV1Table(viewTable)
  }
}
