package io.unitycatalog.spark

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * (De)serialization of Spark's internal `view.*` metadata to/from a UC properties map.
 *
 * Spark's v2 `View` exposes the resolution-critical view metadata (SQL configs, query-output
 * column names, creation catalog/namespace, ...) through typed accessors, but persists it as a flat
 * bag of `view.*` string properties (`CatalogTable.VIEW_*`). UC has no first-class fields for these,
 * so the connector round-trips them through the generic properties map. This object owns that
 * encoding in one place, kept free of Spark-4.2-only types so it compiles on all supported Spark
 * versions and is shared by the Spark 4.2 `createView` / `toView` load paths.
 *
 * Distinct from [[UCViewTypes]], which owns the UC-`TableType`-to-Spark-view-kind mapping.
 */
private[spark] object UCViewProperties {

  /**
   * Splits the `VIEW_SQL_CONFIG_PREFIX`-prefixed entries out of a UC properties map and returns
   * them un-prefixed, as Spark's `View.sqlConfigs()` expects.
   */
  def extractSqlConfigs(properties: util.Map[String, String]): util.Map[String, String] = {
    val configs = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (k.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX)) {
        configs.put(k.substring(CatalogTable.VIEW_SQL_CONFIG_PREFIX.length), v)
      }
    }
    configs
  }

  /** Persists Spark's query-output column names into UC view properties. */
  def addQueryColumnNames(
      properties: util.Map[String, String],
      queryColumnNames: Array[String]): Unit = {
    if (queryColumnNames.isEmpty) {
      return
    }
    properties.put(
      CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS,
      queryColumnNames.length.toString)
    queryColumnNames.zipWithIndex.foreach { case (name, index) =>
      properties.put(s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$index", name)
    }
  }

  /** Persists the catalog/namespace active when the view was created. */
  def addCreationContext(
      properties: util.Map[String, String],
      currentCatalog: String,
      currentNamespace: Array[String]): Unit = {
    CatalogTable.catalogAndNamespaceToProps(currentCatalog, currentNamespace.toSeq).foreach {
      case (key, value) => properties.put(key, value)
    }
  }

  /**
   * Returns the persisted query-output column names when present. Absent keys mean the view was
   * written by an older connector and the read path should fall back to the stored column names.
   */
  def extractQueryColumnNames(properties: util.Map[String, String]): Option[Array[String]] = {
    Option(properties.get(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS)).flatMap { numCols =>
      val count = numCols.toInt
      if (count == 0) {
        None
      } else {
        Some((0 until count).map { index =>
          val key = s"${CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX}$index"
          Option(properties.get(key)).getOrElse(
            throw new IllegalStateException(
              s"Corrupted view metadata: expected $count query-output columns but $key is missing"))
        }.toArray)
      }
    }
  }

  /** Returns the persisted creation-time catalog and namespace when present. */
  def extractCreationContext(
      properties: util.Map[String, String]): Option[(String, Array[String])] = {
    Option(properties.get(CatalogTable.VIEW_CATALOG_AND_NAMESPACE)).flatMap { numParts =>
      val count = numParts.toInt
      if (count == 0) {
        None
      } else {
        val parts = (0 until count).map { index =>
          val key = s"${CatalogTable.VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX}$index"
          Option(properties.get(key)).getOrElse(
            throw new IllegalStateException(
              s"Corrupted view metadata: expected $count catalog/namespace parts but $key is missing"))
        }.toSeq
        Some((parts.head, parts.tail.toArray))
      }
    }
  }

  /**
   * Drops view metadata keys that Spark surfaces through typed {@code View} fields so they do not
   * leak into user-visible {@code properties()} or get double-persisted on a round trip.
   */
  def stripInternalViewProperties(properties: util.Map[String, String]): Unit = {
    properties.keySet().removeIf(_.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX))
    properties.remove(CatalogTable.VIEW_SCHEMA_MODE)
    properties.remove(CatalogTable.VIEW_QUERY_OUTPUT_NUM_COLUMNS)
    properties.keySet().removeIf(_.startsWith(CatalogTable.VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX))
    properties.remove(CatalogTable.VIEW_CATALOG_AND_NAMESPACE)
    properties.keySet().removeIf(_.startsWith(CatalogTable.VIEW_CATALOG_AND_NAMESPACE_PART_PREFIX))
  }
}
