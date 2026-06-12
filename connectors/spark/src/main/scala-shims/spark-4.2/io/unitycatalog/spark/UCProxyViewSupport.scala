/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.unitycatalog.spark

import java.util

import scala.collection.JavaConverters._

import io.unitycatalog.client.ApiException
import io.unitycatalog.client.model.{
  ColumnInfo,
  ColumnTypeName,
  CreateTable,
  Dependency => UCDependency,
  DependencyList => UCDependencyList,
  TableDependency => UCTableDependency,
  TableInfo => UCTableInfo,
  TableType
}
import io.unitycatalog.client.api.TablesApi
import org.apache.spark.sql.catalyst.analysis.{
  NoSuchTableException,
  NoSuchViewException,
  TableAlreadyExistsException,
  ViewAlreadyExistsException
}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{
  Dependency,
  DependencyList,
  Identifier,
  MetadataTable,
  TableCatalog,
  TableDependency,
  TableViewCatalog,
  Table,
  TableInfo,
  ViewInfo
}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.DataType

/**
 * Spark-4.2-only mixin providing the view-side overrides that
 * [[org.apache.spark.sql.connector.catalog.TableViewCatalog]] adds to `UCProxy`. Mixed
 * into `UCProxy` and resolved per Spark version via the `scala-shims/spark-X.Y/`
 * directory mechanism.
 *
 * Self-typed against [[UCProxy]] so it can read the proxy's `tablesApi`, `name()`, and
 * helper methods. The companion `wrapAsView` hook is the Spark-4.2-only half of
 * `UCProxy.loadTable`'s view-dispatch (on Spark 4.0/4.1 the empty trait throws
 * `NoSuchTableException` instead because no view types exist).
 */
trait UCProxyViewSupport extends TableViewCatalog { self: UCProxy =>

  /**
   * Spark 4.2: wrap a view-like UC row as `MetadataTable + ViewInfo`.
   *
   * `loadTable` routes every view-like row here (per `isViewLikeTableType`, which includes
   * listed-but-unmapped kinds like `MATERIALIZED_VIEW`). Spark's resolver calls `loadTable`
   * during ordinary identifier resolution and only understands a returned `Table` or a
   * `NoSuchTableException` -- so a kind we list but cannot build a Spark `ViewInfo` for (no
   * `TableSummary` mapping) must surface as `NoSuchTableException`, not the
   * `UnsupportedOperationException` that `toViewInfo` -> `ucTableTypeToSparkViewType` would
   * otherwise throw.
   */
  protected def wrapAsView(t: UCTableInfo, ident: Identifier): Table =
    if (UCSingleCatalog.isViewCommandsSupportedTableType(t.getTableType)) {
      new MetadataTable(toViewInfo(t), ident.toString)
    } else {
      throw new NoSuchTableException(ident)
    }

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    listUcTables(namespace)
      .filter(t => UCSingleCatalog.isViewLikeTableType(t.getTableType))
      .map(table => Identifier.of(namespace, table.getName))
      .toArray
  }

  override def loadTableOrView(ident: Identifier): Table = loadTable(ident)

  override def loadView(ident: Identifier): ViewInfo = {
    val t = getUcTable(ident)
    // Gate on the command-supported predicate (excludes listed-but-unmapped kinds like
    // MATERIALIZED_VIEW). A kind we list but have no Spark `TableSummary` mapping for cannot be
    // turned into a `ViewInfo` -- `toViewInfo -> ucTableTypeToSparkViewType` would throw
    // `UnsupportedOperationException`, which `DESCRIBE`/`SHOW`/view-resolution callers don't
    // understand. Surface `NoSuchViewException` instead, mirroring `wrapAsView`.
    if (!UCSingleCatalog.isViewCommandsSupportedTableType(t.getTableType)) {
      throw new NoSuchViewException(ident)
    }
    toViewInfo(t)
  }

  override def createView(ident: Identifier, info: ViewInfo): ViewInfo = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val properties: util.Map[String, String] = info.properties()
    val sparkTableType = properties.get(TableCatalog.PROP_TABLE_TYPE)
    val ucTableType = UCSingleCatalog.sparkViewTypeToUcTableType(sparkTableType).getOrElse {
      throw new ApiException(
        s"Unity Catalog does not support creating $sparkTableType via ViewCatalog.createView")
    }

    val ct = new CreateTable()
      .name(ident.name())
      .schemaName(ident.namespace().head)
      .catalogName(this.name)
      .tableType(ucTableType)
      .viewDefinition(info.queryText())

    Option(properties.get(TableCatalog.PROP_COMMENT)).foreach(ct.setComment)
    Option(info.viewDependencies()).foreach { deps =>
      ct.setViewDependencies(toUcDependencyList(deps))
    }
    ct.setColumns(buildColumnInfos(info, convertDataTypeToTypeName).asJava)

    val propertiesToServer = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!UCTableProperties.V2_TABLE_PROPERTIES.contains(k)) {
        propertiesToServer.put(k, v)
      }
    }
    info.sqlConfigs().asScala.foreach { case (k, v) =>
      propertiesToServer.put(CatalogTable.VIEW_SQL_CONFIG_PREFIX + k, v)
    }
    ct.setProperties(propertiesToServer)

    try {
      tablesApi.createTable(ct)
    } catch {
      case e: ApiException if e.getCode == 409 =>
        throw new ViewAlreadyExistsException(ident)
    }
    loadView(ident)
  }

  override def replaceView(ident: Identifier, info: ViewInfo): ViewInfo = {
    throw new UnsupportedOperationException("Replacing a view is not supported yet")
  }

  override def dropView(ident: Identifier): Boolean = {
    val t = try {
      getUcTable(ident)
    } catch {
      case _: NoSuchTableException => return false
    }
    if (!UCSingleCatalog.isViewCommandsSupportedTableType(t.getTableType)) {
      return false
    }
    tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident)) == 200
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a view is not supported yet")
  }

  /**
   * Spark-4.2 `createTable(Identifier, TableInfo)` overload. Delegates to the legacy
   * 4-arg overload that carries all the create-table logic.
   */
  override def createTable(ident: Identifier, tableInfo: TableInfo): Table = {
    createTable(ident, tableInfo.schema(), tableInfo.partitions(), tableInfo.properties())
  }

  /**
   * Builds a Spark `ViewInfo` from a UC `UCTableInfo` for view-like rows. Round-trips
   * dataType / nullable / comment / metadata via the in-connector [[UCColumnJson]]
   * helper.
   */
  private[spark] def toViewInfo(t: UCTableInfo): ViewInfo = {
    val columns = t.getColumns.asScala.map { col =>
      UCColumnJson.parseStructFieldJson(col.getTypeJson)
    }.toArray

    val props = new util.HashMap[String, String]()
    Option(t.getProperties).foreach(props.putAll)
    val sqlConfigs = extractSqlConfigs(props)
    // The VIEW_SQL_CONFIG_PREFIX keys are surfaced (un-prefixed) via `withSqlConfigs`; drop them
    // from `props` so they don't also leak into the user-visible `properties()` map and get
    // re-persisted (double-counted) on a createView/replace round-trip.
    props.keySet().removeIf(_.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX))

    val builder = new ViewInfo.Builder()
      .withColumns(columns)
      .withProperties(props)
      .withTableType(UCSingleCatalog.ucTableTypeToSparkViewType(t.getTableType))
      .withQueryText(t.getViewDefinition)
      .withCurrentCatalog(t.getCatalogName)
      .withCurrentNamespace(Array(t.getSchemaName))
      .withSqlConfigs(sqlConfigs)
      .withSchemaMode("UNSUPPORTED")
      .withQueryColumnNames(columns.map(_.name()))
    Option(t.getComment).foreach(builder.withComment)
    Option(t.getViewDependencies).foreach { ucDeps =>
      builder.withViewDependencies(fromUcDependencyList(ucDeps))
    }
    builder.build()
  }

  private def extractSqlConfigs(properties: util.Map[String, String]): util.Map[String, String] = {
    val configs = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (k.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX)) {
        configs.put(k.substring(CatalogTable.VIEW_SQL_CONFIG_PREFIX.length), v)
      }
    }
    configs
  }

  /**
   * Converts Spark's typed `DependencyList` into the wire-format UC `DependencyList`.
   * Only `TableDependency` is currently translated; UC OSS does not persist function
   * dependencies. The conversion is dot-flatten lossy for identifiers with literal `.`,
   * matching the wire format already used by the UC server.
   */
  private def toUcDependencyList(sparkDeps: DependencyList): UCDependencyList = {
    val ucDeps = new java.util.ArrayList[UCDependency]()
    sparkDeps.dependencies().foreach {
      case td: TableDependency =>
        ucDeps.add(new UCDependency()
          .table(new UCTableDependency()
            .tableFullName(td.nameParts().mkString("."))))
      case _ =>
      // UC OSS does not currently persist function dependencies; drop.
    }
    new UCDependencyList().dependencies(ucDeps)
  }

  /**
   * Converts the wire-format UC `DependencyList` back into Spark's typed `DependencyList`.
   */
  private def fromUcDependencyList(ucDeps: UCDependencyList): DependencyList = {
    val list = Option(ucDeps.getDependencies)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
    val sparkDeps: Array[Dependency] = list.flatMap { d =>
      Option(d.getTable).map { td =>
        Dependency.table(td.getTableFullName.split("\\.", -1)).asInstanceOf[Dependency]
      }
    }.toArray
    DependencyList.of(sparkDeps)
  }

  /**
   * Builds the UC column-info list for `createView` from a Spark `TableInfo` (or its
   * `ViewInfo` subtype). Preserves nullability, comment, partition position, and the
   * canonical Spark column JSON in `typeJson` so column-level analyzer metadata
   * (`metric_view.type`, `metric_view.expr`) survives the round-trip through Unity Catalog.
   */
  private def buildColumnInfos(
      tableInfo: TableInfo,
      convertDataTypeToTypeName: DataType => ColumnTypeName): Seq[ColumnInfo] = {
    val partitionColNames = extractPartitionColumnNames(tableInfo.partitions())
    tableInfo.columns().toSeq.zipWithIndex.map { case (col, i) =>
      val column = new ColumnInfo()
      column.setName(col.name)
      column.setNullable(col.nullable)
      column.setTypeText(col.dataType.catalogString)
      column.setTypeName(convertDataTypeToTypeName(col.dataType))
      column.setTypeJson(UCColumnJson.buildStructFieldJson(col))
      column.setPosition(i)
      Option(col.comment).foreach(column.setComment(_))
      val partitionIdx = partitionColNames.indexWhere(_.equalsIgnoreCase(col.name))
      if (partitionIdx >= 0) column.setPartitionIndex(partitionIdx)
      column
    }
  }

  private def extractPartitionColumnNames(partitions: Array[Transform]): Seq[String] = {
    if (partitions == null) return Seq.empty
    partitions.flatMap { t =>
      t.name() match {
        case "identity" =>
          val fieldNames = t.references().flatMap(_.fieldNames())
          require(fieldNames.length == 1,
            s"Expected single-field partition reference but got: ${fieldNames.mkString(".")}")
          Some(fieldNames.head)
        case "cluster_by" =>
          None
        case other =>
          throw new ApiException(s"Unsupported partition transform: $other")
      }
    }.toSeq
  }
}
