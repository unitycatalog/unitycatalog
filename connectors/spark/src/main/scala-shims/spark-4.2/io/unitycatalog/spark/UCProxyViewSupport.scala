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
import org.apache.spark.sql.catalyst.analysis.{
  NoSuchTableException,
  NoSuchViewException,
  SchemaCompensation,
  ViewAlreadyExistsException
}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{
  Dependency,
  DependencyList,
  Identifier,
  Relation,
  RelationCatalog,
  Table,
  TableCatalog,
  TableDependency,
  TableSummary,
  View
}
import org.apache.spark.sql.types.DataType

/**
 * Spark-4.2-only mixin providing the relation and view overrides that
 * [[org.apache.spark.sql.connector.catalog.RelationCatalog]] adds to `UCProxy`. Mixed
 * into `UCProxy` and resolved per Spark version via the `scala-shims/spark-X.Y/`
 * directory mechanism.
 *
 * Self-typed against [[UCProxy]] so it can read the proxy's `tablesApi`, `name()`, and
 * helper methods.
 */
trait UCProxyViewSupport extends RelationCatalog { self: UCProxy =>

  override def listViews(namespace: Array[String]): Array[Identifier] = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(namespace)
    listUCTableLikes(namespace)
      .filter(t => UCViewTypes.isViewLikeTableType(t.getTableType))
      .map(table => Identifier.of(namespace, table.getName))
      .toArray
  }

  // View-like rows are served through the RelationCatalog/ViewCatalog surface, so reject them from
  // the table surface. Carry the fetched row in the exception so `UCSingleCatalog.loadRelation`'s
  // fallback can build the view without a second `getTable`.
  protected[spark] def loadViewLikeFromTableSurface(t: UCTableInfo, ident: Identifier): Table =
    throw new ViewFoundDuringTableLoadException(ident, t)

  protected[spark] def hideFromTableListing(tableType: TableType): Boolean =
    UCViewTypes.isViewLikeTableType(tableType)

  override def loadRelation(ident: Identifier): Relation = {
    val t = getUCTableLike(ident).getOrElse(throw new NoSuchTableException(ident))
    if (UCViewTypes.isViewLikeTableType(t.getTableType)) {
      if (UCViewTypes.isViewCommandsSupportedTableType(t.getTableType)) {
        toView(t)
      } else {
        throw new NoSuchTableException(ident)
      }
    } else {
      loadV1Table(t)
    }
  }

  override def loadView(ident: Identifier): View = {
    val t = getUCTableLike(ident).getOrElse(throw new NoSuchViewException(ident))
    if (!UCViewTypes.isViewLikeTableType(t.getTableType)) {
      throw new NoSuchViewException(ident)
    }
    if (!UCViewTypes.isViewCommandsSupportedTableType(t.getTableType)) {
      // A listed-but-unmapped view kind (e.g. MATERIALIZED_VIEW, `None` in `viewLikeUcTypes`):
      // it DOES appear in `listViews`, so reporting `NoSuchViewException` here would be
      // confusing ("SHOW VIEWS lists it, DESCRIBE says it's missing"). Report that the kind is
      // not loadable yet instead. (`loadRelation` on the table-resolution path still throws
      // `NoSuchTableException` because Spark's table resolver only understands that.)
      throw new UnsupportedOperationException(
        s"Loading a ${t.getTableType} view is not supported yet")
    }
    toView(t)
  }

  override def createView(ident: Identifier, view: View): View = {
    UCSingleCatalog.checkUnsupportedNestedNamespace(ident.namespace())
    val properties: util.Map[String, String] = view.properties()
    // A plain `CREATE VIEW` may omit PROP_TABLE_TYPE; treat that as a regular VIEW.
    val sparkTableType =
      Option(properties.get(TableCatalog.PROP_TABLE_TYPE)).getOrElse(TableSummary.VIEW_TABLE_TYPE)
    val ucTableType = UCViewTypes.sparkViewTypeToUcTableType(sparkTableType).getOrElse {
      throw new ApiException(
        s"Unity Catalog does not support creating $sparkTableType via ViewCatalog.createView")
    }

    val ct = new CreateTable()
      .name(ident.name())
      .schemaName(ident.namespace().head)
      .catalogName(this.name)
      .tableType(ucTableType)
      .viewDefinition(view.queryText())

    Option(properties.get(TableCatalog.PROP_COMMENT)).foreach(ct.setComment)
    // The server requires a non-null dependency list. Spark only fills `viewDependencies()` for
    // metric views; for a plain view it is null, so derive the base tables from the query text.
    val ucDeps = Option(view.viewDependencies()) match {
      case Some(deps) => toUcDependencyList(deps)
      case None =>
        ucDependencyListFromNames(
          UCViewDependencies.derive(
            view.queryText(), view.currentCatalog(), view.currentNamespace().toSeq))
    }
    ct.setViewDependencies(ucDeps)
    ct.setColumns(buildColumnInfos(view, convertDataTypeToTypeName).asJava)

    val propertiesToServer = new util.HashMap[String, String]()
    properties.asScala.foreach { case (k, v) =>
      if (!UCTableProperties.V2_TABLE_PROPERTIES.contains(k)) {
        propertiesToServer.put(k, v)
      }
    }
    view.sqlConfigs().asScala.foreach { case (k, v) =>
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

  override def replaceView(ident: Identifier, view: View): View = {
    throw new UnsupportedOperationException("Replacing a view is not supported yet")
  }

  override def dropView(ident: Identifier): Boolean = {
    val t = getUCTableLike(ident) match {
      case Some(t) => t
      case None => return false
    }
    // Known limitation: a listed-but-unmapped view kind (e.g. MATERIALIZED_VIEW, `None` in
    // `viewLikeUcTypes`) is rejected here AND by `UCProxy.dropTable` (which passive-filters all
    // `isViewLikeTableType` rows), so such a row is not droppable through this connector and a
    // `DROP ... IF EXISTS` no-ops. This is intentional: the connector treats unsupported view
    // kinds as uniformly inert (no load / create / drop) rather than allowing a drop of a kind it
    // can't otherwise handle. Revisit when MATERIALIZED_VIEW becomes a supported (mapped) kind.
    if (!UCViewTypes.isViewCommandsSupportedTableType(t.getTableType)) {
      return false
    }
    // `deleteTable` returns the (empty) response body, not an HTTP status; a real failure throws
    // ApiException. Issue the delete for its side effect and report success.
    tablesApi.deleteTable(UCSingleCatalog.fullTableNameForApi(this.name, ident))
    true
  }

  override def renameView(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Renaming a view is not supported yet")
  }

  /**
   * Builds a Spark `View` from a UC `UCTableInfo` for view-like rows. Round-trips
   * dataType / nullable / comment / metadata via the in-connector [[UCColumnConversions]]
   * helper.
   */
  private[spark] def toView(t: UCTableInfo): View = {
    // Guard the columns collection itself (nullable on the wire for a row written by an older or
    // non-Spark client), mirroring the `Option(t.getProperties)` handling below and the per-column
    // `type_json` guard in `parseColumnJson` -- so a missing list degrades to an empty schema
    // rather than NPE-ing out of `loadView` / `loadRelation`.
    val columns = Option(t.getColumns).map(_.asScala).getOrElse(Seq.empty).map { col =>
      UCColumnConversions.parseColumnJson(col.getTypeJson)
    }.toArray

    val props = new util.HashMap[String, String]()
    Option(t.getProperties).foreach(props.putAll)
    val sqlConfigs = UCViewTypes.extractSqlConfigs(props)
    // The VIEW_SQL_CONFIG_PREFIX keys are surfaced (un-prefixed) via `withSqlConfigs`; drop them
    // from `props` so they don't also leak into the user-visible `properties()` map and get
    // re-persisted (double-counted) on a createView/replace round-trip.
    props.keySet().removeIf(_.startsWith(CatalogTable.VIEW_SQL_CONFIG_PREFIX))

    val builder = new View.Builder()
      .withColumns(columns)
      .withProperties(props)
      .withTableType(UCViewTypes.ucTableTypeToSparkViewType(t.getTableType))
      .withQueryText(t.getViewDefinition)
      .withCurrentCatalog(t.getCatalogName)
      .withCurrentNamespace(Array(t.getSchemaName))
      .withSqlConfigs(sqlConfigs)
      .withSchemaMode(SchemaCompensation.toString)
      .withQueryColumnNames(columns.map(_.name()))
    Option(t.getComment).foreach(builder.withComment)
    Option(t.getViewDependencies).foreach { ucDeps =>
      builder.withViewDependencies(fromUcDependencyList(ucDeps))
    }
    builder.build()
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

  /** Builds a wire-format UC `DependencyList` from derived `catalog.schema.table` names. */
  private def ucDependencyListFromNames(fullNames: Seq[String]): UCDependencyList = {
    val ucDeps = new java.util.ArrayList[UCDependency]()
    fullNames.foreach { fullName =>
      ucDeps.add(new UCDependency().table(new UCTableDependency().tableFullName(fullName)))
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
   * Builds the UC column-info list for `createView` from a Spark `View`. Preserves
   * nullability, comment, and the
   * canonical Spark column JSON in `typeJson` so column-level analyzer metadata
   * (`metric_view.type`, `metric_view.expr`) survives the round-trip through Unity Catalog.
   *
   * This intentionally mirrors the column loop in `UCProxy.createTable` (shared file), but
   * operates on the Spark V2 `View` / `Column` surface that only exists on Spark 4.2. The
   * shared `UCProxy.createTable` works on `StructType` / `StructField` (via `toStructFieldJson`)
   * and cannot reference these V2 types, so the two loops cannot be merged without breaking the
   * 4.0/4.1 builds -- the near-duplication is a consequence of the cross-Spark type split.
   * Spark's `View` API does not expose partition transforms.
   */
  private def buildColumnInfos(
      view: View,
      convertDataTypeToTypeName: DataType => ColumnTypeName): Seq[ColumnInfo] = {
    view.columns().toSeq.zipWithIndex.map { case (col, i) =>
      val column = new ColumnInfo()
      column.setName(col.name)
      column.setNullable(col.nullable)
      column.setTypeText(col.dataType.catalogString)
      column.setTypeName(convertDataTypeToTypeName(col.dataType))
      column.setTypeJson(UCColumnConversions.buildColumnJson(col))
      column.setPosition(i)
      Option(col.comment).foreach(column.setComment(_))
      column
    }
  }
}
