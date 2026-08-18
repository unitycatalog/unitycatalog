package io.unitycatalog.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{ResolvedTable, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier, Table}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.immutable.Map
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
 * Optional Delta Lake integration for [[ResolvePathCredentials]].
 *
 * Delta preprocess rules can rewrite bare-path `UnresolvedRelation` nodes into Delta-specific
 * unresolved plan nodes without copying relation options. Re-inject credentials on those nodes
 * (and on resolved `DataSourceV2Relation` / `ResolvedTable` wrappers) via reflection so the
 * connector JAR does not need a compile-time dependency on delta-spark.
 *
 * All injection paths skip nodes that already carry a successful UC vend or the allowed-miss
 * markers for the same path, so the analyzer's fixed-point batches do not re-vend credentials.
 * Ambient filesystem options do not suppress UC vending.
 */
private[spark] object DeltaPathCredentialSupport {

  private val resolveDeltaPathTableCompanion =
    "org.apache.spark.sql.delta.ResolveDeltaPathTable$"
  private val resolveAsPathTableMethod =
    "org$apache$spark$sql$delta$ResolveDeltaPathTable$$resolveAsPathTable"

  private val unresolvedRelationClass =
    "org.apache.spark.sql.delta.UnresolvedPathBasedDeltaTableRelation"
  private val unresolvedTableClass =
    "org.apache.spark.sql.delta.UnresolvedPathBasedDeltaTable"
  private val deltaTableV2Class =
    "org.apache.spark.sql.delta.catalog.DeltaTableV2"

  /** True for bare {@code delta.`path`} relations parsed as {@code [delta, path]}. */
  def isDeltaPathRelation(relation: UnresolvedRelation): Boolean =
    relation.multipartIdentifier.length == 2 &&
      relation.multipartIdentifier.head.equalsIgnoreCase("delta")

  /**
   * Resolves a credentialed bare delta path relation to {@link DataSourceV2Relation}, preserving
   * {@link UnresolvedRelation#options}. Delta's public {@code resolveAsPathTableRelation} drops
   * relation options, so the private {@code resolveAsPathTable} helper is invoked via reflection.
   */
  def tryResolveDeltaPathRelation(
      relation: UnresolvedRelation,
      spark: SparkSession): Option[LogicalPlan] = {
    if (!isDeltaPathRelation(relation)) {
      None
    } else {
      try {
        val companionClass = Class.forName(resolveDeltaPathTableCompanion)
        val module = companionClass.getField("MODULE$").get(null)
        val optionsMap = relation.options.asCaseSensitiveMap().asScala.toMap
        val resolveMethod = companionClass.getMethod(
          resolveAsPathTableMethod,
          classOf[SparkSession],
          classOf[scala.collection.immutable.Seq[_]],
          classOf[scala.collection.immutable.Map[_, _]])
        val resolvedTableOpt = resolveMethod
          .invoke(module, spark, relation.multipartIdentifier, optionsMap)
          .asInstanceOf[Option[_]]
        resolvedTableOpt.map { resolvedTable =>
          val table = resolvedTable.getClass.getMethod("table").invoke(resolvedTable)
          val catalog =
            resolvedTable.getClass.getMethod("catalog").invoke(resolvedTable)
          val identifier =
            resolvedTable.getClass.getMethod("identifier").invoke(resolvedTable)
          val plan = DataSourceV2Relation.create(
            table.asInstanceOf[Table],
            Some(catalog.asInstanceOf[CatalogPlugin]),
            Some(identifier.asInstanceOf[Identifier]),
            relation.options)
          plan
        }
      } catch {
        case _: ClassNotFoundException => None
        case NonFatal(_) => None
      }
    }
  }

  def apply(
      plan: LogicalPlan,
      spark: SparkSession,
      uc: UCSingleCatalog,
      isCloudPath: String => Boolean): LogicalPlan =
    plan.resolveOperators {
      case node if isRuntimeClass(node, unresolvedRelationClass) =>
        injectUnresolvedRelation(node, uc, spark, isCloudPath)
      case node if isRuntimeClass(node, unresolvedTableClass) =>
        injectUnresolvedTable(node, uc, spark, isCloudPath)
      case dsv2: DataSourceV2Relation =>
        patchResolvedDeltaRelation(dsv2, uc, spark, isCloudPath)
      case r: ResolvedTable =>
        patchResolvedTable(r, uc, spark, isCloudPath)
    }

  private def injectUnresolvedRelation(
      node: LogicalPlan,
      uc: UCSingleCatalog,
      spark: SparkSession,
      isCloudPath: String => Boolean): LogicalPlan = {
    val path = node.getClass.getMethod("path").invoke(node).asInstanceOf[String]
    val existing =
      node.getClass.getMethod("options").invoke(node).asInstanceOf[CaseInsensitiveStringMap]
    if (!isCloudPath(path) ||
      ResolvePathCredentials.hasUcPathCredentials(existing.asCaseSensitiveMap().asScala, path)) {
      node
    } else {
      val creds = ResolvePathCredentials.optionsAfterLookup(
        path,
        uc.vendPathCredentialConfWithFallback(spark, path))
      val merged = PathCredentialOptions.mergeCredentialOptions(existing, creds)
      node.getClass
        .getMethod("copy", classOf[String], classOf[CaseInsensitiveStringMap])
        .invoke(node, path, merged)
        .asInstanceOf[LogicalPlan]
    }
  }

  private def injectUnresolvedTable(
      node: LogicalPlan,
      uc: UCSingleCatalog,
      spark: SparkSession,
      isCloudPath: String => Boolean): LogicalPlan = {
    val path = node.getClass.getMethod("path").invoke(node).asInstanceOf[String]
    val existing = node.getClass
      .getMethod("options")
      .invoke(node)
      .asInstanceOf[Map[String, String]]
    if (!isCloudPath(path) || ResolvePathCredentials.hasUcPathCredentials(existing, path)) {
      node
    } else {
      val creds = ResolvePathCredentials.optionsAfterLookup(
        path,
        uc.vendPathCredentialConfWithFallback(spark, path))
      val commandName =
        node.getClass.getMethod("commandName").invoke(node).asInstanceOf[String]
      val merged = mergeStringMapOptions(existing, creds)
      node.getClass
        .getMethod(
          "copy",
          classOf[String],
          classOf[scala.collection.immutable.Map[_, _]],
          classOf[String])
        .invoke(node, path, merged, commandName)
        .asInstanceOf[LogicalPlan]
    }
  }

  private def patchResolvedDeltaRelation(
      dsv2: DataSourceV2Relation,
      uc: UCSingleCatalog,
      spark: SparkSession,
      isCloudPath: String => Boolean): DataSourceV2Relation = {
    patchDeltaTable(dsv2.table, uc, spark, isCloudPath) match {
      case None => dsv2
      case Some((_, patchedTable, creds)) =>
        val mergedRelationOptions =
          PathCredentialOptions.mergeCredentialOptions(dsv2.options, creds)
        dsv2.copy(table = patchedTable, options = mergedRelationOptions)
    }
  }

  private def patchResolvedTable(
      resolved: ResolvedTable,
      uc: UCSingleCatalog,
      spark: SparkSession,
      isCloudPath: String => Boolean): ResolvedTable =
    patchDeltaTable(resolved.table, uc, spark, isCloudPath) match {
      case None => resolved
      case Some((_, patchedTable, _)) =>
        resolved.copy(table = patchedTable)
    }

  /** Returns patched table + creds when a path-based DeltaTableV2 needs credential injection. */
  private def patchDeltaTable(
      table: Table,
      uc: UCSingleCatalog,
      spark: SparkSession,
      isCloudPath: String => Boolean): Option[(String, Table, java.util.Map[String, String])] = {
    if (!isRuntimeClass(table, deltaTableV2Class)) {
      None
    } else {
      try {
        val catalogTable = table.getClass.getMethod("catalogTable").invoke(table)
        if (catalogTable.asInstanceOf[Option[_]].isDefined) {
          None
        } else {
          val path = table.getClass.getMethod("path").invoke(table).toString
          if (!isCloudPath(path)) {
            None
          } else {
            val tableOptions = table.getClass
              .getMethod("options")
              .invoke(table)
              .asInstanceOf[Map[String, String]]
            if (ResolvePathCredentials.hasUcPathCredentials(tableOptions, path) ||
              ResolvePathCredentials.hasUcPathCredentials(table.properties().asScala, path)) {
              None
            } else {
              val creds = ResolvePathCredentials.optionsAfterLookup(
                path,
                uc.vendPathCredentialConfWithFallback(spark, path))
              val mergedTableOptions = mergeStringMapOptions(tableOptions, creds)
              val patchedTable = table.getClass
                .getMethod("withOptions", classOf[scala.collection.immutable.Map[_, _]])
                .invoke(table, mergedTableOptions)
                .asInstanceOf[Table]
              Some((path, patchedTable, creds))
            }
          }
        }
      } catch {
        case NonFatal(_) => None
      }
    }
  }

  private def mergeStringMapOptions(
      existing: Map[String, String],
      creds: java.util.Map[String, String]): Map[String, String] = {
    val merged = new java.util.HashMap[String, String](existing.asJava)
    PathCredentialOptions.putAllCredentialEntries(merged, creds)
    merged.asScala.toMap
  }

  private def isRuntimeClass(obj: AnyRef, className: String): Boolean =
    obj != null && obj.getClass.getName == className
}
