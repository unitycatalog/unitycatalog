package io.unitycatalog.spark

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedWith}

/**
 * Derives the base-table dependency list of a view from its query text.
 *
 * Unity Catalog requires a (non-null) dependency list when creating a view, but Spark only
 * populates `View.viewDependencies()` for metric views -- it is null for plain views. So for plain
 * views the connector parses the stored query text and collects the relations it reads from.
 *
 * Dependencies are read from the *parsed* (unresolved) plan rather than the analyzed one: analyzing
 * would resolve each base table, which for a Delta (or credentialed cloud) table means reading its
 * transaction log / vending credentials and can rewrite the relation so its catalog identity is
 * lost -- both of which would drop the dependency. Parsing needs neither, and the `UnresolvedRelation`
 * multi-part names it yields are exactly what we persist.
 *
 * CTE aliases are tracked per-scope so an in-scope CTE reference is not mistaken for a base table,
 * and matching honors the session's case-sensitivity (Spark defaults to case-insensitive), so a CTE
 * whose name differs only in case from a reference is not leaked as a bogus dependency. Function
 * references are not represented. Derivation is best-effort: any failure yields an empty list so
 * view creation still succeeds (the server only requires the field to be present).
 */
private[spark] object UCViewDependencies {

  /**
   * Fully qualified `catalog.schema.table` names of the base relations referenced by `queryText`,
   * resolved against the view's own `currentCatalog` / `currentNamespace` for unqualified names.
   */
  def derive(queryText: String, currentCatalog: String, currentNamespace: Seq[String]): Seq[String] = {
    try {
      val spark = SparkSession.active
      val parsed = spark.sessionState.sqlParser.parsePlan(queryText)
      collectTableDependencies(
        parsed, currentCatalog, currentNamespace, spark.sessionState.conf.caseSensitiveAnalysis)
    } catch {
      case NonFatal(_) => Seq.empty
    }
  }

  def collectTableDependencies(
      parsed: LogicalPlan,
      currentCatalog: String,
      currentNamespace: Seq[String],
      caseSensitive: Boolean): Seq[String] = {
    baseRelations(parsed, Set.empty, caseSensitive)
      .map(parts => qualify(parts, currentCatalog, currentNamespace))
      .distinct
  }

  /**
   * Multi-part names of the base relations read by `plan`. `visibleCtes` holds the (already
   * normalized) CTE names in scope; a single-part relation shadowed by one of them is skipped
   * instead of being reported as a base table.
   */
  private def baseRelations(
      plan: LogicalPlan,
      visibleCtes: Set[String],
      caseSensitive: Boolean): Seq[Seq[String]] = plan match {
    case w: UnresolvedWith =>
      // CTEs resolve in declaration order: each definition sees the ones before it (and, under
      // WITH RECURSIVE, itself); the main query sees them all.
      var visible = visibleCtes
      val fromDefinitions = w.cteRelations.flatMap { cte =>
        val name = normalize(cte._1, caseSensitive)
        val bodyScope = if (w.allowRecursion) visible + name else visible
        val deps = baseRelations(cte._2, bodyScope, caseSensitive)
        visible += name
        deps
      }
      fromDefinitions ++ baseRelations(w.child, visible, caseSensitive)

    case r: UnresolvedRelation =>
      val parts = r.multipartIdentifier
      val shadowedByCte =
        parts.lengthCompare(1) == 0 && visibleCtes.contains(normalize(parts.head, caseSensitive))
      if (shadowedByCte) Seq.empty else Seq(parts)

    case other =>
      // Subquery expressions (WHERE / SELECT / ...) inherit the enclosing CTE scope.
      (other.children ++ other.subqueries).flatMap(baseRelations(_, visibleCtes, caseSensitive))
  }

  private def normalize(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  /** Prefixes an unqualified name with the view's catalog / namespace (UC is catalog.schema.table). */
  private def qualify(parts: Seq[String], currentCatalog: String, currentNamespace: Seq[String]): String = {
    val catalog = Option(currentCatalog).filter(_.nonEmpty).toSeq
    val namespace = Option(currentNamespace).getOrElse(Seq.empty)
    val qualified = parts.size match {
      case n if n >= 3 => parts
      case 2 => catalog ++ parts
      case _ => catalog ++ namespace ++ parts
    }
    qualified.mkString(".")
  }
}
