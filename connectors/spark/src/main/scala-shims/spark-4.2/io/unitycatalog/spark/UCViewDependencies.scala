package io.unitycatalog.spark

import java.util.Locale

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnresolvedWith}

/**
 * Derives the base-table dependency list of a view from its query text, for plain views.
 *
 * Works on the *parsed* (unresolved) plan, not the analyzed one: analyzing would read each base
 * table's log / vend credentials and can rewrite away its catalog identity, dropping the dependency.
 * Best-effort -- derivation failure yields `None` (not an empty list), so the caller can leave the
 * server's `view_dependencies` unset (null) rather than persisting a wrong "no dependencies" list;
 * a successful derivation with no base tables (e.g. `SELECT 1`) yields `Some(Seq.empty)`.
 */
private[spark] object UCViewDependencies extends Logging {

  /**
   * Returns `Some(deps)` when the query text parses and dependencies could be collected (the list
   * may be empty for a view with no base tables), or `None` when parsing/derivation fails so the
   * caller can send null instead of a misleading empty list.
   */
  def derive(
      queryText: String,
      currentCatalog: String,
      currentNamespace: Seq[String]): Option[Seq[String]] = {
    try {
      val spark = SparkSession.active
      val parsed = spark.sessionState.sqlParser.parsePlan(queryText)
      Some(
        collectTableDependencies(
          parsed, currentCatalog, currentNamespace, spark.sessionState.conf.caseSensitiveAnalysis))
    } catch {
      case NonFatal(e) =>
        logWarning(
          s"Failed to derive view dependencies from query text; leaving them unset (null) so the " +
            s"server can decide. Query: $queryText",
          e)
        None
    }
  }

  def collectTableDependencies(
      parsed: LogicalPlan,
      currentCatalog: String,
      currentNamespace: Seq[String],
      caseSensitive: Boolean): Seq[String] = {
    baseRelations(parsed, Set.empty, caseSensitive)
      .flatMap { parts =>
        val qualified = qualify(parts, currentCatalog, currentNamespace)
        // A UC full name is exactly catalog.schema.table; drop anything that can't form one
        // (e.g. a 4-part reference) rather than persisting a name the server would reject.
        if (qualified.size == 3) {
          Some(qualified.mkString("."))
        } else {
          logWarning(
            s"Skipping view dependency with unexpected name shape: ${qualified.mkString(".")}")
          None
        }
      }
      .distinct
  }

  // `visibleCtes` (normalized) are the CTE names in scope; a single-part relation shadowed by one
  // is skipped rather than reported as a base table.
  private def baseRelations(
      plan: LogicalPlan,
      visibleCtes: Set[String],
      caseSensitive: Boolean): Seq[Seq[String]] = plan match {
    case w: UnresolvedWith =>
      // CTEs resolve in declaration order: each definition sees the ones before it (plus itself
      // under WITH RECURSIVE); the main query sees them all.
      var visible = visibleCtes
      val fromDefinitions = w.cteRelations.flatMap { case (cteName, ctePlan, _) =>
        val name = normalize(cteName, caseSensitive)
        val bodyScope = if (w.allowRecursion) visible + name else visible
        val deps = baseRelations(ctePlan, bodyScope, caseSensitive)
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
      (other.children ++ other.subqueries).flatMap(baseRelations(_, visibleCtes, caseSensitive))
  }

  private def normalize(name: String, caseSensitive: Boolean): String =
    if (caseSensitive) name else name.toLowerCase(Locale.ROOT)

  // Prefixes an unqualified name with the view's catalog / namespace (UC is catalog.schema.table).
  private def qualify(
      parts: Seq[String],
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[String] = {
    val catalog = Option(currentCatalog).filter(_.nonEmpty).toSeq
    val namespace = Option(currentNamespace).getOrElse(Seq.empty)
    parts.size match {
      case n if n >= 3 => parts
      case 2 => catalog ++ parts
      case _ => catalog ++ namespace ++ parts
    }
  }
}
