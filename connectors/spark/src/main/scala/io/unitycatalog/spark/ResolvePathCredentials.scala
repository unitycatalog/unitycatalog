package io.unitycatalog.spark

import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, InsertIntoStatement, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogNotFoundException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Injects Unity Catalog path credentials for cloud storage paths referenced directly in a query
 * (e.g. ``SELECT * FROM parquet.`s3://bucket/dir` `` or
 * ``INSERT OVERWRITE DIRECTORY 's3://bucket/dir' USING parquet ...``).
 *
 * Such path-based relations are resolved by Spark's built-in `ResolveSQLOnFile`, which bypasses
 * the UC catalog entirely, so no credentials are ever vended and S3A fails with no credentials.
 * Catalog tables do not have this problem because [[UCSingleCatalog]] vends credentials during
 * `loadTable`/`createTable`.
 *
 * This rule closes that gap. For each bare cloud path it finds, it asks the active
 * [[UCSingleCatalog]] to vend credentials via [[UCSingleCatalog.vendPathCredentialConf]] and
 * attaches the resulting `fs.*` Hadoop options to the relation or write target. For bare
 * `format.`path`` relations, read vs write is ambiguous at analysis time, so credentials use
 * [[UCSingleCatalog.vendPathCredentialConfWithFallback]] (PATH_READ_WRITE with PATH_READ fallback,
 * mirroring loadTable). `INSERT OVERWRITE DIRECTORY` always requests [[PathOperation.PATH_READ_WRITE]].
 * Spark folds these per-relation options into the Hadoop
 * configuration used to open the filesystem, so the credential-scoped filesystem + vended-token
 * provider pick them up — the same mechanism catalog tables use via
 * [[UCSingleCatalog.setCredentialProps]].
 *
 * '''Ordering''': `ResolveSQLOnFile` lists the path (for schema inference) as soon as it resolves
 * the relation, and it is ordered ahead of rules injected via `injectResolutionRule`. This rule is
 * therefore registered by [[UCSparkSessionExtensions]] as a hint resolution rule, whose batch runs
 * ahead of the analyzer's Resolution batch. The parser stays side-effect free.
 *
 * The rule is a no-op unless the session's current catalog is a [[UCSingleCatalog]]. It can be
 * disabled with `spark.sql.catalog.<catalog>.vendPathCredentials.enabled=false`.
 *
 * When UC cannot vend credentials for a path (not managed by UC, no permission, etc.), the plan
 * node is left unchanged so Spark can use ambient storage credentials (e.g.
 * `spark.hadoop.fs.s3a.*`, instance profile) configured on the session.
 *
 * Bare `delta.`path`` cloud relations are excluded: Delta's analysis path does not propagate
 * relation options into `DeltaLog`, so vended credentials would not reach execution and can
 * interfere with ambient credentials. Delta bare-path support is tracked separately.
 */
case class ResolvePathCredentials(spark: SparkSession) extends Rule[LogicalPlan] {

  import ResolvePathCredentials._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    currentUcCatalog match {
      case None => plan
      case Some(uc) if !uc.vendPathCredentialsEnabled => plan
      case Some(uc) =>
        plan.resolveOperators {
          // Bare `format`.`<cloud path>` — used for reads and in query FROM clauses.
          case u: UnresolvedRelation if isEligibleBarePathRelation(u) =>
            injectUnresolvedRelationCredentials(u, uc)

          // `InsertIntoStatement.table` is not a tree child (only `query` is), so the generic
          // `UnresolvedRelation` case above never visits bare-path INSERT targets such as
          // INSERT INTO parquet.`s3://...`. On Spark 4.0–4.2, executed INSERT INTO on a bare path
          // still fails at analysis (TABLE_OR_VIEW_NOT_FOUND); this branch is kept for future
          // Spark versions.
          case i: InsertIntoStatement =>
            i.table match {
              case u: UnresolvedRelation if isEligibleBarePathRelation(u) =>
                i.copy(table = injectUnresolvedRelationCredentials(u, uc))
              case _ => i
            }

          // Write: INSERT OVERWRITE DIRECTORY '<cloud path>' USING <format> ...
          case i: InsertIntoDir
              if !isDeltaProvider(i.provider) &&
                i.storage.locationUri.exists(u => isCloudPath(u.toString)) =>
            val location = i.storage.locationUri.get.toString
            val conf =
              uc.vendPathCredentialConfOrEmpty(
                spark, location, PathOperation.PATH_READ_WRITE)
            if (conf.isEmpty) {
              i
            } else {
              i.copy(storage =
                i.storage.copy(properties = i.storage.properties ++ conf.asScala))
            }
        }
    }
  }

  private def injectUnresolvedRelationCredentials(
      relation: UnresolvedRelation,
      uc: UCSingleCatalog): UnresolvedRelation = {
    val path = relation.multipartIdentifier.last
    val conf = uc.vendPathCredentialConfWithFallback(spark, path)
    if (conf.isEmpty) relation else relation.copy(options = mergeOptions(relation.options, conf))
  }

  private def currentUcCatalog: Option[UCSingleCatalog] = {
    val manager = spark.sessionState.catalogManager
    val catalog =
      try SQLConf.withExistingConf(spark.sessionState.conf) {
        manager.currentCatalog
      } catch {
        case _: CatalogNotFoundException => null
      }
    catalog match {
      case uc: UCSingleCatalog => Some(uc)
      case _ => None
    }
  }
}

object ResolvePathCredentials {

  /**
   * URI schemes for which UC can vend credentials. These match [[CloudType]] (including Hadoop's
   * `s3a://` alias for S3). UC external locations and the path-credentials API use canonical
   * `s3://` URLs; [[UCSingleCatalog.vendPathCredentialConf]] rewrites `s3a://` for lookup only.
   */
  private val CLOUD_SCHEMES = Set("s3", "s3a", "gs", "abfs", "abfss")

  private def isDeltaProvider(provider: Option[String]): Boolean =
    provider.exists(_.equalsIgnoreCase("delta"))

  /** True for bare `format.`cloud-path`` relations that should receive UC path credentials. */
  private def isEligibleBarePathRelation(relation: UnresolvedRelation): Boolean = {
    relation.multipartIdentifier.length == 2 &&
    !isDeltaProvider(Some(relation.multipartIdentifier.head)) &&
    isCloudPath(relation.multipartIdentifier.last)
  }

  /** True when `pathStr` is an absolute URI whose scheme is one UC can vend credentials for. */
  private def isCloudPath(pathStr: String): Boolean = {
    val scheme = try {
      new Path(pathStr).toUri.getScheme
    } catch {
      case _: IllegalArgumentException => null
    }
    scheme != null && CLOUD_SCHEMES.contains(scheme.toLowerCase)
  }

  /** Merges vended `fs.*` credential entries into an existing relation option map. */
  private def mergeOptions(
      options: CaseInsensitiveStringMap,
      credentialConf: java.util.Map[String, String]): CaseInsensitiveStringMap = {
    val merged = new java.util.HashMap[String, String](options.asCaseSensitiveMap())
    merged.putAll(credentialConf)
    new CaseInsensitiveStringMap(merged)
  }
}
