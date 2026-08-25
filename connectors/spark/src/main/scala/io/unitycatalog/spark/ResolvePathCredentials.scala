package io.unitycatalog.spark

import io.unitycatalog.hadoop.UCCredentialHadoopConfs
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants

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
 * '''Idempotence''': the analyzer runs its batches to a fixed point, so this rule is applied
 * repeatedly to the same plan. Nodes that already had a lookup for this URI — a successful vend
 * (`credentials.type=path` and `fs.unitycatalog.path`) or skip markers after an allowed miss —
 * are left untouched. Ambient `fs.s3a.*` (or other cloud) keys do not skip vending.
 *
 * The rule is a no-op unless the session's current catalog is a [[UCSingleCatalog]]. It can be
 * disabled with `spark.sql.catalog.<catalog>.vendPathCredentials.enabled=false`.
 *
 * When UC cannot vend credentials for a path (not managed by UC, no permission, etc.), cloud
 * secrets are omitted so Spark can use ambient storage credentials (`spark.hadoop.fs.s3a.*`,
 * instance profile). The node is stamped with skip markers
 * (`fs.unitycatalog.path.vending.attempted` and `fs.unitycatalog.path.vending.location`), not
 * `credentials.type=path`, so later analyzer iterations do not re-issue the failing RPCs and
 * Hadoop does not treat the job conf as a path-cred scope.
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
                i.storage.locationUri.exists { u =>
                  val location = u.toString
                  isCloudPath(location) &&
                    !hasUcPathCredentials(i.storage.properties, location)
                } =>
            val location = i.storage.locationUri.get.toString
            val conf = optionsAfterLookup(
              location,
              uc.vendPathCredentialConfOrEmpty(
                spark, location, PathOperation.PATH_READ_WRITE))
            i.copy(storage =
              i.storage.copy(properties = i.storage.properties ++ conf.asScala))
        }
    }
  }

  private def injectUnresolvedRelationCredentials(
      relation: UnresolvedRelation,
      uc: UCSingleCatalog): UnresolvedRelation = {
    val path = relation.multipartIdentifier.last
    val conf = optionsAfterLookup(
      path,
      uc.vendPathCredentialConfWithFallback(spark, path))
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

  private def isDeltaProvider(provider: Option[String]): Boolean =
    provider.exists(_.equalsIgnoreCase("delta"))

  /** True for bare `format.`cloud-path`` relations that should receive UC path credentials. */
  private def isEligibleBarePathRelation(relation: UnresolvedRelation): Boolean = {
    relation.multipartIdentifier.length == 2 &&
    !isDeltaProvider(Some(relation.multipartIdentifier.head)) &&
    isCloudPath(relation.multipartIdentifier.last) &&
    !hasUcPathCredentials(
      relation.options.asCaseSensitiveMap().asScala,
      relation.multipartIdentifier.last)
  }

  /**
   * True when this node already had a path-credential lookup for `location`. A successful vend
   * carries `credentials.type=path` and `fs.unitycatalog.path`. An allowed miss carries only the
   * vending skip markers (not a Hadoop {@code PathCredId}). Ambient `fs.s3a.*` keys do not count.
   */
  private def hasUcPathCredentials(
      options: scala.collection.Map[String, String],
      location: String): Boolean = {
    val byLowerKey = options.collect {
      case (k, v) if k != null => k.toLowerCase -> v
    }
    val credType = byLowerKey.getOrElse(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY, "")
    val storedPath = byLowerKey.get(UCHadoopConfConstants.UC_PATH_KEY)
    val successfulVend =
      credType.equalsIgnoreCase(UCHadoopConfConstants.UC_CREDENTIALS_TYPE_PATH_VALUE) &&
        storedPath.exists(pathsMatch(_, location))
    val attempted =
      byLowerKey
        .get(UCHadoopConfConstants.UC_PATH_VENDING_ATTEMPTED_KEY)
        .exists(_.equalsIgnoreCase(UCHadoopConfConstants.UC_PATH_VENDING_ATTEMPTED_VALUE))
    val skipLocation = byLowerKey.get(UCHadoopConfConstants.UC_PATH_VENDING_LOCATION_KEY)
    successfulVend || (attempted && skipLocation.exists(pathsMatch(_, location)))
  }

  /** True when stored identity path is this location or the UC API canonical form of it. */
  private def pathsMatch(stored: String, location: String): Boolean = {
    stored == location || stored == UCSingleCatalog.pathForCredentialApi(location)._1
  }

  /**
   * True when `pathStr` is an absolute URI whose scheme [[UCCredentialHadoopConfs]] can vend
   * credentials for. Same scheme set as name-based table resolution (including `s3a://`). UC API
   * lookup still rewrites `s3a://` to `s3://` in [[UCSingleCatalog.vendPathCredentialConf]].
   */
  private def isCloudPath(pathStr: String): Boolean = {
    val scheme = try {
      new Path(pathStr).toUri.getScheme
    } catch {
      case _: IllegalArgumentException => null
    }
    UCCredentialHadoopConfs.isSupportedScheme(scheme)
  }

  /**
   * Successful vending already includes path identity. An allowed miss returns empty secrets;
   * stamp skip markers so [[hasUcPathCredentials]] skips this node on the next analyzer pass.
   */
  private def optionsAfterLookup(
      location: String,
      conf: java.util.Map[String, String]): java.util.Map[String, String] = {
    if (!conf.isEmpty) {
      conf
    } else {
      UCSingleCatalog.pathCredentialSkipProps(location)
    }
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
