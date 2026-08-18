package io.unitycatalog.spark

import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{UnresolvedIdentifier, UnresolvedRelation}
import org.apache.spark.sql.catalyst.plans.logical.{
  CreateTable,
  CreateTableAsSelect,
  InsertIntoDir,
  InsertIntoStatement,
  LogicalPlan,
  ReplaceTableAsSelect,
  TableSpec,
  TableSpecBase
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogNotFoundException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Injects Unity Catalog path credentials for cloud storage paths referenced directly in a query
 * (e.g. ``SELECT * FROM parquet.`s3://bucket/dir` ``,
 * ``SELECT * FROM delta.`s3://bucket/dir` ``,
 * ``CREATE TABLE delta.`s3://bucket/dir` USING delta AS SELECT ...``,
 * ``INSERT OVERWRITE delta.`s3://bucket/dir` SELECT ...``, or
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
 * mirroring loadTable). `INSERT OVERWRITE DIRECTORY` and Delta path-table DDL/DML
 * (`CREATE TABLE delta.`path``, `INSERT OVERWRITE delta.`path``) always request
 * [[PathOperation.PATH_READ_WRITE]].
 * Spark folds these per-relation options into the Hadoop
 * configuration used to open the filesystem, so the credential-scoped filesystem + vended-token
 * provider pick them up — the same mechanism catalog tables use via
 * [[UCSingleCatalog.setCredentialProps]].
 *
 * Delta path tables may rewrite bare-path relations during analysis without copying those options
 * into `DeltaLog`. [[DeltaPathCredentialSupport]] early-resolves credentialed `delta.`path``
 * relations (preserving options) and re-injects on Delta-specific plan nodes if Delta already
 * rewrote the tree.
 *
 * '''Ordering''': `ResolveSQLOnFile` lists the path (for schema inference) as soon as it resolves
 * the relation, and it is ordered ahead of rules injected via `injectResolutionRule`. This rule is
 * therefore registered by [[UCSparkSessionExtensions]] as a hint resolution rule, whose batch runs
 * ahead of the analyzer's Resolution batch. A second registration via `injectResolutionRule`
 * patches Delta nodes after Delta's own rewrite. The parser stays side-effect free.
 *
 * '''Idempotence''': the analyzer runs its batches to a fixed point, so this rule is applied
 * repeatedly to the same plan. Nodes that already carry `fs.*` (or Delta's `option.fs.*`)
 * options are therefore left untouched: credentials are vended at most once per path per query,
 * and the plan converges even when the credential cache
 * (`fs.unitycatalog.credential.cache.enabled`) is disabled and every vend would otherwise return
 * a fresh session token. It also means explicit user-supplied `fs.*` options win over vended ones.
 *
 * The rule is a no-op unless the session's current catalog is a [[UCSingleCatalog]]. It can be
 * disabled with `spark.sql.catalog.<catalog>.vendPathCredentials.enabled=false`.
 *
 * When UC cannot vend credentials for a path (not managed by UC, no permission, etc.), the plan
 * node is left unchanged so Spark can use ambient storage credentials (e.g.
 * `spark.hadoop.fs.s3a.*`, instance profile) configured on the session.
 *
 * `INSERT OVERWRITE DIRECTORY ... USING delta` is not a Delta path-table write: Spark requires a
 * `FileFormat` for directory overwrite, and `DeltaDataSource` is not one. The parquet write analog
 * is `CREATE TABLE delta.`path`` / `INSERT OVERWRITE delta.`path``.
 */
case class ResolvePathCredentials(
    spark: SparkSession,
    resolveDeltaPathRelations: Boolean = true) extends Rule[LogicalPlan] {

  import ResolvePathCredentials._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    currentUcCatalog match {
      case None => plan
      case Some(uc) if !uc.vendPathCredentialsEnabled => plan
      case Some(uc) =>
        val withCore = plan.resolveOperators {
          // Bare `format`.`<cloud path>` — used for reads and in query FROM clauses.
          case u: UnresolvedRelation if isEligibleBarePathRelation(u) =>
            injectAndMaybeResolveDelta(u, uc)

          // `InsertIntoStatement.table` is not a tree child (only `query` is), so the generic
          // `UnresolvedRelation` case above never visits bare-path INSERT targets such as
          // INSERT INTO parquet.`s3://...` or INSERT OVERWRITE delta.`s3://...`.
          // Parquet INSERT INTO still fails at analysis on Spark 4.0–4.2
          // (TABLE_OR_VIEW_NOT_FOUND). Delta path tables are early-resolved to
          // DataSourceV2Relation so V2 overwrite/append can run.
          case i: InsertIntoStatement =>
            i.table match {
              case u: UnresolvedRelation if isEligibleBarePathRelation(u) =>
                i.copy(table = injectAndMaybeResolveDelta(u, uc))
              case _ => i
            }

          // CREATE TABLE delta.`<cloud path>` USING delta [AS SELECT ...]
          case c: CreateTableAsSelect if isEligibleDeltaPathDdl(c.name, c.tableSpec, c.writeOptions.keys) =>
            injectCreateTableAsSelectCredentials(c, uc)

          case c: ReplaceTableAsSelect if isEligibleDeltaPathDdl(c.name, c.tableSpec, c.writeOptions.keys) =>
            injectReplaceTableAsSelectCredentials(c, uc)

          case c: CreateTable if isEligibleDeltaPathDdl(c.name, c.tableSpec, Nil) =>
            injectCreateTableCredentials(c, uc)

          // Write: INSERT OVERWRITE DIRECTORY '<cloud path>' USING <format> ...
          case i: InsertIntoDir
              if !isDeltaProvider(i.provider) &&
                !PathCredentialOptions.hasCredentialKeys(i.storage.properties.keys) &&
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
        DeltaPathCredentialSupport.apply(withCore, spark, uc, isCloudPath)
    }
  }

  private def injectUnresolvedRelationCredentials(
      relation: UnresolvedRelation,
      uc: UCSingleCatalog): UnresolvedRelation = {
    val path = relation.multipartIdentifier.last
    val conf = uc.vendPathCredentialConfWithFallback(spark, path)
    if (conf.isEmpty) {
      relation
    } else if (DeltaPathCredentialSupport.isDeltaPathRelation(relation)) {
      relation.copy(
        options = PathCredentialOptions.mergeCredentialOptions(relation.options, conf))
    } else {
      relation.copy(options = mergeOptions(relation.options, conf))
    }
  }

  private def injectAndMaybeResolveDelta(
      relation: UnresolvedRelation,
      uc: UCSingleCatalog): LogicalPlan = {
    val injected = injectUnresolvedRelationCredentials(relation, uc)
    if (resolveDeltaPathRelations &&
      DeltaPathCredentialSupport.isDeltaPathRelation(injected) &&
      PathCredentialOptions.hasCredentialKeys(injected.options.keySet.asScala)) {
      DeltaPathCredentialSupport.tryResolveDeltaPathRelation(injected, spark)
        .getOrElse(injected)
    } else {
      injected
    }
  }

  private def injectCreateTableAsSelectCredentials(
      create: CreateTableAsSelect,
      uc: UCSingleCatalog): CreateTableAsSelect = {
    val path = deltaCloudPathFromName(create.name).get
    val conf = uc.vendPathCredentialConfOrEmpty(spark, path, PathOperation.PATH_READ_WRITE)
    if (conf.isEmpty) {
      create
    } else {
      create.copy(
        writeOptions = mergeWriteOptions(create.writeOptions, conf),
        tableSpec = mergeTableSpecCredentials(create.tableSpec, conf))
    }
  }

  private def injectReplaceTableAsSelectCredentials(
      replace: ReplaceTableAsSelect,
      uc: UCSingleCatalog): ReplaceTableAsSelect = {
    val path = deltaCloudPathFromName(replace.name).get
    val conf = uc.vendPathCredentialConfOrEmpty(spark, path, PathOperation.PATH_READ_WRITE)
    if (conf.isEmpty) {
      replace
    } else {
      replace.copy(
        writeOptions = mergeWriteOptions(replace.writeOptions, conf),
        tableSpec = mergeTableSpecCredentials(replace.tableSpec, conf))
    }
  }

  private def injectCreateTableCredentials(
      create: CreateTable,
      uc: UCSingleCatalog): CreateTable = {
    val path = deltaCloudPathFromName(create.name).get
    val conf = uc.vendPathCredentialConfOrEmpty(spark, path, PathOperation.PATH_READ_WRITE)
    if (conf.isEmpty) {
      create
    } else {
      create.copy(tableSpec = mergeTableSpecCredentials(create.tableSpec, conf))
    }
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
    isCloudPath(relation.multipartIdentifier.last) &&
    !PathCredentialOptions.hasCredentialKeys(relation.options.keySet.asScala)
  }

  /** True for `CREATE/REPLACE TABLE delta.`cloud-path`` DDL that still needs credentials. */
  private def isEligibleDeltaPathDdl(
      name: LogicalPlan,
      spec: TableSpecBase,
      writeOptionKeys: Iterable[String]): Boolean =
    isDeltaProvider(spec.provider) &&
      deltaCloudPathFromName(name).isDefined &&
      !PathCredentialOptions.hasCredentialKeys(writeOptionKeys) &&
      !hasTableSpecCredentialKeys(spec)

  private def deltaCloudPathFromName(name: LogicalPlan): Option[String] = name match {
    case u: UnresolvedRelation if DeltaPathCredentialSupport.isDeltaPathRelation(u) =>
      Some(u.multipartIdentifier.last).filter(isCloudPath)
    case u: UnresolvedIdentifier if u.nameParts.length == 2 &&
        u.nameParts.head.equalsIgnoreCase("delta") &&
        isCloudPath(u.nameParts.last) =>
      Some(u.nameParts.last)
    case _ => None
  }

  private def hasTableSpecCredentialKeys(spec: TableSpecBase): Boolean = spec match {
    case t: TableSpec =>
      PathCredentialOptions.hasCredentialKeys(t.options.keys) ||
        PathCredentialOptions.hasCredentialKeys(t.properties.keys)
    case _ =>
      PathCredentialOptions.hasCredentialKeys(spec.properties.keys)
  }

  private def mergeWriteOptions(
      existing: Map[String, String],
      creds: java.util.Map[String, String]): Map[String, String] = {
    val merged = new java.util.HashMap[String, String](existing.asJava)
    PathCredentialOptions.putAllCredentialEntries(merged, creds)
    merged.asScala.toMap
  }

  private def mergeTableSpecCredentials(
      spec: TableSpecBase,
      creds: java.util.Map[String, String]): TableSpecBase = spec match {
    case t: TableSpec =>
      t.copy(options = mergeWriteOptions(t.options, creds))
    case other => other
  }

  /** True when `pathStr` is an absolute URI whose scheme is one UC can vend credentials for. */
  private[spark] def isCloudPath(pathStr: String): Boolean = {
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
