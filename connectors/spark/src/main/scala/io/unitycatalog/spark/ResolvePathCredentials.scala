/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.unitycatalog.spark

import io.unitycatalog.hadoop.UCCredentialHadoopConfs.PathOperation

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoDir, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
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
 * `format.`path`` relations, read vs write is ambiguous at parse time, so credentials use
 * [[UCSingleCatalog.vendPathCredentialConfWithFallback]] (PATH_READ_WRITE with PATH_READ fallback,
 * mirroring loadTable). `INSERT OVERWRITE DIRECTORY` always requests [[PathOperation.PATH_READ_WRITE]].
 * Spark folds these per-relation options into the Hadoop
 * configuration used to open the filesystem, so the credential-scoped filesystem + vended-token
 * provider pick them up — the same mechanism catalog tables use via
 * [[UCSingleCatalog.setCredentialProps]].
 *
 * '''Ordering''': this must run before the analyzer, because `ResolveSQLOnFile` lists the path
 * (for schema inference) as soon as it resolves the relation, and it is ordered ahead of injected
 * resolution rules. It is therefore invoked from [[UCSparkSqlExtensionsParser]] on the freshly
 * parsed plan via [[UCSparkSessionExtensions]].
 *
 * The rule is a no-op unless the session's current catalog is a [[UCSingleCatalog]]. It can be
 * disabled with `spark.sql.catalog.<catalog>.vendPathCredentials.enabled=false`.
 */
case class ResolvePathCredentials(spark: SparkSession) extends Rule[LogicalPlan] {

  import ResolvePathCredentials._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    currentUcCatalog match {
      case None => plan
      case Some(uc) if !uc.vendPathCredentialsEnabled => plan
      case Some(uc) =>
        plan.resolveOperators {
          // Bare `format`.`<cloud path>` — used for reads and writes (e.g. INSERT INTO parquet.`s3://...`).
          case u: UnresolvedRelation
              if u.multipartIdentifier.length == 2 && isCloudPath(u.multipartIdentifier.last) =>
            val path = u.multipartIdentifier.last
            val conf = uc.vendPathCredentialConfWithFallback(path)
            if (conf.isEmpty) u else u.copy(options = mergeOptions(u.options, conf))

          // Write: INSERT OVERWRITE DIRECTORY '<cloud path>' USING <format> ...
          case i: InsertIntoDir if i.storage.locationUri.exists(u => isCloudPath(u.toString)) =>
            val location = i.storage.locationUri.get.toString
            val conf = uc.vendPathCredentialConf(location, PathOperation.PATH_READ_WRITE)
            if (conf.isEmpty) {
              i
            } else {
              i.copy(storage =
                i.storage.copy(properties = i.storage.properties ++ conf.asScala))
            }
        }
    }
  }

  private def currentUcCatalog: Option[UCSingleCatalog] =
    spark.sessionState.catalogManager.currentCatalog match {
      case uc: UCSingleCatalog => Some(uc)
      case _ => None
    }
}

object ResolvePathCredentials {

  /**
   * URI schemes for which UC can vend credentials. These match the schemes UC external locations
   * are registered with and the ones [[UCCredentialHadoopConfs]] understands; other schemes (local
   * paths, `s3a`, HDFS, ...) are left untouched.
   */
  private val CLOUD_SCHEMES = Set("s3", "gs", "abfs", "abfss")

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
