package io.unitycatalog.spark

import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/** Helpers for merging UC vended Hadoop credential keys into relation/table option maps. */
private[spark] object PathCredentialOptions {

  /**
   * True when `keys` already carry Hadoop filesystem credentials, either vended by an earlier
   * pass of [[ResolvePathCredentials]] or supplied explicitly by the user. Delta also stores the
   * same keys under Spark's `option.` prefix.
   */
  def hasCredentialKeys(keys: Iterable[String]): Boolean =
    keys.exists { key =>
      val lower = key.toLowerCase
      lower.startsWith("fs.") || lower.startsWith("option.fs.")
    }

  /**
   * Merges vended credential entries into `options`. When `includeOptionPrefix` is true, also
   * writes `option.<key>` duplicates — Delta catalog and path resolution expect both forms.
   */
  def mergeCredentialOptions(
      options: CaseInsensitiveStringMap,
      credentialConf: java.util.Map[String, String],
      includeOptionPrefix: Boolean = true): CaseInsensitiveStringMap = {
    if (credentialConf.isEmpty) {
      options
    } else {
      val merged = new java.util.HashMap[String, String](options.asCaseSensitiveMap())
      putAllCredentialEntries(merged, credentialConf, includeOptionPrefix)
      new CaseInsensitiveStringMap(merged)
    }
  }

  /** Writes credential keys into a Java map (used by catalog table property preparation). */
  def putAllCredentialEntries(
      target: java.util.Map[String, String],
      credentialConf: java.util.Map[String, String],
      includeOptionPrefix: Boolean = true): Unit = {
    target.putAll(credentialConf)
    if (includeOptionPrefix) {
      val prefix = TableCatalog.OPTION_PREFIX
      credentialConf.asScala.foreach { case (k, v) =>
        target.put(prefix + k, v)
      }
    }
  }
}
