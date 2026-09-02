package io.unitycatalog.spark

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Spark session extensions that vend UC credentials for bare cloud paths in SQL.
 *
 * [[ResolvePathCredentials]] is registered as a hint resolution rule rather than a resolution rule
 * so it runs before `ResolveSQLOnFile` lists the path for schema inference. The parser is left
 * untouched, so it stays side-effect free.
 */
class UCSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectHintResolutionRule(ResolvePathCredentials(_))
  }
}
