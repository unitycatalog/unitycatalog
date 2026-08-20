package io.unitycatalog.spark

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Spark session extensions that vend UC credentials for bare cloud paths in SQL.
 *
 * [[ResolvePathCredentials]] is registered as a hint resolution rule rather than only a resolution
 * rule so it runs before `ResolveSQLOnFile` lists the path for schema inference. For `delta.`path``
 * the same hint-batch pass early-resolves the relation with options intact. A resolution-batch
 * pass then patches Delta-specific nodes if Delta rewrote the tree without those options. The
 * parser is left untouched, so it stays side-effect free.
 */
class UCSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectHintResolutionRule { spark =>
      ResolvePathCredentials(spark, resolveDeltaPathRelations = true)
    }
    extensions.injectResolutionRule { spark =>
      ResolvePathCredentials(spark, resolveDeltaPathRelations = false)
    }
  }
}
