package io.unitycatalog.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.parser.extensions.UCSparkSqlExtensionsParser

/** Spark session extensions that vend UC credentials for bare cloud paths in SQL. */
class UCSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (spark, parser) =>
      new UCSparkSqlExtensionsParser(spark, parser)
    }
    // Parse-time injection is required before ResolveSQLOnFile lists cloud paths. Delta path
    // tables additionally need hint-resolution injection so bare delta relations are resolved
    // (with options) before ResolveSQLOnFile replaces UnresolvedRelation nodes.
    extensions.injectHintResolutionRule { spark =>
      ResolvePathCredentials(spark, resolveDeltaPathRelations = true)
    }
    // Post-resolution pass re-injects on Delta-specific plan nodes and patches resolved relations.
    extensions.injectResolutionRule { spark =>
      ResolvePathCredentials(spark, resolveDeltaPathRelations = false)
    }
  }
}
