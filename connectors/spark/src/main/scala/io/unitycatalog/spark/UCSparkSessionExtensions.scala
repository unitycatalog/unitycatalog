package io.unitycatalog.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.parser.extensions.UCSparkSqlExtensionsParser

/** Spark session extensions that vend UC credentials for bare cloud paths in SQL. */
class UCSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (spark, parser) =>
      new UCSparkSqlExtensionsParser(spark, parser)
    }
  }
}
