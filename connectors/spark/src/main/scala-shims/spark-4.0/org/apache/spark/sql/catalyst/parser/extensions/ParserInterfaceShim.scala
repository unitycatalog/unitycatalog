package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Spark 4.0 does not have {@code parsePlanWithParameters} or {@code ParameterContext}, so this
 * shim adds no parameterized-parser override. {@code SparkSession.sql(text, args)} on 4.0 routes
 * through plain {@code parsePlan}, which the common {@link UCSparkSqlExtensionsParser} overrides.
 */
trait ParserInterfaceShim extends ParserInterface {
  protected def delegateParser: ParserInterface
  protected def applyPathCredentials(plan: LogicalPlan): LogicalPlan
}
