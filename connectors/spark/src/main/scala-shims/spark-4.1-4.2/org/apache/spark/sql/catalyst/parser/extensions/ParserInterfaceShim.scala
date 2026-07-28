package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.spark.sql.catalyst.parser.{ParameterContext, ParserInterface}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Spark 4.1+ routes {@code SparkSession.sql(text, args)} through {@code parsePlanWithParameters}.
 * This shim provides that override so the common {@link UCSparkSqlExtensionsParser} never names
 * {@code ParameterContext}.
 */
trait ParserInterfaceShim extends ParserInterface {
  protected def delegateParser: ParserInterface
  protected def applyPathCredentials(plan: LogicalPlan): LogicalPlan

  override def parsePlanWithParameters(
      sqlText: String,
      parameterContext: ParameterContext): LogicalPlan =
    applyPathCredentials(delegateParser.parsePlanWithParameters(sqlText, parameterContext))
}
