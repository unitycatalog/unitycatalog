package org.apache.spark.sql.catalyst.parser.extensions

import io.unitycatalog.spark.ResolvePathCredentials

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

class UCSparkSqlExtensionsParser(spark: SparkSession, delegate: ParserInterface)
    extends ParserInterfaceShim {

  protected def delegateParser: ParserInterface = delegate

  protected def applyPathCredentials(plan: LogicalPlan): LogicalPlan =
    ResolvePathCredentials(spark).apply(plan)

  override def parseDataType(sqlText: String): DataType = delegate.parseDataType(sqlText)

  override def parseExpression(sqlText: String): Expression = delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType = delegate.parseTableSchema(sqlText)

  override def parseRoutineParam(sqlText: String): StructType =
    delegate.parseRoutineParam(sqlText)

  override def parsePlan(sqlText: String): LogicalPlan =
    applyPathCredentials(delegate.parsePlan(sqlText))

  override def parseQuery(sqlText: String): LogicalPlan =
    applyPathCredentials(delegate.parseQuery(sqlText))
}
