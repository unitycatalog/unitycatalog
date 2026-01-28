package io.unitycatalog.spark

import io.unitycatalog.client.model.{ViewInfo => UCViewInfo}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.types._

import scala.jdk.CollectionConverters._

object UCViewMapper {
  def toSparkView(ucViewInfo: UCViewInfo, ident: Identifier): View = {
    val schema = buildSchema(ucViewInfo)
    val sql = Option(ucViewInfo.getRepresentations)
      .flatMap(_.asScala.find(_.getDialect == "spark"))
      .orElse(Option(ucViewInfo.getRepresentations).flatMap(_.asScala.headOption))
      .map(_.getSql)
      .getOrElse("")

    val properties = Option(ucViewInfo.getProperties)
      .map(_.asScala.toMap)
      .getOrElse(Map.empty[String, String])

    new UCView(ident, schema, sql, properties)
  }

  private def buildSchema(ucViewInfo: UCViewInfo): StructType = {
    val fields = Option(ucViewInfo.getColumns)
      .map(_.asScala)
      .getOrElse(Seq.empty)
      .map { col =>
        val dataType = parseTypeText(col.getTypeText)
        val nullable = Option(col.getNullable).map(_.booleanValue()).getOrElse(true)
        StructField(col.getName, dataType, nullable)
      }
    StructType(fields.toSeq)
  }

  private def parseTypeText(typeText: String): DataType = {
    try {
      DataType.fromDDL(typeText)
    } catch {
      case _: Exception => StringType
    }
  }
}

class UCView(
    private val identifier: Identifier,
    private val viewSchema: StructType,
    private val viewSql: String,
    private val viewProperties: Map[String, String]
) extends View {
  override def name(): String = identifier.name()
  override def schema(): StructType = viewSchema
  override def query(): String = viewSql
  override def queryColumnNames(): Array[String] = viewSchema.fieldNames
  override def currentCatalog(): String = identifier.namespace()(0)
  override def currentNamespace(): Array[String] = identifier.namespace()
  override def properties(): java.util.Map[String, String] = viewProperties.asJava
  override def columnAliases(): Array[String] = Array.empty
  override def columnComments(): Array[String] = Array.empty
}

