package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.StructType

import java.util;


case class SparkView(table: CatalogTable) extends View {

  override def name(): String = table.identifier.unquotedString

  override def query(): String = table.viewText.getOrElse("")

  override def currentCatalog(): String = table.identifier.catalog.getOrElse("")


  override def currentNamespace(): Array[String] = table.identifier.nameParts.toArray

  override def schema(): StructType = table.schema

  override def queryColumnNames(): Array[String] = table.viewQueryColumnNames.toArray

  override def columnAliases(): Array[String] = table.viewQueryColumnNames.toArray

  override def columnComments(): Array[String] = Array[String]()

  override def properties(): util.Map[String, String] = {
    import scala.jdk.CollectionConverters._
    table.properties.asJava
  }
}
