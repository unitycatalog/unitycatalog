package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.types.StructType

import java.net.URI
import java.util
import scala.collection.JavaConverters._

/**
 * Builds a Spark [[CatalogTable]] from Java-friendly parameters, hiding the
 * Scala interop ceremony. Called from the Java backend implementations.
 */
object CatalogTableBuilder {

  def build(
    identifier: TableIdentifier,
    isManaged: Boolean,
    locationUri: URI,
    properties: util.Map[String, String],
    schema: StructType,
    provider: String,
    createTime: Long,
    partitionColumnNames: util.List[String]): CatalogTable = {
    CatalogTable(
      identifier,
      tableType = if (isManaged) CatalogTableType.MANAGED else CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(locationUri),
        properties = properties.asScala.toMap
      ),
      schema = schema,
      provider = Some(provider),
      createTime = createTime,
      tracksPartitionsInCatalog = false,
      partitionColumnNames = partitionColumnNames.asScala.toSeq
    )
  }

  def toV1Table(catalogTable: CatalogTable): Table = {
    Class.forName("org.apache.spark.sql.connector.catalog.V1Table")
      .getDeclaredConstructor(classOf[CatalogTable])
      .newInstance(catalogTable)
      .asInstanceOf[Table]
  }
}
