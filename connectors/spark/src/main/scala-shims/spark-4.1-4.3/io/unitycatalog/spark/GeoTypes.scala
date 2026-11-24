package io.unitycatalog.spark

import io.unitycatalog.client.model.ColumnTypeName
import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType, StructType}

/** Maps Spark's geometry and geography types (Spark 4.1+) to and from Unity Catalog's. */
private[spark] object GeoTypes {
  def typeName(dataType: DataType): Option[ColumnTypeName] = dataType match {
    case _: GeometryType => Some(ColumnTypeName.GEOMETRY)
    case _: GeographyType => Some(ColumnTypeName.GEOGRAPHY)
    case _ => None
  }

  private val GeoTypeName = """(?i)\bgeo(?:metry|graphy)\b""".r

  /**
   * The Spark type of a column whose `typeText` names a geo type, read from its Spark StructField
   * JSON, or None for any other column.
   */
  def fromTypeJson(typeText: String, typeJson: String): Option[DataType] = {
    if (typeJson == null || GeoTypeName.findFirstIn(typeText).isEmpty) {
      None
    } else {
      val field = DataType.fromJson(s"""{"type":"struct","fields":[$typeJson]}""")
      Some(field.asInstanceOf[StructType].fields.head.dataType)
    }
  }
}
