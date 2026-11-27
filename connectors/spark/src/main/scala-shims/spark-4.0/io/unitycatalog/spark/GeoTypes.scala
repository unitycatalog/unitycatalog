package io.unitycatalog.spark

import io.unitycatalog.client.model.ColumnTypeName
import org.apache.spark.sql.types.DataType

/** Spark 4.0 has no geometry or geography type, so there is nothing to map. */
private[spark] object GeoTypes {
  def typeName(dataType: DataType): Option[ColumnTypeName] = None

  def fromTypeJson(typeText: String, typeJson: String): Option[DataType] = None
}
