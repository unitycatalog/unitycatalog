package io.unitycatalog.spark

import io.unitycatalog.client.model.ColumnTypeName

import org.apache.spark.sql.types.DataType

/** Spark 4.0 has no geometry or geography types. */
private[spark] object GeoTypesShim {
  def columnTypeName(dataType: DataType): Option[ColumnTypeName] = None
}
