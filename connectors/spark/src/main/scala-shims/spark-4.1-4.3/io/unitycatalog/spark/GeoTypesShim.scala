package io.unitycatalog.spark

import io.unitycatalog.client.model.ColumnTypeName

import org.apache.spark.sql.types.{DataType, GeographyType, GeometryType}

private[spark] object GeoTypesShim {
  def columnTypeName(dataType: DataType): Option[ColumnTypeName] = dataType match {
    case _: GeometryType => Some(ColumnTypeName.GEOMETRY)
    case _: GeographyType => Some(ColumnTypeName.GEOGRAPHY)
    case _ => None
  }
}
