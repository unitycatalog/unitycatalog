package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.types.{MetadataBuilder, StructType}

/** Converts V2 columns to a schema while preserving generated-column metadata. */
object CatalogV2UtilShim {

  def v2ColumnsToStructType(columns: Array[Column]): StructType = {
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    StructType(schema.fields.zip(columns).map { case (field, column) =>
      Option(column.generationExpression()).fold(field) { expression =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, expression)
          .build())
      }
    })
  }
}
