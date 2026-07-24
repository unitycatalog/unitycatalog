package org.apache.spark.sql.connector.catalog

import org.apache.spark.sql.catalyst.util.{GeneratedColumn, IdentityColumn}
import org.apache.spark.sql.types.{MetadataBuilder, StructType}

/** Converts V2 columns to a schema while preserving generated and identity column metadata. */
object CatalogV2UtilShim {

  def v2ColumnsToStructType(columns: Array[Column]): StructType = {
    val schema = CatalogV2Util.v2ColumnsToStructType(columns)
    StructType(schema.fields.zip(columns).map { case (field, column) =>
      val metadata = new MetadataBuilder().withMetadata(field.metadata)
      Option(column.generationExpression()).foreach { expression =>
        metadata.putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, expression)
      }
      Option(column.identityColumnSpec()).foreach { identity =>
        metadata
          .putLong(IdentityColumn.IDENTITY_INFO_START, identity.getStart)
          .putLong(IdentityColumn.IDENTITY_INFO_STEP, identity.getStep)
          .putBoolean(
            IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT,
            identity.isAllowExplicitInsert)
      }
      field.copy(metadata = metadata.build())
    })
  }
}
