package io.unitycatalog.spark

import org.apache.spark.sql.catalyst.util.{GeneratedColumn, IdentityColumn}
import org.apache.spark.sql.connector.catalog.{
  CatalogV2UtilWithColumnMetadata,
  Column,
  ColumnDefaultValue,
  IdentityColumnSpec
}
import org.apache.spark.sql.connector.expressions.Expressions
import org.apache.spark.sql.types.DataTypes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class CatalogV2UtilSuite {

  @Test
  def v2ColumnsToStructTypePreservesGenerationExpression(): Unit = {
    val columns = Array(
      Column.create("base", DataTypes.IntegerType),
      Column.create(
        "with_default",
        DataTypes.IntegerType,
        true,
        null,
        new ColumnDefaultValue("42", Expressions.literal(Integer.valueOf(42))),
        null),
      Column.create(
        "generated", DataTypes.IntegerType, true, "derived value", "base + 1", null))

    val schema = CatalogV2UtilWithColumnMetadata.v2ColumnsToStructType(columns)
    val generated = schema("generated")

    assertEquals(Some("derived value"), generated.getComment)
    assertEquals(Some("42"), schema("with_default").getCurrentDefaultValue)
    assertEquals(
      "base + 1",
      generated.metadata.getString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY))
  }

  @Test
  def v2ColumnsToStructTypePreservesIdentityColumnSpec(): Unit = {
    val identitySpec = new IdentityColumnSpec(100L, 5L, false)
    val columns = Array(
      Column.create("value", DataTypes.StringType),
      Column.create(
        "id",
        DataTypes.LongType,
        false,
        "identity value",
        identitySpec,
        """{"customMetadata":"preserved"}"""))

    val identity = CatalogV2UtilWithColumnMetadata.v2ColumnsToStructType(columns)("id")

    assertEquals(Some("identity value"), identity.getComment)
    assertEquals("preserved", identity.metadata.getString("customMetadata"))
    assertEquals(
      identitySpec.getStart,
      identity.metadata.getLong(IdentityColumn.IDENTITY_INFO_START))
    assertEquals(
      identitySpec.getStep,
      identity.metadata.getLong(IdentityColumn.IDENTITY_INFO_STEP))
    assertEquals(
      identitySpec.isAllowExplicitInsert,
      identity.metadata.getBoolean(IdentityColumn.IDENTITY_INFO_ALLOW_EXPLICIT_INSERT))
  }
}
