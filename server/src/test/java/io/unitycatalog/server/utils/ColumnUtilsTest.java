package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.DeltaArrayType;
import io.unitycatalog.server.delta.model.DeltaDecimalType;
import io.unitycatalog.server.delta.model.DeltaMapType;
import io.unitycatalog.server.delta.model.DeltaPrimitiveType;
import io.unitycatalog.server.delta.model.DeltaStructField;
import io.unitycatalog.server.delta.model.DeltaStructType;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for ColumnUtils.toStructField parsing typeJson into typed Delta DeltaStructField. */
public class ColumnUtilsTest {

  private static ColumnInfo col(String name, String typeJson) {
    return new ColumnInfo().name(name).typeJson(typeJson);
  }

  // ---------- Primitives ----------

  @Test
  public void testPrimitive() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col("id", "{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}"));
    assertThat(f.getName()).isEqualTo("id");
    assertThat(f.getNullable()).isFalse();
    assertThat(f.getType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(f.getType().getType()).isEqualTo("long");
    assertThat(f.getMetadata()).isEmpty();
  }

  @Test
  public void testDecimal() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "price",
                "{\"name\":\"price\",\"type\":\"decimal(10,2)\","
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(DeltaDecimalType.class);
    DeltaDecimalType dt = (DeltaDecimalType) f.getType();
    assertThat(dt.getPrecision()).isEqualTo(10);
    assertThat(dt.getScale()).isEqualTo(2);
  }

  // ---------- Complex types with Spark camelCase ----------

  @Test
  public void testArrayCamelCase() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "tags",
                "{\"name\":\"tags\",\"type\":{\"type\":\"array\","
                    + "\"elementType\":\"string\",\"containsNull\":true},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(DeltaArrayType.class);
    DeltaArrayType at = (DeltaArrayType) f.getType();
    assertThat(at.getElementType()).isInstanceOf(DeltaPrimitiveType.class);
    assertThat(at.getElementType().getType()).isEqualTo("string");
    assertThat(at.getContainsNull()).isTrue();
  }

  @Test
  public void testMapCamelCase() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "scores",
                "{\"name\":\"scores\",\"type\":{\"type\":\"map\","
                    + "\"keyType\":\"string\",\"valueType\":\"double\","
                    + "\"valueContainsNull\":false},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(DeltaMapType.class);
    DeltaMapType mt = (DeltaMapType) f.getType();
    assertThat(mt.getKeyType().getType()).isEqualTo("string");
    assertThat(mt.getValueType().getType()).isEqualTo("double");
    assertThat(mt.getValueContainsNull()).isFalse();
  }

  @Test
  public void testStructCamelCase() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "addr",
                "{\"name\":\"addr\",\"type\":{\"type\":\"struct\","
                    + "\"fields\":[{\"name\":\"zip\",\"type\":\"integer\","
                    + "\"nullable\":false,\"metadata\":{}}]},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(DeltaStructType.class);
    DeltaStructType st = (DeltaStructType) f.getType();
    assertThat(st.getFields()).hasSize(1);
    assertThat(st.getFields().get(0).getName()).isEqualTo("zip");
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("integer");
  }

  // ---------- Nested complex ----------

  @Test
  public void testNestedMapArrayStructCamelCase() {
    // map<string, array<struct<v:double>>>
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "data",
                "{\"name\":\"data\",\"type\":{\"type\":\"map\","
                    + "\"keyType\":\"string\","
                    + "\"valueType\":{\"type\":\"array\","
                    + "\"elementType\":{\"type\":\"struct\","
                    + "\"fields\":[{\"name\":\"v\",\"type\":\"double\","
                    + "\"nullable\":false,\"metadata\":{}}]},"
                    + "\"containsNull\":true},"
                    + "\"valueContainsNull\":true},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    DeltaMapType mt = (DeltaMapType) f.getType();
    DeltaArrayType at = (DeltaArrayType) mt.getValueType();
    DeltaStructType st = (DeltaStructType) at.getElementType();
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("double");
  }

  // ---------- Metadata ----------

  @Test
  public void testMetadataPreserved() {
    DeltaStructField f =
        ColumnUtils.toStructField(
            col(
                "id",
                "{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,"
                    + "\"metadata\":{\"delta.columnMapping.id\":1,"
                    + "\"comment\":\"primary key\"}}"));
    assertThat(f.getMetadata()).containsEntry("comment", "primary key");
    assertThat(f.getMetadata()).containsEntry("delta.columnMapping.id", 1);
  }

  // ---------- Roundtrip (read camelCase -> write camelCase) ----------

  /**
   * One nested round-trip covers all three composite shapes (map, array, struct) plus a primitive
   * leaf, the camelCase-not-kebab-case ser format, and structure preservation. Splitting per shape
   * tests the same property four times.
   */
  @Test
  public void testRoundtripNestedPreservesCamelCaseAndStructure() {
    String typeJson =
        "{\"name\":\"data\",\"type\":{\"type\":\"map\","
            + "\"keyType\":\"string\","
            + "\"valueType\":{\"type\":\"array\","
            + "\"elementType\":{\"type\":\"struct\","
            + "\"fields\":[{\"name\":\"v\",\"type\":\"double\","
            + "\"nullable\":false,\"metadata\":{}}]},"
            + "\"containsNull\":true},"
            + "\"valueContainsNull\":true},"
            + "\"nullable\":true,\"metadata\":{}}";
    DeltaStructField f = ColumnUtils.toStructField(col("data", typeJson));
    String written = ColumnUtils.toTypeJson(f);
    // Wire format is camelCase, not kebab-case.
    assertThat(written)
        .contains("\"keyType\"")
        .contains("\"valueType\"")
        .contains("\"valueContainsNull\"")
        .contains("\"elementType\"")
        .contains("\"containsNull\"")
        .doesNotContain("\"key-type\"")
        .doesNotContain("\"element-type\"")
        .doesNotContain("\"contains-null\"");
    // Re-read and verify structure end-to-end.
    DeltaStructField f2 = ColumnUtils.toStructField(col("data", written));
    DeltaMapType mt = (DeltaMapType) f2.getType();
    DeltaArrayType at = (DeltaArrayType) mt.getValueType();
    DeltaStructType st = (DeltaStructType) at.getElementType();
    assertThat(st.getFields().get(0).getName()).isEqualTo("v");
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("double");
  }

  // ---------- Error handling ----------

  @Test
  public void testNullTypeJson() {
    assertThatThrownBy(() -> ColumnUtils.toStructField(col("bad", null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("null/empty typeJson");
  }

  @Test
  public void testMalformedTypeJson() {
    assertThatThrownBy(() -> ColumnUtils.toStructField(col("bad", "not json")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Failed to parse");
  }

  // ---------- toColumnInfo (Delta DeltaStructField -> UC ColumnInfo) ----------

  @Test
  public void testToColumnInfoPrimitive() {
    DeltaStructField field =
        new DeltaStructField()
            .name("id")
            .type(new DeltaPrimitiveType().type("long"))
            .nullable(false)
            .metadata(Map.of());
    ColumnInfo info = ColumnUtils.toColumnInfo(field, 0);
    assertThat(info.getName()).isEqualTo("id");
    assertThat(info.getNullable()).isFalse();
    assertThat(info.getPosition()).isEqualTo(0);
    assertThat(info.getTypeName()).isEqualTo(ColumnTypeName.LONG);
    // LONG -> "bigint" via the SQL-style alias map.
    assertThat(info.getTypeText()).isEqualTo("bigint");
    assertThat(info.getTypeJson()).contains("\"type\":\"long\"");
  }

  @Test
  public void testToColumnInfoDecimalPreservesPrecisionAndScale() {
    DeltaStructField field =
        new DeltaStructField()
            .name("amount")
            .type(new DeltaDecimalType().precision(10).scale(2))
            .nullable(true)
            .metadata(Map.of());
    ColumnInfo info = ColumnUtils.toColumnInfo(field, 1);
    assertThat(info.getTypeName()).isEqualTo(ColumnTypeName.DECIMAL);
    // Precision/scale must reach typeText so DESCRIBE TABLE renders the right SQL type.
    assertThat(info.getTypeText()).isEqualTo("decimal(10,2)");
  }

  @Test
  public void testToColumnInfoComplex() {
    DeltaStructField arr =
        new DeltaStructField()
            .name("tags")
            .type(
                new DeltaArrayType()
                    .type("array")
                    .elementType(new DeltaPrimitiveType().type("string")))
            .nullable(true)
            .metadata(Map.of());
    ColumnInfo arrInfo = ColumnUtils.toColumnInfo(arr, 0);
    assertThat(arrInfo.getTypeName()).isEqualTo(ColumnTypeName.ARRAY);
    // typeText is the Spark catalogString-equivalent, recursively parameterized.
    assertThat(arrInfo.getTypeText()).isEqualTo("array<string>");

    DeltaStructField map =
        new DeltaStructField()
            .name("attrs")
            .type(
                new DeltaMapType()
                    .type("map")
                    .keyType(new DeltaPrimitiveType().type("string"))
                    .valueType(new DeltaPrimitiveType().type("double")))
            .nullable(true)
            .metadata(Map.of());
    ColumnInfo mapInfo = ColumnUtils.toColumnInfo(map, 0);
    assertThat(mapInfo.getTypeName()).isEqualTo(ColumnTypeName.MAP);
    assertThat(mapInfo.getTypeText()).isEqualTo("map<string,double>");

    DeltaStructField struct =
        new DeltaStructField()
            .name("nested")
            .type(
                new DeltaStructType()
                    .type("struct")
                    .fields(
                        List.of(
                            new DeltaStructField()
                                .name("zip")
                                .type(new DeltaPrimitiveType().type("integer"))
                                .nullable(false)
                                .metadata(Map.of()),
                            new DeltaStructField()
                                .name("city")
                                .type(new DeltaPrimitiveType().type("string"))
                                .nullable(true)
                                .metadata(Map.of()))))
            .nullable(true)
            .metadata(Map.of());
    ColumnInfo structInfo = ColumnUtils.toColumnInfo(struct, 0);
    assertThat(structInfo.getTypeName()).isEqualTo(ColumnTypeName.STRUCT);
    assertThat(structInfo.getTypeText()).isEqualTo("struct<zip:int,city:string>");
  }

  @Test
  public void testToColumnInfoNestedCatalogString() {
    // map<string, array<struct<v:double>>> -- the recursion composes the right way down.
    DeltaStructField field =
        new DeltaStructField()
            .name("data")
            .type(
                new DeltaMapType()
                    .type("map")
                    .keyType(new DeltaPrimitiveType().type("string"))
                    .valueType(
                        new DeltaArrayType()
                            .type("array")
                            .elementType(
                                new DeltaStructType()
                                    .type("struct")
                                    .fields(
                                        List.of(
                                            new DeltaStructField()
                                                .name("v")
                                                .type(new DeltaPrimitiveType().type("double"))
                                                .nullable(false)
                                                .metadata(Map.of()))))))
            .nullable(true)
            .metadata(Map.of());
    assertThat(ColumnUtils.toColumnInfo(field, 0).getTypeText())
        .isEqualTo("map<string,array<struct<v:double>>>");
  }

  @Test
  public void testToColumnInfoLiftsCommentFromMetadata() {
    // Delta spec stores column comments in metadata.comment; UCSingleCatalog lifts them into
    // ColumnInfo.comment via field.getComment(), and this mapper does the same so DESCRIBE
    // renders the comment regardless of which client wrote the table.
    DeltaStructField field =
        new DeltaStructField()
            .name("id")
            .type(new DeltaPrimitiveType().type("long"))
            .nullable(false)
            .metadata(Map.of("comment", "primary key"));
    assertThat(ColumnUtils.toColumnInfo(field, 0).getComment()).isEqualTo("primary key");
  }

  @Test
  public void testToColumnInfoNoCommentWhenMetadataAbsentOrNonString() {
    DeltaStructField noMeta =
        new DeltaStructField()
            .name("x")
            .type(new DeltaPrimitiveType().type("long"))
            .nullable(true)
            .metadata(Map.of());
    assertThat(ColumnUtils.toColumnInfo(noMeta, 0).getComment()).isNull();

    DeltaStructField nonStringComment =
        new DeltaStructField()
            .name("y")
            .type(new DeltaPrimitiveType().type("long"))
            .nullable(true)
            .metadata(Map.of("comment", 42));
    // Non-string comment values (spec-invalid but tolerated) are ignored, not coerced.
    assertThat(ColumnUtils.toColumnInfo(nonStringComment, 0).getComment()).isNull();
  }

  /**
   * Both "void" (Spark's NullType wire form, which Spark Delta drops before persisting and Kernel
   * rejects on read) and any other non-spec primitive must be rejected with a clean 400. The
   * server-side {@code ColumnTypeName} enum has no {@code UNKNOWN_DEFAULT_OPEN_API} sentinel
   * (OpenAPI generator adds it client-only), so the only safe answer is to reject.
   */
  @Test
  public void testToColumnInfoRejectsUnsupportedPrimitives() {
    for (String unsupported : List.of("void", "hyperdecimal")) {
      DeltaStructField field =
          new DeltaStructField()
              .name("x")
              .type(new DeltaPrimitiveType().type(unsupported))
              .nullable(true)
              .metadata(Map.of());
      assertThatThrownBy(() -> ColumnUtils.toColumnInfo(field, 0))
          .as("unsupported primitive: %s", unsupported)
          .isInstanceOf(BaseException.class)
          .hasMessageContaining("Unsupported Delta primitive type: " + unsupported);
    }
  }

  // ---------- applyPartitionColumns ----------

  @Test
  public void testApplyPartitionColumnsStampsIndicesByName() {
    List<ColumnInfo> columns =
        new ArrayList<>(
            List.of(
                new ColumnInfo().name("id").position(0),
                new ColumnInfo().name("region").position(1),
                new ColumnInfo().name("date").position(2)));
    // Order of the partition list is the partition-index order; not the column position.
    ColumnUtils.applyPartitionColumns(columns, List.of("date", "region"));
    assertThat(columns.get(0).getPartitionIndex()).isNull();
    assertThat(columns.get(1).getPartitionIndex()).isEqualTo(1); // region -> index 1
    assertThat(columns.get(2).getPartitionIndex()).isEqualTo(0); // date   -> index 0
  }

  @Test
  public void testApplyPartitionColumnsNullAndEmptyAreNoOp() {
    List<ColumnInfo> columns = new ArrayList<>(List.of(new ColumnInfo().name("id").position(0)));
    ColumnUtils.applyPartitionColumns(columns, null);
    ColumnUtils.applyPartitionColumns(columns, List.of());
    assertThat(columns.get(0).getPartitionIndex()).isNull();
  }

  @Test
  public void testApplyPartitionColumnsUnknownColumnRejected() {
    List<ColumnInfo> columns = new ArrayList<>(List.of(new ColumnInfo().name("id").position(0)));
    assertThatThrownBy(() -> ColumnUtils.applyPartitionColumns(columns, List.of("nope")))
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("partition-columns references unknown column: nope");
  }
}
