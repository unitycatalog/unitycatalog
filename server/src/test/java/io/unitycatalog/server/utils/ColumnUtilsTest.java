package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.delta.model.ArrayType;
import io.unitycatalog.server.delta.model.DecimalType;
import io.unitycatalog.server.delta.model.MapType;
import io.unitycatalog.server.delta.model.PrimitiveType;
import io.unitycatalog.server.delta.model.StructField;
import io.unitycatalog.server.delta.model.StructType;
import io.unitycatalog.server.model.ColumnInfo;
import org.junit.jupiter.api.Test;

/** Tests for ColumnUtils.toStructField parsing typeJson into typed Delta StructField. */
public class ColumnUtilsTest {

  private static ColumnInfo col(String name, String typeJson) {
    return new ColumnInfo().name(name).typeJson(typeJson);
  }

  // ---------- Primitives ----------

  @Test
  public void testPrimitive() {
    StructField f =
        ColumnUtils.toStructField(
            col("id", "{\"name\":\"id\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}"));
    assertThat(f.getName()).isEqualTo("id");
    assertThat(f.getNullable()).isFalse();
    assertThat(f.getType()).isInstanceOf(PrimitiveType.class);
    assertThat(f.getType().getType()).isEqualTo("long");
    assertThat(f.getMetadata()).isEmpty();
  }

  @Test
  public void testDecimal() {
    StructField f =
        ColumnUtils.toStructField(
            col(
                "price",
                "{\"name\":\"price\",\"type\":\"decimal(10,2)\","
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(DecimalType.class);
    DecimalType dt = (DecimalType) f.getType();
    assertThat(dt.getPrecision()).isEqualTo(10);
    assertThat(dt.getScale()).isEqualTo(2);
  }

  // ---------- Complex types with Spark camelCase ----------

  @Test
  public void testArrayCamelCase() {
    StructField f =
        ColumnUtils.toStructField(
            col(
                "tags",
                "{\"name\":\"tags\",\"type\":{\"type\":\"array\","
                    + "\"elementType\":\"string\",\"containsNull\":true},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(ArrayType.class);
    ArrayType at = (ArrayType) f.getType();
    assertThat(at.getElementType()).isInstanceOf(PrimitiveType.class);
    assertThat(at.getElementType().getType()).isEqualTo("string");
    assertThat(at.getContainsNull()).isTrue();
  }

  @Test
  public void testMapCamelCase() {
    StructField f =
        ColumnUtils.toStructField(
            col(
                "scores",
                "{\"name\":\"scores\",\"type\":{\"type\":\"map\","
                    + "\"keyType\":\"string\",\"valueType\":\"double\","
                    + "\"valueContainsNull\":false},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(MapType.class);
    MapType mt = (MapType) f.getType();
    assertThat(mt.getKeyType().getType()).isEqualTo("string");
    assertThat(mt.getValueType().getType()).isEqualTo("double");
    assertThat(mt.getValueContainsNull()).isFalse();
  }

  @Test
  public void testStructCamelCase() {
    StructField f =
        ColumnUtils.toStructField(
            col(
                "addr",
                "{\"name\":\"addr\",\"type\":{\"type\":\"struct\","
                    + "\"fields\":[{\"name\":\"zip\",\"type\":\"integer\","
                    + "\"nullable\":false,\"metadata\":{}}]},"
                    + "\"nullable\":true,\"metadata\":{}}"));
    assertThat(f.getType()).isInstanceOf(StructType.class);
    StructType st = (StructType) f.getType();
    assertThat(st.getFields()).hasSize(1);
    assertThat(st.getFields().get(0).getName()).isEqualTo("zip");
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("integer");
  }

  // ---------- Nested complex ----------

  @Test
  public void testNestedMapArrayStructCamelCase() {
    // map<string, array<struct<v:double>>>
    StructField f =
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
    MapType mt = (MapType) f.getType();
    ArrayType at = (ArrayType) mt.getValueType();
    StructType st = (StructType) at.getElementType();
    assertThat(st.getFields().get(0).getType().getType()).isEqualTo("double");
  }

  // ---------- Metadata ----------

  @Test
  public void testMetadataPreserved() {
    StructField f =
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

  @Test
  public void testRoundtripPrimitive() {
    String typeJson = "{\"name\":\"id\",\"type\":\"long\"," + "\"nullable\":false,\"metadata\":{}}";
    StructField f = ColumnUtils.toStructField(col("id", typeJson));
    String written = ColumnUtils.toTypeJson(f);
    assertThat(written).contains("\"type\":\"long\"");
    assertThat(written).contains("\"name\":\"id\"");
  }

  @Test
  public void testRoundtripArray() {
    String typeJson =
        "{\"name\":\"tags\",\"type\":{\"type\":\"array\","
            + "\"elementType\":\"string\",\"containsNull\":true},"
            + "\"nullable\":true,\"metadata\":{}}";
    StructField f = ColumnUtils.toStructField(col("tags", typeJson));
    String written = ColumnUtils.toTypeJson(f);
    // Must serialize back to camelCase, not kebab-case
    assertThat(written).contains("\"elementType\"");
    assertThat(written).contains("\"containsNull\"");
    assertThat(written).doesNotContain("\"element-type\"");
    assertThat(written).doesNotContain("\"contains-null\"");
  }

  @Test
  public void testRoundtripMap() {
    String typeJson =
        "{\"name\":\"m\",\"type\":{\"type\":\"map\","
            + "\"keyType\":\"string\",\"valueType\":\"double\","
            + "\"valueContainsNull\":false},"
            + "\"nullable\":true,\"metadata\":{}}";
    StructField f = ColumnUtils.toStructField(col("m", typeJson));
    String written = ColumnUtils.toTypeJson(f);
    assertThat(written).contains("\"keyType\"");
    assertThat(written).contains("\"valueType\"");
    assertThat(written).contains("\"valueContainsNull\"");
    assertThat(written).doesNotContain("\"key-type\"");
  }

  @Test
  public void testRoundtripNestedPreservesStructure() {
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
    StructField f = ColumnUtils.toStructField(col("data", typeJson));
    String written = ColumnUtils.toTypeJson(f);
    // Re-read and verify structure
    StructField f2 = ColumnUtils.toStructField(col("data", written));
    MapType mt = (MapType) f2.getType();
    ArrayType at = (ArrayType) mt.getValueType();
    StructType st = (StructType) at.getElementType();
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
}
