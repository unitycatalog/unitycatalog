package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Unit tests for the Spark V2 {@code Column} &lt;-&gt; UC {@code ColumnInfo.type_json} round-trip
 * helpers in {@link UCColumnConversions}. Pins the wire-format contract: a happy-path round-trip
 * plus the three malformed-input guards that {@code parseColumnJson} adds so a bad/missing {@code
 * type_json} fails with a clear, actionable message instead of an opaque cast/parse exception out
 * of the view-load path.
 *
 * <p>{@code UCColumnConversions} is a shared (non-shim) helper that uses only public V2 APIs
 * present on all supported Spark versions, so this suite lives in the shared {@code src/test/java/}
 * tree even though the {@code Column}-based helpers are only called from the Spark-4.2 view shim
 * today.
 */
public class UCColumnConversionsSuite {

  private Column parse(String json) {
    return UCColumnConversions.parseColumnJson(json);
  }

  @Test
  public void testRoundTripPreservesNameTypeNullableAndComment() {
    Column original = Column.create("region", DataTypes.StringType, true, "geo dimension", null);

    String json = UCColumnConversions.buildColumnJson(original);
    Column parsed = parse(json);

    assertThat(parsed.name()).isEqualTo("region");
    assertThat(parsed.dataType()).isEqualTo(DataTypes.StringType);
    assertThat(parsed.nullable()).isTrue();
    assertThat(parsed.comment()).isEqualTo("geo dimension");
  }

  @Test
  public void testNullJsonThrowsNullPointer() {
    assertThatThrownBy(() -> parse(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Column type_json is missing");
  }

  @Test
  public void testNonStringNameThrowsIllegalArgument() {
    String json = "{\"name\":123,\"type\":\"string\",\"nullable\":true,\"metadata\":{}}";
    assertThatThrownBy(() -> parse(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected string `name`");
  }

  @Test
  public void testNonBooleanNullableThrowsIllegalArgument() {
    String json = "{\"name\":\"region\",\"type\":\"string\",\"nullable\":\"yes\",\"metadata\":{}}";
    assertThatThrownBy(() -> parse(json))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected boolean `nullable`");
  }

  private static String typeJsonWithMetadata(String metadataJson) {
    return "{\"name\":\"b\",\"type\":\"integer\",\"nullable\":true,\"metadata\":"
        + metadataJson
        + "}";
  }

  @Test
  public void testMetadataOfReadsTypeJsonMetadata() {
    String json = typeJsonWithMetadata("{\"CURRENT_DEFAULT\":\"42\",\"EXISTS_DEFAULT\":\"42\"}");
    Metadata expected =
        new MetadataBuilder()
            .putString("CURRENT_DEFAULT", "42")
            .putString("EXISTS_DEFAULT", "42")
            .build();
    assertThat(UCColumnConversions.metadataOf("b", json)).isEqualTo(expected);
  }

  @Test
  public void testMetadataOfDropsDeltaColumnMappingKeys() {
    String json =
        typeJsonWithMetadata(
            "{\"delta.columnMapping.id\":3,"
                + "\"delta.columnMapping.physicalName\":\"col-5f1c\","
                + "\"delta.columnMapping.nested.ids\":{\"b.element\":4},"
                + "\"delta.columnMappingMode\":\"kept\","
                + "\"__CHAR_VARCHAR_TYPE_STRING\":\"varchar(3)\","
                + "\"CURRENT_DEFAULT\":\"'x'\"}");
    Metadata expected =
        new MetadataBuilder()
            .putString("delta.columnMappingMode", "kept")
            .putString("__CHAR_VARCHAR_TYPE_STRING", "varchar(3)")
            .putString("CURRENT_DEFAULT", "'x'")
            .build();
    assertThat(UCColumnConversions.metadataOf("b", json)).isEqualTo(expected);
  }

  @Test
  public void testMetadataOfReadsNonStringValues() {
    String json =
        typeJsonWithMetadata(
            "{\"n\":7,\"d\":1.5,\"flag\":true,\"tags\":[\"x\",\"y\"],\"nested\":{\"k\":\"v\"}}");
    Metadata expected =
        new MetadataBuilder()
            .putLong("n", 7)
            .putDouble("d", 1.5)
            .putBoolean("flag", true)
            .putStringArray("tags", new String[] {"x", "y"})
            .putMetadata("nested", new MetadataBuilder().putString("k", "v").build())
            .build();
    assertThat(UCColumnConversions.metadataOf("b", json)).isEqualTo(expected);
  }

  static Stream<Arguments> typeJsonWithoutMetadata() {
    return Stream.of(
        Arguments.of("null type_json", null),
        Arguments.of("empty type_json", ""),
        Arguments.of("missing metadata", "{\"name\":\"b\",\"type\":\"integer\"}"),
        Arguments.of("empty metadata", typeJsonWithMetadata("{}")),
        Arguments.of("null metadata", typeJsonWithMetadata("null")),
        Arguments.of("string metadata", typeJsonWithMetadata("\"x\"")),
        Arguments.of("array metadata", typeJsonWithMetadata("[1]")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("typeJsonWithoutMetadata")
  public void testMetadataOfAbsentMetadataIsEmpty(String description, String typeJson) {
    assertThat(UCColumnConversions.metadataOf("b", typeJson)).isEqualTo(Metadata.empty());
  }

  @Test
  public void testMetadataOfMalformedTypeJsonIsEmpty() {
    assertThat(UCColumnConversions.metadataOf("b", "{not json")).isEqualTo(Metadata.empty());
  }

  @Test
  public void testMetadataOfUnrepresentableMetadataIsEmpty() {
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("{\"mixed\":[1,\"a\"]}")))
        .isEqualTo(Metadata.empty());
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("{\"nested\":[[1]]}")))
        .isEqualTo(Metadata.empty());
  }
}
