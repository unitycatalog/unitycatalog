package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testMetadataOfNullOrEmptyTypeJsonIsEmpty() {
    assertThat(UCColumnConversions.metadataOf("b", null)).isEqualTo(Metadata.empty());
    assertThat(UCColumnConversions.metadataOf("b", "")).isEqualTo(Metadata.empty());
  }

  @Test
  public void testMetadataOfWithoutMetadataKeyIsEmpty() {
    assertThat(UCColumnConversions.metadataOf("b", "{\"name\":\"b\",\"type\":\"integer\"}"))
        .isEqualTo(Metadata.empty());
  }

  @Test
  public void testMetadataOfEmptyNullOrNonObjectMetadataIsEmpty() {
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("{}")))
        .isEqualTo(Metadata.empty());
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("null")))
        .isEqualTo(Metadata.empty());
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("\"x\"")))
        .isEqualTo(Metadata.empty());
    assertThat(UCColumnConversions.metadataOf("b", typeJsonWithMetadata("[1]")))
        .isEqualTo(Metadata.empty());
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
