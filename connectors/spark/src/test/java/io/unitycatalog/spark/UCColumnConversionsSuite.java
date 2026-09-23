package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
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
}
