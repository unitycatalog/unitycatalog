package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link UCColumnJson}, the Spark V2 {@code Column} &lt;-&gt; UC {@code
 * ColumnInfo.type_json} round-trip helper. Pins the wire-format contract: a happy-path round-trip
 * plus the three malformed-input guards that {@code parseStructFieldJson} adds so a bad/missing
 * {@code type_json} fails with a clear, actionable error instead of a raw NPE / cast exception out
 * of the view-load path.
 *
 * <p>{@code UCColumnJson} is a shared (non-shim) helper that uses only public V2 APIs present on
 * all supported Spark versions, so this suite lives in the shared {@code src/test/java/} tree even
 * though its only caller today is the Spark-4.2 view shim. It calls the Scala {@code object}'s
 * methods through the synthetic {@code MODULE$} singleton.
 */
public class UCColumnJsonSuite {

  private Column parse(String json) {
    return UCColumnJson$.MODULE$.parseStructFieldJson(json);
  }

  @Test
  public void testRoundTripPreservesNameTypeNullableAndComment() {
    Column original = Column.create("region", DataTypes.StringType, true, "geo dimension", null);

    String json = UCColumnJson$.MODULE$.buildStructFieldJson(original);
    Column parsed = parse(json);

    assertThat(parsed.name()).isEqualTo("region");
    assertThat(parsed.dataType()).isEqualTo(DataTypes.StringType);
    assertThat(parsed.nullable()).isTrue();
    assertThat(parsed.comment()).isEqualTo("geo dimension");
  }

  @Test
  public void testNullJsonThrowsIllegalArgument() {
    assertThatThrownBy(() -> parse(null))
        .isInstanceOf(IllegalArgumentException.class)
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
