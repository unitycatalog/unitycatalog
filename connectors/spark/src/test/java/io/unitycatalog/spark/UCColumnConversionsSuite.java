package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.model.ColumnInfo;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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

  /**
   * A nested struct field name that needs quoting (here: one containing a dot) survives the
   * type_json read path. {@code type_text} is a {@code catalogString} and leaves such names
   * unquoted, so the {@code type_text} below is what the server stores and is NOT valid Spark type
   * DDL -- reading it instead would fail with PARSE_SYNTAX_ERROR.
   */
  @Test
  public void testToStructFieldReadsNestedFieldNameNeedingQuotesFromTypeJson() {
    ColumnInfo col =
        new ColumnInfo()
            .name("attributes")
            .typeText("struct<dotted.value:string,dotted.name:string>")
            .typeJson(
                "{\"name\":\"attributes\",\"type\":{\"type\":\"struct\",\"fields\":["
                    + "{\"name\":\"dotted.value\",\"type\":\"string\","
                    + "\"nullable\":true,\"metadata\":{}},"
                    + "{\"name\":\"dotted.name\",\"type\":\"string\","
                    + "\"nullable\":true,\"metadata\":{}}]},"
                    + "\"nullable\":false,\"metadata\":{}}")
            .nullable(false)
            .comment("parcel attributes");

    StructField field = UCColumnConversions.toStructField(col);

    assertThat(field.name()).isEqualTo("attributes");
    assertThat(field.dataType())
        .isEqualTo(
            new StructType()
                .add("dotted.value", DataTypes.StringType)
                .add("dotted.name", DataTypes.StringType));
    assertThat(field.nullable()).isFalse();
    assertThat(field.getComment().get()).isEqualTo("parcel attributes");
  }

  /**
   * A dot in a top-level column name arrives on {@code ColumnInfo.name}, never inside the type
   * text, so it must come through verbatim -- the name is not handed to the type parser.
   */
  @Test
  public void testToStructFieldKeepsDotInTopLevelColumnName() {
    ColumnInfo col =
        new ColumnInfo()
            .name("dotted.value")
            .typeText("string")
            .typeJson(
                "{\"name\":\"dotted.value\",\"type\":\"string\","
                    + "\"nullable\":true,\"metadata\":{}}")
            .nullable(true);

    StructField field = UCColumnConversions.toStructField(col);

    assertThat(field.name()).isEqualTo("dotted.value");
    assertThat(field.dataType()).isEqualTo(DataTypes.StringType);
  }

  /** Columns predating the server's create-time type_json validation still read via type_text. */
  @Test
  public void testToStructFieldFallsBackToTypeTextWhenTypeJsonIsMissing() {
    ColumnInfo col = new ColumnInfo().name("id").typeText("int").nullable(true);

    StructField field = UCColumnConversions.toStructField(col);

    assertThat(field.dataType()).isEqualTo(DataTypes.IntegerType);
    assertThat(field.nullable()).isTrue();
    assertThat(field.getComment().isEmpty()).isTrue();
  }

  @Test
  public void testToStructFieldWithTypeJsonMissingTypeThrowsIllegalArgument() {
    ColumnInfo col =
        new ColumnInfo()
            .name("id")
            .typeText("int")
            .typeJson("{\"name\":\"id\",\"nullable\":true,\"metadata\":{}}")
            .nullable(true);

    assertThatThrownBy(() -> UCColumnConversions.toStructField(col))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Expected `type` in StructField JSON");
  }
}
