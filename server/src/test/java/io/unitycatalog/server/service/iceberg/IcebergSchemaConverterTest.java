package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class IcebergSchemaConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static Stream<Arguments> nativePrimitives() {
    return Stream.of(
        Arguments.of("boolean", Types.BooleanType.get()),
        Arguments.of("integer", Types.IntegerType.get()),
        Arguments.of("int", Types.IntegerType.get()),
        Arguments.of("long", Types.LongType.get()),
        Arguments.of("float", Types.FloatType.get()),
        Arguments.of("double", Types.DoubleType.get()),
        Arguments.of("date", Types.DateType.get()),
        Arguments.of("timestamp", Types.TimestampType.withZone()),
        Arguments.of("timestamp_ntz", Types.TimestampType.withoutZone()),
        Arguments.of("string", Types.StringType.get()),
        Arguments.of("binary", Types.BinaryType.get()),
        Arguments.of("variant", Types.VariantType.get()),
        Arguments.of("decimal(18,4)", Types.DecimalType.of(18, 4)));
  }

  private static ColumnInfo nativeColumn(String typeJson) {
    return new ColumnInfo()
        .name("v")
        .position(0)
        .nullable(false)
        .typeJson(typeJson)
        .comment("a column");
  }

  @ParameterizedTest
  @MethodSource("nativePrimitives")
  void convertsNativePrimitivesWithoutWidening(String nativeType, Type expected) {
    var column =
        nativeColumn(
            "{\"name\":\"v\",\"type\":\"" + nativeType + "\",\"nullable\":false,\"metadata\":{}}");
    var schema = IcebergSchemaConverter.toIcebergSchema(List.of(column));
    assertThat(schema.columns())
        .containsExactly(Types.NestedField.required(1, "v", expected, "a column"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void convertsNestedNativeTypesWithStableUniqueIdsAndNullability(boolean nullable) {
    String json =
        """
        {"name":"v","nullable":false,"metadata":{},"type":{"type":"struct","fields":[
          {"name":"items","nullable":true,"metadata":{"comment":"items comment"},
           "type":{"type":"array","elementType":"long","containsNull":%s}},
          {"name":"lookup","nullable":false,"metadata":{},
           "type":{"type":"map","keyType":"string","valueType":"integer","valueContainsNull":%s}}
        ]}}
        """
            .formatted(nullable, nullable);
    var column = nativeColumn(json);
    var schema = IcebergSchemaConverter.toIcebergSchema(List.of(column));
    assertThat(schema.findField("v.items").doc()).isEqualTo("items comment");
    assertThat(schema.findField("v.items").type().asListType().isElementOptional())
        .isEqualTo(nullable);
    assertThat(schema.findField("v.items.element").type()).isEqualTo(Types.LongType.get());
    assertThat(schema.findField("v.lookup").type().asMapType().isValueOptional())
        .isEqualTo(nullable);
    assertThat(schema.findField("v.lookup.key").type()).isEqualTo(Types.StringType.get());
    assertThat(schema.findField("v.lookup.value").type()).isEqualTo(Types.IntegerType.get());
    assertThat(TypeUtil.indexById(schema.asStruct()).keySet())
        .containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
    assertThat(IcebergSchemaConverter.toIcebergSchema(List.of(column)).asStruct())
        .isEqualTo(schema.asStruct());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"byte", "short", "void", "char(8)", "unknown", "string collate UTF8_LCASE"})
  void rejectsUnsupportedNativeTypes(String type) {
    var column =
        nativeColumn(
            "{\"name\":\"v\",\"nullable\":true,\"metadata\":{},\"type\":\"" + type + "\"}");
    assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(column)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Unsupported native view type");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"", "not-json", "{}", "{\"name\":\"v\",\"nullable\":true,\"metadata\":{}}"})
  void rejectsMalformedNativeSchemas(String typeJson) {
    assertThatThrownBy(
            () -> IcebergSchemaConverter.toIcebergSchema(List.of(nativeColumn(typeJson))))
        .isInstanceOf(BadRequestException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        """
        {"type":"struct","fields":[{"name":"x","type":"string"},{"name":"x","type":"long"}]}
        """,
        """
        {"type":"struct","fields":[{"name":"a.b","type":"string"},
          {"name":"a","type":{"type":"struct","fields":[{"name":"b","type":"long"}]}}]}
        """,
        "{\"type\":\"struct\",\"fields\":[{\"type\":\"long\"}]}",
        "{\"type\":\"struct\",\"fields\":null}",
        "{\"type\":\"struct\",\"fields\":[null]}",
        "{\"type\":\"decimal\"}",
        "{\"type\":\"decimal\",\"precision\":8}",
        "{\"type\":\"decimal\",\"precision\":0,\"scale\":0}",
        "{\"type\":\"decimal\",\"precision\":8,\"scale\":9}"
      })
  void rejectsMalformedOrAmbiguousNativeTypes(String type) {
    var column =
        nativeColumn(
            """
            {"name":"v","type":%s,"nullable":false,"metadata":{}}
            """
                .formatted(type));
    assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(column)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Native view");
  }

  @Test
  void rejectsCollationsInSparkFieldMetadataAtEveryDepth() {
    String field =
        """
        {"name":"v","type":"string","nullable":false,
         "metadata":{"__COLLATIONS":{"v":"spark.UTF8_LCASE"}}}
        """;
    var nested =
        """
        {"name":"v","type":{"type":"array","elementType":{"type":"map","keyType":"string",
         "valueType":{"type":"struct","fields":[%s]}}},"nullable":false,"metadata":{}}
        """
            .formatted(field);
    for (var fieldJson : List.of(field, nested)) {
      assertThatThrownBy(
              () -> IcebergSchemaConverter.toIcebergSchema(List.of(nativeColumn(fieldJson))))
          .isInstanceOf(BadRequestException.class)
          .hasMessageContaining("string collation");
      assertThat(
              IcebergSchemaConverter.toIcebergSchema(
                      List.of(
                          nativeColumn(fieldJson.replace("spark.UTF8_LCASE", "spark.UTF8_BINARY"))))
                  .columns())
          .hasSize(1);
    }
  }

  @Test
  void checksCollationWhenTheConnectorStoresItOnlyInTypeText() {
    String type =
        """
        {"type":"array","elementType":"string","containsNull":true}
        """;
    var column =
        nativeColumn(
            """
            {"name":"v","type":%s,"nullable":false,"metadata":{}}
            """
                .formatted(type));
    column.setTypeText("array<string collate UTF8_LCASE>");
    assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(column)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("string collation");
    column.setTypeText("array<string collate UTF8_BINARY>");
    assertThat(IcebergSchemaConverter.toIcebergSchema(List.of(column)).columns()).hasSize(1);

    var struct =
        nativeColumn(
            """
            {"name":"v","type":{"type":"struct","fields":[{"name":"x:string collate UTF8_LCASE>",
            "type":"string","nullable":true,"metadata":{}}]},"nullable":false,"metadata":{}}
            """);
    struct.setTypeText("struct<`x:string collate UTF8_LCASE>`:string>");
    assertThat(IcebergSchemaConverter.toIcebergSchema(List.of(struct)).columns()).hasSize(1);
  }

  @Test
  void nativeColumnsFollowPositionsAndRejectAmbiguousOrderingOrNames() {
    var first =
        nativeColumn("{\"name\":\"v\",\"type\":\"long\",\"nullable\":false,\"metadata\":{}}");
    var second =
        nativeColumn("{\"name\":\"w\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}")
            .name("w")
            .position(1);
    assertThat(IcebergSchemaConverter.toIcebergSchema(List.of(second, first)).columns())
        .extracting(Types.NestedField::name)
        .containsExactly("v", "w");
    for (int invalid : List.of(-1, 1)) {
      first.setPosition(invalid);
      assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(first)))
          .isInstanceOf(BadRequestException.class)
          .hasMessageContaining("order or name");
    }
    first.setPosition(0);
    second.setPosition(0);
    assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(first, second)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("order or name");
    second.setPosition(1);
    second.setName("v");
    assertThatThrownBy(() -> IcebergSchemaConverter.toIcebergSchema(List.of(first, second)))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("order or name");
  }

  @Test
  public void convertsPrimitivesWithPositionAndNullability() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get(), "the name"));

    List<ColumnInfo> columns = IcebergSchemaConverter.toColumnInfos(schema);

    assertThat(columns).hasSize(2);

    ColumnInfo id = columns.get(0);
    assertThat(id.getName()).isEqualTo("id");
    assertThat(id.getPosition()).isEqualTo(0);
    assertThat(id.getNullable()).isFalse();
    assertThat(id.getTypeName()).isEqualTo(ColumnTypeName.LONG);
    // Spark DDL text renders LONG as "bigint" while the type JSON uses "long".
    assertThat(id.getTypeText()).isEqualTo("bigint");

    ColumnInfo name = columns.get(1);
    assertThat(name.getName()).isEqualTo("name");
    assertThat(name.getPosition()).isEqualTo(1);
    assertThat(name.getNullable()).isTrue();
    assertThat(name.getComment()).isEqualTo("the name");
    assertThat(name.getTypeName()).isEqualTo(ColumnTypeName.STRING);
  }

  @Test
  public void rendersStructFieldTypeJsonInSparkShape() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    ColumnInfo id = IcebergSchemaConverter.toColumnInfos(schema).get(0);
    JsonNode json = MAPPER.readTree(id.getTypeJson());

    assertThat(json.get("name").asText()).isEqualTo("id");
    assertThat(json.get("type").asText()).isEqualTo("long");
    assertThat(json.get("nullable").asBoolean()).isFalse();
    assertThat(json.get("metadata")).isNotNull();

    ColumnInfo named =
        IcebergSchemaConverter.toColumnInfos(
                new Schema(
                    Types.NestedField.optional(2, "name", Types.StringType.get(), "the name")))
            .get(0);
    assertThat(MAPPER.readTree(named.getTypeJson()).path("metadata").path("comment").asText())
        .isEqualTo("the name");
  }

  @Test
  public void carriesDecimalPrecisionAndScale() {
    Schema schema =
        new Schema(Types.NestedField.required(1, "amount", Types.DecimalType.of(18, 4)));

    ColumnInfo amount = IcebergSchemaConverter.toColumnInfos(schema).get(0);
    assertThat(amount.getTypeName()).isEqualTo(ColumnTypeName.DECIMAL);
    assertThat(amount.getTypePrecision()).isEqualTo(18);
    assertThat(amount.getTypeScale()).isEqualTo(4);
    assertThat(amount.getTypeText()).isEqualTo("decimal(18,4)");
  }

  @Test
  public void distinguishesTimestampWithAndWithoutZone() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "ts", Types.TimestampType.withZone()),
            Types.NestedField.required(2, "ts_ntz", Types.TimestampType.withoutZone()));

    List<ColumnInfo> columns = IcebergSchemaConverter.toColumnInfos(schema);
    assertThat(columns.get(0).getTypeName()).isEqualTo(ColumnTypeName.TIMESTAMP);
    assertThat(columns.get(1).getTypeName()).isEqualTo(ColumnTypeName.TIMESTAMP_NTZ);
  }

  @Test
  public void mapsUuidToStringLossily() {
    Schema schema = new Schema(Types.NestedField.required(1, "key", Types.UUIDType.get()));

    ColumnInfo key = IcebergSchemaConverter.toColumnInfos(schema).get(0);
    assertThat(key.getTypeName()).isEqualTo(ColumnTypeName.STRING);
    assertThat(key.getTypeText()).isEqualTo("string");
  }

  @Test
  public void quotesNestedFieldNamesInSparkDdl() {
    var struct =
        Types.StructType.of(
            Types.NestedField.optional(5, "a.b", Types.IntegerType.get()),
            Types.NestedField.optional(6, "c`d", Types.StringType.get()));
    var map = Types.MapType.ofRequired(3, 4, Types.StringType.get(), struct);
    var schema =
        new Schema(Types.NestedField.required(1, "items", Types.ListType.ofRequired(2, map)));

    assertThat(IcebergSchemaConverter.toColumnInfos(schema).get(0).getTypeText())
        .isEqualTo("array<map<string,struct<`a.b`:int,`c``d`:string>>>");
  }

  @Test
  public void convertsNestedStructListAndMap() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "props",
                Types.StructType.of(
                    Types.NestedField.optional(2, "a", Types.IntegerType.get()),
                    Types.NestedField.required(3, "b", Types.StringType.get()))),
            Types.NestedField.required(
                4, "tags", Types.ListType.ofOptional(5, Types.StringType.get())),
            Types.NestedField.required(
                6,
                "scores",
                Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.DoubleType.get())));

    List<ColumnInfo> columns = IcebergSchemaConverter.toColumnInfos(schema);

    ColumnInfo props = columns.get(0);
    assertThat(props.getTypeName()).isEqualTo(ColumnTypeName.STRUCT);
    assertThat(props.getTypeText()).isEqualTo("struct<a:int,b:string>");

    ColumnInfo tags = columns.get(1);
    assertThat(tags.getTypeName()).isEqualTo(ColumnTypeName.ARRAY);
    assertThat(tags.getTypeText()).isEqualTo("array<string>");
    JsonNode tagsJson = MAPPER.readTree(tags.getTypeJson()).get("type");
    assertThat(tagsJson.get("type").asText()).isEqualTo("array");
    assertThat(tagsJson.get("elementType").asText()).isEqualTo("string");
    assertThat(tagsJson.get("containsNull").asBoolean()).isTrue();

    ColumnInfo scores = columns.get(2);
    assertThat(scores.getTypeName()).isEqualTo(ColumnTypeName.MAP);
    assertThat(scores.getTypeText()).isEqualTo("map<string,double>");
    JsonNode scoresJson = MAPPER.readTree(scores.getTypeJson()).get("type");
    assertThat(scoresJson.get("type").asText()).isEqualTo("map");
    assertThat(scoresJson.get("keyType").asText()).isEqualTo("string");
    assertThat(scoresJson.get("valueType").asText()).isEqualTo("double");
    assertThat(scoresJson.get("valueContainsNull").asBoolean()).isFalse();
  }
}
