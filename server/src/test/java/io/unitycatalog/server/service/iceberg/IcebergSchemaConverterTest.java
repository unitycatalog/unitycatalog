package io.unitycatalog.server.service.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class IcebergSchemaConverterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

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
