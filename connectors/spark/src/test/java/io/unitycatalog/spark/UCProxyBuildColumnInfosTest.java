package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.TableInfo;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;
import scala.Function1;
import scala.collection.JavaConverters;
import scala.runtime.AbstractFunction1;

/**
 * Unit tests that pin down the on-the-wire shape of {@code ColumnInfo.type_json} produced by {@code
 * UCProxy.buildColumnInfos}.
 *
 * <p>The contract -- matching Databricks runtime's {@code TypeConversionUtils.toProto}, which emits
 * {@code typeJson = Some(field.toJson)} -- is that {@code type_json} is a full Spark {@code
 * StructField} JSON object (with {@code name}, {@code type}, {@code nullable}, and {@code
 * metadata}), not just the raw {@code DataType} JSON. This is required so that column-level
 * metadata attached by Spark's analyzer (for example {@code metric_view.type} and {@code
 * metric_view.expr} for metric view dimension / measure columns) survives the round-trip through
 * Unity Catalog.
 */
public class UCProxyBuildColumnInfosTest {

  private static final ObjectMapper JSON = new ObjectMapper();

  /**
   * Scala singleton objects are exposed to Java as {@code ClassName$.MODULE$}. Using reflection
   * here keeps the test robust to future private-method rearrangement inside the UCProxy object.
   */
  @SuppressWarnings("unchecked")
  private static List<ColumnInfo> invokeBuildColumnInfos(TableInfo tableInfo) throws Exception {
    Class<?> moduleClass = Class.forName("io.unitycatalog.spark.UCProxy$");
    Object module = moduleClass.getField("MODULE$").get(null);
    Method method = moduleClass.getMethod("buildColumnInfos", TableInfo.class, Function1.class);
    Function1<DataType, ColumnTypeName> convert =
        new AbstractFunction1<DataType, ColumnTypeName>() {
          @Override
          public ColumnTypeName apply(DataType dt) {
            if (dt instanceof org.apache.spark.sql.types.IntegerType) return ColumnTypeName.INT;
            if (dt instanceof org.apache.spark.sql.types.LongType) return ColumnTypeName.LONG;
            if (dt instanceof org.apache.spark.sql.types.StringType) return ColumnTypeName.STRING;
            return ColumnTypeName.UNKNOWN_DEFAULT_OPEN_API;
          }
        };
    scala.collection.Seq<ColumnInfo> result =
        (scala.collection.Seq<ColumnInfo>) method.invoke(module, tableInfo, convert);
    return JavaConverters.seqAsJavaList(result);
  }

  @Test
  public void testTypeJsonIsFullStructFieldJsonForPlainColumn() throws Exception {
    // Plain column, no metric_view.* metadata. Must still be shaped as a full StructField JSON.
    Column col = Column.create("region", DataTypes.StringType, true, null, null);
    TableInfo tableInfo = new TableInfo.Builder().withColumns(new Column[] {col}).build();

    List<ColumnInfo> out = invokeBuildColumnInfos(tableInfo);
    assertThat(out).hasSize(1);
    ColumnInfo info = out.get(0);

    JsonNode typeJson = JSON.readTree(info.getTypeJson());
    assertThat(typeJson.isObject()).isTrue();
    assertThat(typeJson.get("name").asText()).isEqualTo("region");
    assertThat(typeJson.get("type").asText()).isEqualTo("string");
    assertThat(typeJson.get("nullable").asBoolean()).isTrue();
    // `metadata` is always present in Spark's StructField JSON; empty for plain columns.
    assertThat(typeJson.has("metadata")).isTrue();
    assertThat(typeJson.get("metadata").isObject()).isTrue();
    assertThat(typeJson.get("metadata").size()).isEqualTo(0);

    // Type-text / type-name remain type-only views of the DataType.
    assertThat(info.getTypeText()).isEqualTo("string");
    assertThat(info.getTypeName()).isEqualTo(ColumnTypeName.STRING);
    assertThat(info.getName()).isEqualTo("region");
    assertThat(info.getNullable()).isTrue();
    assertThat(info.getPosition()).isEqualTo(0);
  }

  @Test
  public void testTypeJsonCarriesMetricViewMetadata() throws Exception {
    // Simulate what Spark's analyzer produces for a metric view dimension and a measure: each
    // column's `metadataInJSON` contains `metric_view.type` and `metric_view.expr`.
    String dimMeta = "{\"metric_view.type\":\"dimension\",\"metric_view.expr\":\"region\"}";
    String measMeta = "{\"metric_view.type\":\"measure\",\"metric_view.expr\":\"sum(count)\"}";

    Column dimension =
        Column.create("region", DataTypes.StringType, true, /* comment */ null, dimMeta);
    Column measure =
        Column.create("count_sum", DataTypes.LongType, true, /* comment */ null, measMeta);

    TableInfo tableInfo =
        new TableInfo.Builder().withColumns(new Column[] {dimension, measure}).build();

    List<ColumnInfo> out = invokeBuildColumnInfos(tableInfo);
    assertThat(out).hasSize(2);

    Map<String, ColumnInfo> byName = new HashMap<>();
    for (ColumnInfo ci : out) byName.put(ci.getName(), ci);

    JsonNode regionJson = JSON.readTree(byName.get("region").getTypeJson());
    assertThat(regionJson.get("name").asText()).isEqualTo("region");
    assertThat(regionJson.get("type").asText()).isEqualTo("string");
    assertThat(regionJson.get("nullable").asBoolean()).isTrue();
    JsonNode regionMeta = regionJson.get("metadata");
    assertThat(regionMeta).isNotNull();
    assertThat(regionMeta.get("metric_view.type").asText()).isEqualTo("dimension");
    assertThat(regionMeta.get("metric_view.expr").asText()).isEqualTo("region");

    JsonNode measureJson = JSON.readTree(byName.get("count_sum").getTypeJson());
    assertThat(measureJson.get("name").asText()).isEqualTo("count_sum");
    assertThat(measureJson.get("type").asText()).isEqualTo("long");
    JsonNode measureMeta = measureJson.get("metadata");
    assertThat(measureMeta).isNotNull();
    assertThat(measureMeta.get("metric_view.type").asText()).isEqualTo("measure");
    assertThat(measureMeta.get("metric_view.expr").asText()).isEqualTo("sum(count)");

    // Sanity: the type-only fields are still the DataType view, not the field JSON.
    assertThat(byName.get("region").getTypeText()).isEqualTo("string");
    assertThat(byName.get("count_sum").getTypeText()).isEqualTo("bigint");
    assertThat(byName.get("count_sum").getTypeName()).isEqualTo(ColumnTypeName.LONG);
  }
}
