package io.unitycatalog.server.auth.decorator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.FunctionInfo;
import io.unitycatalog.server.model.FunctionParameterInfo;
import io.unitycatalog.server.model.FunctionParameterInfos;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class BrowseOnlyConverterTest {
  private static final ObjectMapper MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  @ParameterizedTest
  @EnumSource(
      value = TableType.class,
      names = {"MANAGED", "EXTERNAL", "VIEW"})
  void tableDiscoveryKeepsColumnFieldsAndOmitsStorageAndDefinitions(TableType tableType)
      throws Exception {
    ObjectNode full =
        (ObjectNode)
            MAPPER.readTree(
                """
                {
                  "browse_only": false,
                  "name": "asset", "catalog_name": "catalog", "schema_name": "schema",
                  "data_source_format": "DELTA", "table_id": "table-id", "owner": "owner",
                  "comment": "Discovery asset", "created_at": 1, "created_by": "creator",
                  "updated_at": 2, "updated_by": "editor",
                  "columns": [
                    {"name": "amount", "type_text": "decimal(12,2)", "type_json": "{}",
                     "type_name": "DECIMAL", "type_precision": 12, "type_scale": 2,
                     "position": 0, "comment": "Amount", "nullable": false, "partition_index": 0},
                    {"name": "duration", "type_text": "interval year to month", "type_json": "{}",
                     "type_name": "INTERVAL", "type_interval_type": "YEAR TO MONTH",
                     "position": 1, "comment": "Duration", "nullable": true}
                  ],
                  "storage_location": "file:///data/asset", "properties": {"private": "value"},
                  "view_definition": "SELECT amount FROM catalog.schema.source",
                  "view_dependencies": {"dependencies": [
                    {"table": {"table_full_name": "catalog.schema.source"}}
                  ]}
                }
                """);
    full.put("table_type", tableType.getValue());
    assertAllFieldsCovered(TableInfo.class, full);
    assertAllFieldsCovered(
        ColumnInfo.class, full.path("columns").get(0), full.path("columns").get(1));
    TableInfo info = MAPPER.treeToValue(full, TableInfo.class);
    ObjectNode expected = full.deepCopy();
    expected.remove(
        java.util.List.of(
            "storage_location", "properties", "view_definition", "view_dependencies"));
    expected.put("browse_only", true);

    TableInfo browse = BrowseOnlyConverter.toBrowseOnly(info);

    assertThat(MAPPER.readTree(MAPPER.writeValueAsString(browse))).isEqualTo(expected);
    JsonNode wire = new ObjectMapper().valueToTree(browse);
    for (String field :
        java.util.List.of(
            "storage_location", "properties", "view_definition", "view_dependencies")) {
      assertThat(wire.has(field)).isFalse();
    }
    assertThat(MAPPER.readTree(MAPPER.writeValueAsString(info))).isEqualTo(full);
    assertThat(browse.getColumns().get(0)).isNotSameAs(info.getColumns().get(0));
    assertThat(BrowseOnlyConverter.toBrowseOnly(new TableInfo().columns(null)).getColumns())
        .isNull();
  }

  @Test
  void functionDiscoveryKeepsSignatureDependenciesAndPropertiesWithoutBody() throws Exception {
    ObjectNode full =
        (ObjectNode)
            MAPPER.readTree(
                """
                {
                  "browse_only": false,
                  "name": "function", "catalog_name": "catalog", "schema_name": "schema",
                  "input_params": {"parameters": [
                    {"name": "input", "type_text": "decimal(12,2)", "type_name": "DECIMAL",
                     "type_json": "{}", "type_precision": 12, "type_scale": 2,
                     "position": 0, "parameter_mode": "IN", "parameter_type": "PARAM",
                     "parameter_default": "7.00", "comment": "Input"}
                  ]},
                  "data_type": "TABLE_TYPE",
                  "full_data_type": "TABLE(result INTERVAL YEAR TO MONTH)",
                  "return_params": {"parameters": [
                    {"name": "result", "type_text": "interval year to month",
                     "type_name": "INTERVAL",
                     "type_json": "{}", "type_interval_type": "YEAR TO MONTH",
                     "position": 0, "parameter_type": "COLUMN", "comment": "Result"}
                  ]},
                  "routine_body": "SQL", "routine_definition": "SELECT INTERVAL '1-0' YEAR TO MONTH",
                  "routine_dependencies": {"dependencies": [
                    {"function": {"function_full_name": "catalog.schema.dependency"}}
                  ]},
                  "parameter_style": "S", "is_deterministic": true,
                  "sql_data_access": "CONTAINS_SQL", "is_null_call": false,
                  "security_type": "DEFINER", "specific_name": "function",
                  "comment": "Discovery function", "properties": "{}",
                  "full_name": "catalog.schema.function", "owner": "owner",
                  "created_at": 1, "created_by": "creator", "updated_at": 2, "updated_by": "editor",
                  "function_id": "function-id", "external_language": "PYTHON"
                }
                """);
    assertAllFieldsCovered(FunctionInfo.class, full);
    assertAllFieldsCovered(
        FunctionParameterInfos.class, full.path("input_params"), full.path("return_params"));
    assertAllFieldsCovered(
        FunctionParameterInfo.class,
        full.path("input_params").path("parameters").get(0),
        full.path("return_params").path("parameters").get(0));
    FunctionInfo info = MAPPER.treeToValue(full, FunctionInfo.class);
    ObjectNode expected = full.deepCopy();
    expected.remove("routine_definition");
    expected.put("browse_only", true);

    JsonNode browse =
        MAPPER.readTree(MAPPER.writeValueAsString(BrowseOnlyConverter.toBrowseOnly(info)));
    assertThat(browse).isEqualTo(expected);
    assertThat(fieldNames(browse.path("input_params"), browse.path("return_params")))
        .containsExactly("parameters");
    assertThat(
            fieldNames(
                browse.path("input_params").path("parameters").get(0),
                browse.path("return_params").path("parameters").get(0)))
        .containsExactlyInAnyOrder(
            "name",
            "type_text",
            "type_json",
            "type_name",
            "type_precision",
            "type_scale",
            "type_interval_type",
            "position",
            "parameter_mode",
            "parameter_type",
            "parameter_default",
            "comment");
    assertThat(
            new ObjectMapper()
                .<JsonNode>valueToTree(BrowseOnlyConverter.toBrowseOnly(info))
                .has("routine_definition"))
        .isFalse();
    assertThat(MAPPER.readTree(MAPPER.writeValueAsString(info))).isEqualTo(full);
  }

  private static void assertAllFieldsCovered(Class<?> type, JsonNode... examples) {
    assertThat(fieldNames(examples))
        .as("Fields covered for %s", type.getSimpleName())
        .containsExactlyInAnyOrderElementsOf(
            MAPPER
                .getSerializationConfig()
                .introspect(MAPPER.constructType(type))
                .findProperties()
                .stream()
                .map(BeanPropertyDefinition::getName)
                .toList());
  }

  private static Set<String> fieldNames(JsonNode... examples) {
    Set<String> fields = new HashSet<>();
    for (JsonNode example : examples) {
      example.fieldNames().forEachRemaining(fields::add);
    }
    return fields;
  }
}
