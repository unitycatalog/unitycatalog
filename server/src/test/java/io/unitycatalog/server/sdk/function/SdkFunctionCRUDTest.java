package io.unitycatalog.server.sdk.function;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.FUNCTION_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.FUNCTION_NAME;
import static io.unitycatalog.server.utils.TestUtils.LONG_COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.function.BaseFunctionCRUDTest;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

public class SdkFunctionCRUDTest extends BaseFunctionCRUDTest {
  // Over 255 characters, like the type of a parameter that takes a record with many fields
  private static final String LONG_STRUCT_TYPE =
      "STRUCT<"
          + IntStream.range(0, 20)
              .mapToObj(i -> "field_" + i + ": ARRAY<STRING>")
              .collect(Collectors.joining(", "))
          + ">";

  @Test
  public void testFunctionWithLongTypesAndComments() throws ApiException {
    createCommonResources();
    FunctionParameterInfo parameter =
        new FunctionParameterInfo()
            .name("records")
            .typeName(ColumnTypeName.STRUCT)
            .typeText(LONG_STRUCT_TYPE)
            .typeJson("{\"type\":\"struct\",\"fields\":[]}")
            .comment(LONG_COMMENT)
            .parameterDefault(LONG_COMMENT)
            .position(0);
    CreateFunction createFunction =
        new CreateFunction()
            .name(FUNCTION_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .parameterStyle(CreateFunction.ParameterStyleEnum.S)
            .isDeterministic(true)
            .externalLanguage("python")
            .dataType(ColumnTypeName.STRUCT)
            .fullDataType(LONG_STRUCT_TYPE)
            .isNullCall(false)
            .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
            .routineDefinition("return records")
            .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
            .specificName(FUNCTION_NAME)
            .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL)
            .inputParams(new FunctionParameterInfos().parameters(List.of(parameter)));
    functionOperations.createFunction(new CreateFunctionRequest().functionInfo(createFunction));

    FunctionInfo functionInfo = functionOperations.getFunction(FUNCTION_FULL_NAME);
    assertThat(functionInfo.getFullDataType()).isEqualTo(LONG_STRUCT_TYPE);
    assertThat(functionInfo.getInputParams().getParameters())
        .singleElement()
        .satisfies(
            stored -> {
              assertThat(stored.getTypeText()).isEqualTo(LONG_STRUCT_TYPE);
              assertThat(stored.getComment()).isEqualTo(LONG_COMMENT);
              assertThat(stored.getParameterDefault()).isEqualTo(LONG_COMMENT);
            });
  }

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected FunctionOperations createFunctionOperations(ServerConfig config) {
    return new SdkFunctionOperations(TestUtils.createApiClient(config));
  }
}
