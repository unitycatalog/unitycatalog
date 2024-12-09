package io.unitycatalog.server.base.function;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class BaseFunctionCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected FunctionOperations functionOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract FunctionOperations createFunctionOperations(ServerConfig serverConfig);

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    schemaOperations = createSchemaOperations(serverConfig);
    functionOperations = createFunctionOperations(serverConfig);
  }

  protected void createCommonResources() throws ApiException {
    CreateCatalog createCatalog = new CreateCatalog().name(CATALOG_NAME).comment(COMMENT);
    catalogOperations.createCatalog(createCatalog);
    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
  }

  protected void assertFunction(FunctionInfo functionInfo, String functionName) {
    assertThat(functionInfo.getName()).isEqualTo(functionName);
    assertThat(functionInfo.getCatalogName()).isEqualTo(CATALOG_NAME);
    assertThat(functionInfo.getSchemaName()).isEqualTo(SCHEMA_NAME);
    assertThat(functionInfo.getFunctionId()).isNotNull();
  }

  @Test
  public void testFunctionCRUD() throws ApiException {
    assertThatThrownBy(() -> functionOperations.getFunction(FUNCTION_FULL_NAME))
        .isInstanceOf(Exception.class);
    // Create a catalog
    createCommonResources();

    // Create a function
    System.out.println("Testing create function..");
    FunctionParameterInfos functionParameterInfos =
        new FunctionParameterInfos()
            .parameters(
                List.of(
                    new FunctionParameterInfo()
                        .name("param1")
                        .typeName(ColumnTypeName.INT)
                        .typeText("int")
                        .typeJson("{\"type\":\"int\"}")
                        .position(0)));
    CreateFunction createFunction =
        new CreateFunction()
            .name(FUNCTION_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .parameterStyle(CreateFunction.ParameterStyleEnum.S)
            .isDeterministic(true)
            .comment(COMMENT)
            .externalLanguage("python")
            .dataType(ColumnTypeName.INT)
            .fullDataType("Integer")
            .isNullCall(false)
            .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
            .routineDefinition("def test():\n  return 1")
            .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
            .specificName("test")
            .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL)
            .inputParams(functionParameterInfos);
    CreateFunctionRequest createFunctionRequest =
        new CreateFunctionRequest().functionInfo(createFunction);
    FunctionInfo functionInfo = functionOperations.createFunction(createFunctionRequest);
    assertFunction(functionInfo, FUNCTION_NAME);

    // Create another function to test pagination
    FunctionParameterInfos functionParameterInfos2 =
        new FunctionParameterInfos()
            .parameters(
                List.of(
                    new FunctionParameterInfo()
                        .name("param2")
                        .typeName(ColumnTypeName.INT)
                        .typeText("int")
                        .typeJson("{\"type\":\"int\"}")
                        .position(0)));
    CreateFunction createFunction2 =
        new CreateFunction()
            .name(COMMON_ENTITY_NAME)
            .catalogName(CATALOG_NAME)
            .schemaName(SCHEMA_NAME)
            .parameterStyle(CreateFunction.ParameterStyleEnum.S)
            .isDeterministic(true)
            .comment(COMMENT)
            .externalLanguage("python")
            .dataType(ColumnTypeName.INT)
            .fullDataType("Integer")
            .isNullCall(false)
            .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
            .routineDefinition("def test():\n  return 1")
            .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
            .specificName("test")
            .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL)
            .inputParams(functionParameterInfos2);
    CreateFunctionRequest createFunctionRequest2 =
        new CreateFunctionRequest().functionInfo(createFunction2);
    FunctionInfo functionInfo2 = functionOperations.createFunction(createFunctionRequest2);
    assertFunction(functionInfo2, COMMON_ENTITY_NAME);

    // List functions
    System.out.println("Testing list functions..");
    Iterable<FunctionInfo> functionInfos =
        functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.empty());
    assertThat(functionInfos)
        .as(
            "Function with ID '%s' and parameter '%s' does not exist",
            functionInfo.getFunctionId(), "param1")
        .anySatisfy(
            f -> {
              assertThat(f.getFunctionId()).isNotNull().isEqualTo(functionInfo.getFunctionId());
              assertThat(f.getInputParams())
                  .isNotNull()
                  .extracting(
                      FunctionParameterInfos::getParameters,
                      Assertions.as(InstanceOfAssertFactories.list(FunctionParameterInfo.class)))
                  .isNotNull()
                  .anySatisfy(parameter -> assertThat(parameter.getName()).isEqualTo("param1"));
            });

    // List functions with page token
    System.out.println("Testing list functions with page token..");
    functionInfos =
        functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.of(FUNCTION_NAME));
    assertThat(functionInfos)
        .as(
            "Function with ID '%s' and parameter '%s' does not exist",
            functionInfo2.getFunctionId(), "param2")
        .noneSatisfy(f -> assertThat(f.getFunctionId()).isEqualTo(functionInfo.getFunctionId()))
        .anySatisfy(
            f -> {
              assertThat(f.getFunctionId()).isNotNull().isEqualTo(functionInfo2.getFunctionId());
              assertThat(f.getInputParams())
                  .isNotNull()
                  .extracting(
                      FunctionParameterInfos::getParameters,
                      Assertions.as(InstanceOfAssertFactories.list(FunctionParameterInfo.class)))
                  .isNotNull()
                  .anySatisfy(parameter -> assertThat(parameter.getName()).isEqualTo("param2"));
            });

    // Get function
    System.out.println("Testing get function..");
    FunctionInfo retrievedFunctionInfo = functionOperations.getFunction(FUNCTION_FULL_NAME);
    assertThat(retrievedFunctionInfo).isEqualTo(functionInfo);

    // now update the parent catalog
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
    // get the function again
    FunctionInfo retrievedFunctionInfoAfterCatUpdate =
        functionOperations.getFunction(CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME);
    assertThat(retrievedFunctionInfoAfterCatUpdate.getFunctionId())
        .isEqualTo(retrievedFunctionInfo.getFunctionId());

    // Delete function
    System.out.println("Testing delete function..");
    functionOperations.deleteFunction(
        CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME, true);
    assertThat(functionOperations.listFunctions(CATALOG_NEW_NAME, SCHEMA_NAME, Optional.empty()))
        .as("Function with ID '%s' exists", functionInfo.getFunctionId())
        .noneSatisfy(f -> assertThat(f.getFunctionId()).isEqualTo(functionInfo.getFunctionId()));
  }
}
