package io.unitycatalog.server.base.function;

import static io.unitycatalog.server.utils.TestUtils.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;
import java.util.List;
import org.junit.*;

public abstract class BaseFunctionCRUDTest extends BaseCRUDTest {
  protected SchemaOperations schemaOperations;
  protected FunctionOperations functionOperations;

  protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

  protected abstract FunctionOperations createFunctionOperations(ServerConfig serverConfig);

  @Before
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

  @Test
  public void testFunctionCRUD() throws ApiException {
    Assert.assertThrows(Exception.class, () -> functionOperations.getFunction(FUNCTION_FULL_NAME));
    // Create a catalog
    createCommonResources();

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

    // Create a function
    FunctionInfo functionInfo = functionOperations.createFunction(createFunctionRequest);
    assertEquals(FUNCTION_NAME, functionInfo.getName());
    assertEquals(CATALOG_NAME, functionInfo.getCatalogName());
    assertEquals(SCHEMA_NAME, functionInfo.getSchemaName());
    assertNotNull(functionInfo.getFunctionId());

    // List functions
    Iterable<FunctionInfo> functionInfos =
        functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME);
    assertTrue(
        contains(
            functionInfos,
            functionInfo,
            f -> {
              assertThat(f.getFunctionId()).isNotNull();
              if (!f.getFunctionId().equals(functionInfo.getFunctionId())) return false;
              if (f.getInputParams() != null && f.getInputParams().getParameters() != null) {
                return f.getInputParams().getParameters().stream()
                    .anyMatch(p -> p.getName().equals("param1"));
              }
              return true;
            }));

    // Get function
    FunctionInfo retrievedFunctionInfo = functionOperations.getFunction(FUNCTION_FULL_NAME);
    assertEquals(functionInfo, retrievedFunctionInfo);

    // now update the parent catalog
    UpdateCatalog updateCatalog = new UpdateCatalog().newName(CATALOG_NEW_NAME);
    catalogOperations.updateCatalog(CATALOG_NAME, updateCatalog);
    // get the function again
    FunctionInfo retrievedFunctionInfoAfterCatUpdate =
        functionOperations.getFunction(CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME);
    assertEquals(
        retrievedFunctionInfo.getFunctionId(), retrievedFunctionInfoAfterCatUpdate.getFunctionId());

    // Delete function
    functionOperations.deleteFunction(
        CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME, true);
    assertFalse(
        contains(
            functionOperations.listFunctions(CATALOG_NEW_NAME, SCHEMA_NAME),
            functionInfo,
            f -> f.getFunctionId().equals(functionInfo.getFunctionId())));
  }
}
