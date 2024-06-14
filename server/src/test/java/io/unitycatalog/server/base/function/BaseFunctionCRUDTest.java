package io.unitycatalog.server.base.function;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.model.*;
import io.unitycatalog.server.base.BaseCRUDTest;
import io.unitycatalog.server.utils.TestUtils;
import org.junit.*;

import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.schema.SchemaOperations;

import java.util.List;

import static org.junit.Assert.*;
import static io.unitycatalog.server.utils.TestUtils.*;

public abstract class BaseFunctionCRUDTest extends BaseCRUDTest {
    protected SchemaOperations schemaOperations;
    protected FunctionOperations functionOperations;
    protected static final String FUNCTION_NAME = "test_function";
    protected static final String FUNCTION_FULL_NAME = CATALOG_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME;

    @Before
    public void setUp() {
        super.setUp();
        schemaOperations = createSchemaOperations(serverConfig);
        functionOperations = createFunctionOperations(serverConfig);
        cleanUp();
    }

    protected abstract SchemaOperations createSchemaOperations(ServerConfig serverConfig);

    protected abstract FunctionOperations createFunctionOperations(ServerConfig serverConfig);

    protected void cleanUp() {
        try {
            functionOperations.deleteFunction(FUNCTION_FULL_NAME, true);
        } catch (Exception e) {
            // Ignore
        }
        try {
            schemaOperations.deleteSchema(SCHEMA_FULL_NAME);
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NEW_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        try {
            if (schemaOperations.getSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME) != null) {
                schemaOperations.deleteSchema(TestUtils.CATALOG_NEW_NAME + "." + TestUtils.SCHEMA_NAME);
            }
        } catch (Exception e) {
            // Ignore
        }
        super.cleanUp();
    }

    protected void createCommonResources() throws ApiException {
        catalogOperations.createCatalog(CATALOG_NAME, "Common catalog for functions");
        schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    }

    @Test
    public void testFunctionCRUD() throws ApiException {
        Assert.assertThrows(Exception.class, () -> functionOperations.getFunction(FUNCTION_FULL_NAME));
        // Create a catalog
        createCommonResources();

        FunctionParameterInfos functionParameterInfos = new FunctionParameterInfos()
                .parameters(List.of(new FunctionParameterInfo()
                        .name("param1")
                        .typeName(ColumnTypeName.INT)
                        .typeText("int")
                        .typeJson("{\"type\":\"int\"}")
                        .position(0)));
        CreateFunction createFunction = new CreateFunction()
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
        CreateFunctionRequest createFunctionRequest = new CreateFunctionRequest().functionInfo(createFunction);


        // Create a function
        FunctionInfo functionInfo = functionOperations.createFunction(createFunctionRequest);
        assertEquals(FUNCTION_NAME, functionInfo.getName());
        assertEquals(CATALOG_NAME, functionInfo.getCatalogName());
        assertEquals(SCHEMA_NAME, functionInfo.getSchemaName());
        assertNotNull(functionInfo.getFunctionId());

        // List functions
        Iterable<FunctionInfo> functionInfos = functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME);
        assertTrue(contains(functionInfos, functionInfo, f -> {
            assert f.getFunctionId() != null;
            if (!f.getFunctionId().equals(functionInfo.getFunctionId())) return false;
            if (f.getInputParams() != null && f.getInputParams().getParameters() != null) {
                return f.getInputParams().getParameters().stream().anyMatch(p -> p.getName().equals("param1"));
            }
            return true;
        }));

        // Get function
        FunctionInfo retrievedFunctionInfo = functionOperations.getFunction(FUNCTION_FULL_NAME);
        assertEquals(functionInfo, retrievedFunctionInfo);

        // now update the parent catalog
        catalogOperations.updateCatalog(CATALOG_NAME, CATALOG_NEW_NAME, "");
        // get the function again
        FunctionInfo retrievedFunctionInfoAfterCatUpdate = functionOperations.getFunction(
                CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME);
        assertEquals(retrievedFunctionInfo.getFunctionId(),
                retrievedFunctionInfoAfterCatUpdate.getFunctionId());

        // Delete function
        functionOperations.deleteFunction(CATALOG_NEW_NAME + "." + SCHEMA_NAME + "." + FUNCTION_NAME, true);
        assertFalse(contains(functionOperations.listFunctions(CATALOG_NEW_NAME, SCHEMA_NAME),
                functionInfo, f -> f.getFunctionId().equals(functionInfo.getFunctionId())));
    }
}
