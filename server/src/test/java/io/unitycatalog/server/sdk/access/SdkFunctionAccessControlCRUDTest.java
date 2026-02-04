package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.FunctionParameterMode;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/**
 * SDK-based access control tests for Function CRUD operations.
 *
 * <p>This test class verifies:
 *
 * <ul>
 *   <li>Function creation requires USE CATALOG and USE SCHEMA permissions
 *   <li>Function listing is filtered based on ownership
 *   <li>Function get requires ownership
 *   <li>Function delete requires ownership
 * </ul>
 */
public class SdkFunctionAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  private static final FunctionParameterInfos TEST_PARAMS =
      new FunctionParameterInfos()
          .parameters(
              List.of(
                  new FunctionParameterInfo()
                      .name("param1")
                      .typeName(ColumnTypeName.INT)
                      .typeText("INT")
                      .typeJson("{\"type\": \"integer\"}")
                      .position(0)
                      .parameterMode(FunctionParameterMode.IN)));

  @Test
  @SneakyThrows
  public void testFunctionAccess() {
    createCommonTestUsers();
    setupCommonCatalogAndSchema();

    ServerConfig principal1Config = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig principal2Config = createTestUserServerConfig(PRINCIPAL_2);

    FunctionsApi principal1FunctionsApi =
        new FunctionsApi(TestUtils.createApiClient(principal1Config));
    FunctionsApi principal2FunctionsApi =
        new FunctionsApi(TestUtils.createApiClient(principal2Config));

    // give user USE CATALOG and USE SCHEMA on cat_pr1.sch_pr1
    grantPermissions(PRINCIPAL_1, SecurableType.CATALOG, "cat_pr1", Privileges.USE_CATALOG);
    grantPermissions(PRINCIPAL_1, SecurableType.SCHEMA, "cat_pr1.sch_pr1", Privileges.USE_SCHEMA);

    // create function -> use catalog, use "schema" -> allow
    CreateFunctionRequest createFunction1 =
        new CreateFunctionRequest()
            .functionInfo(
                new CreateFunction()
                    .name("fun_pr1")
                    .catalogName("cat_pr1")
                    .schemaName("sch_pr1")
                    .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                    .isDeterministic(true)
                    .isNullCall(false)
                    .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                    .specificName("fun_pr1")
                    .dataType(ColumnTypeName.INT)
                    .fullDataType("INT")
                    .inputParams(TEST_PARAMS)
                    .externalLanguage("python")
                    .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                    .routineDefinition("return x"));
    FunctionInfo function1Info = principal1FunctionsApi.createFunction(createFunction1);
    assertThat(function1Info).isNotNull();
    assertThat(function1Info.getName()).isEqualTo("fun_pr1");

    // create function -> no use catalog, no use "schema" -> deny
    CreateFunctionRequest createFunction2 =
        new CreateFunctionRequest()
            .functionInfo(
                new CreateFunction()
                    .name("fun_pr2")
                    .catalogName("cat_pr1")
                    .schemaName("sch_pr1")
                    .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                    .isDeterministic(true)
                    .isNullCall(false)
                    .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                    .specificName("fun_pr2")
                    .dataType(ColumnTypeName.INT)
                    .fullDataType("INT")
                    .inputParams(TEST_PARAMS)
                    .externalLanguage("python")
                    .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                    .routineDefinition("return x"));
    assertPermissionDenied(() -> principal2FunctionsApi.createFunction(createFunction2));

    // list functions -> no privileges -> allow - filtered list
    List<FunctionInfo> principal2Functions =
        listAllFunctions(principal2FunctionsApi, "cat_pr1", "sch_pr1");
    assertThat(principal2Functions).isEmpty();

    // list functions -> owner -> allow - filtered
    List<FunctionInfo> principal1Functions =
        listAllFunctions(principal1FunctionsApi, "cat_pr1", "sch_pr1");
    assertThat(principal1Functions).hasSize(1);
    assertThat(principal1Functions.get(0).getName()).isEqualTo("fun_pr1");

    // get function -> no privileges -> denied
    assertPermissionDenied(() -> principal2FunctionsApi.getFunction("cat_pr1.sch_pr1.fun_pr1"));

    // get function -> owner -> allow
    FunctionInfo functionInfo = principal1FunctionsApi.getFunction("cat_pr1.sch_pr1.fun_pr1");
    assertThat(functionInfo).isNotNull();
    assertThat(functionInfo.getName()).isEqualTo("fun_pr1");

    // delete function -> no privileges -> denied
    assertPermissionDenied(() -> principal2FunctionsApi.deleteFunction("cat_pr1.sch_pr1.fun_pr1"));

    // delete function -> owner -> allow
    principal1FunctionsApi.deleteFunction("cat_pr1.sch_pr1.fun_pr1");

    List<FunctionInfo> functionsAfterDelete =
        listAllFunctions(principal1FunctionsApi, "cat_pr1", "sch_pr1");
    assertThat(functionsAfterDelete).isEmpty();
  }
}
