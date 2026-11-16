package io.unitycatalog.server.sdk.function;

import static io.unitycatalog.server.utils.TestUtils.CATALOG_NAME;
import static io.unitycatalog.server.utils.TestUtils.FUNCTION_FULL_NAME;
import static io.unitycatalog.server.utils.TestUtils.FUNCTION_NAME;
import static io.unitycatalog.server.utils.TestUtils.LONG_COMMENT;
import static io.unitycatalog.server.utils.TestUtils.SCHEMA_NAME;
import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.FunctionInfo;
import io.unitycatalog.client.model.FunctionParameterInfo;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.ListFunctionsResponse;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.function.BaseFunctionCRUDTest;
import io.unitycatalog.server.base.function.FunctionOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.TestUtils;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

public class SdkFunctionCRUDTest extends BaseFunctionCRUDTest {
  private static final String EMOJI = "😀";
  // Over 255 characters, like the type of a parameter that takes a record with many fields
  private static final String LONG_STRUCT_TYPE = longStructType("field_");
  private static final String LONG_RETURN_TYPE = longStructType("result_");
  private static final String LONG_DEFAULT = "d".repeat(300);

  private static String longStructType(String fieldPrefix) {
    return "STRUCT<"
        + IntStream.range(0, 20)
            .mapToObj(i -> fieldPrefix + i + ": ARRAY<STRING>")
            .collect(Collectors.joining(", "))
        + ">";
  }

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
            .parameterDefault(LONG_DEFAULT)
            .position(0);
    FunctionParameterInfo returnParameter =
        new FunctionParameterInfo()
            .name("result")
            .typeName(ColumnTypeName.STRUCT)
            .typeText(LONG_RETURN_TYPE)
            .typeJson("{\"type\":\"struct\",\"fields\":[]}")
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
            .inputParams(new FunctionParameterInfos().parameters(List.of(parameter)))
            .returnParams(new FunctionParameterInfos().parameters(List.of(returnParameter)));
    functionOperations.createFunction(new CreateFunctionRequest().functionInfo(createFunction));

    FunctionInfo functionInfo = functionOperations.getFunction(FUNCTION_FULL_NAME);
    assertThat(functionInfo.getFullDataType()).isEqualTo(LONG_STRUCT_TYPE);
    assertThat(functionInfo.getInputParams().getParameters())
        .singleElement()
        .satisfies(
            stored -> {
              assertThat(stored.getTypeText()).isEqualTo(LONG_STRUCT_TYPE);
              assertThat(stored.getComment()).isEqualTo(LONG_COMMENT);
              assertThat(stored.getParameterDefault()).isEqualTo(LONG_DEFAULT);
            });
    assertThat(functionInfo.getReturnParams().getParameters())
        .singleElement()
        .satisfies(stored -> assertThat(stored.getTypeText()).isEqualTo(LONG_RETURN_TYPE));
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

  private FunctionInfo createFunction(String name, String properties) throws ApiException {
    return functionOperations.createFunction(
        new CreateFunctionRequest()
            .functionInfo(
                new CreateFunction()
                    .name(name)
                    .catalogName(CATALOG_NAME)
                    .schemaName(SCHEMA_NAME)
                    .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                    .isDeterministic(true)
                    .externalLanguage("python")
                    .dataType(ColumnTypeName.INT)
                    .fullDataType("INT")
                    .isNullCall(false)
                    .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                    .routineDefinition("return 1")
                    .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                    .specificName(name)
                    .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL)
                    .inputParams(new FunctionParameterInfos().parameters(List.of()))
                    .properties(properties)));
  }

  private long countPropertyRows(String entityId) {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session
          .createSelectionQuery(
              "SELECT count(*) FROM PropertyDAO WHERE entityId = :entityId", Long.class)
          .setParameter("entityId", UUID.fromString(entityId))
          .getSingleResult();
    }
  }

  private long countFunctionPropertyRows() {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      return session
          .createSelectionQuery(
              "SELECT count(*) FROM PropertyDAO WHERE entityType = :entityType", Long.class)
          .setParameter("entityType", Constants.FUNCTION)
          .getSingleResult();
    }
  }

  @Test
  public void testFunctionPropertiesRoundTrip() throws ApiException {
    createCommonResources();
    String maxLengthKey = "k".repeat(255);
    // Function name, the properties it is created with, and what create, get and list return.
    String[][] cases = {
      {
        "fn_props",
        "{\"tier\": \"gold\", \"owner_team\": \"ml\"}",
        "{\"owner_team\":\"ml\",\"tier\":\"gold\"}"
      },
      {
        "fn_quoted",
        "{\"env.deps\":\"[\\\"requests\\\"]\"}",
        "{\"env.deps\":\"[\\\"requests\\\"]\"}"
      },
      {
        "fn_non_string",
        "{\"attempts\": 3, \"enabled\": true, \"unset\": null, \"nested\": {\"b\": [1, \"c\"]}}",
        "{\"attempts\":\"3\",\"enabled\":\"true\",\"nested\":\"{\\\"b\\\":[1,\\\"c\\\"]}\","
            + "\"unset\":\"null\"}"
      },
      {
        "fn_decimal",
        "{\"amount\": 0.1234567890123456789, \"big\": 1e400, \"price\": 1.50, \"list\": [2.0]}",
        "{\"amount\":\"0.1234567890123456789\",\"big\":\"1E+400\",\"list\":\"[2.0]\","
            + "\"price\":\"1.50\"}"
      },
      {"fn_max_key", "{\"" + maxLengthKey + "\":\"v\"}", "{\"" + maxLengthKey + "\":\"v\"}"},
      {"fn_max_digits", "{\"n\":" + "9".repeat(1000) + "}", "{\"n\":\"" + "9".repeat(1000) + "\"}"},
      {
        "fn_max_nesting",
        "{\"n\":" + "[".repeat(999) + "]".repeat(999) + "}",
        "{\"n\":\"" + "[".repeat(999) + "]".repeat(999) + "\"}"
      },
      {"fn_absent", null, null},
      {"fn_json_null", "null", null},
      {"fn_empty", "{}", null},
      {"fn_empty_string", "", null},
      {"fn_blank", " \n\t", null},
    };
    Map<String, String> expectedByName = new HashMap<>();
    for (String[] c : cases) {
      String name = c[0], properties = c[1], expected = c[2];
      expectedByName.put(name, expected);
      assertThat(createFunction(name, properties).getProperties()).as(name).isEqualTo(expected);
      assertThat(functionOperations.getFunction(CATALOG_NAME + "." + SCHEMA_NAME + "." + name))
          .extracting(FunctionInfo::getProperties)
          .as(name)
          .isEqualTo(expected);
    }
    List<FunctionInfo> listed =
        functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.empty());
    assertThat(listed).hasSize(cases.length);
    for (FunctionInfo f : listed) {
      assertThat(f.getProperties()).as(f.getName()).isEqualTo(expectedByName.get(f.getName()));
    }
  }

  @Test
  public void testInvalidFunctionPropertiesRejected() throws ApiException {
    createCommonResources();
    for (String properties :
        List.of(
            "not json",
            "[\"a\"]",
            "\"a\"",
            "1",
            "{\"a\":\"b\"} x",
            "{\"a\":\"b\"}{\"c\":\"d\"}",
            "{\"a\":\"1\",\"a\":\"2\"}")) {
      assertApiException(
          () -> createFunction("fn_invalid", properties),
          ErrorCode.INVALID_ARGUMENT,
          "Function properties must be a single JSON object with unique keys");
    }
    for (String key : List.of("k".repeat(256), EMOJI.repeat(256))) {
      assertApiException(
          () -> createFunction("fn_invalid", "{\"" + key + "\":\"v\"}"),
          ErrorCode.INVALID_ARGUMENT,
          "Function property keys cannot be longer than 255 characters");
    }
    for (String properties : List.of("{\"a\\u0000b\":\"v\"}", "{\"k\":\"a\\u0000b\"}")) {
      assertApiException(
          () -> createFunction("fn_invalid", properties),
          ErrorCode.INVALID_ARGUMENT,
          "Function property keys and values cannot contain the NUL character");
    }
    assertApiException(
        () -> createFunction("fn_invalid", "{\"n\":1e9999999999}"),
        ErrorCode.INVALID_ARGUMENT,
        "Function properties contain a number whose exponent is out of range");
    for (String properties :
        List.of(
            "{\"n\":" + "9".repeat(1001) + "}",
            "{\"n\":" + "[".repeat(1000) + "]".repeat(1000) + "}")) {
      assertApiException(
          () -> createFunction("fn_invalid", properties),
          ErrorCode.INVALID_ARGUMENT,
          "Function properties are too large: numbers can have at most 1000 digits, strings at"
              + " most 20000000 characters, and nesting at most 1000 levels.");
    }
    assertThat(functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.empty()))
        .isEmpty();
    assertThat(countFunctionPropertyRows()).isZero();
  }

  @Test
  public void testListPagesKeepEachFunctionsProperties() throws ApiException {
    createCommonResources();
    Map<String, String> expected = new HashMap<>();
    expected.put("fn_a", "{\"a\":\"1\"}");
    expected.put("fn_b", null);
    expected.put("fn_c", "{\"c\":\"3\",\"x\":\"y\"}");
    expected.put("fn_d", null);
    createFunction("fn_a", "{\"a\": \"1\"}");
    createFunction("fn_b", null);
    createFunction("fn_c", "{\"x\": \"y\", \"c\": 3}");
    createFunction("fn_d", "{}");

    FunctionsApi functionsApi = new FunctionsApi(TestUtils.createApiClient(serverConfig));
    Map<String, String> listed = new HashMap<>();
    int pages = 0;
    String pageToken = null;
    do {
      ListFunctionsResponse page =
          functionsApi.listFunctions(
              CATALOG_NAME, SCHEMA_NAME, 1, pageToken, /* includeBrowse= */ false);
      pages++;
      for (FunctionInfo f : page.getFunctions()) {
        listed.put(f.getName(), f.getProperties());
      }
      pageToken = page.getNextPageToken();
    } while (pageToken != null && !pageToken.isEmpty());
    assertThat(listed).isEqualTo(expected);
    assertThat(pages).isGreaterThanOrEqualTo(expected.size());
  }

  @Test
  public void testDuplicateCreateKeepsTheExistingProperties() throws ApiException {
    createCommonResources();
    String functionId = createFunction("fn_dup", "{\"k\":\"v1\"}").getFunctionId();

    assertApiException(
        () -> createFunction("fn_dup", "{\"k\":\"v2\",\"other\":\"x\"}"),
        ErrorCode.RESOURCE_ALREADY_EXISTS,
        "Function already exists: fn_dup");
    assertThat(functionOperations.getFunction(CATALOG_NAME + "." + SCHEMA_NAME + ".fn_dup"))
        .extracting(FunctionInfo::getFunctionId, FunctionInfo::getProperties)
        .containsExactly(functionId, "{\"k\":\"v1\"}");
    assertThat(functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.empty()))
        .hasSize(1);
    assertThat(countPropertyRows(functionId)).isEqualTo(1);
    assertThat(countFunctionPropertyRows()).isEqualTo(1);
  }

  @Test
  public void testPropertyKeyLengthCountsCharacters() throws ApiException {
    createCommonResources();
    String properties = "{\"" + EMOJI.repeat(255) + "\":\"v\"}";
    switch (SupportedDatabase.selected()) {
      case H2 ->
          // H2 counts UTF-16 units, so 255 emoji are 510 units in a varchar(255).
          assertApiException(
              () -> createFunction("fn_max_key", properties),
              ErrorCode.INVALID_ARGUMENT,
              "The database cannot store a key or value of these function properties");
      case POSTGRES_18, MYSQL_8 -> {
        assertThat(createFunction("fn_max_key", properties).getProperties()).isEqualTo(properties);
        assertThat(functionOperations.getFunction(CATALOG_NAME + "." + SCHEMA_NAME + ".fn_max_key"))
            .extracting(FunctionInfo::getProperties)
            .isEqualTo(properties);
      }
    }
  }

  @Test
  public void testPropertyKeysTheDatabaseTreatsAsEqualRejected() throws ApiException {
    createCommonResources();
    String properties = "{\"Tier\":\"a\",\"tier\":\"b\"}";
    switch (SupportedDatabase.selected()) {
      case H2, POSTGRES_18 ->
          assertThat(createFunction("fn_case", properties).getProperties()).isEqualTo(properties);
      case MYSQL_8 -> {
        // The test databases compare text exactly. MySQL's default collation ignores case.
        try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
          session.doWork(
              connection -> {
                try (Statement statement = connection.createStatement()) {
                  statement.execute(
                      "ALTER TABLE uc_properties MODIFY property_key varchar(255)"
                          + " COLLATE utf8mb4_0900_ai_ci NOT NULL");
                }
              });
        }
        assertApiException(
            () -> createFunction("fn_case", properties),
            ErrorCode.INVALID_ARGUMENT,
            "Function property keys must be unique, and the database treats two of these keys");
        assertThat(functionOperations.listFunctions(CATALOG_NAME, SCHEMA_NAME, Optional.empty()))
            .isEmpty();
        assertThat(countFunctionPropertyRows()).isZero();
      }
    }
  }

  @Test
  public void testDeletingFunctionRemovesItsProperties() throws ApiException {
    String properties = "{\"k1\":\"v1\",\"k2\":\"v2\"}";
    createCommonResources();

    String deletedDirectly = createFunction("fn", properties).getFunctionId();
    assertThat(countPropertyRows(deletedDirectly)).isEqualTo(2);
    functionOperations.deleteFunction(CATALOG_NAME + "." + SCHEMA_NAME + ".fn", true);
    assertThat(countPropertyRows(deletedDirectly)).isZero();

    String deletedWithSchema = createFunction("fn", properties).getFunctionId();
    assertThat(countPropertyRows(deletedWithSchema)).isEqualTo(2);
    schemaOperations.deleteSchema(CATALOG_NAME + "." + SCHEMA_NAME, Optional.of(true));
    assertThat(countPropertyRows(deletedWithSchema)).isZero();

    schemaOperations.createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
    String deletedWithCatalog = createFunction("fn", properties).getFunctionId();
    assertThat(countPropertyRows(deletedWithCatalog)).isEqualTo(2);
    catalogOperations.deleteCatalog(CATALOG_NAME, Optional.of(true));
    assertThat(countPropertyRows(deletedWithCatalog)).isZero();
  }
}
