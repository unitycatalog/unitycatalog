package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertDeltaPermissionDenied;
import static io.unitycatalog.server.utils.TestUtils.assertHttpApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static io.unitycatalog.server.utils.TestUtils.sendRaw;
import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.GenerateTemporaryModelVersionCredential;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.GenerateTemporaryVolumeCredential;
import io.unitycatalog.client.model.ModelVersionOperation;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.client.model.VolumeOperation;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.TestUtils;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkBrowseAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String BASE = "/api/2.1/unity-catalog";
  private static final String CATALOG = "browse_catalog";
  private static final String SCHEMA = "assets";
  private static final String NAMESPACE = CATALOG + "." + SCHEMA;
  private static final List<String> COLLECTIONS =
      List.of("catalogs", "schemas", "tables", "functions", "models", "volumes");
  private static final Map<String, List<String>> BROWSE_FIELDS =
      Map.of(
          "catalogs",
          List.of(
              "name",
              "comment",
              "properties",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "id",
              "storage_root",
              "storage_location"),
          "schemas",
          List.of(
              "name",
              "catalog_name",
              "comment",
              "properties",
              "full_name",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "schema_id",
              "storage_root",
              "storage_location"),
          "tables",
          List.of(
              "name",
              "catalog_name",
              "schema_name",
              "table_type",
              "data_source_format",
              "columns",
              "comment",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "table_id"),
          "functions",
          List.of(
              "name",
              "catalog_name",
              "schema_name",
              "input_params",
              "data_type",
              "full_data_type",
              "return_params",
              "routine_body",
              "routine_dependencies",
              "parameter_style",
              "is_deterministic",
              "sql_data_access",
              "is_null_call",
              "security_type",
              "specific_name",
              "comment",
              "properties",
              "full_name",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "function_id",
              "external_language"),
          "models",
          List.of(
              "name",
              "catalog_name",
              "schema_name",
              "storage_location",
              "full_name",
              "comment",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "id"),
          "volumes",
          List.of(
              "catalog_name",
              "schema_name",
              "name",
              "comment",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by",
              "volume_id",
              "volume_type",
              "storage_location",
              "full_name"),
          "external-locations",
          List.of(
              "name",
              "id",
              "url",
              "comment",
              "owner",
              "created_at",
              "created_by",
              "updated_at",
              "updated_by"));

  private ServerConfig browser;
  private ServerConfig catalogOwner;
  private ServerConfig schemaOwner;
  private ServerConfig objectOwner;
  private ApiClient objectOwnerClient;
  private final Map<String, JsonNode> fullMetadata = new LinkedHashMap<>();

  @BeforeEach
  public void createFixtures() throws Exception {
    createCommonTestUsers();
    browser = createTestUserServerConfig(REGULAR_1);
    catalogOwner = createTestUserServerConfig(PRINCIPAL_1);
    schemaOwner = createTestUserServerConfig(PRINCIPAL_2);
    objectOwner = createTestUserServerConfig(REGULAR_2);
    objectOwnerClient = TestUtils.createApiClient(objectOwner);

    String storageRoot = testDirectoryRoot.resolve("managed").toString();
    createExternalLocationWithCredential("storage_credential", "managed_root", storageRoot);
    new ExternalLocationsApi(adminApiClient)
        .createExternalLocation(
            new CreateExternalLocation()
                .name("other_location")
                .credentialName("storage_credential")
                .url(testDirectoryRoot.resolve("other_location").toString())
                .comment("Other storage"));
    for (String user : List.of(PRINCIPAL_1, PRINCIPAL_2)) {
      grantPermissions(
          user, SecurableType.EXTERNAL_LOCATION, "managed_root", Privileges.CREATE_MANAGED_STORAGE);
    }
    grantPermissions(
        PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.CREATE_CATALOG);
    new CatalogsApi(TestUtils.createApiClient(catalogOwner))
        .createCatalog(
            new CreateCatalog()
                .name(CATALOG)
                .comment("Discovery catalog")
                .properties(Map.of("purpose", "discovery"))
                .storageRoot(storageRoot + "/catalog"));
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        CATALOG,
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    new SchemasApi(TestUtils.createApiClient(schemaOwner))
        .createSchema(
            new CreateSchema()
                .catalogName(CATALOG)
                .name(SCHEMA)
                .comment("Discovery schema")
                .properties(Map.of("purpose", "discovery"))
                .storageRoot(storageRoot + "/schema"));
    grantPermissions(
        PRINCIPAL_1,
        SecurableType.SCHEMA,
        NAMESPACE,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE,
        Privileges.CREATE_FUNCTION,
        Privileges.CREATE_MODEL,
        Privileges.CREATE_VOLUME);
    grantPermissions(REGULAR_2, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_2,
        SecurableType.SCHEMA,
        NAMESPACE,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE,
        Privileges.CREATE_FUNCTION,
        Privileges.CREATE_MODEL,
        Privileges.CREATE_VOLUME);
    createTable("a_table");
    new TablesApi(objectOwnerClient)
        .createTable(
            new CreateTable()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .name("b_view")
                .tableType(TableType.VIEW)
                .columns(TEST_COLUMNS)
                .viewDefinition("SELECT id FROM " + NAMESPACE + ".a_table")
                .viewDependencies(
                    new DependencyList()
                        .dependencies(
                            List.of(
                                new Dependency()
                                    .table(
                                        new TableDependency()
                                            .tableFullName(NAMESPACE + ".a_table")))))
                .properties(Map.of("definition", "private"))
                .comment("Discovery view"));
    createFunction("function");
    createVolume("volume");
    createModel("model");
    new ModelVersionsApi(objectOwnerClient)
        .createModelVersion(
            new CreateModelVersion()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .modelName("model")
                .source(testDirectoryRoot.resolve("source").toString())
                .comment("First version"));

    revoke(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        CATALOG,
        Privilege.USE_CATALOG,
        Privilege.CREATE_SCHEMA);
    revoke(REGULAR_2, SecurableType.CATALOG, CATALOG, Privilege.USE_CATALOG);
    revoke(
        REGULAR_2,
        SecurableType.SCHEMA,
        NAMESPACE,
        Privilege.USE_SCHEMA,
        Privilege.CREATE_TABLE,
        Privilege.CREATE_FUNCTION,
        Privilege.CREATE_MODEL,
        Privilege.CREATE_VOLUME);

    for (String path :
        List.of(
            "/catalogs/" + CATALOG,
            "/schemas/" + NAMESPACE,
            "/tables/" + NAMESPACE + ".a_table",
            "/tables/" + NAMESPACE + ".b_view",
            "/functions/" + NAMESPACE + ".function",
            "/models/" + NAMESPACE + ".model",
            "/volumes/" + NAMESPACE + ".volume")) {
      fullMetadata.put(path, getJson(adminConfig, path));
    }
  }

  @Test
  public void catalogBrowseRequiresOptInAndReturnsOnlyDiscoveryFields() throws Exception {
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.BROWSE);
    assertThat(grantsApi.get(SecurableType.CATALOG, CATALOG, null).getPrivilegeAssignments())
        .anySatisfy(
            assignment -> {
              assertThat(assignment.getPrincipal()).isEqualTo(REGULAR_1);
              assertThat(assignment.getPrivileges()).containsExactly(Privilege.BROWSE);
            });
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      String path = entry.getKey();
      String collection = path.split("/")[1];
      for (String query : List.of("", "?include_browse=false")) {
        assertHttpApiException(
            sendRawGet(browser, BASE + path + query), ErrorCode.PERMISSION_DENIED);
      }
      assertHttpApiException(
          sendRawGet(objectOwner, BASE + path + "?include_browse=true"),
          ErrorCode.PERMISSION_DENIED);
      assertThat(getJson(browser, path + "?include_browse=true"))
          .isEqualTo(browseMetadata(collection, entry.getValue()));
      assertThat(getJson(adminConfig, path)).isEqualTo(entry.getValue());
    }
    assertThat(fullMetadata.get("/tables/" + NAMESPACE + ".a_table").path("properties").size())
        .isPositive();
    assertThat(fullMetadata.get("/tables/" + NAMESPACE + ".b_view").has("view_definition"))
        .isTrue();
    assertThat(
            fullMetadata
                .get("/functions/" + NAMESPACE + ".function")
                .path("routine_definition")
                .asText())
        .isEqualTo("return 7");
    for (String collection : List.of("catalogs", "schemas", "models", "volumes")) {
      JsonNode full =
          fullMetadata.entrySet().stream()
              .filter(entry -> entry.getKey().startsWith("/" + collection + "/"))
              .findFirst()
              .orElseThrow()
              .getValue();
      assertThat(full.path("storage_location").isTextual()).isTrue();
    }
    for (String collection : COLLECTIONS) {
      String path = listPath(collection);
      String field = responseField(collection);
      ArrayNode expected = MAPPER.createArrayNode();
      for (JsonNode full : getJson(adminConfig, path).path(field)) {
        if (!collection.equals("catalogs") || full.path("name").asText().equals(CATALOG)) {
          expected.add(browseMetadata(collection, full));
        }
      }
      assertThat(getJson(browser, query(path, "include_browse=true")).path(field))
          .isEqualTo(expected);
      for (String option : List.of("", "include_browse=false")) {
        assertThat(getJson(browser, option.isEmpty() ? path : query(path, option)).path(field))
            .isEmpty();
      }
      assertThat(getJson(objectOwner, query(path, "include_browse=true")).path(field)).isEmpty();
    }
    new RegisteredModelsApi(adminApiClient)
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .name("hidden_model"));
    assertThat(getJson(browser, "/models?include_browse=true").path("registered_models"))
        .isEqualTo(
            MAPPER
                .createArrayNode()
                .add(
                    browseMetadata("models", fullMetadata.get("/models/" + NAMESPACE + ".model"))));
    assertThat(
            new CatalogsApi(TestUtils.createApiClient(browser))
                .getCatalog(CATALOG, true)
                .getBrowseOnly())
        .isTrue();

    createTableAsCatalogOwner("future_table");
    JsonNode future = getJson(adminConfig, "/tables/" + NAMESPACE + ".future_table");
    assertThat(getJson(browser, "/tables/" + NAMESPACE + ".future_table?include_browse=true"))
        .isEqualTo(browseMetadata("tables", future));
    assertHttpApiException(
        sendRawGet(browser, BASE + "/external-locations/managed_root" + "?include_browse=true"),
        ErrorCode.PERMISSION_DENIED);
  }

  @Test
  public void mixedPagesPreferFullMetadataAndSurviveBrowseRevocation() throws Exception {
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.BROWSE, Privileges.USE_CATALOG);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.BROWSE);
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, NAMESPACE, Privileges.USE_SCHEMA);
    createTableAsCatalogOwner("c_table");
    createTableAsCatalogOwner("d_table");
    createFunctionAsCatalogOwner("later_function");
    createVolumeAsCatalogOwner("later_volume");
    new RegisteredModelsApi(TestUtils.createApiClient(catalogOwner))
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .name("later_model")
                .comment("Another model"));
    new SchemasApi(TestUtils.createApiClient(catalogOwner))
        .createSchema(new CreateSchema().catalogName(CATALOG).name("later_schema"));
    for (String table : List.of("a_table", "d_table")) {
      grantPermissions(REGULAR_1, SecurableType.TABLE, NAMESPACE + "." + table, Privileges.SELECT);
    }
    grantPermissions(
        REGULAR_1, SecurableType.FUNCTION, NAMESPACE + ".function", Privileges.EXECUTE);
    grantPermissions(
        REGULAR_1, SecurableType.REGISTERED_MODEL, NAMESPACE + ".model", Privileges.EXECUTE);
    grantPermissions(
        REGULAR_1, SecurableType.VOLUME, NAMESPACE + ".volume", Privileges.READ_VOLUME);
    Map<String, Set<String>> fullNames =
        Map.of(
            "catalogs",
            Set.of(CATALOG),
            "schemas",
            Set.of(SCHEMA),
            "tables",
            Set.of("a_table", "d_table"),
            "functions",
            Set.of("function"),
            "models",
            Set.of("model"),
            "volumes",
            Set.of("volume"));
    for (String collection : COLLECTIONS) {
      String token = null;
      int fullCount = 0;
      int browseCount = 0;
      do {
        String path =
            query(
                listPath(collection),
                "max_results=2" + (token == null ? "" : "&page_token=" + token));
        String field = responseField(collection);
        JsonNode adminPage = getJson(adminConfig, path);
        ArrayNode expected = MAPPER.createArrayNode();
        ArrayNode expectedFull = MAPPER.createArrayNode();
        for (JsonNode full : adminPage.path(field)) {
          if (fullNames.get(collection).contains(full.path("name").asText())) {
            expected.add(full);
            expectedFull.add(full);
            fullCount++;
          } else {
            expected.add(browseMetadata(collection, full));
            browseCount++;
          }
        }
        JsonNode page = getJson(browser, query(path, "include_browse=true"));
        assertThat(page.path(field)).isEqualTo(expected);
        assertThat(page.get("next_page_token")).isEqualTo(adminPage.get("next_page_token"));
        JsonNode ordinaryPage = getJson(browser, path);
        assertThat(ordinaryPage.path(field)).isEqualTo(expectedFull);
        assertThat(ordinaryPage.get("next_page_token")).isEqualTo(adminPage.get("next_page_token"));
        token =
            adminPage.path("next_page_token").isTextual()
                ? adminPage.path("next_page_token").asText()
                : null;
      } while (token != null);
      assertThat(fullCount).isPositive();
      assertThat(browseCount).isPositive();
    }
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      if (!entry.getKey().endsWith(".b_view")) {
        assertThat(getJson(browser, entry.getKey() + "?include_browse=true"))
            .isEqualTo(entry.getValue());
      }
    }
    revoke(REGULAR_1, SecurableType.CATALOG, CATALOG, Privilege.BROWSE);
    assertThat(getJson(browser, "/tables/" + NAMESPACE + ".a_table?include_browse=true"))
        .isEqualTo(fullMetadata.get("/tables/" + NAMESPACE + ".a_table"));
    assertHttpApiException(
        sendRawGet(browser, BASE + "/tables/" + NAMESPACE + ".b_view?include_browse=true"),
        ErrorCode.PERMISSION_DENIED);
    assertThat(
            new TablesApi(TestUtils.createApiClient(browser))
                .listTables(CATALOG, SCHEMA, 100, null, true)
                .getTables())
        .extracting(io.unitycatalog.client.model.TableInfo::getName)
        .containsExactly("a_table", "d_table");
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.SELECT);
    assertThat(getJson(browser, "/tables/" + NAMESPACE + ".b_view?include_browse=true"))
        .isEqualTo(fullMetadata.get("/tables/" + NAMESPACE + ".b_view"));
    grantPermissions(REGULAR_1, SecurableType.SCHEMA, NAMESPACE, Privileges.EXECUTE);
    assertThat(
            new FunctionsApi(TestUtils.createApiClient(browser))
                .getFunction(NAMESPACE + ".later_function", true)
                .getRoutineDefinition())
        .isEqualTo("return 7");
    assertThat(
            new RegisteredModelsApi(TestUtils.createApiClient(browser))
                .getRegisteredModel(NAMESPACE + ".later_model", true)
                .getBrowseOnly())
        .isNull();
  }

  @Test
  public void ownersKeepTheirOrdinaryUsageRequirements() throws Exception {
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      assertThat(getJson(catalogOwner, entry.getKey() + "?include_browse=true"))
          .isEqualTo(entry.getValue());
      assertThat(getJson(adminConfig, entry.getKey() + "?include_browse=true"))
          .isEqualTo(entry.getValue());
    }
    for (String owner : List.of(PRINCIPAL_2, REGULAR_2)) {
      grantPermissions(owner, SecurableType.CATALOG, CATALOG, Privileges.BROWSE);
    }
    for (ServerConfig caller : List.of(schemaOwner, objectOwner)) {
      for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
        assertHttpApiException(
            sendRawGet(caller, BASE + entry.getKey()), ErrorCode.PERMISSION_DENIED);
        assertThat(getJson(caller, entry.getKey() + "?include_browse=true"))
            .isEqualTo(browseMetadata(entry.getKey().split("/")[1], entry.getValue()));
      }
    }
    grantPermissions(PRINCIPAL_2, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
    grantPermissions(REGULAR_2, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      assertThat(getJson(schemaOwner, entry.getKey() + "?include_browse=true"))
          .isEqualTo(entry.getValue());
      if (!entry.getKey().startsWith("/catalogs/")) {
        assertThat(getJson(objectOwner, entry.getKey() + "?include_browse=true"))
            .isEqualTo(browseMetadata(entry.getKey().split("/")[1], entry.getValue()));
      }
    }
    grantPermissions(REGULAR_2, SecurableType.SCHEMA, NAMESPACE, Privileges.USE_SCHEMA);
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      assertThat(getJson(objectOwner, entry.getKey() + "?include_browse=true"))
          .isEqualTo(entry.getValue());
    }
  }

  @Test
  public void externalLocationBrowseIsLocalAndHidesCredentialReferences() throws Exception {
    JsonNode full = getJson(adminConfig, "/external-locations/managed_root");
    assertThat(full.path("credential_id").isTextual()).isTrue();
    grantPermissions(REGULAR_1, SecurableType.EXTERNAL_LOCATION, "managed_root", Privileges.BROWSE);
    for (String option : List.of("", "?include_browse=false")) {
      assertHttpApiException(
          sendRawGet(browser, BASE + "/external-locations/managed_root" + option),
          ErrorCode.PERMISSION_DENIED);
    }
    assertThat(getJson(browser, "/external-locations/managed_root?include_browse=true"))
        .isEqualTo(browseMetadata("external-locations", full));
    assertThat(getJson(adminConfig, "/external-locations/managed_root?include_browse=true"))
        .isEqualTo(full);
    grantPermissions(
        REGULAR_1, SecurableType.EXTERNAL_LOCATION, "managed_root", Privileges.READ_FILES);
    assertThat(getJson(browser, "/external-locations/managed_root?include_browse=true"))
        .isEqualTo(full);
    revoke(REGULAR_1, SecurableType.EXTERNAL_LOCATION, "managed_root", Privilege.READ_FILES);
    assertHttpApiException(
        sendRawGet(browser, BASE + "/external-locations/other_location" + "?include_browse=true"),
        ErrorCode.PERMISSION_DENIED);
    for (String path : fullMetadata.keySet()) {
      assertHttpApiException(
          sendRawGet(browser, BASE + path + "?include_browse=true"), ErrorCode.PERMISSION_DENIED);
    }
    grantPermissions(
        REGULAR_1, SecurableType.EXTERNAL_LOCATION, "other_location", Privileges.READ_FILES);
    JsonNode other = getJson(adminConfig, "/external-locations/other_location");
    assertThat(
            getJson(browser, "/external-locations?max_results=2&include_browse=true")
                .path("external_locations"))
        .isEqualTo(
            MAPPER.createArrayNode().add(browseMetadata("external-locations", full)).add(other));
    assertThat(getJson(browser, "/external-locations?max_results=1").path("external_locations"))
        .isEmpty();
    assertThat(
            getJson(browser, "/external-locations?max_results=1").path("next_page_token").asText())
        .isEqualTo("managed_root");
    assertThat(
            getJson(
                    browser,
                    "/external-locations?max_results=1&page_token=managed_root"
                        + "&include_browse=true")
                .path("external_locations"))
        .isEqualTo(MAPPER.createArrayNode().add(other));
    revoke(REGULAR_1, SecurableType.EXTERNAL_LOCATION, "managed_root", Privilege.BROWSE);
    assertHttpApiException(
        sendRawGet(browser, BASE + "/external-locations/managed_root" + "?include_browse=true"),
        ErrorCode.PERMISSION_DENIED);
    assertThat(
            getJson(browser, "/external-locations?include_browse=true").path("external_locations"))
        .isEqualTo(MAPPER.createArrayNode().add(other));
    assertThat(getJson(adminConfig, "/external-locations/managed_root")).isEqualTo(full);
  }

  @Test
  public void browseNeverAuthorizesEngineLoadsCredentialsVersionsOrMutations() throws Exception {
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.BROWSE);
    grantPermissions(REGULAR_1, SecurableType.EXTERNAL_LOCATION, "managed_root", Privileges.BROWSE);
    new IcebergRestClient(catalogOwner)
        .createTable(
            CATALOG,
            SCHEMA,
            CreateTableRequest.builder()
                .withName("iceberg_table")
                .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .build());
    JsonNode managed = getJson(adminConfig, "/tables/" + NAMESPACE + ".iceberg_table");
    assertThat(managed.path("storage_location").isTextual()).isTrue();
    ApiClient client = TestUtils.createApiClient(browser);
    TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(client);
    DeltaTablesApi delta = new DeltaTablesApi(client);
    DeltaTemporaryCredentialsApi deltaCredentials = new DeltaTemporaryCredentialsApi(client);
    for (boolean withUsage : List.of(false, true)) {
      if (withUsage) {
        grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
        grantPermissions(REGULAR_1, SecurableType.SCHEMA, NAMESPACE, Privileges.USE_SCHEMA);
      }
      assertThat(getJson(browser, "/tables/" + NAMESPACE + ".a_table?include_browse=true"))
          .isEqualTo(
              browseMetadata("tables", fullMetadata.get("/tables/" + NAMESPACE + ".a_table")));
      assertThat(getJson(browser, "/tables/" + NAMESPACE + ".iceberg_table?include_browse=true"))
          .isEqualTo(browseMetadata("tables", managed));
      assertDeltaPermissionDenied(() -> delta.loadTable(CATALOG, SCHEMA, "a_table"));
      HttpResponse<String> deltaBrowse =
          sendRawGet(
              browser,
              BASE
                  + "/delta/v1/catalogs/"
                  + CATALOG
                  + "/schemas/"
                  + SCHEMA
                  + "/tables/a_table?include_browse=true");
      assertThat(deltaBrowse.statusCode()).isEqualTo(403);
      assertThat(MAPPER.readTree(deltaBrowse.body()).path("error").path("type").asText())
          .isEqualTo("PermissionDeniedException");
      for (DeltaCredentialOperation operation :
          List.of(DeltaCredentialOperation.READ, DeltaCredentialOperation.READ_WRITE)) {
        assertDeltaPermissionDenied(
            () -> deltaCredentials.getTableCredentials(operation, CATALOG, SCHEMA, "a_table"));
      }
      HttpResponse<String> iceberg =
          sendRawGet(
              browser,
              BASE
                  + "/iceberg/v1/catalogs/"
                  + CATALOG
                  + "/namespaces/"
                  + SCHEMA
                  + "/tables/iceberg_table?include_browse=true");
      assertThat(iceberg.statusCode()).isEqualTo(403);
      assertThat(MAPPER.readTree(iceberg.body()).path("error").path("code").asInt()).isEqualTo(403);
      assertThat(
              getJson(
                      browser,
                      "/iceberg/v1/catalogs/"
                          + CATALOG
                          + "/namespaces/"
                          + SCHEMA
                          + "/tables?include_browse=true")
                  .path("identifiers"))
          .isEmpty();
      for (TableOperation operation : List.of(TableOperation.READ, TableOperation.READ_WRITE)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryTableCredentials(
                    new GenerateTemporaryTableCredential()
                        .tableId(
                            fullMetadata
                                .get("/tables/" + NAMESPACE + ".a_table")
                                .path("table_id")
                                .asText())
                        .operation(operation)));
      }
      for (VolumeOperation operation :
          List.of(VolumeOperation.READ_VOLUME, VolumeOperation.WRITE_VOLUME)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryVolumeCredentials(
                    new GenerateTemporaryVolumeCredential()
                        .volumeId(
                            fullMetadata
                                .get("/volumes/" + NAMESPACE + ".volume")
                                .path("volume_id")
                                .asText())
                        .operation(operation)));
      }
      for (ModelVersionOperation operation :
          List.of(
              ModelVersionOperation.READ_MODEL_VERSION,
              ModelVersionOperation.READ_WRITE_MODEL_VERSION)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryModelVersionCredentials(
                    new GenerateTemporaryModelVersionCredential()
                        .catalogName(CATALOG)
                        .schemaName(SCHEMA)
                        .modelName("model")
                        .version(1L)
                        .operation(operation)));
      }
      for (PathOperation operation :
          List.of(PathOperation.PATH_READ, PathOperation.PATH_READ_WRITE)) {
        assertPermissionDenied(
            () ->
                credentials.generateTemporaryPathCredentials(
                    new GenerateTemporaryPathCredential()
                        .url(testDirectoryRoot.resolve("managed/unmanaged").toString())
                        .operation(operation)));
      }
      ModelVersionsApi versions = new ModelVersionsApi(client);
      assertPermissionDenied(() -> versions.getModelVersion(NAMESPACE + ".model", 1L));
      assertPermissionDenied(() -> versions.listModelVersions(NAMESPACE + ".model", 100, null));
    }
    fullMetadata.put(
        "/external-locations/managed_root",
        getJson(adminConfig, "/external-locations/managed_root"));
    for (Map.Entry<String, JsonNode> entry : fullMetadata.entrySet()) {
      assertHttpApiException(
          sendRaw(
              browser, "DELETE", BASE + entry.getKey() + "?include_browse=true", Optional.empty()),
          ErrorCode.PERMISSION_DENIED);
      assertThat(getJson(adminConfig, entry.getKey())).isEqualTo(entry.getValue());
    }
  }

  private void createTable(String name) throws Exception {
    new TablesApi(objectOwnerClient)
        .createTable(
            new CreateTable()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .name(name)
                .tableType(TableType.EXTERNAL)
                .dataSourceFormat(DataSourceFormat.DELTA)
                .columns(TEST_COLUMNS)
                .storageLocation(testDirectoryRoot.resolve(name).toString())
                .properties(Map.of("storage-detail", "private"))
                .comment("Discovery table"));
  }

  private void createTableAsCatalogOwner(String name) throws Exception {
    createExternalTable(
        new TablesApi(TestUtils.createApiClient(catalogOwner)),
        CATALOG,
        SCHEMA,
        name,
        testDirectoryRoot.resolve(name).toString());
  }

  private void createFunction(String name) throws Exception {
    new FunctionsApi(objectOwnerClient).createFunction(functionRequest(name));
  }

  private void createFunctionAsCatalogOwner(String name) throws Exception {
    new FunctionsApi(TestUtils.createApiClient(catalogOwner)).createFunction(functionRequest(name));
  }

  private CreateFunctionRequest functionRequest(String name) {
    return new CreateFunctionRequest()
        .functionInfo(
            new CreateFunction()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .name(name)
                .dataType(ColumnTypeName.INT)
                .fullDataType("INT")
                .inputParams(new FunctionParameterInfos().parameters(List.of()))
                .returnParams(new FunctionParameterInfos().parameters(List.of()))
                .routineBody(CreateFunction.RoutineBodyEnum.EXTERNAL)
                .routineDefinition("return 7")
                .parameterStyle(CreateFunction.ParameterStyleEnum.S)
                .isDeterministic(true)
                .isNullCall(false)
                .sqlDataAccess(CreateFunction.SqlDataAccessEnum.NO_SQL)
                .securityType(CreateFunction.SecurityTypeEnum.DEFINER)
                .specificName(name)
                .externalLanguage("PYTHON")
                .properties("{\"purpose\":\"discovery\"}")
                .comment("Discovery function"));
  }

  private void createVolume(String name) throws Exception {
    new VolumesApi(objectOwnerClient).createVolume(volumeRequest(name));
  }

  private void createVolumeAsCatalogOwner(String name) throws Exception {
    new VolumesApi(TestUtils.createApiClient(catalogOwner)).createVolume(volumeRequest(name));
  }

  private CreateVolumeRequestContent volumeRequest(String name) {
    return new CreateVolumeRequestContent()
        .catalogName(CATALOG)
        .schemaName(SCHEMA)
        .name(name)
        .volumeType(VolumeType.EXTERNAL)
        .storageLocation(testDirectoryRoot.resolve(name).toString())
        .comment("Discovery volume");
  }

  private void createModel(String name) throws Exception {
    new RegisteredModelsApi(objectOwnerClient)
        .createRegisteredModel(
            new CreateRegisteredModel()
                .catalogName(CATALOG)
                .schemaName(SCHEMA)
                .name(name)
                .comment("Discovery model"));
  }

  private void revoke(String user, SecurableType type, String name, Privilege... privileges)
      throws Exception {
    grantsApi.update(
        type,
        name,
        new UpdatePermissions()
            .changes(List.of(new PermissionsChange().principal(user).remove(List.of(privileges)))));
  }

  private JsonNode getJson(ServerConfig caller, String path) throws Exception {
    HttpResponse<String> response = sendRawGet(caller, BASE + path);
    assertThat(response.statusCode()).isEqualTo(200);
    return MAPPER.readTree(response.body());
  }

  private static ObjectNode browseMetadata(String collection, JsonNode full) {
    ObjectNode expected = MAPPER.createObjectNode();
    for (String field : BROWSE_FIELDS.get(collection)) {
      if (full.hasNonNull(field)) {
        expected.set(field, full.get(field));
      }
    }
    return expected.put("browse_only", true);
  }

  private static String listPath(String collection) {
    return "/"
        + collection
        + switch (collection) {
          case "catalogs", "external-locations" -> "";
          case "schemas" -> "?catalog_name=" + CATALOG;
          default -> "?catalog_name=" + CATALOG + "&schema_name=" + SCHEMA;
        };
  }

  private static String responseField(String collection) {
    return collection.equals("models") ? "registered_models" : collection.replace('-', '_');
  }

  private static String query(String path, String parameter) {
    return path + (path.contains("?") ? "&" : "?") + parameter;
  }
}
