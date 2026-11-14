package io.unitycatalog.cli;

import static io.unitycatalog.server.utils.TestUtils.TEST_AWS_MASTER_ROLE_ARN;
import static io.unitycatalog.server.utils.TestUtils.createApiClient;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.CredentialsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.FunctionsApi;
import io.unitycatalog.client.api.ModelVersionsApi;
import io.unitycatalog.client.api.RegisteredModelsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.VolumesApi;
import io.unitycatalog.client.model.AwsIamRoleRequest;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateCredentialRequest;
import io.unitycatalog.client.model.CreateExternalLocation;
import io.unitycatalog.client.model.CreateFunction;
import io.unitycatalog.client.model.CreateFunctionRequest;
import io.unitycatalog.client.model.CreateModelVersion;
import io.unitycatalog.client.model.CreateRegisteredModel;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.CreateVolumeRequestContent;
import io.unitycatalog.client.model.CredentialPurpose;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.FunctionParameterInfos;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.VolumeType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.persist.utils.PagedListingHelper;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Every list command must return the results of all pages, not only the first one. */
public class CliListPaginationTest extends BaseServerTest {

  private static final String CATALOG = "pg_catalog";
  private static final String SCHEMA = "pg_schema";
  private static final String CREDENTIAL = "pg_credential";

  private Integer originalPageSize;
  private ApiClient apiClient;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AWS_MASTER_ROLE_ARN.getKey(), TEST_AWS_MASTER_ROLE_ARN);
    serverProperties.setProperty(
        Property.MODEL_STORAGE_ROOT.getKey(),
        getManagedStorageCloudPath(testDirectoryRoot.resolve("models")));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    originalPageSize = PagedListingHelper.DEFAULT_PAGE_SIZE;
    PagedListingHelper.DEFAULT_PAGE_SIZE = 2;
    apiClient = createApiClient(serverConfig);
  }

  @AfterEach
  @Override
  public void tearDown() {
    PagedListingHelper.DEFAULT_PAGE_SIZE = originalPageSize;
    super.tearDown();
  }

  @Test
  public void testListCatalogs() throws Exception {
    for (String name : List.of("cat_a", "cat_b", "cat_c")) {
      new CatalogsApi(apiClient).createCatalog(new CreateCatalog().name(name));
    }
    assertThat(list(List.of("catalog"), "name")).containsExactly("cat_a", "cat_b", "cat_c");
  }

  @Test
  public void testListSchemas() throws Exception {
    createSchemas("sch_a", "sch_b", "sch_c");
    assertThat(list(List.of("schema", "--catalog", CATALOG), "name"))
        .containsExactly("sch_a", "sch_b", "sch_c");
  }

  @Test
  public void testListTables() throws Exception {
    createSchemas(SCHEMA);
    for (String name : List.of("tbl_a", "tbl_b", "tbl_c")) {
      new TablesApi(apiClient)
          .createTable(
              new CreateTable()
                  .name(name)
                  .catalogName(CATALOG)
                  .schemaName(SCHEMA)
                  .tableType(TableType.EXTERNAL)
                  .dataSourceFormat(DataSourceFormat.DELTA)
                  .storageLocation(getManagedStorageCloudPath(testDirectoryRoot.resolve(name)))
                  .columns(
                      List.of(
                          new ColumnInfo()
                              .name("id")
                              .typeText("INTEGER")
                              .typeJson(
                                  "{\"name\":\"id\",\"type\":\"integer\","
                                      + "\"nullable\":true,\"metadata\":{}}")
                              .typeName(ColumnTypeName.INT)
                              .position(0)
                              .nullable(true))));
    }
    assertThat(list(List.of("table", "--catalog", CATALOG, "--schema", SCHEMA), "name"))
        .containsExactly("tbl_a", "tbl_b", "tbl_c");
  }

  @Test
  public void testListVolumes() throws Exception {
    createSchemas(SCHEMA);
    for (String name : List.of("vol_a", "vol_b", "vol_c")) {
      new VolumesApi(apiClient)
          .createVolume(
              new CreateVolumeRequestContent()
                  .name(name)
                  .catalogName(CATALOG)
                  .schemaName(SCHEMA)
                  .volumeType(VolumeType.EXTERNAL)
                  .storageLocation(getManagedStorageCloudPath(testDirectoryRoot.resolve(name))));
    }
    assertThat(list(List.of("volume", "--catalog", CATALOG, "--schema", SCHEMA), "name"))
        .containsExactly("vol_a", "vol_b", "vol_c");
  }

  @Test
  public void testListFunctions() throws Exception {
    createSchemas(SCHEMA);
    for (String name : List.of("fn_a", "fn_b", "fn_c")) {
      new FunctionsApi(apiClient)
          .createFunction(
              new CreateFunctionRequest()
                  .functionInfo(
                      new CreateFunction()
                          .name(name)
                          .catalogName(CATALOG)
                          .schemaName(SCHEMA)
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
                          .inputParams(new FunctionParameterInfos().parameters(List.of()))));
    }
    assertThat(list(List.of("function", "--catalog", CATALOG, "--schema", SCHEMA), "name"))
        .containsExactly("fn_a", "fn_b", "fn_c");
  }

  @Test
  public void testListRegisteredModelsAndModelVersions() throws Exception {
    createSchemas(SCHEMA);
    for (String name : List.of("mdl_a", "mdl_b", "mdl_c")) {
      new RegisteredModelsApi(apiClient)
          .createRegisteredModel(
              new CreateRegisteredModel().name(name).catalogName(CATALOG).schemaName(SCHEMA));
    }
    for (int i = 0; i < 3; i++) {
      new ModelVersionsApi(apiClient)
          .createModelVersion(
              new CreateModelVersion()
                  .catalogName(CATALOG)
                  .schemaName(SCHEMA)
                  .modelName("mdl_a")
                  .source("source")
                  .runId("run"));
    }
    assertThat(list(List.of("registered_model", "--catalog", CATALOG, "--schema", SCHEMA), "name"))
        .containsExactly("mdl_a", "mdl_b", "mdl_c");
    assertThat(list(List.of("registered_model"), "name"))
        .containsExactlyInAnyOrder("mdl_a", "mdl_b", "mdl_c");
    assertThat(
            list(
                List.of("model_version", "--full_name", CATALOG + "." + SCHEMA + ".mdl_a"),
                "version"))
        .containsExactlyInAnyOrder("1", "2", "3");
  }

  @Test
  public void testListCredentialsAndExternalLocations() throws Exception {
    for (String name : List.of("cred_a", "cred_b", "cred_c")) {
      createCredential(name);
    }
    assertThat(list(List.of("credential"), "name")).containsExactly("cred_a", "cred_b", "cred_c");

    createCredential(CREDENTIAL);
    for (String name : List.of("loc_a", "loc_b", "loc_c")) {
      new ExternalLocationsApi(apiClient)
          .createExternalLocation(
              new CreateExternalLocation()
                  .name(name)
                  .url(getManagedStorageCloudPath(testDirectoryRoot.resolve(name)))
                  .credentialName(CREDENTIAL));
    }
    assertThat(list(List.of("external_location"), "name"))
        .containsExactly("loc_a", "loc_b", "loc_c");
  }

  @Test
  public void testMaxResultsLimitsTheTotalAcrossPages() throws Exception {
    createSchemas("sch_a", "sch_b", "sch_c", "sch_d", "sch_e");
    assertThat(list(List.of("schema", "--catalog", CATALOG, "--max_results", "3"), "name"))
        .containsExactly("sch_a", "sch_b", "sch_c");
    assertThat(list(List.of("schema", "--catalog", CATALOG, "--max_results", "0"), "name"))
        .containsExactly("sch_a", "sch_b", "sch_c", "sch_d", "sch_e");
  }

  @Test
  public void testPageTokenListsFromThatPageToTheEnd() throws Exception {
    createSchemas("sch_a", "sch_b", "sch_c", "sch_d", "sch_e");
    String pageToken =
        new SchemasApi(apiClient)
            .listSchemas(CATALOG, 1, null, /* includeBrowse= */ false)
            .getNextPageToken();
    assertThat(list(List.of("schema", "--catalog", CATALOG, "--page_token", pageToken), "name"))
        .containsExactly("sch_b", "sch_c", "sch_d", "sch_e");
    assertThat(
            list(
                List.of(
                    "schema",
                    "--catalog",
                    CATALOG,
                    "--page_token",
                    pageToken,
                    "--max_results",
                    "3"),
                "name"))
        .containsExactly("sch_b", "sch_c", "sch_d");
  }

  private void createSchemas(String... names) throws Exception {
    new CatalogsApi(apiClient).createCatalog(new CreateCatalog().name(CATALOG));
    for (String name : names) {
      new SchemasApi(apiClient).createSchema(new CreateSchema().name(name).catalogName(CATALOG));
    }
  }

  private void createCredential(String name) throws Exception {
    new CredentialsApi(apiClient)
        .createCredential(
            new CreateCredentialRequest()
                .name(name)
                .purpose(CredentialPurpose.STORAGE)
                .awsIamRole(
                    new AwsIamRoleRequest().roleArn("arn:aws:iam::123456789012:role/role-name")));
  }

  private List<String> list(List<String> entityAndArgs, String field) {
    List<String> args = new ArrayList<>(entityAndArgs);
    args.add(1, "list");
    JsonNode output = TestUtils.executeCliCommand(serverConfig, args);
    return StreamSupport.stream(output.spliterator(), false)
        .map(item -> item.get(field).asText())
        .toList();
  }
}
