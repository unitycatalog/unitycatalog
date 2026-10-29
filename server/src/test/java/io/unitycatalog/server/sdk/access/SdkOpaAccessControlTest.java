package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertDeltaApiException;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.MediaType;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.api.ExternalLocationsApi;
import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.api.DeltaConfigurationApi;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryPathCredential;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PathOperation;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.JsonUtils;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** Runs UC requests against a real OPA server using the documented example policy. */
@Testcontainers(disabledWithoutDocker = true)
public class SdkOpaAccessControlTest extends SdkAccessControlBaseCRUDTest {
  private static final String ALICE = "alice@example.com";
  private static final String BOB = "bob@example.com";

  @Container
  private static final GenericContainer<?> OPA =
      new GenericContainer<>("openpolicyagent/opa:1.21.4")
          .withCommand("run", "--server", "--addr=0.0.0.0:8181")
          .withExposedPorts(8181)
          .waitingFor(Wait.forHttp("/health"));

  private final List<Map<String, String>> policyGrants = new ArrayList<>();
  private WebClient opaAdmin;
  private ApiClient aliceClient;
  private TablesApi aliceTables;
  private TablesApi adminTables;
  private String catalogId;
  private String schemaId;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUTHORIZATION_PROVIDER.getKey(), "opa");
    serverProperties.setProperty(
        Property.OPA_DECISION_URL.getKey(),
        "http://" + OPA.getHost() + ":" + OPA.getMappedPort(8181) + "/v1/data/uc/authz");
  }

  @BeforeEach
  @Override
  @SneakyThrows
  public void setUp() {
    opaAdmin = WebClient.of("http://" + OPA.getHost() + ":" + OPA.getMappedPort(8181));
    installPolicy();
    updateGrants();
    cloudCredentialVendor = mock(CloudCredentialVendor.class);
    when(cloudCredentialVendor.vendCredential(any()))
        .thenReturn(
            new io.unitycatalog.server.model.TemporaryCredentials()
                .awsTempCredentials(
                    new io.unitycatalog.server.model.AwsCredentials().accessKeyId("test-key")));
    super.setUp();
    createTestUser(ALICE);
    aliceClient = TestUtils.createApiClient(createTestUserServerConfig(ALICE));
    aliceTables = new TablesApi(aliceClient);
    adminTables = new TablesApi(adminApiClient);
    catalogId = new CatalogsApi(adminApiClient).getCatalog(TestUtils.CATALOG_NAME).getId();
    schemaId = new SchemasApi(adminApiClient).getSchema(TestUtils.SCHEMA_FULL_NAME).getSchemaId();
  }

  @Test
  public void nativeGrantsDoNotReplaceOpaPrivilegeDecisions() throws Exception {
    grantPermissions(ALICE, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    grantPermissions(
        ALICE,
        SecurableType.SCHEMA,
        TestUtils.SCHEMA_FULL_NAME,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE);
    assertThat(
            grantsApi
                .get(SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, null)
                .getPrivilegeAssignments())
        .anySatisfy(
            assignment -> {
              assertThat(assignment.getPrincipal()).isEqualTo(ALICE);
              assertThat(assignment.getPrivileges()).contains(Privilege.CREATE_TABLE);
            });

    CreateTable request =
        externalTable("separate_checks", testDirectoryRoot.resolve("external").toUri().toString());
    assertPermissionDenied(() -> aliceTables.createTable(request));
    allow(ALICE, SecurableType.CATALOG, catalogId, Privileges.USE_CATALOG);
    assertPermissionDenied(() -> aliceTables.createTable(request));
    allow(ALICE, SecurableType.SCHEMA, schemaId, Privileges.USE_SCHEMA);
    assertPermissionDenied(() -> aliceTables.createTable(request));
    allow(ALICE, SecurableType.SCHEMA, schemaId, Privileges.CREATE_TABLE);
    TableInfo table = aliceTables.createTable(request);
    String fullName = TestUtils.SCHEMA_FULL_NAME + "." + request.getName();
    assertThat(adminTables.getTable(fullName, null, null).getTableId())
        .isEqualTo(table.getTableId());

    // Creating the table records native ownership, but OPA still decides its privileges.
    assertPermissionDenied(() -> aliceTables.getTable(fullName, null, null));
    assertThat(listAllTables(aliceTables, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME)).isEmpty();
    allow(ALICE, SecurableType.TABLE, table.getTableId(), Privileges.SELECT);
    assertThat(aliceTables.getTable(fullName, null, null).getTableId())
        .isEqualTo(table.getTableId());
    assertThat(listAllTables(aliceTables, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME))
        .extracting(TableInfo::getTableId)
        .containsExactly(table.getTableId());
    policyGrants.removeIf(grant -> grant.get("privilege").equals("SELECT"));
    updateGrants();
    assertPermissionDenied(() -> aliceTables.getTable(fullName, null, null));
  }

  @Test
  public void externalLocationPermissionsDoNotBypassPathChecks() throws Exception {
    allowCreate(ALICE);
    String root = "s3://test-bucket/opa-external";
    createExternalLocationWithCredential("opa_credential", "opa_location", root);
    String locationId =
        new ExternalLocationsApi(adminApiClient).getExternalLocation("opa_location").getId();
    CreateTable request = externalTable("external_checks", root + "/table");
    assertPermissionDenied(() -> aliceTables.createTable(request));
    allow(ALICE, SecurableType.EXTERNAL_LOCATION, locationId, Privileges.CREATE_EXTERNAL_TABLE);
    TableInfo table = aliceTables.createTable(request);
    assertPermissionDenied(
        () -> aliceTables.createTable(externalTable("overlap", root + "/table/child")));

    TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(aliceClient);
    GenerateTemporaryPathCredential read =
        new GenerateTemporaryPathCredential()
            .operation(PathOperation.PATH_READ)
            .url(root + "/free");
    assertPermissionDenied(() -> credentials.generateTemporaryPathCredentials(read));
    allow(ALICE, SecurableType.EXTERNAL_LOCATION, locationId, Privileges.READ_FILES);
    assertThat(credentials.generateTemporaryPathCredentials(read).getAwsTempCredentials())
        .isNotNull();
    read.url(root + "/table/data");
    assertPermissionDenied(() -> credentials.generateTemporaryPathCredentials(read));
    allow(ALICE, SecurableType.TABLE, table.getTableId(), Privileges.SELECT);
    assertThat(credentials.generateTemporaryPathCredentials(read).getAwsTempCredentials())
        .isNotNull();

    allow(ALICE, SecurableType.CATALOG, catalogId, Privileges.CREATE_SCHEMA);
    CreateSchema schema =
        new CreateSchema()
            .catalogName(TestUtils.CATALOG_NAME)
            .name("managed_schema")
            .storageRoot(root + "/managed");
    SchemasApi schemas = new SchemasApi(aliceClient);
    assertPermissionDenied(() -> schemas.createSchema(schema));
    allow(ALICE, SecurableType.EXTERNAL_LOCATION, locationId, Privileges.CREATE_MANAGED_STORAGE);
    assertThat(schemas.createSchema(schema).getStorageRoot()).isEqualTo(schema.getStorageRoot());
  }

  @Test
  public void stagingCreatorValidationRemainsLocal() throws Exception {
    allowCreate(ALICE);
    StagingTableInfo staging =
        aliceTables.createStagingTable(
            new CreateStagingTable()
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .name("managed_table"));
    TemporaryCredentialsApi credentials = new TemporaryCredentialsApi(aliceClient);
    GenerateTemporaryTableCredential credentialRequest =
        new GenerateTemporaryTableCredential()
            .tableId(staging.getId())
            .operation(TableOperation.READ_WRITE);
    assertPermissionDenied(() -> credentials.generateTemporaryTableCredentials(credentialRequest));
    allow(ALICE, SecurableType.TABLE, staging.getId(), Privileges.OWNER);
    credentials.generateTemporaryTableCredentials(credentialRequest);

    createTestUser(BOB);
    allowCreate(BOB);
    allow(BOB, SecurableType.TABLE, staging.getId(), Privileges.OWNER);
    TablesApi bobTables = new TablesApi(TestUtils.createApiClient(createTestUserServerConfig(BOB)));
    CreateTable request =
        externalTable("managed_table", staging.getStagingLocation())
            .tableType(TableType.MANAGED)
            .properties(Map.of(TableProperties.UC_TABLE_ID, staging.getId()));
    assertPermissionDenied(
        () -> bobTables.createTable(request),
        "User attempts to create table on a staging location without ownership");
    assertThat(aliceTables.createTable(request).getTableId()).isEqualTo(staging.getId());
    assertThat(
            adminTables
                .getTable(TestUtils.SCHEMA_FULL_NAME + ".managed_table", null, null)
                .getTableId())
        .isEqualTo(staging.getId());
  }

  @Test
  public void icebergUsesOpaAndUndefinedDecisionsFailEveryApi() throws Exception {
    IcebergRestClient iceberg = new IcebergRestClient(createTestUserServerConfig(ALICE));
    CreateTableRequest request =
        CreateTableRequest.builder()
            .withName("iceberg_table")
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .build();
    assertIcebergApiException(
        () -> iceberg.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request), 403);
    allowCreate(ALICE);
    iceberg.createTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request);
    TableInfo table =
        adminTables.getTable(TestUtils.SCHEMA_FULL_NAME + ".iceberg_table", null, null);
    assertIcebergApiException(
        () ->
            iceberg.loadCredentials(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request.name()),
        403);
    allow(ALICE, SecurableType.TABLE, table.getTableId(), Privileges.SELECT);
    assertThat(
            iceberg
                .loadCredentials(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request.name())
                .credentials())
        .isEmpty(); // Local file storage has no cloud credentials.

    assertThat(opaAdmin.delete("/v1/policies/uc").aggregate().join().status().isSuccess()).isTrue();
    try {
      assertApiException(
          () ->
              aliceTables.createTable(
                  externalTable(
                      "must_not_exist", testDirectoryRoot.resolve("new").toUri().toString())),
          ErrorCode.UNAVAILABLE,
          "OPA response must contain a boolean result.allow");
      assertApiException(
          () -> listAllTables(aliceTables, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME),
          ErrorCode.UNAVAILABLE,
          "OPA response must contain a boolean result.allow");
      assertIcebergApiException(
          () -> iceberg.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, request.name()),
          503,
          "OPA response must contain a boolean result.allow");
      assertDeltaApiException(
          () -> new DeltaConfigurationApi(aliceClient).getConfig(TestUtils.CATALOG_NAME, "1.0"),
          DeltaErrorType.SERVICE_UNAVAILABLE_EXCEPTION,
          "OPA response must contain a boolean result.allow");
    } finally {
      installPolicy();
    }
    assertThat(listAllTables(adminTables, TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME))
        .extracting(TableInfo::getName)
        .containsExactly("iceberg_table");
  }

  private CreateTable externalTable(String name, String location) {
    return new CreateTable()
        .catalogName(TestUtils.CATALOG_NAME)
        .schemaName(TestUtils.SCHEMA_NAME)
        .name(name)
        .columns(TEST_COLUMNS)
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation(location);
  }

  private void allowCreate(String email) throws Exception {
    allow(email, SecurableType.CATALOG, catalogId, Privileges.USE_CATALOG);
    allow(email, SecurableType.SCHEMA, schemaId, Privileges.USE_SCHEMA, Privileges.CREATE_TABLE);
  }

  private void allow(String email, SecurableType type, String id, Privileges... privileges)
      throws Exception {
    for (Privileges privilege : privileges) {
      policyGrants.add(
          Map.of("principal", email, "type", type.name(), "id", id, "privilege", privilege.name()));
    }
    updateGrants();
  }

  private void updateGrants() throws Exception {
    assertThat(
            opaAdmin
                .prepare()
                .put("/v1/data/grants")
                .content(MediaType.JSON, JsonUtils.getInstance().writeValueAsString(policyGrants))
                .execute()
                .aggregate()
                .join()
                .status()
                .isSuccess())
        .isTrue();
  }

  private void installPolicy() throws Exception {
    assertThat(
            opaAdmin
                .prepare()
                .put("/v1/policies/uc")
                .content(
                    MediaType.PLAIN_TEXT, Files.readString(Path.of("etc/conf/opa-example.rego")))
                .execute()
                .aggregate()
                .join()
                .status()
                .isSuccess())
        .isTrue();
  }
}
