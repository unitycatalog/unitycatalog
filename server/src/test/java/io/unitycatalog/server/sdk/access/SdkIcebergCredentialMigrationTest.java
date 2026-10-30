package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class SdkIcebergCredentialMigrationTest extends SdkAccessControlBaseCRUDTest {

  private static final String CATALOG = TestUtils.CATALOG_NAME2;
  private static final String SCHEMA = "migration_schema";
  private static final String TABLE = "migration_table";
  private static final Map<String, String> CREDENTIAL_CONFIG =
      Map.of(S3FileIOProperties.ACCESS_KEY_ID, "test-key");

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STRICT_PRIVILEGE_CHECKS_ENABLED.getKey(), "false");
  }

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    FileOperations operations = spy(fileOperations);
    doReturn(CREDENTIAL_CONFIG).when(operations).getFileIOConfig(any(), any(), any());
    return operations;
  }

  @Test
  public void manageStillRequiresDataPrivilegesDuringMigration() throws Exception {
    createTestUser(PRINCIPAL_1);
    IcebergRestClient admin = new IcebergRestClient(adminConfig);
    IcebergRestClient manager = new IcebergRestClient(createTestUserServerConfig(PRINCIPAL_1));
    admin.createTable(
        TestUtils.CATALOG_NAME,
        TestUtils.SCHEMA_NAME,
        CreateTableRequest.builder()
            .withName(TABLE)
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .build());
    for (SecurableType type :
        List.of(SecurableType.CATALOG, SecurableType.SCHEMA, SecurableType.TABLE)) {
      String name =
          switch (type) {
            case CATALOG -> TestUtils.CATALOG_NAME;
            case SCHEMA -> TestUtils.SCHEMA_FULL_NAME;
            case TABLE -> TestUtils.SCHEMA_FULL_NAME + "." + TABLE;
            default -> throw new IllegalArgumentException();
          };
      grantPermissions(PRINCIPAL_1, type, name, Privileges.MANAGE);
      if (type != SecurableType.CATALOG) {
        grantPermissions(
            PRINCIPAL_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      }
      if (type == SecurableType.TABLE) {
        grantPermissions(
            PRINCIPAL_1, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
      }
      assertThat(
              manager
                  .loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TABLE)
                  .tableMetadata())
          .isNotNull();
      assertThat(manager.loadTable(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TABLE).config())
          .isEmpty();
      assertIcebergApiException(
          () -> manager.loadCredentials(TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TABLE),
          403,
          "Access denied");
      grantsApi.update(
          type,
          name,
          new UpdatePermissions()
              .changes(
                  List.of(
                      new PermissionsChange()
                          .principal(PRINCIPAL_1)
                          .remove(List.of(Privilege.MANAGE)))));
    }
    assertApiException(
        () ->
            grantPermissions(
                PRINCIPAL_1, SecurableType.METASTORE, METASTORE_NAME, Privileges.MANAGE),
        ErrorCode.INVALID_ARGUMENT,
        "Privilege MANAGE cannot be granted on metastore");
  }

  @Test
  public void disabledStrictChecksRestorePreviousIcebergCredentials() throws Exception {
    createCommonTestUsers();
    createTestUser("no-table-access@localhost");
    setupCommonCatalogAndSchema(CATALOG, TestUtils.SCHEMA_NAME2);
    IcebergRestClient admin = new IcebergRestClient(adminConfig);
    IcebergRestClient catalogOwner = new IcebergRestClient(createTestUserServerConfig(PRINCIPAL_1));
    IcebergRestClient schemaOwner = new IcebergRestClient(createTestUserServerConfig(PRINCIPAL_2));
    IcebergRestClient tableOwner = new IcebergRestClient(createTestUserServerConfig(REGULAR_1));
    IcebergRestClient modifier = new IcebergRestClient(createTestUserServerConfig(REGULAR_2));
    IcebergRestClient noTableAccess =
        new IcebergRestClient(createTestUserServerConfig("no-table-access@localhost"));

    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        CATALOG,
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    schemaOwner.createNamespace(CATALOG, SCHEMA);
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        CATALOG + "." + SCHEMA,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE);
    tableOwner.createTable(
        CATALOG,
        SCHEMA,
        CreateTableRequest.builder()
            .withName(TABLE)
            .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
            .build());
    String fullName = CATALOG + "." + SCHEMA + "." + TABLE;
    grantPermissions(REGULAR_2, SecurableType.TABLE, fullName, Privileges.MODIFY);
    assertIcebergApiException(
        () -> modifier.loadTable(CATALOG, SCHEMA, TABLE), 403, "Access denied");
    assertIcebergApiException(
        () -> modifier.loadCredentials(CATALOG, SCHEMA, TABLE), 403, "Access denied");
    for (String user : List.of(REGULAR_2, "no-table-access@localhost")) {
      grantPermissions(user, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
      grantPermissions(user, SecurableType.SCHEMA, CATALOG + "." + SCHEMA, Privileges.USE_SCHEMA);
    }

    // Adding MANAGE must preserve legacy owner and MODIFY-only credential access.
    for (Map.Entry<String, IcebergRestClient> entry :
        Map.of(
                "admin",
                admin,
                PRINCIPAL_1,
                catalogOwner,
                PRINCIPAL_2,
                schemaOwner,
                REGULAR_1,
                tableOwner,
                REGULAR_2,
                modifier)
            .entrySet()) {
      for (boolean addManage : List.of(false, true)) {
        if (addManage) {
          grantPermissions(entry.getKey(), SecurableType.TABLE, fullName, Privileges.MANAGE);
        }
        assertThat(entry.getValue().loadTable(CATALOG, SCHEMA, TABLE).config())
            .isEqualTo(CREDENTIAL_CONFIG);
        assertThat(entry.getValue().loadCredentials(CATALOG, SCHEMA, TABLE).credentials())
            .singleElement()
            .satisfies(credential -> assertThat(credential.config()).isEqualTo(CREDENTIAL_CONFIG));
      }
    }
    assertIcebergApiException(
        () -> noTableAccess.loadTable(CATALOG, SCHEMA, TABLE), 403, "Access denied");
    assertIcebergApiException(
        () -> noTableAccess.loadCredentials(CATALOG, SCHEMA, TABLE), 403, "Access denied");

    String tableId = getTable(new TablesApi(adminApiClient), fullName).getTableId();
    TemporaryCredentialsApi nativeCredentials =
        new TemporaryCredentialsApi(
            TestUtils.createApiClient(createTestUserServerConfig(REGULAR_2)));
    for (TableOperation operation : List.of(TableOperation.READ, TableOperation.READ_WRITE)) {
      assertPermissionDenied(
          () ->
              nativeCredentials.generateTemporaryTableCredentials(
                  new GenerateTemporaryTableCredential().tableId(tableId).operation(operation)));
    }
    UpdateTableRequest update =
        new UpdateTableRequest(
            List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("migration-test", "true"))));
    assertIcebergApiException(
        () -> modifier.updateTable(CATALOG, SCHEMA, TABLE, update), 403, "Access denied");
    tableOwner.updateTable(CATALOG, SCHEMA, TABLE, update);
  }
}
