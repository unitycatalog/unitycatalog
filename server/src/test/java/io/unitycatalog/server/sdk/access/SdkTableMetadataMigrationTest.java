package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertDeltaPermissionDenied;
import static io.unitycatalog.server.utils.TestUtils.assertIcebergApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.SchemasApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.IcebergRestClient;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.Schema;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class SdkTableMetadataMigrationTest extends SdkAccessControlBaseCRUDTest {

  private static final String CATALOG = "cat_pr1";
  private static final String SCHEMA = "migration_schema";
  private static final String DELTA_TABLE = "delta_table";
  private static final String ICEBERG_TABLE = "iceberg_table";

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.STRICT_PRIVILEGE_CHECKS_ENABLED.getKey(), "false");
  }

  @Test
  public void disabledStrictChecksRestorePreviousTableMetadataAccess() throws Exception {
    createCommonTestUsers();
    createTestUser("no-table-access@localhost");
    setupCommonCatalogAndSchema();
    ServerConfig catalogOwner = createTestUserServerConfig(PRINCIPAL_1);
    ServerConfig schemaOwner = createTestUserServerConfig(PRINCIPAL_2);
    ServerConfig tableOwner = createTestUserServerConfig(REGULAR_1);
    ServerConfig modifier = createTestUserServerConfig(REGULAR_2);
    ServerConfig noTableAccess = createTestUserServerConfig("no-table-access@localhost");
    grantPermissions(
        PRINCIPAL_2,
        SecurableType.CATALOG,
        CATALOG,
        Privileges.USE_CATALOG,
        Privileges.CREATE_SCHEMA);
    new SchemasApi(TestUtils.createApiClient(schemaOwner))
        .createSchema(new CreateSchema().catalogName(CATALOG).name(SCHEMA));
    grantPermissions(REGULAR_1, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
    grantPermissions(
        REGULAR_1,
        SecurableType.SCHEMA,
        CATALOG + "." + SCHEMA,
        Privileges.USE_SCHEMA,
        Privileges.CREATE_TABLE);
    String deltaId =
        new TablesApi(TestUtils.createApiClient(tableOwner))
            .createTable(
                new CreateTable()
                    .catalogName(CATALOG)
                    .schemaName(SCHEMA)
                    .name(DELTA_TABLE)
                    .columns(TEST_COLUMNS)
                    .dataSourceFormat(DataSourceFormat.DELTA)
                    .tableType(TableType.EXTERNAL)
                    .storageLocation(testDirectoryRoot.resolve("delta_table").toString()))
            .getTableId();
    new IcebergRestClient(tableOwner)
        .createTable(
            CATALOG,
            SCHEMA,
            CreateTableRequest.builder()
                .withName(ICEBERG_TABLE)
                .withSchema(new Schema(Types.NestedField.required(1, "id", Types.LongType.get())))
                .build());
    for (String table : List.of(DELTA_TABLE, ICEBERG_TABLE)) {
      grantPermissions(
          REGULAR_2, SecurableType.TABLE, CATALOG + "." + SCHEMA + "." + table, Privileges.MODIFY);
    }
    TablesApi modifierTables = new TablesApi(TestUtils.createApiClient(modifier));
    assertPermissionDenied(
        () -> getTable(modifierTables, CATALOG + "." + SCHEMA + "." + DELTA_TABLE));
    assertThat(listAllTables(modifierTables, CATALOG, SCHEMA)).isEmpty();
    for (String user : List.of(REGULAR_2, "no-table-access@localhost")) {
      grantPermissions(user, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
      grantPermissions(user, SecurableType.SCHEMA, CATALOG + "." + SCHEMA, Privileges.USE_SCHEMA);
    }

    // The migration keeps every ownership rule and metadata access through MODIFY.
    for (ServerConfig config :
        List.of(adminConfig, catalogOwner, schemaOwner, tableOwner, modifier)) {
      TablesApi tables = new TablesApi(TestUtils.createApiClient(config));
      assertThat(getTable(tables, CATALOG + "." + SCHEMA + "." + DELTA_TABLE).getTableId())
          .isEqualTo(deltaId);
      assertThat(listAllTables(tables, CATALOG, SCHEMA))
          .extracting(io.unitycatalog.client.model.TableInfo::getName)
          .containsExactlyInAnyOrder(DELTA_TABLE, ICEBERG_TABLE);
      DeltaTablesApi delta = new DeltaTablesApi(TestUtils.createApiClient(config));
      assertThat(delta.loadTable(CATALOG, SCHEMA, DELTA_TABLE)).isNotNull();
      assertThat(delta.tableExistsWithHttpInfo(CATALOG, SCHEMA, DELTA_TABLE).getStatusCode())
          .isEqualTo(204);
      IcebergRestClient iceberg = new IcebergRestClient(config);
      assertThat(iceberg.loadTable(CATALOG, SCHEMA, ICEBERG_TABLE).tableMetadata()).isNotNull();
      iceberg.loadCredentials(CATALOG, SCHEMA, ICEBERG_TABLE);
      assertThat(iceberg.tableExists(CATALOG, SCHEMA, ICEBERG_TABLE)).isTrue();
    }

    TablesApi deniedTables = new TablesApi(TestUtils.createApiClient(noTableAccess));
    assertPermissionDenied(
        () -> getTable(deniedTables, CATALOG + "." + SCHEMA + "." + DELTA_TABLE));
    assertThat(listAllTables(deniedTables, CATALOG, SCHEMA)).isEmpty();
    assertDeltaPermissionDenied(
        () ->
            new DeltaTablesApi(TestUtils.createApiClient(noTableAccess))
                .loadTable(CATALOG, SCHEMA, DELTA_TABLE));
    IcebergRestClient deniedIceberg = new IcebergRestClient(noTableAccess);
    assertIcebergApiException(
        () -> deniedIceberg.loadTable(CATALOG, SCHEMA, ICEBERG_TABLE), 403, "Access denied");
    assertIcebergApiException(
        () -> deniedIceberg.loadCredentials(CATALOG, SCHEMA, ICEBERG_TABLE), 403, "Access denied");

    TemporaryCredentialsApi nativeCredentials =
        new TemporaryCredentialsApi(TestUtils.createApiClient(modifier));
    for (TableOperation operation : List.of(TableOperation.READ, TableOperation.READ_WRITE)) {
      assertPermissionDenied(
          () ->
              nativeCredentials.generateTemporaryTableCredentials(
                  new GenerateTemporaryTableCredential().tableId(deltaId).operation(operation)));
    }
    DeltaTemporaryCredentialsApi deltaCredentials =
        new DeltaTemporaryCredentialsApi(TestUtils.createApiClient(modifier));
    for (DeltaCredentialOperation operation :
        List.of(DeltaCredentialOperation.READ, DeltaCredentialOperation.READ_WRITE)) {
      assertDeltaPermissionDenied(
          () -> deltaCredentials.getTableCredentials(operation, CATALOG, SCHEMA, DELTA_TABLE));
    }
    UpdateTableRequest update =
        new UpdateTableRequest(
            List.of(), List.of(new MetadataUpdate.SetProperties(Map.of("migration-test", "true"))));
    assertIcebergApiException(
        () -> new IcebergRestClient(modifier).updateTable(CATALOG, SCHEMA, ICEBERG_TABLE, update),
        403,
        "Access denied");
    new IcebergRestClient(tableOwner).updateTable(CATALOG, SCHEMA, ICEBERG_TABLE, update);
  }
}
