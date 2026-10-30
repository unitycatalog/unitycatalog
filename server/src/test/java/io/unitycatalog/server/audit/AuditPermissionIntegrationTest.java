package io.unitycatalog.server.audit;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.PermissionsChange;
import io.unitycatalog.client.model.Privilege;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.client.model.UpdatePermissions;
import io.unitycatalog.control.model.UserResource;
import io.unitycatalog.server.audit.AuditRecord.Purpose;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** Audit records for permission changes and drops, with authorization on. */
class AuditPermissionIntegrationTest extends SdkAccessControlBaseCRUDTest {
  private static final String TABLE = "audited";
  private static final List<ColumnInfo> COLUMNS =
      List.of(
          new ColumnInfo()
              .name("id")
              .typeText("INTEGER")
              .typeJson("{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    CapturingAuditSink.reset();
  }

  private TableInfo createManagedTable() throws Exception {
    TablesApi tables = new TablesApi(adminApiClient);
    StagingTableInfo staging =
        tables.createStagingTable(
            new CreateStagingTable()
                .catalogName(TestUtils.CATALOG_NAME)
                .schemaName(TestUtils.SCHEMA_NAME)
                .name(TABLE));
    return tables.createTable(
        new CreateTable()
            .name(TABLE)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(COLUMNS)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation(staging.getStagingLocation())
            .properties(Map.of(TableProperties.UC_TABLE_ID, staging.getId())));
  }

  @Test
  void recordsGrantsRevokeDropAndUserDelete() throws Exception {
    String admin = JWT.decode(adminConfig.getAuthToken()).getSubject();
    UserResource user = createTestUser(REGULAR_1);
    UUID userId = UUID.fromString(user.getId());
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    // The second grant changes nothing, so it records nothing.
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    // The second revoke finds nothing to remove, and is recorded all the same.
    for (int i = 0; i < 2; i++) {
      grantsApi.update(
          SecurableType.CATALOG,
          TestUtils.CATALOG_NAME,
          new UpdatePermissions()
              .changes(
                  List.of(
                      new PermissionsChange()
                          .principal(REGULAR_1)
                          .add(List.of())
                          .remove(List.of(Privilege.USE_CATALOG)))));
    }
    // A grant left for the user delete to clear.
    grantPermissions(
        REGULAR_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.CREATE_SCHEMA);
    TableInfo table = createManagedTable();
    UUID tableId = UUID.fromString(table.getTableId());
    new TemporaryCredentialsApi(adminApiClient)
        .generateTemporaryTableCredentials(
            new GenerateTemporaryTableCredential()
                .tableId(table.getTableId())
                .operation(TableOperation.READ));
    new TablesApi(adminApiClient)
        .deleteTable(TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TABLE);
    usersApi.deleteUser(user.getId());

    List<AuditRecord> records = List.copyOf(CapturingAuditSink.RECORDS);
    assertThat(records)
        .filteredOn(
            r ->
                r.getAction() == AuditRecord.Action.GRANT && "USE CATALOG".equals(r.getPrivilege()))
        .singleElement()
        .satisfies(
            r -> {
              assertThat(r.getUser()).isEqualTo(admin);
              assertThat(r.getPrincipalId()).isEqualTo(userId);
              assertThat(FileAuditSink.toJson(r))
                  .contains("\"principal_id\":\"" + userId + "\"", "\"privilege\":\"USE CATALOG\"");
            });
    assertThat(records)
        .filteredOn(r -> r.getAction() == AuditRecord.Action.REVOKE)
        .hasSize(2)
        .allSatisfy(
            r -> {
              assertThat(r.getPrivilege()).isEqualTo("USE CATALOG");
              assertThat(r.getResourceId()).isNotNull();
              assertThat(r.getPrincipalId()).isEqualTo(userId);
              assertThat(r.getUser()).isEqualTo(admin);
            });
    assertThat(records)
        .filteredOn(r -> r.getAction() == AuditRecord.Action.CLEAR_PRINCIPAL_GRANTS)
        .singleElement()
        .satisfies(
            r -> {
              assertThat(r.getPrincipalId()).isEqualTo(userId);
              assertThat(r.getResourceId()).isNull();
              assertThat(r.getUser()).isEqualTo(admin);
            });
    // The admin's OWNER grant made when the server first starts.
    assertThat(records)
        .anySatisfy(
            r -> {
              assertThat(r.getAction()).isEqualTo(AuditRecord.Action.GRANT);
              assertThat(r.getPrivilege()).isEqualTo("OWNER");
              assertThat(r.getUser()).isEqualTo(Auditor.SERVER_USER);
            });
    assertThat(records)
        .anySatisfy(
            r -> {
              assertThat(r.getAction()).isEqualTo(AuditRecord.Action.GRANT);
              assertThat(r.getPrivilege()).isEqualTo("OWNER");
              assertThat(r.getResourceId()).isEqualTo(tableId);
              assertThat(r.getUser()).isEqualTo(admin);
            });
    assertThat(records)
        .filteredOn(r -> r.getAction() == AuditRecord.Action.ISSUE_CREDENTIAL)
        .singleElement()
        .satisfies(
            r -> {
              assertThat(r.getPurpose()).isEqualTo(Purpose.CLIENT);
              assertThat(r.getAccess()).isEqualTo(io.unitycatalog.server.model.TableOperation.READ);
              assertThat(r.getUser()).isEqualTo(admin);
            });
    // The drop clears the table's grants.
    List<AuditRecord> drop =
        records.stream()
            .filter(
                r -> tableId.equals(r.getResourceId()) && r.getAction() != AuditRecord.Action.GRANT)
            .toList();
    assertThat(drop)
        .extracting(AuditRecord::getAction)
        .containsExactly(AuditRecord.Action.CLEAR_RESOURCE_GRANTS);
    assertThat(drop).allSatisfy(r -> assertThat(r.getUser()).isEqualTo(admin));
  }
}
