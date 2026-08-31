package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertApiException;
import static io.unitycatalog.server.utils.TestUtils.assertPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.IdentitySequencesApi;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateIdentitySequences;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.DropIdentitySequences;
import io.unitycatalog.client.model.IdentityIdRange;
import io.unitycatalog.client.model.IdentityReservation;
import io.unitycatalog.client.model.IdentitySequenceSpec;
import io.unitycatalog.client.model.ReserveIdentityRanges;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.service.delta.DeltaConsts.TableProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Access control tests for the identity sequence API. All three operations share the {@code
 * UPDATE_TABLE} expression (MODIFY on the table), so this pins that:
 *
 * <ul>
 *   <li>a SELECT-only user cannot create, reserve, or drop;
 *   <li>a MODIFY user can;
 *   <li>an unauthenticated caller is rejected with 401.
 * </ul>
 */
public class SdkIdentitySequenceAccessControlCRUDTest extends SdkAccessControlBaseCRUDTest {

  private static final String READ_USER_EMAIL = "reader@example.com";
  private static final String WRITE_USER_EMAIL = "writer@example.com";
  private static final String NO_ACCESS_USER_EMAIL = "noaccess@example.com";

  private final List<ColumnInfo> columns =
      List.of(
          new ColumnInfo()
              .name("test_col")
              .typeText("INTEGER")
              .typeJson(
                  "{\"name\":\"test_col\",\"type\":\"integer\","
                      + "\"nullable\":true,\"metadata\":{}}")
              .typeName(ColumnTypeName.INT)
              .position(0)
              .nullable(true));

  private TableInfo tableInfo;
  private String tableFullName;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    // The identity-sequence service is off by default; enable it for these tests.
    serverProperties.setProperty(
        io.unitycatalog.server.utils.ServerProperties.Property.IDENTITY_SEQUENCES_ENABLED.getKey(),
        "true");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    tableInfo = createManagedTable();
    tableFullName =
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TestUtils.TABLE_NAME;
  }

  /** Create a managed table as admin. */
  @SneakyThrows
  private TableInfo createManagedTable() {
    TablesApi adminTablesApi = new TablesApi(TestUtils.createApiClient(adminConfig));

    // When authorization is enabled, managed tables must be created from a staging table location.
    CreateStagingTable createStagingTable =
        new CreateStagingTable()
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .name(TestUtils.TABLE_NAME);
    StagingTableInfo stagingTableInfo = adminTablesApi.createStagingTable(createStagingTable);

    Map<String, String> properties = new HashMap<>(TestUtils.PROPERTIES);
    properties.put(TableProperties.UC_TABLE_ID, stagingTableInfo.getId());
    CreateTable createTable =
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .columns(columns)
            .properties(properties)
            .comment(TestUtils.COMMENT)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA);
    return adminTablesApi.createTable(createTable);
  }

  private CreateIdentitySequences createRequest(String sequenceId) {
    return new CreateIdentitySequences()
        .tableId(tableInfo.getTableId())
        .addSequencesItem(new IdentitySequenceSpec().sequenceId(sequenceId).start(1L).step(1L));
  }

  @Test
  public void testIdentitySequencePermissions() throws Exception {
    createTestUser(READ_USER_EMAIL, "Read User");
    createTestUser(WRITE_USER_EMAIL, "Write User");
    createTestUser(NO_ACCESS_USER_EMAIL, "No Access User");

    // Both read and write users can traverse the catalog/schema.
    for (String email : List.of(READ_USER_EMAIL, WRITE_USER_EMAIL)) {
      grantPermissions(
          email, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
      grantPermissions(
          email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    }
    // Read user gets SELECT (no MODIFY); write user gets MODIFY.
    grantPermissions(READ_USER_EMAIL, SecurableType.TABLE, tableFullName, Privileges.SELECT);
    grantPermissions(WRITE_USER_EMAIL, SecurableType.TABLE, tableFullName, Privileges.MODIFY);

    ServerConfig readUserConfig = createTestUserServerConfig(READ_USER_EMAIL);
    ServerConfig writeUserConfig = createTestUserServerConfig(WRITE_USER_EMAIL);
    ServerConfig noAccessUserConfig = createTestUserServerConfig(NO_ACCESS_USER_EMAIL);
    IdentitySequencesApi readUserApi =
        new IdentitySequencesApi(TestUtils.createApiClient(readUserConfig));
    IdentitySequencesApi writeUserApi =
        new IdentitySequencesApi(TestUtils.createApiClient(writeUserConfig));
    IdentitySequencesApi noAccessUserApi =
        new IdentitySequencesApi(TestUtils.createApiClient(noAccessUserConfig));
    IdentitySequencesApi unauthApi =
        new IdentitySequencesApi(TestUtils.createApiClient(serverConfig));

    String seq = UUID.randomUUID().toString();

    // SELECT-only and no-access users cannot create.
    assertPermissionDenied(() -> readUserApi.createIdentitySequences(createRequest(seq)));
    assertPermissionDenied(() -> noAccessUserApi.createIdentitySequences(createRequest(seq)));

    // Write user (MODIFY) can create and reserve.
    writeUserApi.createIdentitySequences(createRequest(seq));
    IdentityIdRange range =
        writeUserApi
            .reserveIdentityRanges(
                new ReserveIdentityRanges()
                    .tableId(tableInfo.getTableId())
                    .addReservationsItem(new IdentityReservation().sequenceId(seq).count(3L)))
            .getRanges()
            .get(0);
    assertThat(range.getRangeStart()).isEqualTo(1L);
    assertThat(range.getRangeEnd()).isEqualTo(3L);

    // Read user cannot reserve or drop (both require MODIFY).
    assertPermissionDenied(
        () ->
            readUserApi.reserveIdentityRanges(
                new ReserveIdentityRanges()
                    .tableId(tableInfo.getTableId())
                    .addReservationsItem(new IdentityReservation().sequenceId(seq).count(1L))));
    assertPermissionDenied(
        () ->
            readUserApi.dropIdentitySequences(
                new DropIdentitySequences()
                    .tableId(tableInfo.getTableId())
                    .addSequenceIdsItem(seq)));

    // Unauthenticated caller is rejected with 401.
    assertApiException(
        () -> unauthApi.createIdentitySequences(createRequest(UUID.randomUUID().toString())),
        ErrorCode.UNAUTHENTICATED,
        "authorization");

    // Write user can drop.
    assertThat(
            writeUserApi
                .dropIdentitySequences(
                    new DropIdentitySequences()
                        .tableId(tableInfo.getTableId())
                        .addSequenceIdsItem(seq))
                .getResults()
                .get(0)
                .getExisted())
        .isTrue();
  }
}
