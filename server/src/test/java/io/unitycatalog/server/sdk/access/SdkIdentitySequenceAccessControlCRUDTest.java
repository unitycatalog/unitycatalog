package io.unitycatalog.server.sdk.access;

import static io.unitycatalog.server.utils.TestUtils.assertDeltaApiException;
import static io.unitycatalog.server.utils.TestUtils.assertDeltaPermissionDenied;
import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.delta.api.DeltaIdentitySequencesApi;
import io.unitycatalog.client.delta.model.DeltaCreateIdentitySequences;
import io.unitycatalog.client.delta.model.DeltaDropIdentitySequences;
import io.unitycatalog.client.delta.model.DeltaErrorType;
import io.unitycatalog.client.delta.model.DeltaIdentityIdRange;
import io.unitycatalog.client.delta.model.DeltaIdentityReservation;
import io.unitycatalog.client.delta.model.DeltaIdentitySequenceSpec;
import io.unitycatalog.client.delta.model.DeltaReserveIdentityRanges;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateStagingTable;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.client.model.StagingTableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
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
 * Access control tests for the identity sequence endpoints of the UC Delta API. All three
 * operations share the {@code UPDATE_TABLE} expression (MODIFY on the table), so this pins that:
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

  private static final String CATALOG = TestUtils.CATALOG_NAME;
  private static final String SCHEMA = TestUtils.SCHEMA_NAME;
  private static final String TABLE = TestUtils.TABLE_NAME;

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
    createManagedTable();
  }

  /** Create a managed table as admin. */
  @SneakyThrows
  private void createManagedTable() {
    TablesApi adminTablesApi = new TablesApi(TestUtils.createApiClient(adminConfig));

    // When authorization is enabled, managed tables must be created from a staging table location.
    CreateStagingTable createStagingTable =
        new CreateStagingTable().catalogName(CATALOG).schemaName(SCHEMA).name(TABLE);
    StagingTableInfo stagingTableInfo = adminTablesApi.createStagingTable(createStagingTable);

    Map<String, String> properties = new HashMap<>(TestUtils.PROPERTIES);
    properties.put(TableProperties.UC_TABLE_ID, stagingTableInfo.getId());
    CreateTable createTable =
        new CreateTable()
            .name(TABLE)
            .catalogName(CATALOG)
            .schemaName(SCHEMA)
            .columns(columns)
            .properties(properties)
            .comment(TestUtils.COMMENT)
            .storageLocation(stagingTableInfo.getStagingLocation())
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA);
    adminTablesApi.createTable(createTable);
  }

  private DeltaCreateIdentitySequences createBody(String sequenceId) {
    return new DeltaCreateIdentitySequences()
        .addSequencesItem(
            new DeltaIdentitySequenceSpec().sequenceId(sequenceId).start(1L).step(1L));
  }

  @Test
  public void testIdentitySequencePermissions() throws Exception {
    createTestUser(READ_USER_EMAIL, "Read User");
    createTestUser(WRITE_USER_EMAIL, "Write User");
    createTestUser(NO_ACCESS_USER_EMAIL, "No Access User");

    // Both read and write users can traverse the catalog/schema.
    for (String email : List.of(READ_USER_EMAIL, WRITE_USER_EMAIL)) {
      grantPermissions(email, SecurableType.CATALOG, CATALOG, Privileges.USE_CATALOG);
      grantPermissions(
          email, SecurableType.SCHEMA, TestUtils.SCHEMA_FULL_NAME, Privileges.USE_SCHEMA);
    }
    // Read user gets SELECT (no MODIFY); write user gets MODIFY.
    grantPermissions(
        READ_USER_EMAIL, SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privileges.SELECT);
    grantPermissions(
        WRITE_USER_EMAIL, SecurableType.TABLE, TestUtils.TABLE_FULL_NAME, Privileges.MODIFY);

    ServerConfig readUserConfig = createTestUserServerConfig(READ_USER_EMAIL);
    ServerConfig writeUserConfig = createTestUserServerConfig(WRITE_USER_EMAIL);
    ServerConfig noAccessUserConfig = createTestUserServerConfig(NO_ACCESS_USER_EMAIL);
    DeltaIdentitySequencesApi readUserApi =
        new DeltaIdentitySequencesApi(TestUtils.createApiClient(readUserConfig));
    DeltaIdentitySequencesApi writeUserApi =
        new DeltaIdentitySequencesApi(TestUtils.createApiClient(writeUserConfig));
    DeltaIdentitySequencesApi noAccessUserApi =
        new DeltaIdentitySequencesApi(TestUtils.createApiClient(noAccessUserConfig));
    DeltaIdentitySequencesApi unauthApi =
        new DeltaIdentitySequencesApi(TestUtils.createApiClient(serverConfig));

    String seq = UUID.randomUUID().toString();

    // SELECT-only and no-access users cannot create.
    assertDeltaPermissionDenied(
        () -> readUserApi.createIdentitySequences(CATALOG, SCHEMA, TABLE, createBody(seq)));
    assertDeltaPermissionDenied(
        () -> noAccessUserApi.createIdentitySequences(CATALOG, SCHEMA, TABLE, createBody(seq)));

    // Write user (MODIFY) can create and reserve.
    writeUserApi.createIdentitySequences(CATALOG, SCHEMA, TABLE, createBody(seq));
    DeltaIdentityIdRange range =
        writeUserApi
            .reserveIdentityRanges(
                CATALOG,
                SCHEMA,
                TABLE,
                new DeltaReserveIdentityRanges()
                    .addReservationsItem(new DeltaIdentityReservation().sequenceId(seq).count(3L)))
            .getRanges()
            .get(0);
    assertThat(range.getRangeStart()).isEqualTo(1L);
    assertThat(range.getRangeEnd()).isEqualTo(3L);

    // Read user cannot reserve or drop (both require MODIFY).
    assertDeltaPermissionDenied(
        () ->
            readUserApi.reserveIdentityRanges(
                CATALOG,
                SCHEMA,
                TABLE,
                new DeltaReserveIdentityRanges()
                    .addReservationsItem(
                        new DeltaIdentityReservation().sequenceId(seq).count(1L))));
    assertDeltaPermissionDenied(
        () ->
            readUserApi.dropIdentitySequences(
                CATALOG, SCHEMA, TABLE, new DeltaDropIdentitySequences().addSequenceIdsItem(seq)));

    // Unauthenticated caller is rejected with 401.
    assertDeltaApiException(
        () ->
            unauthApi.createIdentitySequences(
                CATALOG, SCHEMA, TABLE, createBody(UUID.randomUUID().toString())),
        DeltaErrorType.NOT_AUTHORIZED_EXCEPTION,
        "authorization");

    // Write user can drop.
    assertThat(
            writeUserApi
                .dropIdentitySequences(
                    CATALOG,
                    SCHEMA,
                    TABLE,
                    new DeltaDropIdentitySequences().addSequenceIdsItem(seq))
                .getResults()
                .get(0)
                .getExisted())
        .isTrue();
  }
}
