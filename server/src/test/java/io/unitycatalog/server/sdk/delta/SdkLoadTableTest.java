package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.DeltaCommitsApi;
import io.unitycatalog.client.delta.api.TablesApi;
import io.unitycatalog.client.delta.model.LoadTableResponse;
import io.unitycatalog.client.delta.model.PrimitiveType;
import io.unitycatalog.client.delta.model.StructField;
import io.unitycatalog.client.delta.model.TableMetadata;
import io.unitycatalog.client.delta.model.UniformMetadataIceberg;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.client.model.CreateSchema;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.DeltaCommit;
import io.unitycatalog.client.model.DeltaCommitInfo;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkLoadTableTest extends BaseServerTest {

  private CatalogOperations catalogOps;
  private SchemaOperations schemaOps;
  private TableOperations tableOps;
  private DeltaCommitsApi commitsApi;
  private TablesApi deltaTablesApi;

  @BeforeEach
  public void setUp() {
    super.setUp();
    var apiClient = TestUtils.createApiClient(serverConfig);
    catalogOps = new SdkCatalogOperations(apiClient);
    schemaOps = new SdkSchemaOperations(apiClient);
    tableOps = new SdkTableOperations(apiClient);
    commitsApi = new DeltaCommitsApi(apiClient);
    deltaTablesApi = new TablesApi(apiClient);
    cleanUp();
    createCatalogAndSchema();
  }

  private void cleanUp() {
    try {
      catalogOps.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
  }

  private void createCatalogAndSchema() {
    try {
      catalogOps.createCatalog(new CreateCatalog().name(TestUtils.CATALOG_NAME).comment("test"));
      schemaOps.createSchema(
          new CreateSchema().name(TestUtils.SCHEMA_NAME).catalogName(TestUtils.CATALOG_NAME));
    } catch (ApiException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLoadTable() throws Exception {
    // --- External table: verify metadata and columns ---
    tableOps.createTable(
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .storageLocation("file:///tmp/uc_test/delta_table")
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.LONG)
                        .typeText("bigint")
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"long\","
                                + "\"nullable\":false,\"metadata\":{}}")
                        .position(0)
                        .nullable(false),
                    new ColumnInfo()
                        .name("name")
                        .typeName(ColumnTypeName.STRING)
                        .typeText("string")
                        .typeJson(
                            "{\"name\":\"name\",\"type\":\"string\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .position(1)
                        .nullable(true))));

    LoadTableResponse response =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    TableMetadata metadata = response.getMetadata();

    assertThat(metadata.getTableUuid()).isNotNull();
    assertThat(metadata.getDataSourceFormat().getValue()).isEqualTo("DELTA");
    assertThat(metadata.getTableType().getValue()).isEqualTo("EXTERNAL");
    assertThat(metadata.getLocation()).isNotNull();
    assertThat(metadata.getCreatedTime()).isNotNull();
    assertThat(metadata.getUpdatedTime()).isNotNull();
    assertThat(metadata.getEtag()).isNotNull();
    assertThat(metadata.getProperties()).isNotNull();

    List<StructField> fields = metadata.getColumns().getFields();
    assertThat(fields).hasSize(2);
    assertThat(fields.get(0).getName()).isEqualTo("id");
    assertThat(fields.get(0).getType()).isInstanceOf(PrimitiveType.class);
    assertThat(fields.get(0).getType().getType()).isEqualTo("long");
    assertThat(fields.get(0).getNullable()).isFalse();
    assertThat(fields.get(1).getName()).isEqualTo("name");
    assertThat(fields.get(1).getType()).isInstanceOf(PrimitiveType.class);
    assertThat(fields.get(1).getType().getType()).isEqualTo("string");
    assertThat(fields.get(1).getNullable()).isTrue();

    // External table: no commits
    assertThat(response.getCommits()).isNullOrEmpty();
    assertThat(response.getLatestTableVersion()).isNull();

    // --- Managed table: commit flow ---
    tableOps.deleteTable(
        TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TestUtils.TABLE_NAME);
    TableInfo tableInfo =
        BaseTableCRUDTestEnv.createTestingTable(
            TestUtils.TABLE_NAME, TableType.MANAGED, Optional.empty(), tableOps);
    String tableId = tableInfo.getTableId();
    String tableUri = tableInfo.getStorageLocation();

    // Load before any commits: version 0, empty list
    LoadTableResponse r1 =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(r1.getCommits()).isEmpty();
    assertThat(r1.getLatestTableVersion()).isEqualTo(0L);

    // Commit v1, load: 1 commit
    commitsApi.commit(
        new DeltaCommit()
            .tableId(tableId)
            .tableUri(tableUri)
            .commitInfo(
                new DeltaCommitInfo()
                    .version(1L)
                    .fileName("00000001.json")
                    .fileSize(1024L)
                    .timestamp(1700000001L)
                    .fileModificationTimestamp(1700000001L)));
    LoadTableResponse r2 =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(r2.getCommits()).hasSize(1);
    assertThat(r2.getCommits().get(0).getVersion()).isEqualTo(1);
    assertThat(r2.getLatestTableVersion()).isEqualTo(1L);

    // Commit v2, load: 2 commits in descending order
    commitsApi.commit(
        new DeltaCommit()
            .tableId(tableId)
            .tableUri(tableUri)
            .commitInfo(
                new DeltaCommitInfo()
                    .version(2L)
                    .fileName("00000002.json")
                    .fileSize(2048L)
                    .timestamp(1700000002L)
                    .fileModificationTimestamp(1700000002L)));
    LoadTableResponse r3 =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(r3.getCommits()).hasSize(2);
    assertThat(r3.getCommits().get(0).getVersion()).isEqualTo(2);
    assertThat(r3.getCommits().get(1).getVersion()).isEqualTo(1);
    assertThat(r3.getLatestTableVersion()).isEqualTo(2L);

    // Backfill v1, load: v1 removed, only v2 remains
    commitsApi.commit(
        new DeltaCommit().tableId(tableId).tableUri(tableUri).latestBackfilledVersion(1L));
    LoadTableResponse r4 =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(r4.getCommits()).hasSize(1);
    assertThat(r4.getCommits().get(0).getVersion()).isEqualTo(2);
    assertThat(r4.getLatestTableVersion()).isEqualTo(2L);

    // Backfill v2, load: all backfilled, empty commits
    commitsApi.commit(
        new DeltaCommit().tableId(tableId).tableUri(tableUri).latestBackfilledVersion(2L));
    LoadTableResponse r5 =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(r5.getCommits()).isEmpty();
    assertThat(r5.getLatestTableVersion()).isEqualTo(2L);
  }

  @Test
  public void testLoadTableNotFound() {
    ApiException ex =
        assertThrows(
            ApiException.class,
            () ->
                deltaTablesApi.loadTable(
                    TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, "nonexistent"));
    assertThat(ex.getMessage()).contains("Table not found");
    assertThat(ex.getCode()).isEqualTo(404);
  }

  /**
   * Exercises the partition-column, property-derived version/timestamp, and uniform Iceberg
   * metadata paths of loadTable on a MANAGED Delta table (the primary target of loadTable).
   */
  @Test
  public void testLoadTableFullMetadata() throws Exception {
    // Build a MANAGED Delta table with 2 partition columns and table properties including
    // delta.lastUpdateVersion / delta.lastCommitTimestamp. storageLocation is filled in by
    // SdkTableOperations from the staging flow.
    tableOps.createTable(
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .properties(
                Map.of(
                    "delta.lastUpdateVersion", "42",
                    "delta.lastCommitTimestamp", "1700000000000",
                    "user.custom", "value"))
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.LONG)
                        .typeText("bigint")
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"long\","
                                + "\"nullable\":false,\"metadata\":{}}")
                        .position(0)
                        .partitionIndex(0)
                        .nullable(false),
                    new ColumnInfo()
                        .name("region")
                        .typeName(ColumnTypeName.STRING)
                        .typeText("string")
                        .typeJson(
                            "{\"name\":\"region\",\"type\":\"string\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .position(1)
                        .partitionIndex(1)
                        .nullable(true),
                    new ColumnInfo()
                        .name("amount")
                        .typeName(ColumnTypeName.DECIMAL)
                        .typeText("decimal(10,2)")
                        .typeJson(
                            "{\"name\":\"amount\",\"type\":\"decimal(10,2)\","
                                + "\"nullable\":true,\"metadata\":{}}")
                        .position(2)
                        .nullable(true))));

    // Uniform Iceberg metadata is not exposed through CreateTable, so set it directly on the DAO.
    TableInfo tableInfo =
        tableOps.getTable(
            TestUtils.CATALOG_NAME + "." + TestUtils.SCHEMA_NAME + "." + TestUtils.TABLE_NAME);
    String icebergLocation = "file:///tmp/uc_test/iceberg/v5.metadata.json";
    long icebergVersion = 5L;
    long icebergTimestampMs = 1700000100000L;
    updateUniformMetadata(
        UUID.fromString(tableInfo.getTableId()),
        icebergLocation,
        icebergVersion,
        new Date(icebergTimestampMs));

    LoadTableResponse response =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    TableMetadata metadata = response.getMetadata();

    // Partition columns come back in partitionIndex order and match the two columns set above.
    assertThat(metadata.getPartitionColumns()).containsExactly("id", "region");

    // Property-derived fields resolved from delta.lastUpdateVersion / delta.lastCommitTimestamp.
    assertThat(metadata.getLastCommitVersion()).isEqualTo(42L);
    assertThat(metadata.getLastCommitTimestampMs()).isEqualTo(1700000000000L);
    assertThat(metadata.getProperties()).containsEntry("user.custom", "value");

    // Uniform Iceberg metadata is populated from the DAO fields.
    assertThat(response.getUniform()).isNotNull();
    UniformMetadataIceberg iceberg = response.getUniform().getIceberg();
    assertThat(iceberg).isNotNull();
    assertThat(iceberg.getMetadataLocation()).isEqualTo(icebergLocation);
    assertThat(iceberg.getConvertedDeltaVersion()).isEqualTo(icebergVersion);
    assertThat(iceberg.getConvertedDeltaTimestamp()).isEqualTo(icebergTimestampMs);
  }

  /**
   * Verifies that a malformed long value in a property (e.g., delta.lastUpdateVersion="abc") is
   * handled gracefully: loadTable still succeeds, and the affected field is absent from the
   * response (the catch branch in parseLongProperty logs a warning and returns empty).
   */
  @Test
  public void testLoadTableMalformedLongProperty() throws Exception {
    tableOps.createTable(
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .properties(Map.of("delta.lastUpdateVersion", "not-a-number"))
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.LONG)
                        .typeText("bigint")
                        .typeJson(
                            "{\"name\":\"id\",\"type\":\"long\","
                                + "\"nullable\":false,\"metadata\":{}}")
                        .position(0)
                        .nullable(false))));

    LoadTableResponse response =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    // Response still succeeds, but lastCommitVersion is absent because the property was malformed.
    assertThat(response.getMetadata()).isNotNull();
    assertThat(response.getMetadata().getLastCommitVersion()).isNull();
  }

  /**
   * Verifies that a corrupt typeJson on a stored column is handled gracefully: loadTable still
   * succeeds, returning an empty schema rather than throwing (the catch branch in
   * buildTableMetadata logs a warning and substitutes an empty StructType).
   */
  @Test
  public void testLoadTableCorruptColumnTypeJson() throws Exception {
    tableOps.createTable(
        new CreateTable()
            .name(TestUtils.TABLE_NAME)
            .catalogName(TestUtils.CATALOG_NAME)
            .schemaName(TestUtils.SCHEMA_NAME)
            .tableType(TableType.MANAGED)
            .dataSourceFormat(DataSourceFormat.DELTA)
            .columns(
                List.of(
                    new ColumnInfo()
                        .name("id")
                        .typeName(ColumnTypeName.LONG)
                        .typeText("bigint")
                        // Malformed typeJson -- not valid JSON. loadTable should swallow the parse
                        // error and return an empty schema rather than 5xx'ing.
                        .typeJson("not json at all")
                        .position(0)
                        .nullable(false))));

    LoadTableResponse response =
        deltaTablesApi.loadTable(
            TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, TestUtils.TABLE_NAME);
    assertThat(response.getMetadata()).isNotNull();
    assertThat(response.getMetadata().getColumns().getFields()).isEmpty();
  }

  private void updateUniformMetadata(
      UUID tableId, String metadataLocation, long convertedVersion, Date convertedTimestamp) {
    var sessionFactory = hibernateConfigurator.getSessionFactory();
    try (Session session = sessionFactory.openSession()) {
      Transaction tx = session.beginTransaction();
      TableInfoDAO dao = session.get(TableInfoDAO.class, tableId);
      dao.setUniformIcebergMetadataLocation(metadataLocation);
      dao.setUniformIcebergConvertedDeltaVersion(convertedVersion);
      dao.setUniformIcebergConvertedDeltaTimestamp(convertedTimestamp);
      tx.commit();
    }
  }
}
