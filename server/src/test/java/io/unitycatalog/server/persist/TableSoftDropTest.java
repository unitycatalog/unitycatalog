package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TemporaryCredentialsApi;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.Dependency;
import io.unitycatalog.client.model.DependencyList;
import io.unitycatalog.client.model.GenerateTemporaryTableCredential;
import io.unitycatalog.client.model.TableDependency;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableOperation;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DeltaGetCommits;
import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.model.PurgeState;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.Constants;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hibernate.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableSoftDropTest extends BaseTableCRUDTestEnv {
  private Repositories repositories;
  private TemporaryCredentialsApi temporaryCredentialsApi;
  private DeltaTablesApi deltaTablesApi;

  @Override
  protected CatalogOperations createCatalogOperations(ServerConfig config) {
    return new SdkCatalogOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected SchemaOperations createSchemaOperations(ServerConfig config) {
    return new SdkSchemaOperations(TestUtils.createApiClient(config));
  }

  @Override
  protected TableOperations createTableOperations(ServerConfig config) {
    return new SdkTableOperations(TestUtils.createApiClient(config));
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    repositories =
        new Repositories(
            hibernateConfigurator.getSessionFactory(), new ServerProperties(serverProperties));
    var apiClient = TestUtils.createApiClient(serverConfig);
    temporaryCredentialsApi = new TemporaryCredentialsApi(apiClient);
    deltaTablesApi = new DeltaTablesApi(apiClient);
  }

  @Test
  void dropRetainsTheRowAndRejectsEveryNormalAccessPath() throws Exception {
    TableInfo original = createAndVerifyManagedTable();
    UUID tableId = UUID.fromString(original.getTableId());
    Path tablePath = Path.of(URI.create(original.getStorageLocation()));
    Path dataFile = Files.createDirectories(tablePath).resolve("data.parquet");
    Files.writeString(dataFile, "data");

    tableOperations.deleteTable(fullName(TestUtils.TABLE_NAME));

    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      TableInfoDAO dropped = session.get(TableInfoDAO.class, tableId);
      assertThat(dropped).isNotNull();
      assertThat(dropped.getName()).isEqualTo("$deleted$" + tableId);
      assertThat(dropped.getDroppedName()).isEqualTo(TestUtils.TABLE_NAME);
      assertThat(dropped.getDroppedAt()).isNotNull();
      assertThat(dropped.getPurgeState()).isEqualTo(PurgeState.PENDING.getValue());
      assertThat(dropped.getNumCleanupRetries()).isZero();
      assertThat(dropped.getLastCleanupAt()).isNull();
      assertThat(dropped.getColumns()).hasSize(COLUMNS.size());
      assertThat(
              PropertyDAO.toMap(
                  PropertyRepository.findProperties(session, tableId, Constants.TABLE)))
          .containsAllEntriesOf(TestUtils.PROPERTIES);
    }
    assertThat(dataFile).exists();

    assertApiNotFound(() -> tableOperations.getTable(fullName(TestUtils.TABLE_NAME)));
    assertThat(
            tableOperations.listTables(
                TestUtils.CATALOG_NAME, TestUtils.SCHEMA_NAME, Optional.empty()))
        .isEmpty();
    assertApiNotFound(
        () ->
            temporaryCredentialsApi.generateTemporaryTableCredentials(
                new GenerateTemporaryTableCredential()
                    .tableId(original.getTableId())
                    .operation(TableOperation.READ)));

    TableRepository tables = repositories.getTableRepository();
    assertRepositoryNotFound(() -> tables.findTableOrThrow(catalog(), schema(), table()));
    assertRepositoryNotFound(() -> tables.getTableStorageLocation(catalog(), schema(), table()));
    assertRepositoryNotFound(() -> tables.getStorageLocationForTableOrStagingTable(tableId));
    assertRepositoryNotFound(() -> tables.getCatalogSchemaIdsByTableOrStagingTableId(tableId));
    assertRepositoryNotFound(() -> tables.loadTableForDelta(catalog(), schema(), table()));
    assertRepositoryNotFound(() -> tables.getIcebergTableState(catalog(), schema(), table()));
    assertRepositoryNotFound(
        () ->
            repositories
                .getDeltaCommitRepository()
                .getCommits(new DeltaGetCommits().tableId(original.getTableId()).startVersion(0L)));

    assertApiNotFound(() -> tableOperations.deleteTable(fullName(TestUtils.TABLE_NAME)));

    TableInfo replacement = createAndVerifyManagedTable();
    assertThat(replacement.getTableId()).isNotEqualTo(original.getTableId());
    assertThat(tableOperations.getTable(fullName(TestUtils.TABLE_NAME)).getTableId())
        .isEqualTo(replacement.getTableId());
  }

  @Test
  void commitLockRejectsTableDroppedAfterLookup() throws Exception {
    TableInfo original = createAndVerifyManagedTable();
    UUID tableId = UUID.fromString(original.getTableId());

    try (Session staleSession = hibernateConfigurator.getSessionFactory().openSession()) {
      var transaction = staleSession.beginTransaction();
      try {
        TableInfoDAO staleTable = staleSession.get(TableInfoDAO.class, tableId);
        assertThat(staleTable.getDroppedAt()).isNull();

        tableOperations.deleteTable(fullName(TestUtils.TABLE_NAME));

        assertThatThrownBy(
                () ->
                    RepositoryUtils.lockTableForCommit(
                        staleSession,
                        staleTable,
                        tableId,
                        Optional.of(fullName(TestUtils.TABLE_NAME))))
            .isInstanceOfSatisfying(
                BaseException.class,
                e -> {
                  assertThat(e.getErrorCode()).isEqualTo(ErrorCode.TABLE_NOT_FOUND);
                  assertThat(e.getMessage()).contains(fullName(TestUtils.TABLE_NAME));
                });
      } finally {
        if (transaction.isActive()) {
          transaction.rollback();
        }
      }
    }
  }

  @Test
  void externalTableDropRemovesMetadataAndKeepsStorage() throws Exception {
    String externalTable = "external_table";
    Path location = Files.createDirectory(testDirectoryRoot.resolve("external-location"));
    Path dataFile = Files.writeString(location.resolve("data.parquet"), "data");
    TableInfo original =
        createTestingTable(
            externalTable, TableType.EXTERNAL, Optional.of(location.toString()), tableOperations);
    UUID originalId = UUID.fromString(original.getTableId());

    tableOperations.deleteTable(fullName(externalTable));

    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      assertThat(session.get(TableInfoDAO.class, originalId)).isNull();
      assertThat(PropertyRepository.findProperties(session, originalId, Constants.TABLE)).isEmpty();
    }
    assertThat(dataFile).exists();

    TableInfo replacement =
        createTestingTable(
            externalTable, TableType.EXTERNAL, Optional.of(location.toString()), tableOperations);
    assertThat(replacement.getTableId()).isNotEqualTo(original.getTableId());
  }

  @Test
  void viewDropRemovesMetadataAndDependencies() throws Exception {
    String source = "view_source";
    String view = "test_view";
    createExternalTable(source);
    TableInfo created =
        tableOperations.createTable(
            new CreateTable()
                .name(view)
                .catalogName(catalog())
                .schemaName(schema())
                .columns(COLUMNS)
                .properties(TestUtils.PROPERTIES)
                .tableType(TableType.VIEW)
                .viewDefinition("SELECT * FROM " + fullName(source))
                .viewDependencies(
                    new DependencyList()
                        .dependencies(
                            List.of(
                                new Dependency()
                                    .table(
                                        new TableDependency().tableFullName(fullName(source)))))));
    UUID viewId = UUID.fromString(created.getTableId());

    tableOperations.deleteTable(fullName(view));

    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      assertThat(session.get(TableInfoDAO.class, viewId)).isNull();
      assertThat(
              repositories
                  .getDependencyRepository()
                  .getDependencies(session, viewId, DependencyDAO.DependentType.TABLE))
          .isEmpty();
      assertThat(PropertyRepository.findProperties(session, viewId, Constants.TABLE)).isEmpty();
    }
  }

  @Test
  void concurrentDropAndRenameCannotRestoreOrRenameADroppedRow() throws Exception {
    createAndVerifyManagedTable();

    CyclicBarrier barrier = new CyclicBarrier(2);
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      Future<Integer> drop =
          pool.submit(
              () -> {
                barrier.await();
                return runApiOperation(
                    () -> deltaTablesApi.deleteTable(catalog(), schema(), table()));
              });
      Future<Integer> rename = renameAfterBarrier(pool, barrier, table(), "renamed");

      assertThat(Arrays.asList(drop.get(30, TimeUnit.SECONDS), rename.get(30, TimeUnit.SECONDS)))
          .containsExactlyInAnyOrder(null, ErrorCode.TABLE_NOT_FOUND.getHttpStatus().code());

      try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
        List<TableInfoDAO> rows =
            session.createQuery("FROM TableInfoDAO", TableInfoDAO.class).getResultList();
        assertThat(rows).hasSize(1);
        TableInfoDAO row = rows.get(0);
        if (row.getDroppedAt() == null) {
          assertThat(row.getName()).isEqualTo("renamed");
          assertThat(row.getDroppedName()).isNull();
        } else {
          assertThat(row.getName()).isEqualTo("$deleted$" + row.getId());
          assertThat(row.getDroppedName()).isEqualTo(table());
        }
      }
    } finally {
      shutdown(pool);
    }
  }

  private Future<Integer> renameAfterBarrier(
      ExecutorService pool, CyclicBarrier barrier, String source, String target) {
    return pool.submit(
        () -> {
          barrier.await();
          return runApiOperation(
              () ->
                  deltaTablesApi.renameTable(
                      catalog(), schema(), source, new DeltaRenameTableRequest().newName(target)));
        });
  }

  private static Integer runApiOperation(ApiOperation operation) {
    try {
      operation.run();
      return null;
    } catch (ApiException e) {
      return e.getCode();
    }
  }

  private void createExternalTable(String name) throws Exception {
    createTestingTable(
        name,
        TableType.EXTERNAL,
        Optional.of(Files.createDirectory(testDirectoryRoot.resolve(name)).toString()),
        tableOperations);
  }

  private static void shutdown(ExecutorService pool) throws InterruptedException {
    pool.shutdown();
    assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
  }

  private static void assertApiNotFound(ApiOperation operation) {
    assertThatThrownBy(operation::run)
        .isInstanceOfSatisfying(
            ApiException.class,
            e ->
                assertThat(e.getCode())
                    .isEqualTo(ErrorCode.TABLE_NOT_FOUND.getHttpStatus().code()));
  }

  private static void assertRepositoryNotFound(RepositoryOperation operation) {
    assertThatThrownBy(operation::run)
        .isInstanceOfSatisfying(
            BaseException.class,
            e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.TABLE_NOT_FOUND));
  }

  private static String catalog() {
    return TestUtils.CATALOG_NAME;
  }

  private static String schema() {
    return TestUtils.SCHEMA_NAME;
  }

  private static String table() {
    return TestUtils.TABLE_NAME;
  }

  private static String fullName(String table) {
    return catalog() + "." + schema() + "." + table;
  }

  @FunctionalInterface
  private interface ApiOperation {
    void run() throws ApiException;
  }

  @FunctionalInterface
  private interface RepositoryOperation {
    void run();
  }
}
