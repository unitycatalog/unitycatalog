package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpStatus;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTablesApi;
import io.unitycatalog.client.delta.model.DeltaRenameTableRequest;
import io.unitycatalog.client.model.CreateTable;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.base.schema.SchemaOperations;
import io.unitycatalog.server.base.table.BaseTableCRUDTestEnv;
import io.unitycatalog.server.base.table.TableOperations;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.sdk.schema.SdkSchemaOperations;
import io.unitycatalog.server.sdk.tables.SdkTableOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableNameUniquenessTest extends BaseTableCRUDTestEnv {
  private static final int CREATE_ATTEMPTS = 8;
  private static final int RENAME_ATTEMPTS = 2;
  private static final int RESULT_TIMEOUT_SECONDS = 30;
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 10;

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
    deltaTablesApi = new DeltaTablesApi(TestUtils.createApiClient(serverConfig));
  }

  @Test
  void concurrentCreatesHaveOneWinner() throws Exception {
    List<Path> locations = new ArrayList<>();
    for (int i = 0; i < CREATE_ATTEMPTS; i++) {
      locations.add(Files.createDirectory(testDirectoryRoot.resolve("create-" + i)));
    }

    CyclicBarrier barrier = new CyclicBarrier(CREATE_ATTEMPTS);
    ExecutorService pool = Executors.newFixedThreadPool(CREATE_ATTEMPTS);
    List<Future<Integer>> results = new ArrayList<>();
    try {
      for (Path location : locations) {
        results.add(
            pool.submit(
                () -> {
                  TableOperations operations =
                      new SdkTableOperations(TestUtils.createApiClient(serverConfig));
                  barrier.await();
                  try {
                    operations.createTable(
                        new CreateTable()
                            .name("create_race")
                            .catalogName(catalog())
                            .schemaName(schema())
                            .columns(COLUMNS)
                            .tableType(TableType.EXTERNAL)
                            .dataSourceFormat(DataSourceFormat.DELTA)
                            .storageLocation(location.toString()));
                    return HttpStatus.OK.code();
                  } catch (ApiException e) {
                    return e.getCode();
                  }
                }));
      }

      List<Integer> statuses = getResults(results);
      assertThat(statuses)
          .containsOnly(
              HttpStatus.OK.code(), ErrorCode.TABLE_ALREADY_EXISTS.getHttpStatus().code());
      assertThat(statuses).filteredOn(status -> status == HttpStatus.OK.code()).hasSize(1);
    } finally {
      shutdown(pool);
    }
  }

  @Test
  void concurrentRenamesToOneNameHaveOneWinner() throws Exception {
    createExternalTable("source_a");
    createExternalTable("source_b");

    CyclicBarrier barrier = new CyclicBarrier(RENAME_ATTEMPTS);
    ExecutorService pool = Executors.newFixedThreadPool(RENAME_ATTEMPTS);
    try {
      List<Future<Integer>> results =
          List.of(
              renameAfterBarrier(pool, barrier, "source_a", "target"),
              renameAfterBarrier(pool, barrier, "source_b", "target"));
      assertThat(getResults(results)).containsExactlyInAnyOrder(null, HttpStatus.CONFLICT.code());
    } finally {
      shutdown(pool);
    }
  }

  private Future<Integer> renameAfterBarrier(
      ExecutorService pool, CyclicBarrier barrier, String source, String target) {
    return pool.submit(
        () -> {
          barrier.await();
          try {
            deltaTablesApi.renameTable(
                catalog(), schema(), source, new DeltaRenameTableRequest().newName(target));
            return null;
          } catch (ApiException e) {
            return e.getCode();
          }
        });
  }

  private void createExternalTable(String name) throws Exception {
    createTestingTable(
        name,
        TableType.EXTERNAL,
        Optional.of(Files.createDirectory(testDirectoryRoot.resolve(name)).toString()),
        tableOperations);
  }

  private static <T> List<T> getResults(List<Future<T>> futures) throws Exception {
    List<T> results = new ArrayList<>();
    for (Future<T> future : futures) {
      results.add(future.get(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }
    return results;
  }

  private static void shutdown(ExecutorService pool) throws InterruptedException {
    pool.shutdown();
    assertThat(pool.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
  }

  private static String catalog() {
    return TestUtils.CATALOG_NAME;
  }

  private static String schema() {
    return TestUtils.SCHEMA_NAME;
  }
}
