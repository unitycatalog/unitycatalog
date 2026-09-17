package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.CreateSchema;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TableRepositoryTest {

  private static final String CATALOG_NAME = "table_repo_catalog";
  private static final String SCHEMA_NAME = "table_repo_schema";

  private static SessionFactory sessionFactory;
  private static TableRepository tableRepository;

  @BeforeAll
  static void setUp() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    ServerProperties serverProperties = new ServerProperties(properties);
    HibernateConfigurator hibernateConfigurator = new HibernateConfigurator(serverProperties);
    sessionFactory = hibernateConfigurator.getSessionFactory();

    Repositories repositories = new Repositories(sessionFactory, serverProperties);
    tableRepository = repositories.getTableRepository();
    withRequestContext(
        () -> {
          repositories.getCatalogRepository().addCatalog(new CreateCatalog().name(CATALOG_NAME));
          return repositories
              .getSchemaRepository()
              .createSchema(new CreateSchema().name(SCHEMA_NAME).catalogName(CATALOG_NAME));
        });
  }

  @AfterAll
  static void tearDown() {
    if (sessionFactory != null) {
      sessionFactory.close();
    }
  }

  /**
   * Two creates of one name race. Exactly one row must survive: a second row makes every later
   * lookup of that name resolve two results, which leaves the name unusable and undeletable.
   */
  @Test
  void concurrentCreatesOfTheSameNameKeepTheNameUsable() throws Exception {
    int threads = 4;
    // Several independent names, because whether the two transactions actually overlap is a
    // scheduling accident; one name that happens not to race proves nothing.
    for (int round = 0; round < 8; round++) {
      String tableName = "racing_table_" + round;
      CyclicBarrier startBarrier = new CyclicBarrier(threads);
      ExecutorService executor = Executors.newFixedThreadPool(threads);
      List<Future<Object>> futures = new ArrayList<>();
      try {
        for (int i = 0; i < threads; i++) {
          Callable<Object> create =
              () -> {
                startBarrier.await(10, TimeUnit.SECONDS);
                // The context is thread-local, so every worker needs its own.
                return withRequestContext(
                    () -> tableRepository.createTable(externalTable(tableName)));
              };
          futures.add(executor.submit(create));
        }

        int created = 0;
        for (Future<Object> future : futures) {
          try {
            future.get(20, TimeUnit.SECONDS);
            created++;
          } catch (ExecutionException e) {
            assertThat(e.getCause())
                .isInstanceOf(BaseException.class)
                .satisfies(
                    cause ->
                        assertThat(((BaseException) cause).getErrorCode())
                            .as("a losing create must report the name as taken, not a server error")
                            .isEqualTo(ErrorCode.TABLE_ALREADY_EXISTS));
          }
        }
        assertThat(created).as("exactly one create wins the race").isEqualTo(1);
      } finally {
        executor.shutdownNow();
      }

      String fullName = CATALOG_NAME + "." + SCHEMA_NAME + "." + tableName;
      // The damage the duplicate rows do: both of these resolve the name with a unique-result
      // query, so a second row turns them into a server error and the name cannot be removed.
      assertThat(withRequestContext(() -> tableRepository.getTable(fullName)).getName())
          .isEqualTo(tableName);
      withRequestContext(
          () -> {
            tableRepository.deleteTable(fullName);
            return null;
          });
    }
  }

  /** The repositories read the caller from Armeria's thread-local context, which tests must set. */
  private static <T> T withRequestContext(Callable<T> action) throws Exception {
    ServiceRequestContext context =
        ServiceRequestContext.builder(HttpRequest.of(HttpMethod.POST, "/")).build();
    try (SafeCloseable ignored = context.push()) {
      return action.call();
    }
  }

  private static CreateTable externalTable(String name) {
    return new CreateTable()
        .name(name)
        .catalogName(CATALOG_NAME)
        .schemaName(SCHEMA_NAME)
        .tableType(TableType.EXTERNAL)
        .dataSourceFormat(DataSourceFormat.DELTA)
        .storageLocation("file:///tmp/uc-table-repo-test/" + UUID.randomUUID())
        .columns(
            List.of(
                new ColumnInfo()
                    .name("id")
                    .typeName(ColumnTypeName.INT)
                    .typeText("int")
                    .typeJson(
                        "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}")
                    .nullable(true)
                    .position(0)));
  }
}
