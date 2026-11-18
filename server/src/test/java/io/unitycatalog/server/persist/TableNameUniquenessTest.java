package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.ColumnInfo;
import io.unitycatalog.server.model.ColumnTypeName;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TableNameUniquenessTest {
  @TempDatabase TestDatabase database;
  private SessionFactory sessions;
  private TableRepository tables;
  private UUID schemaId;
  private UUID catalogId;

  @BeforeEach
  void setUp() {
    sessions = database.sessionFactory();
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    tables =
        new Repositories(
                sessions, new ServerProperties(properties), mock(CloudCredentialVendor.class))
            .getTableRepository();
    schemaId = UUID.randomUUID();
    catalogId = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessions,
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name("catalog").createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaId)
                  .catalogId(catalogId)
                  .name("schema")
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "seed namespace",
        false);
  }

  private CreateTable request(String name) {
    return new CreateTable()
        .catalogName("catalog")
        .schemaName("schema")
        .name(name)
        .tableType(TableType.VIEW)
        .viewDefinition("select 1 as id")
        .columns(
            List.of(
                new ColumnInfo()
                    .name("id")
                    .position(0)
                    .nullable(true)
                    .typeName(ColumnTypeName.LONG)
                    .typeText("bigint")
                    .typeJson(
                        "{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}")));
  }

  private <T> T context(Callable<T> action) throws Exception {
    try (var ignored = ServiceRequestContext.of(HttpRequest.of(HttpMethod.POST, "/")).push()) {
      return action.call();
    }
  }

  @Test
  void concurrentCreatesHaveOneWinnerAndOneConflict() throws Exception {
    var pool = Executors.newFixedThreadPool(2);
    CyclicBarrier start = new CyclicBarrier(2);
    Callable<Boolean> create =
        () -> {
          start.await(10, TimeUnit.SECONDS);
          try {
            context(() -> tables.createTable(request("same")));
            return true;
          } catch (BaseException e) {
            assertThat(e.getErrorCode()).isEqualTo(ErrorCode.TABLE_ALREADY_EXISTS);
            assertThat(e.getMessage())
                .contains("same")
                .doesNotContain("ConstraintViolationException");
            return false;
          }
        };
    try {
      var first = pool.submit(create);
      var second = pool.submit(create);
      assertThat(List.of(first.get(30, TimeUnit.SECONDS), second.get(30, TimeUnit.SECONDS)))
          .containsExactlyInAnyOrder(true, false);
      assertThat(count("same")).isEqualTo(1);
    } finally {
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void renameLosesToACreateAfterItsDuplicateCheck() throws Exception {
    String originalId = context(() -> tables.createTable(request("source"))).getTableId();
    CountDownLatch checked = new CountDownLatch(1);
    CountDownLatch created = new CountDownLatch(1);
    SessionFactory delayed = mock(SessionFactory.class, delegatesTo(sessions));
    when(delayed.openSession())
        .thenAnswer(
            call -> {
              Session real = sessions.openSession();
              Session wrapper = mock(Session.class, delegatesTo(real));
              doAnswer(
                      merge -> {
                        checked.countDown();
                        if (!created.await(15, TimeUnit.SECONDS)) {
                          throw new IllegalStateException("create did not finish");
                        }
                        return real.merge(merge.getArgument(0));
                      })
                  .when(wrapper)
                  .merge(any(TableInfoDAO.class));
              return wrapper;
            });
    Properties values = new Properties();
    values.setProperty("server.env", "test");
    TableRepository renaming =
        new Repositories(delayed, new ServerProperties(values), mock(CloudCredentialVendor.class))
            .getTableRepository();
    var pool = Executors.newSingleThreadExecutor();
    try {
      var rename =
          pool.submit(
              () ->
                  context(
                      () -> {
                        try {
                          renaming.renameTable("catalog", "schema", "source", "target");
                          return null;
                        } catch (BaseException e) {
                          assertThat(e.getMessage())
                              .contains("target")
                              .doesNotContain("ConstraintViolationException");
                          return e.getErrorCode();
                        }
                      }));
      assertThat(checked.await(15, TimeUnit.SECONDS)).isTrue();
      String targetId;
      try {
        targetId = context(() -> tables.createTable(request("target"))).getTableId();
      } finally {
        created.countDown();
      }
      assertThat(rename.get(30, TimeUnit.SECONDS)).isEqualTo(ErrorCode.TABLE_ALREADY_EXISTS);
      assertThat(count("target")).isEqualTo(1);
      assertThat(tables.getTable("catalog.schema.source").getTableId()).isEqualTo(originalId);
      assertThat(tables.getTable("catalog.schema.target").getTableId()).isEqualTo(targetId);
    } finally {
      created.countDown();
      pool.shutdownNow();
      assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  void identicalNamesInDifferentSchemasRemainValid() throws Exception {
    var first = context(() -> tables.createTable(request("same")));
    UUID otherSchema = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessions,
        session -> {
          session.persist(
              SchemaInfoDAO.builder()
                  .id(otherSchema)
                  .catalogId(catalogId)
                  .name("other")
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "create another schema",
        false);
    var second = context(() -> tables.createTable(request("same").schemaName("other")));
    assertThat(second.getTableId()).isNotEqualTo(first.getTableId());
    assertThat(tables.getTable("catalog.other.same").getTableId()).isEqualTo(second.getTableId());
    assertThat(count("same")).isEqualTo(1);
  }

  private long count(String name) {
    return TransactionManager.executeWithTransaction(
        sessions,
        session ->
            session
                .createQuery(
                    "select count(t) from TableInfoDAO t where t.schemaId = :schema and t.name ="
                        + " :name",
                    Long.class)
                .setParameter("schema", schemaId)
                .setParameter("name", name)
                .getSingleResult(),
        "count objects",
        true);
  }
}
