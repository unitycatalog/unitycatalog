package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.unitycatalog.server.cleanup.StorageCleanupTestSupport;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.BaseExceptionHandler;
import io.unitycatalog.server.exception.DeltaApiExceptionHandler;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.exception.IcebergRestExceptionHandler;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.RepositoryUtils;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.testdatabase.SupportedDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagedTableCleanupTaskTest {
  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  @TempDir Path tempDir;

  private SessionFactory sessionFactory;
  private TestDatabase testDatabase;
  private Repositories repositories;
  private ServerProperties serverProperties;
  private UUID schemaId;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    serverProperties = new ServerProperties(properties);
    testDatabase = TestDatabases.newDatabase();
    sessionFactory = testDatabase.openHibernateConfigurator(serverProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, serverProperties);

    UUID catalogId = UUID.randomUUID();
    schemaId = UUID.randomUUID();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(
              CatalogInfoDAO.builder().id(catalogId).name(CATALOG).createdAt(new Date()).build());
          session.persist(
              SchemaInfoDAO.builder()
                  .id(schemaId)
                  .catalogId(catalogId)
                  .name(SCHEMA)
                  .createdAt(new Date())
                  .build());
          return null;
        },
        "Failed to create test namespace",
        /* readOnly= */ false);
  }

  @AfterEach
  void tearDown() {
    testDatabase.close();
  }

  @Test
  void localAndCloudManagedDropsCreateTasksThroughBothRepositoryEntryPoints() throws Exception {
    TableInfoDAO local =
        createTable(
            "local_table",
            TableType.MANAGED,
            id -> tempDir.resolve("__unitystorage/tables").resolve(id.toString()).toString());
    Path localData = Path.of(local.getUrl()).resolve("part-00000");
    Files.createDirectories(localData.getParent());
    Files.writeString(localData, "data");
    Date beforeDrop = new Date();
    repositories.getTableRepository().deleteTable(CATALOG + "." + SCHEMA + ".local_table");
    assertDroppedWithTask(local, beforeDrop);
    assertThat(localData).exists();

    TableInfoDAO s3 =
        createTable(
            "s3_table", TableType.MANAGED, id -> "s3://bucket/root/unused/../tables/" + id + "///");
    beforeDrop = new Date();
    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "s3_table");
    assertDroppedWithTask(s3, beforeDrop);

    TableInfoDAO gcs =
        createTable("gcs_table", TableType.MANAGED, id -> "gs://bucket/root/tables/" + id);
    beforeDrop = new Date();
    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, "gcs_table");
    assertDroppedWithTask(gcs, beforeDrop);

    for (String scheme : List.of("abfs", "abfss")) {
      TableInfoDAO adls =
          createTable(
              scheme + "_table",
              TableType.MANAGED,
              id -> scheme + "://container@account.dfs.core.windows.net/root/tables/" + id);
      beforeDrop = new Date();
      repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, adls.getName());
      assertDroppedWithTask(adls, beforeDrop);
    }
  }

  @Test
  void externalTableDropsDoNotCreateTasks() {
    TableInfoDAO external =
        createTable(
            "external_table",
            TableType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());

    repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, external.getName());

    // findTask == null is the real guard: an external drop queues no cleanup task, so the worker
    // never touches its files. (No synchronous delete happens for any drop, managed or external.)
    assertThat(findTable(external.getId())).isNull();
    assertThat(findTask(external.getId())).isNull();
  }

  @Test
  void cascadingSchemaDropQueuesCleanupForManagedTablesOnly() {
    TableInfoDAO managed =
        createTable(
            "managed_cascade",
            TableType.MANAGED,
            id -> tempDir.resolve("__unitystorage/tables").resolve(id.toString()).toString());
    TableInfoDAO external =
        createTable(
            "external_cascade",
            TableType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());
    Date beforeDrop = new Date();

    // A force schema drop cascades each child through TableRepository.deleteTable(session, ...),
    // the same entry point a direct drop uses, so managed children still queue a cleanup task and
    // external children still queue none.
    repositories.getSchemaRepository().deleteSchema(CATALOG + "." + SCHEMA, /* force= */ true);

    assertDroppedWithTask(managed, beforeDrop);
    assertThat(findTable(external.getId())).isNull();
    assertThat(findTask(external.getId())).isNull();
    // Exactly one task: the managed child queued one, the external child queued none. Guards
    // against a future change queueing a second task with a different id (the resource_id primary
    // key only blocks a duplicate id).
    assertThat(allTasks()).hasSize(1);
  }

  @Test
  void taskInsertFailureRollsBackTableDeletion() {
    TableInfoDAO table =
        createTable("rollback_table", TableType.MANAGED, id -> "s3://bucket/root/tables/" + id);
    String existingTaskLocation = "s3://bucket/existing/tables/" + table.getId();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          repositories
              .getStorageCleanupTaskRepository()
              .create(
                  session,
                  ManagedResourceType.TABLE,
                  table.getId(),
                  table.getName(),
                  existingTaskLocation);
          return null;
        },
        "Failed to create existing cleanup task",
        /* readOnly= */ false);

    assertThatThrownBy(
            () -> repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, table.getName()))
        .isInstanceOf(BaseException.class);

    // The rollback is what these assertions prove: the table delete and the duplicate task insert
    // share one transaction, so the insert failure must undo both writes. The table row is still
    // present, and the pre-existing task keeps its original location.
    assertThat(findTable(table.getId())).isNotNull();
    assertThat(findTask(table.getId()).getStorageLocation()).isEqualTo(existingTaskLocation);
  }

  @Test
  void dropBlockedByCommitLockIsUnavailable() {
    TableInfoDAO table = createTableWithColumn("locked_table");
    TableRepository impatientTables = impatientRepositories().getTableRepository();

    try (Session commit = sessionFactory.openSession()) {
      Transaction tx = commit.beginTransaction();
      lockAndReplaceColumns(commit, table);
      // The drop stops at the table lock. A drop that deleted the columns before taking the lock
      // would block on the column rows this commit replaced and fail as INTERNAL instead.
      Throwable dropFailure =
          catchThrowable(() -> impatientTables.deleteTable(CATALOG, SCHEMA, table.getName()));
      assertThat(dropFailure)
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE));
      // Iceberg clients retry a 503 on drop table. The Delta API has no 503 error type, so it
      // answers with its "retry after a delay" type.
      AggregatedHttpResponse iceberg =
          errorResponse(IcebergRestExceptionHandler.INSTANCE, dropFailure);
      assertThat(iceberg.status().code()).isEqualTo(503);
      assertThat(iceberg.contentUtf8()).contains("\"type\":\"ServiceUnavailableException\"");
      AggregatedHttpResponse delta = errorResponse(DeltaApiExceptionHandler.INSTANCE, dropFailure);
      assertThat(delta.status().code()).isEqualTo(429);
      assertThat(delta.contentUtf8()).contains("TooManyRequestsException");
      tx.rollback();
    }
    assertThat(findTable(table.getId())).isNotNull();
  }

  private static AggregatedHttpResponse errorResponse(
      BaseExceptionHandler handler, Throwable error) {
    HttpRequest request = HttpRequest.of(HttpMethod.DELETE, "/");
    return handler
        .handleException(ServiceRequestContext.of(request), request, error)
        .aggregate()
        .join();
  }

  @Test
  void forceSchemaDeleteBlockedByCommitLockDeletesNothing() {
    TableInfoDAO table = createTableWithColumn("locked_table");
    SchemaRepository impatientSchemas = impatientRepositories().getSchemaRepository();

    try (Session commit = sessionFactory.openSession()) {
      Transaction tx = commit.beginTransaction();
      lockAndReplaceColumns(commit, table);
      // One locked table fails the whole force delete, which runs in a single transaction.
      assertThatThrownBy(() -> impatientSchemas.deleteSchema(CATALOG + "." + SCHEMA, true))
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE));
      tx.rollback();
    }
    assertThat(findTable(table.getId())).isNotNull();
    try (Session session = sessionFactory.openSession()) {
      assertThat(session.get(SchemaInfoDAO.class, schemaId)).isNotNull();
    }
  }

  @Test
  void commitOnATableDroppedAfterItWasReadIsNotFound() {
    TableInfoDAO table = createTableWithColumn("dropped_table");
    String fullName = CATALOG + "." + SCHEMA + "." + table.getName();

    try (Session commit = sessionFactory.openSession()) {
      Transaction tx = commit.beginTransaction();
      TableInfoDAO read = commit.get(TableInfoDAO.class, table.getId());
      // The drop commits before this commit takes the lock; reading takes no lock, so it does not
      // wait.
      repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, table.getName());
      assertThatThrownBy(
              () ->
                  RepositoryUtils.lockTableForCommit(
                      commit,
                      read,
                      read.getId(),
                      Optional.of(fullName),
                      ErrorCode.UPDATE_REQUIREMENT_CONFLICT))
          .isInstanceOfSatisfying(
              BaseException.class,
              e -> assertThat(e.getErrorCode()).isEqualTo(ErrorCode.TABLE_NOT_FOUND))
          .hasMessage("Table not found: " + fullName);
      tx.rollback();
    }
  }

  /** Repositories on this database whose connections give up waiting for a lock after 50 ms. */
  private Repositories impatientRepositories() {
    Properties impatient = testDatabase.hibernateProperties(serverProperties);
    impatient.setProperty("hibernate.hbm2ddl.auto", "none");
    // Set on the URL, not with SET: a PostgreSQL SET is undone when its transaction rolls back.
    impatient.setProperty(
        "hibernate.connection.url",
        testDatabase.jdbcUrl()
            + switch (SupportedDatabase.selected()) {
              case H2 -> ";LOCK_TIMEOUT=50";
              case POSTGRES_18 -> "?options=-c%20lock_timeout%3D50";
            });
    return new Repositories(
        testDatabase.openHibernateConfigurator(impatient).getSessionFactory(), serverProperties);
  }

  /** Takes the table lock in {@code commit} and, like a commit, replaces the table's columns. */
  private static void lockAndReplaceColumns(Session commit, TableInfoDAO table) {
    TableInfoDAO locked = commit.get(TableInfoDAO.class, table.getId());
    RepositoryUtils.lockTableForCommit(
        commit, locked, locked.getId(), Optional.empty(), ErrorCode.UPDATE_REQUIREMENT_CONFLICT);
    locked.getColumns().clear();
    commit.flush();
    locked.getColumns().add(column(locked, "replacement"));
    commit.flush();
  }

  private TableInfoDAO createTableWithColumn(String name) {
    TableInfoDAO table =
        createTable(
            name,
            TableType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(column(session.get(TableInfoDAO.class, table.getId()), "original"));
          return null;
        },
        "Failed to add test column",
        /* readOnly= */ false);
    return table;
  }

  private static ColumnInfoDAO column(TableInfoDAO table, String name) {
    return ColumnInfoDAO.builder()
        .id(UUID.randomUUID())
        .name(name)
        .table(table)
        .ordinalPosition((short) 0)
        .typeText("int")
        .typeJson("{\"name\":\"" + name + "\",\"type\":\"integer\",\"nullable\":true}")
        .typeName("INT")
        .nullable(true)
        .build();
  }

  private TableInfoDAO createTable(
      String name, TableType tableType, Function<UUID, String> location) {
    UUID tableId = UUID.randomUUID();
    TableInfoDAO table =
        TableInfoDAO.builder()
            .id(tableId)
            .schemaId(schemaId)
            .name(name)
            .type(tableType.getValue())
            .dataSourceFormat(DataSourceFormat.DELTA.getValue())
            .url(location.apply(tableId))
            .createdAt(new Date())
            .build();
    return TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          session.persist(table);
          return table;
        },
        "Failed to create test table",
        /* readOnly= */ false);
  }

  private void assertDroppedWithTask(TableInfoDAO table, Date beforeDrop) {
    StorageCleanupTaskDAO task = findTask(table.getId());
    assertThat(findTable(table.getId())).isNull();
    assertThat(task).isNotNull();
    assertThat(task.getId()).isEqualTo(table.getId());
    assertThat(task.getName()).isEqualTo(table.getName());
    assertThat(task.getResourceType()).isEqualTo(ManagedResourceType.TABLE);
    assertThat(task.getStorageLocation()).isEqualTo(NormalizedURL.normalize(table.getUrl()));
    assertThat(task.getDeletedAt().getTime())
        .isBetween(beforeDrop.getTime(), System.currentTimeMillis());
  }

  private TableInfoDAO findTable(UUID id) {
    try (var session = sessionFactory.openSession()) {
      return session.get(TableInfoDAO.class, id);
    }
  }

  private StorageCleanupTaskDAO findTask(UUID id) {
    return StorageCleanupTestSupport.findTask(sessionFactory, id);
  }

  private List<StorageCleanupTaskDAO> allTasks() {
    return StorageCleanupTestSupport.allTasks(sessionFactory);
  }
}
