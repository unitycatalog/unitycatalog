package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.NormalizedURL;
import io.unitycatalog.server.utils.ServerProperties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagedTableCleanupTaskTest {
  private static final String CATALOG = "catalog";
  private static final String SCHEMA = "schema";

  @TempDir Path tempDir;

  private SessionFactory sessionFactory;
  private Repositories repositories;
  private UUID schemaId;

  @BeforeEach
  void setUp() {
    Properties hibernateProperties = new Properties();
    hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    hibernateProperties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    hibernateProperties.setProperty("hibernate.show_sql", "false");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
    repositories = new Repositories(sessionFactory, new ServerProperties(new Properties()));

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
    sessionFactory.close();
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
  }

  @Test
  void externalAndDeferredProviderDropsDoNotCreateTasks() {
    TableInfoDAO external =
        createTable(
            "external_table",
            TableType.EXTERNAL,
            id -> tempDir.resolve("external").resolve(id.toString()).toString());
    TableInfoDAO adls =
        createTable(
            "adls_table",
            TableType.MANAGED,
            id -> "abfs://container@account.dfs.core.windows.net/root/tables/" + id);

    for (TableInfoDAO table : java.util.List.of(external, adls)) {
      repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, table.getName());
      assertThat(findTable(table.getId())).isNull();
      assertThat(findTask(table.getId())).isNull();
    }
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
              .create(session, ResourceType.TABLE, table.getId(), existingTaskLocation);
          return null;
        },
        "Failed to create existing cleanup task",
        /* readOnly= */ false);

    assertThatThrownBy(
            () -> repositories.getTableRepository().deleteTable(CATALOG, SCHEMA, table.getName()))
        .isInstanceOf(RuntimeException.class);

    assertThat(findTable(table.getId())).isNotNull();
    assertThat(findTask(table.getId()).getStorageLocation()).isEqualTo(existingTaskLocation);
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
    assertThat(task.getResourceType()).isEqualTo(ResourceType.TABLE);
    assertThat(task.getStorageLocation()).isEqualTo(NormalizedURL.normalize(table.getUrl()));
    assertThat(task.getDeletedAt()).isBetween(beforeDrop, new Date());
  }

  private TableInfoDAO findTable(UUID id) {
    try (var session = sessionFactory.openSession()) {
      return session.get(TableInfoDAO.class, id);
    }
  }

  private StorageCleanupTaskDAO findTask(UUID id) {
    try (var session = sessionFactory.openSession()) {
      return session.get(StorageCleanupTaskDAO.class, id);
    }
  }
}
