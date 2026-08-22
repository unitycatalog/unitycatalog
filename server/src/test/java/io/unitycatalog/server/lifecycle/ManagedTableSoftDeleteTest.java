package io.unitycatalog.server.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.persist.utils.TransactionManager;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ManagedTableSoftDeleteTest {
  private static final String CATALOG = "soft_delete_catalog";
  private static final String SCHEMA = "soft_delete_schema";
  private static final String TABLE = "soft_delete_table";

  @TempDir Path temporaryDirectory;

  private SessionFactory sessionFactory;
  private Repositories repositories;
  private UUID schemaId;

  @BeforeEach
  void setUp() {
    Properties settings = new Properties();
    settings.setProperty(Property.SERVER_ENV.getKey(), "test");
    settings.setProperty(Property.MANAGED_TABLE_RETENTION_DURATION.getKey(), "PT0S");
    ServerProperties serverProperties = new ServerProperties(settings);

    Properties hibernateProperties = new Properties();
    hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    hibernateProperties.setProperty(
        "hibernate.connection.url",
        "jdbc:h2:mem:managed-soft-delete-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
    hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    sessionFactory = new HibernateConfigurator(hibernateProperties).getSessionFactory();
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
        "Failed to persist test namespace",
        /* readOnly = */ false);
  }

  @AfterEach
  void tearDown() {
    sessionFactory.close();
  }

  @Test
  void dropHidesManagedTableUntilRestore() {
    UUID tableId = persistManagedTable();
    TableRepository tableRepository = repositories.getTableRepository();

    assertThat(tableRepository.deleteTable(CATALOG, SCHEMA, TABLE).softDeleted()).isTrue();
    assertTableNotFound(() -> tableRepository.getTable(fullName()));
    assertTableNotFound(() -> tableRepository.getStorageLocationForTableOrStagingTable(tableId));
    assertTableNotFound(() -> tableRepository.getCatalogSchemaIdsByTableOrStagingTableId(tableId));
    assertThat(
            tableRepository
                .listTables(CATALOG, SCHEMA, Optional.empty(), Optional.empty(), true, true)
                .getTables())
        .isEmpty();

    TableInfo restored = tableRepository.restoreTable(fullName());
    assertThat(restored.getTableId()).isEqualTo(tableId.toString());
    try (Session session = sessionFactory.openSession()) {
      TableInfoDAO table = session.get(TableInfoDAO.class, tableId);
      assertThat(table.getDeletedAt()).isNull();
      assertThat(table.getPurgeAfter()).isNull();
    }
  }

  @Test
  void dropPersistsTheConfiguredRetentionDeadline() {
    UUID tableId = persistManagedTable();
    Properties settings = new Properties();
    settings.setProperty(Property.SERVER_ENV.getKey(), "test");
    settings.setProperty(Property.MANAGED_TABLE_RETENTION_DURATION.getKey(), "PT1H");
    TableRepository retainedTableRepository =
        new TableRepository(repositories, sessionFactory, new ServerProperties(settings));

    retainedTableRepository.deleteTable(CATALOG, SCHEMA, TABLE);

    try (Session session = sessionFactory.openSession()) {
      TableInfoDAO table = session.get(TableInfoDAO.class, tableId);
      assertThat(table.getPurgeAfter().getTime() - table.getDeletedAt().getTime())
          .isEqualTo(Duration.ofHours(1).toMillis());
    }
  }

  private UUID persistManagedTable() {
    UUID tableId = UUID.randomUUID();
    String storageLocation = temporaryDirectory.resolve(TABLE).toUri().toString();
    TransactionManager.executeWithTransaction(
        sessionFactory,
        session -> {
          Date now = new Date();
          session.persist(
              StagingTableDAO.builder()
                  .id(tableId)
                  .schemaId(schemaId)
                  .name(TABLE)
                  .stagingLocation(storageLocation)
                  .createdAt(now)
                  .accessedAt(now)
                  .stageCommitted(true)
                  .stageCommittedAt(now)
                  .build());
          session.persist(
              TableInfoDAO.builder()
                  .id(tableId)
                  .schemaId(schemaId)
                  .name(TABLE)
                  .type(TableType.MANAGED.getValue())
                  .dataSourceFormat(DataSourceFormat.DELTA.getValue())
                  .url(storageLocation)
                  .columns(List.of())
                  .createdAt(now)
                  .build());
          return null;
        },
        "Failed to persist test table",
        /* readOnly = */ false);
    return tableId;
  }

  private String fullName() {
    return CATALOG + "." + SCHEMA + "." + TABLE;
  }

  private static void assertTableNotFound(Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(BaseException.class)
        .extracting(error -> ((BaseException) error).getErrorCode())
        .isEqualTo(ErrorCode.TABLE_NOT_FOUND);
  }
}
