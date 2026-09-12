package io.unitycatalog.server.persist;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO;
import io.unitycatalog.server.persist.dao.StorageCleanupTaskDAO.ResourceType;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import java.time.Instant;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.exception.ConstraintViolationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class StorageCleanupTaskSchemaTest {
  private SessionFactory sessionFactory;

  protected void configureDatabase(Properties properties) {
    properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    properties.setProperty(
        "hibernate.connection.url", "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
  }

  @AfterEach
  void closeSessionFactory() {
    if (sessionFactory != null) {
      sessionFactory.close();
    }
  }

  @Test
  void freshSchemaPersistsCleanupTask() {
    sessionFactory = new HibernateConfigurator(properties("create")).getSessionFactory();
    UUID resourceId = UUID.randomUUID();
    Instant cleanableAt = Instant.parse("2026-09-08T13:34:56Z");
    String storageLocation = "s3://bucket/" + "a".repeat(4084);

    StorageCleanupTaskDAO task = task(ResourceType.TABLE, resourceId, storageLocation, cleanableAt);
    task.setLeaseExpiresAt(cleanableAt.plusSeconds(3600));
    inTransaction(session -> session.persist(task));

    try (Session session = sessionFactory.openSession()) {
      assertTaskSchema(session);
      StorageCleanupTaskDAO stored = session.find(StorageCleanupTaskDAO.class, resourceId);
      assertThat(stored.getResourceType()).isEqualTo(ResourceType.TABLE);
      assertThat(stored.getResourceId()).isEqualTo(resourceId);
      assertThat(stored.getStorageLocation()).isEqualTo(storageLocation);
      assertThat(stored.getCleanableAt()).isEqualTo(cleanableAt);
      assertThat(stored.getLeaseToken()).isNull();
      assertThat(stored.getLeaseExpiresAt()).isEqualTo(cleanableAt.plusSeconds(3600));
      assertThat(stored.getFailureCount()).isZero();
      assertThat(stored.getLastError()).isNull();
    }
  }

  @Test
  void resourceIdIsUnique() {
    sessionFactory = new HibernateConfigurator(properties("create")).getSessionFactory();
    UUID resourceId = UUID.randomUUID();
    Instant cleanableAt = Instant.parse("2026-09-08T12:34:56Z");
    inTransaction(
        session ->
            session.persist(
                task(ResourceType.TABLE, resourceId, "s3://bucket/table", cleanableAt)));

    assertThatThrownBy(
            () ->
                inTransaction(
                    session ->
                        session.persist(
                            task(
                                ResourceType.VOLUME,
                                resourceId,
                                "s3://bucket/duplicate",
                                cleanableAt))))
        .isInstanceOf(ConstraintViolationException.class);
  }

  @Test
  void updateAddsCleanupTaskTableWithoutReplacingExistingSchema() {
    Properties properties = properties("create");
    sessionFactory = new HibernateConfigurator(properties).getSessionFactory();
    inTransaction(
        session -> {
          session
              .createNativeMutationQuery("CREATE TABLE uc_migration_marker (marker_value INTEGER)")
              .executeUpdate();
          session
              .createNativeMutationQuery("INSERT INTO uc_migration_marker VALUES (7)")
              .executeUpdate();
          session.createNativeMutationQuery("DROP TABLE uc_storage_cleanup_tasks").executeUpdate();
        });
    sessionFactory.close();

    properties.setProperty("hibernate.hbm2ddl.auto", "update");
    sessionFactory = new HibernateConfigurator(properties).getSessionFactory();

    try (Session session = sessionFactory.openSession()) {
      assertTaskSchema(session);
      assertThat(
              session
                  .createNativeQuery("SELECT marker_value FROM uc_migration_marker", Integer.class)
                  .getSingleResult())
          .isEqualTo(7);
      assertThat(
              session
                  .createNativeQuery("SELECT COUNT(*) FROM uc_storage_cleanup_tasks", Long.class)
                  .getSingleResult())
          .isZero();
    }
  }

  private Properties properties(String schemaAction) {
    Properties properties = new Properties();
    configureDatabase(properties);
    properties.setProperty("hibernate.hbm2ddl.auto", schemaAction);
    properties.setProperty("hibernate.show_sql", "false");
    return properties;
  }

  private void assertTaskSchema(Session session) {
    session.doWork(
        connection -> {
          Set<String> columns = new HashSet<>();
          try (java.sql.ResultSet result =
              connection
                  .createStatement()
                  .executeQuery("SELECT * FROM uc_storage_cleanup_tasks WHERE 1 = 0")) {
            java.sql.ResultSetMetaData metadata = result.getMetaData();
            for (int index = 1; index <= metadata.getColumnCount(); index++) {
              columns.add(metadata.getColumnName(index).toLowerCase(Locale.ROOT));
            }
          }
          assertThat(columns)
              .containsExactlyInAnyOrder(
                  "resource_type",
                  "resource_id",
                  "storage_location",
                  "cleanable_at",
                  "lease_token",
                  "lease_expires_at",
                  "failure_count",
                  "last_error");

          String tableName =
              connection.getMetaData().storesUpperCaseIdentifiers()
                  ? "UC_STORAGE_CLEANUP_TASKS"
                  : "uc_storage_cleanup_tasks";
          Set<String> indexes = new HashSet<>();
          try (java.sql.ResultSet result =
              connection.getMetaData().getIndexInfo(null, null, tableName, false, false)) {
            while (result.next()) {
              String indexName = result.getString("INDEX_NAME");
              if (indexName != null) {
                indexes.add(indexName.toLowerCase(Locale.ROOT));
              }
            }
          }
          assertThat(indexes).contains("uc_storage_cleanup_tasks_ready_idx");
        });
  }

  private StorageCleanupTaskDAO task(
      ResourceType resourceType, UUID resourceId, String location, Instant cleanableAt) {
    return StorageCleanupTaskDAO.builder()
        .resourceType(resourceType)
        .resourceId(resourceId)
        .storageLocation(location)
        .cleanableAt(cleanableAt)
        .build();
  }

  private void inTransaction(java.util.function.Consumer<Session> action) {
    try (Session session = sessionFactory.openSession()) {
      org.hibernate.Transaction transaction = session.beginTransaction();
      action.accept(session);
      transaction.commit();
    }
  }
}
