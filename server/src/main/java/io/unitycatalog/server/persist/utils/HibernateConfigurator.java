package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.ColumnInfoDAO;
import io.unitycatalog.server.persist.dao.CredentialDAO;
import io.unitycatalog.server.persist.dao.DeltaCommitDAO;
import io.unitycatalog.server.persist.dao.DependencyDAO;
import io.unitycatalog.server.persist.dao.ExternalLocationDAO;
import io.unitycatalog.server.persist.dao.FunctionInfoDAO;
import io.unitycatalog.server.persist.dao.FunctionParameterInfoDAO;
import io.unitycatalog.server.persist.dao.MetastoreDAO;
import io.unitycatalog.server.persist.dao.ModelVersionInfoDAO;
import io.unitycatalog.server.persist.dao.PropertyDAO;
import io.unitycatalog.server.persist.dao.RegisteredModelInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.StagingTableDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.dao.VolumeInfoDAO;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class configures the hibernate properties and adds annotated classes to the session factory.
 * This session factory is used to create sessions for database operations across the repository
 * classes.
 */
@Getter
public class HibernateConfigurator {

  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateConfigurator.class);
  private static final String TABLE_NAME_UNIQUE_CONSTRAINT = "uc_tables_schema_id_name_unique";
  private static final Set<String> TABLE_NAME_UNIQUE_COLUMNS = Set.of("schema_id", "name");

  private final SessionFactory sessionFactory;
  private final Properties hibernateProperties;

  public HibernateConfigurator(ServerProperties serverProperties) {
    this(setupHibernateProperties(serverProperties));
  }

  /**
   * Builds a session factory from explicit hibernate properties. Lets tests customize the
   * properties (e.g. point at PostgreSQL via Testcontainers) before construction.
   */
  public HibernateConfigurator(Properties hibernateProperties) {
    this.hibernateProperties = hibernateProperties;
    this.sessionFactory = createSessionFactory(hibernateProperties);
  }

  private static SessionFactory createSessionFactory(Properties hibernateProperties) {
    try {
      Configuration configuration = new Configuration().setProperties(hibernateProperties);

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(StagingTableDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(ModelVersionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);
      configuration.addAnnotatedClass(UserDAO.class);
      configuration.addAnnotatedClass(MetastoreDAO.class);
      configuration.addAnnotatedClass(CredentialDAO.class);
      configuration.addAnnotatedClass(ExternalLocationDAO.class);
      configuration.addAnnotatedClass(DeltaCommitDAO.class);
      configuration.addAnnotatedClass(DependencyDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      SessionFactory sessionFactory = configuration.buildSessionFactory(serviceRegistry);
      try {
        validateTableNameUniqueness(sessionFactory);
        return sessionFactory;
      } catch (RuntimeException e) {
        sessionFactory.close();
        throw e;
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }

  private static void validateTableNameUniqueness(SessionFactory sessionFactory) {
    try (var session = sessionFactory.openSession()) {
      if (session.doReturningWork(HibernateConfigurator::hasTableNameUniqueConstraint)) {
        return;
      }
      boolean hasDuplicates =
          !session
              .createQuery(
                  "SELECT t.schemaId, t.name FROM TableInfoDAO t "
                      + "GROUP BY t.schemaId, t.name HAVING COUNT(t) > 1",
                  Object[].class)
              .setMaxResults(1)
              .getResultList()
              .isEmpty();
      if (hasDuplicates) {
        throw new IllegalStateException(
            "Cannot enforce unique table names because a schema contains duplicate names.");
      }
      throw new IllegalStateException(
          "Cannot enforce unique table names because the database did not create constraint "
              + TABLE_NAME_UNIQUE_CONSTRAINT
              + ".");
    }
  }

  private static boolean hasTableNameUniqueConstraint(Connection connection) throws SQLException {
    DatabaseMetaData metadata = connection.getMetaData();
    TableReference table = findTable(metadata, connection.getCatalog(), connection.getSchema());
    if (table == null) {
      return false;
    }

    Map<String, Set<String>> indexes = new HashMap<>();
    try (ResultSet rows =
        metadata.getIndexInfo(table.catalog(), table.schema(), table.name(), true, false)) {
      while (rows.next()) {
        String indexName = rows.getString("INDEX_NAME");
        String columnName = rows.getString("COLUMN_NAME");
        if (indexName != null && columnName != null && !rows.getBoolean("NON_UNIQUE")) {
          indexes
              .computeIfAbsent(indexName, ignored -> new HashSet<>())
              .add(columnName.toLowerCase(Locale.ROOT));
        }
      }
    }

    return indexes.values().stream().anyMatch(TABLE_NAME_UNIQUE_COLUMNS::equals);
  }

  private static TableReference findTable(DatabaseMetaData metadata, String catalog, String schema)
      throws SQLException {
    try (ResultSet rows = metadata.getTables(catalog, schema, null, new String[] {"TABLE"})) {
      while (rows.next()) {
        if ("uc_tables".equalsIgnoreCase(rows.getString("TABLE_NAME"))) {
          return new TableReference(
              rows.getString("TABLE_CAT"),
              rows.getString("TABLE_SCHEM"),
              rows.getString("TABLE_NAME"));
        }
      }
    }
    return null;
  }

  private record TableReference(String catalog, String schema, String name) {}

  public static Properties setupHibernateProperties(ServerProperties serverProperties) {
    Path hibernatePropertiesPath = Paths.get("etc/conf/hibernate.properties");
    Properties hibernateProperties = new Properties();
    if (!hibernatePropertiesPath.toFile().exists()) {
      LOGGER.warn("Hibernate properties file not found: {}", hibernatePropertiesPath);
      hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      hibernateProperties.setProperty(
          "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
      hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
    } else {
      try (InputStream input = Files.newInputStream(hibernatePropertiesPath)) {
        hibernateProperties.load(input);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    if ("test".equals(serverProperties.get(Property.SERVER_ENV))) {
      hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      hibernateProperties.setProperty(
          "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
      hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
      LOGGER.debug("Hibernate configuration set for testing");
    }
    return hibernateProperties;
  }
}
