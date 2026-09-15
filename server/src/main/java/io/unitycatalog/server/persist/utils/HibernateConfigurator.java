package io.unitycatalog.server.persist.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.util.Properties;
import lombok.Getter;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.JdbcSettings;
import org.hibernate.service.ServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class configures the hibernate properties and adds annotated classes to the session factory.
 * This session factory is used to create sessions for database operations across the repository
 * classes. Casbin's JDBC adapter is given the same DataSource so both sit under one Hikari {@code
 * maximumPoolSize}. jdbc-adapter 2.7.0 still holds one pooled connection for its lifetime; Casbin
 * enables autocommit on that checkout only.
 */
@Getter
public class HibernateConfigurator implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateConfigurator.class);
  private static final String HIKARI_PREFIX = "hibernate.hikari.";

  private final SessionFactory sessionFactory;
  private final Properties hibernateProperties;
  private final HikariDataSource dataSource;

  public HibernateConfigurator(ServerProperties serverProperties) {
    this(setupHibernateProperties(serverProperties));
  }

  /**
   * Builds a session factory from explicit hibernate properties. Lets tests customize the
   * properties (e.g. point at PostgreSQL via Testcontainers) before construction.
   */
  public HibernateConfigurator(Properties hibernateProperties) {
    this.hibernateProperties = hibernateProperties;
    this.dataSource = createDataSource(hibernateProperties);
    try {
      this.sessionFactory = createSessionFactory(hibernateProperties, dataSource);
    } catch (Throwable t) {
      dataSource.close();
      throw t;
    }
  }

  private static HikariDataSource createDataSource(Properties hibernateProperties) {
    Properties hikariProperties = new Properties();
    hibernateProperties.stringPropertyNames().stream()
        .filter(name -> name.startsWith(HIKARI_PREFIX))
        .forEach(
            name ->
                hikariProperties.setProperty(
                    name.substring(HIKARI_PREFIX.length()), hibernateProperties.getProperty(name)));

    HikariConfig hikariConfig = new HikariConfig(hikariProperties);
    hikariConfig.setJdbcUrl(hibernateProperties.getProperty("hibernate.connection.url"));
    hikariConfig.setUsername(resolveConnectionUsername(hibernateProperties));
    hikariConfig.setPassword(hibernateProperties.getProperty("hibernate.connection.password"));
    hikariConfig.setDriverClassName(
        hibernateProperties.getProperty("hibernate.connection.driver_class"));
    if (!hikariProperties.containsKey("maximumPoolSize")) {
      hikariConfig.setMaximumPoolSize(
          Integer.parseInt(
              hibernateProperties.getProperty("hibernate.connection.pool_size", "20")));
    }
    if (!hikariProperties.containsKey("minimumIdle")) {
      hikariConfig.setMinimumIdle(Math.min(2, hikariConfig.getMaximumPoolSize()));
    }
    if (!hikariProperties.containsKey("autoCommit")) {
      // Hibernate manages transactions and issues rollback() when JDBC work ends.
      hikariConfig.setAutoCommit(false);
    }
    if (hikariConfig.getPoolName() == null) {
      hikariConfig.setPoolName("unity-catalog");
    }
    return new HikariDataSource(hikariConfig);
  }

  /**
   * Prefers {@code hibernate.connection.username} and falls back to {@code
   * hibernate.connection.user}, which the deployment examples still document.
   */
  static String resolveConnectionUsername(Properties properties) {
    String username = properties.getProperty("hibernate.connection.username");
    return username != null ? username : properties.getProperty("hibernate.connection.user");
  }

  private static SessionFactory createSessionFactory(
      Properties hibernateProperties, HikariDataSource dataSource) {
    try {
      Properties sessionFactoryProperties = new Properties();
      sessionFactoryProperties.putAll(hibernateProperties);
      sessionFactoryProperties.remove("hibernate.connection.driver_class");
      sessionFactoryProperties.remove("hibernate.connection.url");
      sessionFactoryProperties.remove("hibernate.connection.user");
      sessionFactoryProperties.remove("hibernate.connection.username");
      sessionFactoryProperties.remove("hibernate.connection.password");
      sessionFactoryProperties.remove("hibernate.connection.pool_size");
      sessionFactoryProperties
          .keySet()
          .removeIf(key -> key instanceof String name && name.startsWith(HIKARI_PREFIX));
      sessionFactoryProperties.put(JdbcSettings.JAKARTA_NON_JTA_DATASOURCE, dataSource);

      Configuration configuration = new Configuration().setProperties(sessionFactoryProperties);

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

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }

  @Override
  public void close() {
    try {
      sessionFactory.close();
    } finally {
      dataSource.close();
    }
  }

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
