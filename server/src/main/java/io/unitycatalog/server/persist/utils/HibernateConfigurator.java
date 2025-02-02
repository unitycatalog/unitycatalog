package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
import io.unitycatalog.server.utils.ServerProperties;
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

  private final SessionFactory sessionFactory;
  private final Properties hibernateProperties;

  public HibernateConfigurator(ServerProperties serverProperties) {
    this.hibernateProperties = setupHibernateProperties(serverProperties);
    this.sessionFactory = createSessionFactory(hibernateProperties);
  }

  private static SessionFactory createSessionFactory(Properties hibernateProperties) {
    try {
      Configuration configuration = new Configuration().setProperties(hibernateProperties);

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(ModelVersionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);
      configuration.addAnnotatedClass(UserDAO.class);
      configuration.addAnnotatedClass(MetastoreDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
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
      InputStream input;
      try {
        input = Files.newInputStream(hibernatePropertiesPath);
        hibernateProperties.load(input);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // TODO: use dependency injection for test hibernate properties
    if ("test".equals(serverProperties.getProperty("server.env"))) {
      hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      hibernateProperties.setProperty(
          "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
      hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
      LOGGER.debug("Hibernate configuration set for testing");
    }
    return hibernateProperties;
  }
}
