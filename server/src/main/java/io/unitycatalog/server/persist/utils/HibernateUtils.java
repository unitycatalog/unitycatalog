package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
import io.unitycatalog.server.utils.ServerProperties;
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

public class HibernateUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(HibernateUtils.class);

  @Getter private static final SessionFactory sessionFactory;
  private static final ServerProperties properties;
  @Getter private static final Properties hibernateProperties = new Properties();

  static {
    properties = ServerProperties.getInstance();
    sessionFactory = createSessionFactory();
  }

  private static SessionFactory createSessionFactory() {
    try {
      if (properties == null) {
        throw new RuntimeException("PropertiesUtil instance is null in createSessionFactory");
      }

      Path hibernatePropertiesPath = Paths.get("etc/conf/hibernate.properties");
      if (!hibernatePropertiesPath.toFile().exists()) {
        LOGGER.warn("Hibernate properties file not found: {}", hibernatePropertiesPath);
        hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        hibernateProperties.setProperty(
            "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "update");
      } else {
        InputStream input = Files.newInputStream(hibernatePropertiesPath);
        hibernateProperties.load(input);
      }

      if ("test".equals(properties.getProperty("server.env"))) {
        hibernateProperties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        hibernateProperties.setProperty(
            "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        hibernateProperties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        LOGGER.debug("Hibernate configuration set for testing");
      }
      Configuration configuration = new Configuration().setProperties(hibernateProperties);

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(StagingTableDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(ModelVersionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);
      configuration.addAnnotatedClass(UserDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }
}
