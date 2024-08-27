package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
import java.io.InputStream;
import java.nio.file.Files;
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
  private static final ServerPropertiesUtils properties;

  static {
    properties = ServerPropertiesUtils.getInstance();
    sessionFactory = createSessionFactory();
  }

  private static SessionFactory createSessionFactory() {
    try {
      if (properties == null) {
        throw new RuntimeException("PropertiesUtil instance is null in createSessionFactory");
      }

      Properties hibernateProperties = new Properties();
      try (InputStream input = Files.newInputStream(Paths.get("etc/conf/hibernate.properties"))) {
        hibernateProperties.load(input);
      }
      Configuration configuration = new Configuration().setProperties(hibernateProperties);
      if ("test".equals(properties.getProperty("server.env"))) {
        configuration.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
        configuration.setProperty(
            "hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        configuration.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        LOGGER.debug("Hibernate configuration set for testing");
      }

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(RegisteredModelInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }
}
