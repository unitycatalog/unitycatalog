package io.unitycatalog.server.persist.utils;

import io.unitycatalog.server.persist.dao.*;
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

      Configuration configuration;
      switch (properties.getProperty("db.type")) {
        case "h2":
          configuration = getH2Configuration();
          break;
        case "mysql":
        default:
          configuration = getMysqlConfiguration();
      }
      configuration.setProperty("hibernate.show_sql", "false");
      configuration.setProperty("hibernate.archive.autodetection", "class");
      configuration.setProperty(
          "hibernate.archive.scan.packages", "com.databricks.unitycatalog.persist.dao");
      configuration.setProperty("hibernate.use_sql_comments", "true");
      configuration.setProperty("org.hibernate.SQL", "INFO");
      configuration.setProperty("org.hibernate.type.descriptor.sql.BasicBinder", "TRACE");

      // Add annotated classes
      configuration.addAnnotatedClass(CatalogInfoDAO.class);
      configuration.addAnnotatedClass(SchemaInfoDAO.class);
      configuration.addAnnotatedClass(TableInfoDAO.class);
      configuration.addAnnotatedClass(ColumnInfoDAO.class);
      configuration.addAnnotatedClass(PropertyDAO.class);
      configuration.addAnnotatedClass(FunctionInfoDAO.class);
      configuration.addAnnotatedClass(FunctionParameterInfoDAO.class);
      configuration.addAnnotatedClass(VolumeInfoDAO.class);

      ServiceRegistry serviceRegistry =
          new StandardServiceRegistryBuilder().applySettings(configuration.getProperties()).build();

      return configuration.buildSessionFactory(serviceRegistry);
    } catch (Exception e) {
      throw new RuntimeException("Exception during creation of SessionFactory", e);
    }
  }

  private static Configuration getH2Configuration() {
    Configuration configuration = new Configuration();
    configuration.setProperty("hibernate.connection.driver_class", "org.h2.Driver");

    if ("test".equals(properties.getProperty("server.env"))) {
      configuration.setProperty("hibernate.connection.url", "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
      configuration.setProperty("hibernate.hbm2ddl.auto", "create-drop");
      LOGGER.debug("Hibernate configuration set for testing");
    } else {
      configuration.setProperty(
          "hibernate.connection.url", "jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1");
      configuration.setProperty("hibernate.hbm2ddl.auto", "update");
      LOGGER.debug("Hibernate configuration set for production");
    }

    return configuration;
  }

  private static Configuration getMysqlConfiguration() {
    Configuration configuration = new Configuration();
    configuration.setProperty("hibernate.connection.driver_class", "com.mysql.cj.jdbc.Driver");
    configuration.setProperty("hibernate.connection.url", properties.getProperty("jdbc.url"));
    configuration.setProperty("hibernate.connection.user", properties.getProperty("jdbc.user"));
    configuration.setProperty(
        "hibernate.connection.password", properties.getProperty("jdbc.password"));
    configuration.setProperty("hibernate.hbm2ddl.auto", "update");
    return configuration;
  }
}
