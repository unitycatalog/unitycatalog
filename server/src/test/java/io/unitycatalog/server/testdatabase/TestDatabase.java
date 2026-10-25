package io.unitycatalog.server.testdatabase;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
import org.hibernate.SessionFactory;

/**
 * One test's database on the selected backend. {@link #close()} closes every configurator opened
 * through it, then removes the database, even if closing one fails.
 */
public final class TestDatabase implements AutoCloseable {

  private final String driver;
  private final String url;
  private final String user;
  private final String password;
  private final Runnable release;
  private final List<HibernateConfigurator> openedConfigurators = new ArrayList<>();
  private HibernateConfigurator defaultConfigurator;
  private HikariDataSource dataSource;

  TestDatabase(String driver, String url, String user, String password, Runnable release) {
    this.driver = driver;
    this.url = url;
    this.user = user;
    this.password = password;
    this.release = release;
  }

  public String jdbcUrl() {
    return url;
  }

  /** A small connection pool on this database, closed by {@link #close()}. */
  public synchronized DataSource dataSource() {
    if (dataSource == null) {
      HikariConfig config = new HikariConfig();
      config.setDriverClassName(driver);
      config.setJdbcUrl(url);
      config.setUsername(user);
      config.setPassword(password);
      // Two migration runs at once: one holds its run lock's connection and Flyway's two, the other
      // waits for the lock on its own.
      config.setMaximumPoolSize(4);
      dataSource = new HikariDataSource(config);
    }
    return dataSource;
  }

  /**
   * The server's Hibernate properties pointed at this database, with {@code hbm2ddl.auto=none}: the
   * migration steps own the schema.
   */
  public Properties hibernateProperties() {
    Properties properties =
        HibernateConfigurator.setupHibernateProperties(new ServerProperties(new Properties()));
    properties.setProperty("hibernate.connection.driver_class", driver);
    properties.setProperty("hibernate.connection.url", url);
    properties.setProperty("hibernate.connection.username", user);
    properties.setProperty("hibernate.connection.password", password);
    properties.setProperty("hibernate.hbm2ddl.auto", "none");
    return properties;
  }

  /** The default configurator on this database, opened on first use; {@link #close()} closes it. */
  public synchronized HibernateConfigurator hibernateConfigurator() {
    if (defaultConfigurator == null) {
      defaultConfigurator = openHibernateConfigurator(hibernateProperties());
    }
    return defaultConfigurator;
  }

  public SessionFactory sessionFactory() {
    return hibernateConfigurator().getSessionFactory();
  }

  /** A configurator built from {@code hibernateProperties} that {@link #close()} closes. */
  public HibernateConfigurator openHibernateConfigurator(Properties hibernateProperties) {
    HibernateConfigurator configurator = new HibernateConfigurator(hibernateProperties);
    openedConfigurators.add(configurator);
    return configurator;
  }

  @Override
  public void close() {
    try {
      RuntimeException failure = null;
      for (int i = openedConfigurators.size() - 1; i >= 0; i--) {
        try {
          openedConfigurators.get(i).close();
        } catch (RuntimeException e) {
          if (failure == null) {
            failure = e;
          } else {
            failure.addSuppressed(e);
          }
        }
      }
      if (failure != null) {
        throw failure;
      }
    } finally {
      openedConfigurators.clear();
      defaultConfigurator = null;
      try {
        if (dataSource != null) {
          dataSource.close();
        }
      } finally {
        dataSource = null;
        release.run();
      }
    }
  }
}
