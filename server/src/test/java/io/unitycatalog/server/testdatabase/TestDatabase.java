package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
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
  private final String hbm2ddlAuto;
  private final Runnable release;
  private final List<HibernateConfigurator> openedConfigurators = new ArrayList<>();
  private HibernateConfigurator defaultConfigurator;

  TestDatabase(
      String driver,
      String url,
      String user,
      String password,
      String hbm2ddlAuto,
      Runnable release) {
    this.driver = driver;
    this.url = url;
    this.user = user;
    this.password = password;
    this.hbm2ddlAuto = hbm2ddlAuto;
    this.release = release;
  }

  public String jdbcUrl() {
    return url;
  }

  /**
   * The server's Hibernate properties from {@code etc/conf/hibernate.properties}, pointed at this
   * database, with a failed schema statement made to throw instead of being logged.
   */
  public Properties hibernateProperties() {
    Properties properties =
        HibernateConfigurator.setupHibernateProperties(new ServerProperties(new Properties()));
    properties.setProperty("hibernate.connection.driver_class", driver);
    properties.setProperty("hibernate.connection.url", url);
    properties.setProperty("hibernate.connection.username", user);
    properties.setProperty("hibernate.connection.password", password);
    properties.setProperty("hibernate.hbm2ddl.auto", hbm2ddlAuto);
    properties.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
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
      release.run();
    }
  }
}
