package io.unitycatalog.server.testdatabase;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * One test's database on the selected backend. {@link #close()} closes every configurator opened
 * through it, then removes the database.
 */
public final class TestDatabase implements AutoCloseable {

  private final String driver;
  private final String url;
  private final String user;
  private final String password;
  private final String hbm2ddlAuto;
  private final Runnable release;
  private final List<HibernateConfigurator> openedConfigurators = new ArrayList<>();

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
   * The server's Hibernate properties pointed at this database. Overrides what {@code
   * server.env=test} forces, and makes a failed schema statement throw instead of logging.
   */
  public Properties hibernateProperties(ServerProperties serverProperties) {
    Properties properties = HibernateConfigurator.setupHibernateProperties(serverProperties);
    properties.setProperty("hibernate.connection.driver_class", driver);
    properties.setProperty("hibernate.connection.url", url);
    properties.setProperty("hibernate.connection.username", user);
    properties.setProperty("hibernate.connection.password", password);
    properties.setProperty("hibernate.hbm2ddl.auto", hbm2ddlAuto);
    properties.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
    return properties;
  }

  /** A configurator on this database that {@link #close()} closes. */
  public HibernateConfigurator openHibernateConfigurator(ServerProperties serverProperties) {
    return openHibernateConfigurator(hibernateProperties(serverProperties));
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
      for (int i = openedConfigurators.size() - 1; i >= 0; i--) {
        openedConfigurators.get(i).close();
      }
    } finally {
      openedConfigurators.clear();
      release.run();
    }
  }
}
