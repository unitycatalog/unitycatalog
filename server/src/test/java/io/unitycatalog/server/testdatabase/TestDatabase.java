package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

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
  private HikariDataSource dataSource;

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

  /** A small connection pool on this database, closed by {@link #close()}. */
  public synchronized DataSource dataSource() {
    if (dataSource == null) {
      HikariConfig config = new HikariConfig();
      config.setDriverClassName(driver);
      config.setJdbcUrl(url);
      config.setUsername(user);
      config.setPassword(password);
      // Room for the concurrent-run test: the test's lock, two waiting runs and one more
      // connection.
      config.setMaximumPoolSize(4);
      dataSource = new HikariDataSource(config);
    }
    return dataSource;
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

  /**
   * Whether some connection to this database is blocked waiting for a row lock that another
   * connection holds. Tests poll this to know a racing operation has reached its lock, instead of
   * sleeping. On MySQL, {@code innodb_trx} covers the whole server, not only this database, so this
   * holds only while server tests run one at a time.
   */
  public boolean hasLockWaiter() {
    String query =
        switch (SupportedDatabase.selected()) {
          case H2 ->
              "SELECT COUNT(*) FROM INFORMATION_SCHEMA.SESSIONS WHERE BLOCKER_ID IS NOT NULL";
          case POSTGRES_18 ->
              "SELECT COUNT(*) FROM pg_stat_activity"
                  + " WHERE datname = current_database() AND wait_event_type = 'Lock'";
          case MYSQL_8 ->
              "SELECT COUNT(*) FROM information_schema.innodb_trx WHERE trx_state = 'LOCK WAIT'";
        };
    try (Connection connection = DriverManager.getConnection(url, user, password);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query)) {
      result.next();
      return result.getInt(1) > 0;
    } catch (SQLException e) {
      throw new IllegalStateException("Could not look for blocked connections on " + url, e);
    }
  }

  /**
   * Races {@code operation} against an uncommitted write. {@code heldWrite} runs in a raw session
   * and is flushed, so its row locks stay held; {@code operation} then runs on another thread. Once
   * the database reports a connection blocked on a row lock, the held write commits and the
   * operation's result is returned (or its exception rethrown). Fails if the operation finishes
   * without ever waiting for the lock, since the test would then not exercise the race.
   */
  public <T> T runBlockedByHeldWrite(Consumer<Session> heldWrite, Callable<T> operation)
      throws Exception {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Session held = sessionFactory().openSession()) {
      Transaction transaction = held.beginTransaction();
      heldWrite.accept(held);
      held.flush();
      Future<T> result = executor.submit(operation);
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!hasLockWaiter()) {
        if (result.isDone() || System.nanoTime() > deadline) {
          transaction.rollback();
          if (result.isDone()) {
            // Surface an unrelated early failure instead of reporting a missed lock wait.
            try {
              result.get();
            } catch (ExecutionException e) {
              throw e.getCause() instanceof Exception cause ? cause : e;
            }
          }
          fail("The operation did not wait for the held write's lock");
        }
        // Poll gently: on MySQL, reading innodb_trx every few milliseconds stalls the very
        // statement being waited for.
        Thread.sleep(100);
      }
      transaction.commit();
      try {
        return result.get(30, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
        throw e.getCause() instanceof Exception cause ? cause : e;
      }
    } finally {
      executor.shutdownNow();
    }
  }

  /** Sets one column of the {@code entity} row {@code id} in {@code session}, for a held write. */
  public static void setColumn(
      Session session, Class<?> entity, UUID id, String attribute, Object value) {
    session
        .createMutationQuery(
            "update " + entity.getSimpleName() + " set " + attribute + " = :value where id = :id")
        .setParameter("value", value)
        .setParameter("id", id)
        .executeUpdate();
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
