package io.unitycatalog.server.testdatabase;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.unitycatalog.server.utils.ServerProperties;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;
import org.hibernate.integrator.spi.Integrator;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.LauncherSessionListener;

/**
 * Proves the test-database safeguards are registered in this project's test JVM. Each project that
 * runs the server test harness has a one-line subclass: the server-shaded jar drops META-INF, so
 * without this a missing registration would switch the safeguards off silently.
 */
public abstract class AbstractTestDatabaseRegistrationTest {

  // Public: subclasses live in other packages (cli, spark, hadoop).
  @Test
  public void selectionCheckIsRegistered() {
    assertTrue(isRegistered(LauncherSessionListener.class, SelectionCheck.class));
  }

  @Test
  public void selectedDatabaseCheckIsRegistered() {
    assertTrue(isRegistered(Integrator.class, SelectedDatabaseCheck.class));
  }

  @Test
  public void selectedDatabaseCheckRunsOnSessionFactory() {
    int before = SelectedDatabaseCheck.checkCount();
    try (TestDatabase database = TestDatabases.newDatabase()) {
      database.openHibernateConfigurator(new ServerProperties(new Properties()));
    }
    assertTrue(SelectedDatabaseCheck.checkCount() > before);
  }

  private static boolean isRegistered(Class<?> service, Class<?> implementation) {
    return StreamSupport.stream(ServiceLoader.load(service).spliterator(), false)
        .anyMatch(implementation::isInstance);
  }
}
