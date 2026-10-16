package io.unitycatalog.server.testdatabase;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.service.spi.SessionFactoryServiceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fails any SessionFactory built in a test JVM whose live database is not the one selected by
 * {@value SupportedDatabase#ENV}. Hibernate calls every registered {@link Integrator} for every
 * SessionFactory, so this also catches tests that build their own H2 configuration.
 */
public class SelectedDatabaseCheck implements Integrator {

  private static final Logger LOGGER = LoggerFactory.getLogger(SelectedDatabaseCheck.class);
  private static final AtomicInteger CHECKS = new AtomicInteger();

  @Override
  public void integrate(
      Metadata metadata, BootstrapContext bootstrapContext, SessionFactoryImplementor factory) {
    CHECKS.incrementAndGet();
    verify(
        SupportedDatabase.selected(),
        factory.getJdbcServices().getDialect(),
        UsesOwnDatabaseExtension.currentOwnDatabase());
  }

  @Override
  public void disintegrate(
      SessionFactoryImplementor factory, SessionFactoryServiceRegistry serviceRegistry) {}

  static void verify(SupportedDatabase selected, Dialect dialect, Optional<String> ownDatabase) {
    if (selected.matches(dialect)) {
      return;
    }
    if (ownDatabase.isPresent()) {
      LOGGER.info(
          "Using its own database ({}): {}",
          SupportedDatabase.describe(dialect),
          ownDatabase.get());
      return;
    }
    throw new IllegalStateException(
        "SelectedDatabaseCheck: expected "
            + selected
            + ", connected to "
            + SupportedDatabase.describe(dialect)
            + ". If this test intentionally uses its own database, annotate it with"
            + " @UsesOwnDatabase(reason = \"…\").");
  }

  /** How many SessionFactories this JVM has checked; lets a test prove the check is active. */
  static int checkCount() {
    return CHECKS.get();
  }
}
