package io.unitycatalog.server.testdatabase;

import java.util.Optional;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

/** Tells {@link SelectedDatabaseCheck} which {@link UsesOwnDatabase} class is running, if any. */
public class UsesOwnDatabaseExtension implements BeforeAllCallback, AfterAllCallback {

  private static final String PARALLEL_KEY = "junit.jupiter.execution.parallel.enabled";

  // One mark is enough: sbt runs one test class at a time in each forked JVM.
  private static volatile String currentOwnDatabase;

  @Override
  public void beforeAll(ExtensionContext context) {
    requireSequential(context.getConfigurationParameter(PARALLEL_KEY));
    currentOwnDatabase = reasonFor(context.getRequiredTestClass());
  }

  @Override
  public void afterAll(ExtensionContext context) {
    currentOwnDatabase = null;
  }

  static Optional<String> currentOwnDatabase() {
    return Optional.ofNullable(currentOwnDatabase);
  }

  static String reasonFor(Class<?> testClass) {
    String reason =
        AnnotationSupport.findAnnotation(testClass, UsesOwnDatabase.class)
            .map(UsesOwnDatabase::reason)
            .orElse("");
    if (reason.isBlank()) {
      throw new IllegalArgumentException(
          "@UsesOwnDatabase on " + testClass.getSimpleName() + " needs a reason");
    }
    return testClass.getSimpleName() + ": " + reason;
  }

  static void requireSequential(Optional<String> parallelEnabled) {
    if (parallelEnabled.map(Boolean::parseBoolean).orElse(false)) {
      throw new IllegalStateException(
          "@UsesOwnDatabase assumes one test class at a time per JVM, but "
              + PARALLEL_KEY
              + "=true");
    }
  }
}
