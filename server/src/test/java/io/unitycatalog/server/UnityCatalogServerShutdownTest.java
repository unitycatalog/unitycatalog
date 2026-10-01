package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link UnityCatalogServer#close()} must not wait forever on a wedged blocking task: it gives the
 * executor what is left of {@code server.shutdown-timeout}, logs a warning, and returns.
 */
class UnityCatalogServerShutdownTest {

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(1);

  @TempDir Path tempDir;

  @Test
  void closeReturnsWithinShutdownTimeoutWhenBlockingTaskIsWedged() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    properties.setProperty(Property.TABLE_STORAGE_ROOT.getKey(), tempDir.toUri().toString());
    properties.setProperty(Property.SHUTDOWN_TIMEOUT.getKey(), SHUTDOWN_TIMEOUT.toString());
    ServerProperties serverProperties = new ServerProperties(properties);
    HibernateConfigurator hibernateConfigurator =
        new HibernateConfigurator(HibernateConfigurator.setupHibernateProperties(serverProperties));
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CapturingAppender appender = new CapturingAppender();
    Logger serverLogger = null;
    Level previousLevel = null;
    try {
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(findAvailablePort())
              .serverProperties(serverProperties)
              .hibernateConfigurator(hibernateConfigurator)
              .build();
      // Attach after build(): loading UnityCatalogServer (re)initializes log4j. The test JVM may
      // not find etc/conf/server.log4j2.properties and fall back to ERROR, so enable WARN here.
      serverLogger = (Logger) LogManager.getLogger(UnityCatalogServer.class);
      previousLevel = serverLogger.getLevel();
      Configurator.setLevel(serverLogger.getName(), Level.WARN);
      appender.start();
      serverLogger.addAppender(appender);

      server.start();
      server
          .blockingTaskExecutor()
          .submit(
              () -> {
                entered.countDown();
                release.await();
                return null;
              });
      assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

      long startNanos = System.nanoTime();
      server.close();
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

      assertThat(elapsed)
          .as("close() waits for the wedged task, but only up to server.shutdown-timeout")
          .isGreaterThanOrEqualTo(SHUTDOWN_TIMEOUT.minusMillis(100))
          .isLessThan(SHUTDOWN_TIMEOUT.plusSeconds(2));
      assertThat(appender.warnings())
          .anyMatch(message -> message.startsWith("Blocking task executor did not stop within"));
    } finally {
      release.countDown();
      if (serverLogger != null) {
        serverLogger.removeAppender(appender);
        Configurator.setLevel(serverLogger.getName(), previousLevel);
      }
      appender.stop();
      hibernateConfigurator.close();
    }
  }

  private static int findAvailablePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static final class CapturingAppender extends AbstractAppender {

    private final List<String> warnings = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super(
          "UnityCatalogServerShutdownTest",
          null,
          null,
          true,
          org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
      if (event.getLevel() == Level.WARN) {
        warnings.add(event.getMessage().getFormattedMessage());
      }
    }

    List<String> warnings() {
      return warnings;
    }
  }
}
