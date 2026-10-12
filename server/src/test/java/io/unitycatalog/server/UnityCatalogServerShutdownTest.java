package io.unitycatalog.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import io.unitycatalog.server.model.TemporaryCredentials;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.service.credential.CloudCredentialVendor;
import io.unitycatalog.server.service.credential.CredentialContext;
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
 * {@link UnityCatalogServer#close()} must not wait forever on a wedged request: graceful stop and
 * the blocking-executor drain share one {@code server.shutdown-timeout}, then close logs a warning
 * and returns.
 */
class UnityCatalogServerShutdownTest {

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(2);

  /**
   * Armeria's stop ends with a no-argument {@code shutdownGracefully()} on its internal boss event
   * loop, whose 2s quiet period is added to every stop, idle or not, and cannot be configured.
   */
  private static final Duration ARMERIA_STOP_TAIL = Duration.ofSeconds(2);

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
      int port = findAvailablePort();
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(port)
              .serverProperties(serverProperties)
              .hibernateConfigurator(hibernateConfigurator)
              .credentialOperations(new WedgedCredentialVendor(serverProperties, entered, release))
              .build();
      // Attach after build(): loading UnityCatalogServer (re)initializes log4j. The test JVM may
      // not find etc/conf/server.log4j2.properties and fall back to ERROR, so enable WARN here.
      serverLogger = (Logger) LogManager.getLogger(UnityCatalogServer.class);
      previousLevel = serverLogger.getLevel();
      Configurator.setLevel(serverLogger.getName(), Level.WARN);
      appender.start();
      serverLogger.addAppender(appender);

      server.start();
      WebClient.of("http://127.0.0.1:" + port)
          .execute(
              HttpRequest.builder()
                  .post("/api/2.1/unity-catalog/temporary-path-credentials")
                  .content(
                      MediaType.JSON,
                      "{\"url\":\"s3://bucket/wedged\",\"operation\":\"PATH_READ\"}")
                  .build())
          .aggregate();
      assertThat(entered.await(5, TimeUnit.SECONDS))
          .as("the request reaches the credential vendor")
          .isTrue();

      long startNanos = System.nanoTime();
      server.close();
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startNanos);

      // Graceful stop waits the whole budget for the in-flight request, which leaves the drain
      // nothing: about 2s + 2s here. Separate budgets for stop and drain would add another 2s.
      assertThat(elapsed)
          .as("close() waits for the wedged request, but only up to server.shutdown-timeout")
          .isGreaterThanOrEqualTo(SHUTDOWN_TIMEOUT.minusMillis(100))
          .isLessThan(SHUTDOWN_TIMEOUT.plus(ARMERIA_STOP_TAIL).plusSeconds(1));
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

  /**
   * Holds the request inside the handler, on the blocking executor, the way a stuck JDBC call does.
   */
  private static final class WedgedCredentialVendor extends CloudCredentialVendor {

    private final CountDownLatch entered;
    private final CountDownLatch release;

    private WedgedCredentialVendor(
        ServerProperties serverProperties, CountDownLatch entered, CountDownLatch release) {
      super(serverProperties);
      this.entered = entered;
      this.release = release;
    }

    @Override
    public TemporaryCredentials vendCredential(CredentialContext context) {
      entered.countDown();
      try {
        release.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return new TemporaryCredentials();
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
