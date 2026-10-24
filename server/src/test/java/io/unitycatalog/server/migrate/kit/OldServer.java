package io.unitycatalog.server.migrate.kit;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs an {@link OldImage} on a {@link TestDatabase} the way an operator runs that release: its own
 * {@code etc/conf} files pointed at the database, Hibernate auto-update as the release shipped it,
 * and authorization on. The server jar runs as a child process in a temporary directory holding its
 * {@code etc/conf}, on the test database itself; for MySQL the test's driver jar is added to its
 * classpath.
 */
public final class OldServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OldServer.class);

  private OldServer() {}

  /**
   * Starts {@code image} on {@code db}, waits until it serves requests, passes {@code callback} a
   * client authenticated as the server's admin, then stops the server.
   */
  public static void run(OldImage image, TestDatabase db, Consumer<ApiClient> callback) {
    try {
      Path home = Files.createTempDirectory("uc-old-server");
      try {
        Path conf = Files.createDirectories(home.resolve("etc/conf"));
        Files.writeString(conf.resolve("server.properties"), serverProperties());
        Files.writeString(conf.resolve("hibernate.properties"), hibernateProperties(db));
        // The shaded jar leaves out Vert.x's version file, which the server reads at start; the
        // test's own Vert.x jar, after it, supplies the file.
        String classpath =
            image.name()
                + File.pathSeparator
                + jarOf(io.vertx.core.Vertx.class)
                + (db.kind() == DatabaseKind.MYSQL
                    ? File.pathSeparator + jarOf(com.mysql.cj.jdbc.Driver.class)
                    : "");
        int port = freePortPair();
        Process process =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp",
                    classpath,
                    "io.unitycatalog.server.UnityCatalogServer",
                    "-p",
                    String.valueOf(port))
                .directory(home.toFile())
                .inheritIO()
                .start();
        try {
          URI uri = URI.create("http://localhost:" + port);
          awaitServing(process, uri);
          callback.accept(apiClient(uri, Files.readString(conf.resolve("token.txt")).trim()));
        } finally {
          // SIGTERM, so the server closes the database cleanly.
          process.destroy();
          if (!process.waitFor(2, TimeUnit.MINUTES)) {
            process.destroyForcibly().waitFor();
          }
          LOGGER.info("{} stopped with exit code {}", image, process.exitValue());
        }
      } finally {
        delete(home);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** Waits until the server answers below 500. */
  private static void awaitServing(Process process, URI uri) throws InterruptedException {
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder(uri.resolve("/api/2.1/unity-catalog/catalogs")).build();
    long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
    while (System.nanoTime() < deadline) {
      if (!process.isAlive()) {
        throw new IllegalStateException("The old server exited with code " + process.exitValue());
      }
      try {
        if (client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() < 500) {
          return;
        }
      } catch (IOException e) {
        // Not listening yet.
      }
      Thread.sleep(500);
    }
    throw new IllegalStateException("The old server did not start within 3 minutes");
  }

  /** A port whose next port is free too: the server listens on both. */
  private static int freePortPair() throws IOException {
    while (true) {
      try (ServerSocket first = new ServerSocket(0);
          ServerSocket second = new ServerSocket(first.getLocalPort() + 1)) {
        return first.getLocalPort();
      } catch (BindException | IllegalArgumentException e) {
        // The next port is taken; try another pair.
      }
    }
  }

  private static void delete(Path root) {
    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
    } catch (IOException e) {
      // A leftover temp directory is harmless.
    }
  }

  private static String serverProperties() {
    return String.join(
        "\n",
        "server.authorization=enable",
        "storage-root.tables=file:///tmp/uc-storage",
        "server.external-local-roots=/tmp/uc-storage",
        "");
  }

  /** The connection settings of {@code db}. */
  private static String hibernateProperties(TestDatabase db) {
    Properties test = db.hibernateProperties();
    return String.join(
        "\n",
        "hibernate.connection.driver_class="
            + test.getProperty("hibernate.connection.driver_class"),
        "hibernate.connection.url=" + db.jdbcUrl(),
        "hibernate.connection.username=" + test.getProperty("hibernate.connection.username"),
        "hibernate.connection.password=" + test.getProperty("hibernate.connection.password"),
        // What every published release shipped in etc/conf/hibernate.properties.
        "hibernate.hbm2ddl.auto=update",
        "hibernate.archive.autodetection=class",
        "");
  }

  /** The jar on this test's classpath that holds {@code type}. */
  private static Path jarOf(Class<?> type) {
    try {
      return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  static ApiClient apiClient(URI uri, String token) {
    return ApiClientBuilder.create()
        .uri(uri)
        .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
        .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
        .build();
  }
}
