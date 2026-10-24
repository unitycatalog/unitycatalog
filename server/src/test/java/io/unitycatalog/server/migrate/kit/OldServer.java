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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * Runs an {@link OldImage} on a {@link TestDatabase} the way an operator runs that release: its own
 * {@code etc/conf} files pointed at the database, Hibernate auto-update as the release shipped it,
 * and authorization on.
 *
 * <ul>
 *   <li>A server jar runs as a child process in a temporary directory holding its {@code etc/conf},
 *       on the test database itself; for MySQL the test's driver jar is added to its classpath.
 *   <li>An image runs in a container. H2: the database file is copied into the container and back
 *       out after the server stops (the image runs as a non-root user, so no bind mount).
 *       PostgreSQL and MySQL: the server reaches the test database's container through the host.
 *       MySQL: images bundle no MySQL driver; the test's driver jar is copied in and appended to
 *       the server's classpath file.
 * </ul>
 */
public final class OldServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OldServer.class);
  private static final String HOME = "/home/unitycatalog";
  // Not the image's own etc/db/h2db, which ships sample data.
  private static final String H2_FILE = HOME + "/etc/db/upgrade-test.mv.db";
  private static final String MYSQL_DRIVER = "/opt/uc-test/mysql-connector-j.jar";
  private static final String CLASSPATH_FILE = HOME + "/server/target/classpath";
  private static final int PORT = 8080;
  private static final Map<String, String> CLASSPATHS = new ConcurrentHashMap<>();

  private OldServer() {}

  /**
   * Starts {@code image} on {@code db}, waits until it serves requests, passes {@code callback} a
   * client authenticated as the server's admin, then stops the server.
   */
  public static void run(OldImage image, TestDatabase db, Consumer<ApiClient> callback) {
    if (image.isJar()) {
      runJar(image, db, callback);
      return;
    }
    String imageName = image.name();
    DatabaseKind kind = db.kind();
    try (GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(imageName))) {
      container
          .withExposedPorts(PORT)
          .withLogConsumer(new Slf4jLogConsumer(LOGGER).withPrefix(image.toString()))
          .withCopyToContainer(
              Transferable.of(serverProperties()), HOME + "/etc/conf/server.properties")
          .withCopyToContainer(
              Transferable.of(
                  hibernateProperties(
                      db,
                      kind == DatabaseKind.H2
                          ? "jdbc:h2:file:./etc/db/upgrade-test;DB_CLOSE_DELAY=-1"
                          : throughHost(db.jdbcUrl()))),
              HOME + "/etc/conf/hibernate.properties")
          .waitingFor(
              Wait.forHttp("/api/2.1/unity-catalog/catalogs")
                  .forPort(PORT)
                  .forStatusCodeMatching(code -> code < 500)
                  .withStartupTimeout(Duration.ofMinutes(3)));
      Path h2File = kind == DatabaseKind.H2 ? h2File(db) : null;
      if (h2File != null && Files.exists(h2File)) {
        container.withCopyToContainer(MountableFile.forHostPath(h2File, 0666), H2_FILE);
      }
      if (kind == DatabaseKind.MYSQL) {
        container
            .withCopyToContainer(MountableFile.forHostPath(mysqlDriver(), 0644), MYSQL_DRIVER)
            .withCopyToContainer(
                Transferable.of(classpath(imageName) + ":" + MYSQL_DRIVER, 0644), CLASSPATH_FILE);
      }
      container.start();

      String token =
          container
              .copyFileFromContainer(
                  HOME + "/etc/conf/token.txt",
                  in -> new String(in.readAllBytes(), StandardCharsets.UTF_8))
              .trim();
      callback.accept(
          apiClient(
              URI.create("http://" + container.getHost() + ":" + container.getMappedPort(PORT)),
              token));

      stop(container);
      if (h2File != null) {
        container.copyFileFromContainer(H2_FILE, h2File.toString());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Runs the server jar {@code image} as a child process; see {@link #run}. */
  private static void runJar(OldImage image, TestDatabase db, Consumer<ApiClient> callback) {
    try {
      Path home = Files.createTempDirectory("uc-old-server");
      try {
        Path conf = Files.createDirectories(home.resolve("etc/conf"));
        Files.writeString(conf.resolve("server.properties"), serverProperties());
        Files.writeString(
            conf.resolve("hibernate.properties"), hibernateProperties(db, db.jdbcUrl()));
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

  /** Waits until the server answers below 500, as the container's wait strategy does. */
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

  /**
   * Stops the server with SIGTERM so it closes the database cleanly. The start script runs Java
   * without {@code exec}, so the signal goes to the Java process, not the container's first one.
   */
  private static void stop(GenericContainer<?> container) throws IOException {
    try {
      container.execInContainer("pkill", "-TERM", "java");
      int exit =
          container
              .getDockerClient()
              .waitContainerCmd(container.getContainerId())
              .start()
              .awaitStatusCode(2, TimeUnit.MINUTES);
      LOGGER.info("{} stopped with exit code {}", container.getDockerImageName(), exit);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
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

  /** The connection settings of {@code db}, as the old server reaches it at {@code url}. */
  private static String hibernateProperties(TestDatabase db, String url) {
    Properties test = db.hibernateProperties();
    return String.join(
        "\n",
        "hibernate.connection.driver_class="
            + test.getProperty("hibernate.connection.driver_class"),
        "hibernate.connection.url=" + url,
        "hibernate.connection.username=" + test.getProperty("hibernate.connection.username"),
        "hibernate.connection.password=" + test.getProperty("hibernate.connection.password"),
        // What every published release shipped in etc/conf/hibernate.properties.
        "hibernate.hbm2ddl.auto=update",
        "hibernate.archive.autodetection=class",
        "");
  }

  /** {@code url} with its host replaced by the name containers reach this host's ports through. */
  private static String throughHost(String url) {
    URI uri = URI.create(url.substring("jdbc:".length()));
    Testcontainers.exposeHostPorts(uri.getPort());
    return "jdbc:"
        + uri.getScheme()
        + "://host.testcontainers.internal:"
        + uri.getPort()
        + uri.getRawPath()
        + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
  }

  private static Path h2File(TestDatabase db) {
    return Path.of(db.jdbcUrl().substring("jdbc:h2:file:".length()) + ".mv.db");
  }

  private static Path mysqlDriver() {
    return jarOf(com.mysql.cj.jdbc.Driver.class);
  }

  /** The jar on this test's classpath that holds {@code type}. */
  private static Path jarOf(Class<?> type) {
    try {
      return Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The image's own server classpath file, read once per image. */
  private static String classpath(String imageName) {
    return CLASSPATHS.computeIfAbsent(
        imageName,
        name -> {
          try (GenericContainer<?> cat = new GenericContainer<>(DockerImageName.parse(name))) {
            cat.withCommand("cat", CLASSPATH_FILE)
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy());
            cat.start();
            return cat.getLogs(OutputFrame.OutputType.STDOUT).trim();
          }
        });
  }

  static ApiClient apiClient(URI uri, String token) {
    return ApiClientBuilder.create()
        .uri(uri)
        .tokenProvider(TokenProvider.create(Map.of("type", "static", "token", token)))
        .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
        .build();
  }
}
