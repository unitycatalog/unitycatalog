package io.unitycatalog.server.migrate;

import static io.unitycatalog.server.utils.TestUtils.createApiClient;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.VersionUtils;
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
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a server built from a git ref (a release tag, or this commit) on a {@link TestDatabase} the
 * way an operator runs it: its shaded jar as a child process with its own {@code etc/conf} pointed
 * at the database, Hibernate auto-update as every release shipped it, and authorization on.
 */
final class OldServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(OldServer.class);

  private static final Path JAR_CACHE =
      Path.of(System.getProperty("user.home"), ".cache", "uc-upgrade-jars");

  private OldServer() {}

  // The releases to run.

  /**
   * The releases a database passes through before this build, oldest first: every {@code vX.Y.Z}
   * tag from {@link SchemaMigrator#BASELINE_RELEASE} up to, not including, this build's version.
   * Each one writes its own copy of the data, since an upgrade can break on rows only one release
   * wrote.
   */
  static List<String> releaseChain() {
    return git("tag", "--list")
        .lines()
        .filter(tag -> tag.matches("v\\d+\\.\\d+\\.\\d+"))
        .filter(
            tag ->
                number(tag) >= number(SchemaMigrator.BASELINE_RELEASE)
                    && number(tag) < number(VersionUtils.VERSION))
        .sorted(Comparator.comparingLong(OldServer::number))
        .toList();
  }

  /** {@code v1.2.3} or {@code 1.2.3-SNAPSHOT} as a number that sorts like the version. */
  private static long number(String version) {
    String[] parts = version.replaceFirst("^v", "").replaceFirst("-.*", "").split("\\.");
    return (Long.parseLong(parts[0]) * 1_000_000 + Long.parseLong(parts[1])) * 1_000_000
        + Long.parseLong(parts[2]);
  }

  // Their server jars.

  /** The shaded server jar of {@code ref}'s commit, built once into {@link #JAR_CACHE}. */
  private static String jar(String ref, Path workDir) throws IOException {
    Path repo = Path.of(git("rev-parse", "--show-toplevel"));
    String sha = git("-C", repo.toString(), "rev-parse", ref + "^{commit}");
    Path cached = JAR_CACHE.resolve(sha + ".jar");
    if (!Files.exists(cached)) {
      build(repo, sha, cached, Files.createDirectories(workDir.resolve("source-" + sha)));
    }
    return cached.toString();
  }

  /**
   * Exports {@code sha}'s tree with {@code git archive} (nothing untracked) into {@code source},
   * builds its shaded server jar there and copies it to {@code target}.
   */
  private static void build(Path repo, String sha, Path target, Path source) throws IOException {
    Path archive = source.resolve("source.tar");
    git("-C", repo.toString(), "archive", "-o", archive.toString(), sha);
    run(new ProcessBuilder("tar", "-xf", archive.toString(), "-C", source.toString()));
    System.err.println("Building the server jar of " + sha + " in " + source);
    run(
        new ProcessBuilder("build/sbt", "-J-Xmx2G", "serverShaded/assembly")
            .directory(source.toFile())
            .redirectOutput(ProcessBuilder.Redirect.INHERIT));
    try (DirectoryStream<Path> jars =
        Files.newDirectoryStream(
            source.resolve("server-shaded/target"), "unitycatalog-server-shaded-assembly-*.jar")) {
      Path built = jars.iterator().next();
      Files.createDirectories(target.getParent());
      Path partial = Files.createTempFile(target.getParent(), sha, ".partial");
      Files.copy(built, partial, StandardCopyOption.REPLACE_EXISTING);
      Files.move(partial, target, StandardCopyOption.ATOMIC_MOVE);
    }
  }

  // Running one.

  /**
   * Starts release {@code tag} on {@code db} in a directory under {@code workDir}, waits until it
   * serves requests, passes {@code callback} a client authenticated as the server's admin, then
   * stops the server.
   */
  static void run(String tag, TestDatabase db, Path workDir, Consumer<ApiClient> callback) {
    try {
      Path home = Files.createDirectories(workDir.resolve(tag));
      Path conf = Files.createDirectories(home.resolve("etc/conf"));
      Files.writeString(conf.resolve("server.properties"), serverProperties());
      Files.writeString(conf.resolve("hibernate.properties"), hibernateProperties(db));
      // The shaded jar leaves out Vert.x's version file, which the server reads at start; the
      // test's own Vert.x jar, after it, supplies the file.
      String classpath =
          jar(tag, workDir)
              + File.pathSeparator
              + jarOf(io.vertx.core.Vertx.class)
              + (db.jdbcUrl().startsWith("jdbc:mysql:")
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
        String token = Files.readString(conf.resolve("token.txt")).trim();
        callback.accept(createApiClient(new ServerConfig(uri.toString(), token)));
      } finally {
        // SIGTERM, so the server closes the database cleanly.
        process.destroy();
        if (!process.waitFor(2, TimeUnit.MINUTES)) {
          process.destroyForcibly().waitFor();
        }
        LOGGER.info("{} stopped with exit code {}", tag, process.exitValue());
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

  // Processes.

  private static String git(String... args) {
    List<String> command = new ArrayList<>(List.of("git"));
    command.addAll(List.of(args));
    return run(new ProcessBuilder(command)).trim();
  }

  /** Runs {@code builder}, returning its standard output; its errors go to this process's. */
  private static String run(ProcessBuilder builder) {
    try {
      Process process = builder.redirectError(ProcessBuilder.Redirect.INHERIT).start();
      String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      int exit = process.waitFor();
      if (exit != 0) {
        throw new IllegalStateException(
            String.join(" ", builder.command()) + " exited with " + exit);
      }
      return output;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }
}
