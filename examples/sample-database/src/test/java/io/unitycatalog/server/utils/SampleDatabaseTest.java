package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** {@link SampleDatabase} builds the sample database file. */
public class SampleDatabaseTest {

  @Test
  void buildsTheSampleDatabase(@TempDir Path dir) throws Exception {
    // In a folder that doesn't exist yet, as the tarball builds it.
    Path file = dir.resolve("sample-database").resolve("h2db.mv.db");

    SampleDatabase.build(file);

    assertThat(file).exists();
    // The schema is what this build's steps build, as bin/start-uc-server --check-schema finds.
    Path conf = Files.createDirectories(dir.resolve("conf"));
    Files.writeString(conf.resolve("server.properties"), "server.env=dev\n");
    Files.writeString(
        conf.resolve("hibernate.properties"),
        "hibernate.connection.driver_class=org.h2.Driver\n"
            + "hibernate.connection.url=jdbc:h2:file:"
            + file.resolveSibling("h2db").toAbsolutePath()
            + ";DB_CLOSE_ON_EXIT=FALSE;DB_CLOSE_DELAY=0\n"
            + "hibernate.hbm2ddl.auto=none\n");
    assertThat(
            SchemaMigrator.runCommand(
                false, conf, new PrintStream(OutputStream.nullOutputStream())))
        .isZero();
    String url = "jdbc:h2:file:" + file.resolveSibling("h2db").toAbsolutePath();
    try (Connection connection = DriverManager.getConnection(url, "", "")) {
      assertThat(names(connection, "SELECT name FROM uc_catalogs")).containsExactly("unity");
      assertThat(names(connection, "SELECT name FROM uc_tables"))
          .containsExactlyInAnyOrder("marksheet", "marksheet_uniform", "numbers", "user_countries");
      assertThat(names(connection, "SELECT name FROM uc_volumes"))
          .containsExactlyInAnyOrder("txt_files", "json_files");
      assertThat(names(connection, "SELECT name FROM uc_functions"))
          .containsExactlyInAnyOrder("sum", "lowercase");
      assertThat(
              names(
                  connection,
                  "SELECT c.name FROM uc_columns c JOIN uc_tables t ON c.table_id = t.id"
                      + " WHERE t.name = 'marksheet' ORDER BY c.ordinal_position"))
          .containsExactly("id", "name", "marks");
    }
  }

  @Test
  void refusesAnExistingFileAndLeavesItUnchanged(@TempDir Path dir) throws IOException {
    Path file = dir.resolve("h2db.mv.db");
    byte[] content = {1, 2, 3};
    Files.write(file, content);

    assertThatThrownBy(() -> SampleDatabase.build(file))
        .isInstanceOf(UncheckedIOException.class)
        .hasCauseExactlyInstanceOf(FileAlreadyExistsException.class);

    assertThat(file).hasBinaryContent(content);
  }

  @Test
  void ignoresTheDeploymentEnvironmentVariables(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("h2db.mv.db");
    // An environment that makes any server configuration without OPA authorization settings
    // invalid.
    ProcessBuilder build =
        new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty("java.class.path"),
            SampleDatabase.class.getName(),
            file.toString());
    build.environment().put("UC_SERVER_AUTHORIZATION_PROVIDER", "opa");
    build.redirectErrorStream(true).redirectOutput(dir.resolve("build.log").toFile());

    assertThat(build.start().waitFor()).isZero();
    assertThat(file).exists();
  }

  @Test
  void buildsAtTheSameTimeWithoutDeletingEachOthersFile(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("h2db.mv.db");
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> builds = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        builds.add(
            pool.submit(
                () -> {
                  start.await();
                  SampleDatabase.build(file);
                  return null;
                }));
      }
      start.countDown();
      int failed = 0;
      for (Future<?> build : builds) {
        try {
          build.get();
        } catch (ExecutionException e) {
          // the build that finds the file already there
          assertThat(e.getCause())
              .isInstanceOf(UncheckedIOException.class)
              .hasCauseExactlyInstanceOf(FileAlreadyExistsException.class);
          failed++;
        }
      }

      // The build that finishes second finds the first one's file there, and leaves it alone.
      assertThat(failed).isOne();
      assertThat(file).exists();
      try (Stream<Path> files = Files.list(dir)) {
        assertThat(files.map(f -> f.getFileName().toString())).containsExactly("h2db.mv.db");
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private static List<String> names(Connection connection, String query) throws SQLException {
    List<String> names = new ArrayList<>();
    try (ResultSet rows = connection.createStatement().executeQuery(query)) {
      while (rows.next()) {
        names.add(rows.getString(1));
      }
    }
    return names;
  }

  /**
   * Whether the option parser takes {@code args} as a start of the server: no schema command, no
   * --version, no --help, and no parse error.
   */
  private static boolean startsServer(String[] args) {
    int[] exitCode = {-128};
    OptionParser parser =
        new OptionParser() {
          @Override
          protected void exit(int code) {
            exitCode[0] = code;
          }
        };
    PrintStream out = System.out;
    System.setOut(new PrintStream(OutputStream.nullOutputStream()));
    try {
      parser.parse(args);
    } finally {
      System.setOut(out);
    }
    return exitCode[0] == -128 && !parser.isCheckSchema() && !parser.isUpgradeSchema();
  }

  // Every spelling the option parser takes for a command line, started in a source checkout with
  // no sample database: the launcher builds the database only when the server starts.
  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "-p 9",
        "-p9",
        "-p=9",
        "--port=9",
        "--port 9",
        "--po 9",
        "-port 9",
        "--o 9",
        "--obs-port 9",
        "-p 9 --obs 8",
        "foo",
        "-p 9 foo",
        "--check-schema",
        "--che",
        "-c",
        "--upgrade-schema",
        "-v",
        "--help",
        "-he",
        "-p 9 --check",
        "-p",
        "-p --check",
        "-pv",
        "-x",
        "--unknown",
        "-- --check",
        "-p 9 -- --check",
      })
  void buildsTheSampleDatabaseOnlyWhereTheServerStarts(String commandLine, @TempDir Path dir)
      throws Exception {
    Path calls = dir.resolve("sbt-calls");
    createSourceCheckout(dir, "echo \"$@\" >> '" + calls + "'\n", "exit 0\n");

    String[] args = commandLine.isEmpty() ? new String[0] : commandLine.split(" ");
    ProcessBuilder launcher = new ProcessBuilder();
    launcher.command().add(dir.resolve("bin/start-uc-server").toString());
    launcher.command().addAll(List.of(args));
    launcher.environment().put("PATH", dir.resolve("fake") + ":" + System.getenv("PATH"));
    launcher.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
    assertThat(launcher.start().waitFor()).isZero();

    boolean built = Files.exists(calls) && Files.readString(calls).contains("buildSampleDatabase");
    assertThat(built).as(commandLine).isEqualTo(startsServer(args));
  }

  @Test
  void startsTheServerWhenBuildingTheSampleDatabaseFails(@TempDir Path dir) throws Exception {
    Path javaRan = dir.resolve("java-ran");
    createSourceCheckout(dir, "exit 1\n", "touch '" + javaRan + "'\n");

    ProcessBuilder launcher = new ProcessBuilder(dir.resolve("bin/start-uc-server").toString());
    launcher.environment().put("PATH", dir.resolve("fake") + ":" + System.getenv("PATH"));
    launcher.redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD);
    assertThat(launcher.start().waitFor()).isZero();

    assertThat(javaRan).exists();
  }

  @Test
  void theLauncherKnowsEveryServerOption() {
    // A new option must also be added to starts_server in bin/start-uc-server.
    List<String> names = new ArrayList<>();
    for (org.apache.commons.cli.Option o : new OptionParser().getOptions().getOptions()) {
      names.add(o.getOpt() + "/" + o.getLongOpt());
    }
    assertThat(names)
        .as("A new option must be added to starts_server in bin/start-uc-server")
        .containsExactlyInAnyOrder(
            "p/port",
            "null/obs-port",
            "null/check-schema",
            "null/upgrade-schema",
            "v/version",
            "h/help");
  }

  /**
   * Lays out a source checkout in {@code dir} whose build/sbt runs {@code sbtBody} and whose java
   * runs {@code javaBody}.
   */
  private static void createSourceCheckout(Path dir, String sbtBody, String javaBody)
      throws IOException {
    Path root = Path.of("").toAbsolutePath();
    while (root != null && !Files.isRegularFile(root.resolve("bin/start-uc-server"))) {
      root = root.getParent();
    }
    assertThat(root).isNotNull();
    // A source checkout whose build/sbt only records its arguments and whose java does nothing.
    Path bin = Files.createDirectories(dir.resolve("bin"));
    Files.copy(root.resolve("bin/start-uc-server"), bin.resolve("start-uc-server"));
    Files.createDirectories(dir.resolve("server/src/main/java/io/unitycatalog/server"));
    Files.createFile(
        dir.resolve("server/src/main/java/io/unitycatalog/server/UnityCatalogServer.java"));
    Files.createDirectories(dir.resolve("server/target"));
    Files.createFile(dir.resolve("server/target/unitycatalog-server.jar"));
    Files.writeString(dir.resolve("server/target/classpath"), "none");
    writeExecutable(dir.resolve("build/sbt"), sbtBody);
    writeExecutable(dir.resolve("fake/java"), javaBody);
  }

  private static void writeExecutable(Path file, String body) throws IOException {
    Files.createDirectories(file.getParent());
    Files.writeString(file, "#!/bin/sh\n" + body);
    Files.setPosixFilePermissions(file, PosixFilePermissions.fromString("rwxr-xr-x"));
  }
}
