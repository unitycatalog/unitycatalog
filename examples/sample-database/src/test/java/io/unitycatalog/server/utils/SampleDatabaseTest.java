package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
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

/** {@link SampleDatabase} builds the sample database file. */
public class SampleDatabaseTest {

  @Test
  void buildsTheSampleDatabase(@TempDir Path dir) throws SQLException {
    Path file = dir.resolve("h2db.mv.db");

    // The build's schema upgrade fails unless the schema is what this build's steps build.
    SampleDatabase.build(file);

    assertThat(file).exists();
    String url = "jdbc:h2:file:" + dir.resolve("h2db").toAbsolutePath();
    try (Connection connection = DriverManager.getConnection(url, "", "");
        ResultSet catalogs =
            connection.createStatement().executeQuery("SELECT name FROM uc_catalogs")) {
      assertThat(catalogs.next()).isTrue();
      assertThat(catalogs.getString(1)).isEqualTo("unity");
    }
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
          failed++; // the build that finds the file already there
        }
      }

      assertThat(failed).isLessThanOrEqualTo(1);
      assertThat(file).exists();
      try (Stream<Path> files = Files.list(dir)) {
        assertThat(files.map(f -> f.getFileName().toString())).containsExactly("h2db.mv.db");
      }
    } finally {
      pool.shutdownNow();
    }
  }
}
