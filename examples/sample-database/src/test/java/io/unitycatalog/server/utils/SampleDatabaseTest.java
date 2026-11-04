package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
}
