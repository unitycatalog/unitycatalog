package io.unitycatalog.server.migrate;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.testdatabase.TestDatabases;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Databases and statements the schema migration tests share. */
final class UpgradeTestUtils {

  private UpgradeTestUtils() {}

  /** Builds a database of the baseline release: its schema without Flyway's history. */
  static void create07Schema(TestDatabase db) {
    String path =
        "db/migration/" + DatabaseKind.fromJdbcUrl(db.jdbcUrl()).folder() + "/V1__baseline_0_7.sql";
    String sql;
    try (InputStream in = UpgradeTestUtils.class.getClassLoader().getResourceAsStream(path)) {
      sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    String withoutComments = sql.replaceAll("(?m)^\\s*--.*$", "");
    for (String statement : withoutComments.split(";")) {
      if (!statement.isBlank()) {
        execute(db, statement);
      }
    }
  }

  /** The schema the steps build on a new empty database. */
  static SchemaSnapshot freshInstallSnapshot() {
    try (TestDatabase fresh = TestDatabases.newEmptyDatabase()) {
      new SchemaMigrator(fresh.dataSource()).migrate();
      return SchemaSnapshot.of(fresh.dataSource());
    }
  }

  static void execute(TestDatabase db, String sql) {
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new IllegalStateException(sql, e);
    }
  }
}
