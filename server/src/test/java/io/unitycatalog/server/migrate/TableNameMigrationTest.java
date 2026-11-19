package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import io.unitycatalog.server.utils.ServerProperties;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Properties;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TableNameMigrationTest {
  @TempDatabase(empty = true)
  TestDatabase database;

  @Test
  void duplicateNamesStopUpgradeWithoutRemovingEitherObject() throws Exception {
    UpgradeTestUtils.migrateTo(database, "3");
    try (Connection connection = database.dataSource().getConnection();
        var insert =
            connection.prepareStatement(
                "INSERT INTO uc_tables (id, schema_id, name) VALUES (?, ?, ?)")) {
      connection.setAutoCommit(false);
      UUID schemaId = UUID.randomUUID();
      for (int i = 0; i < 2; i++) {
        insert.setObject(1, jdbcUuid(UUID.randomUUID()));
        insert.setObject(2, jdbcUuid(schemaId));
        insert.setString(3, "duplicate");
        insert.executeUpdate();
      }
      connection.commit();
    }
    SchemaMigrator migrator = new SchemaMigrator(database.dataSource());
    SchemaSnapshot beforeUpgrade = SchemaSnapshot.of(database.dataSource());
    ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
    assertThat(migrator.checkSchema(new PrintStream(diagnostics, true, StandardCharsets.UTF_8)))
        .isOne();
    assertThat(diagnostics.toString(StandardCharsets.UTF_8))
        .contains("Duplicate table name: duplicate");
    assertThat(SchemaSnapshot.of(database.dataSource())).isEqualTo(beforeUpgrade);
    assertThatThrownBy(migrator::upgradeSchema)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Duplicate table name: duplicate");
    assertThat(SchemaSnapshot.of(database.dataSource())).isEqualTo(beforeUpgrade);
    assertRows(2);
    Properties config = database.hibernateProperties();
    config.setProperty("hibernate.hbm2ddl.auto", "update");
    SchemaSnapshot beforeStartup = SchemaSnapshot.of(database.dataSource());
    assertThatThrownBy(
            () -> {
              try (var ignored = new HibernateConfigurator(config)) {
                // Hibernate must not silently serve without the unique index.
              }
            })
        .hasStackTraceContaining("Duplicate table name: duplicate");
    assertThat(SchemaSnapshot.of(database.dataSource())).isEqualTo(beforeStartup);
    assertRows(2);
  }

  @Test
  void serverRefusesAnExistingSchemaWithoutTheNameKey(@TempDir Path configuration) {
    UpgradeTestUtils.migrateTo(database, "3");
    SchemaSnapshot before = SchemaSnapshot.of(database.dataSource());
    Properties config = database.hibernateProperties();
    config.setProperty("hibernate.hbm2ddl.auto", "none");
    Properties server = new Properties();
    server.setProperty("server.env", "test");
    try (var hibernate = new HibernateConfigurator(config)) {
      assertThatThrownBy(
              () ->
                  UnityCatalogServer.builder()
                      .serverProperties(new ServerProperties(server))
                      .configurationFolder(configuration)
                      .hibernateConfigurator(hibernate)
                      .build())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Table name uniqueness is not enforced");
    }
    assertThat(SchemaSnapshot.of(database.dataSource())).isEqualTo(before);
  }

  @Test
  void upgradesExistingObjectsWithoutRewritingThem() throws Exception {
    UpgradeTestUtils.migrateTo(database, "3");
    UUID id = UUID.randomUUID();
    try (Connection connection = database.dataSource().getConnection();
        var insert =
            connection.prepareStatement(
                "INSERT INTO uc_tables (id, schema_id, name) VALUES (?, ?, ?)")) {
      connection.setAutoCommit(false);
      insert.setObject(1, jdbcUuid(id));
      insert.setObject(2, jdbcUuid(UUID.randomUUID()));
      insert.setString(3, "kept");
      insert.executeUpdate();
      connection.commit();
    }
    SchemaMigrator migrator = new SchemaMigrator(database.dataSource());
    ByteArrayOutputStream diagnostics = new ByteArrayOutputStream();
    assertThat(migrator.checkSchema(new PrintStream(diagnostics, true, StandardCharsets.UTF_8)))
        .isZero();
    assertThat(migrator.upgradeSchema()).isPositive();
    assertThat(migrator.upgradeSchema()).isZero();
    try (Connection connection = database.dataSource().getConnection();
        var select = connection.prepareStatement("SELECT name FROM uc_tables WHERE id = ?")) {
      select.setObject(1, jdbcUuid(id));
      try (var rows = select.executeQuery()) {
        assertThat(rows.next()).isTrue();
        assertThat(rows.getString(1)).isEqualTo("kept");
        assertThat(rows.next()).isFalse();
      }
    }
    HibernateConfigurator.requireUniqueTableNameIndex(database.dataSource());
  }

  private Object jdbcUuid(UUID uuid) {
    if (database.jdbcUrl().startsWith("jdbc:mysql:")) {
      return java.nio.ByteBuffer.allocate(16)
          .putLong(uuid.getMostSignificantBits())
          .putLong(uuid.getLeastSignificantBits())
          .array();
    }
    return uuid;
  }

  private void assertRows(int expected) throws Exception {
    try (Connection connection = database.dataSource().getConnection();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery("SELECT COUNT(*) FROM uc_tables WHERE name='duplicate'")) {
      assertThat(rows.next()).isTrue();
      assertThat(rows.getInt(1)).isEqualTo(expected);
    }
  }
}
