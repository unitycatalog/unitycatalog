package io.unitycatalog.server.migrate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SchemaGateTest {

  @TempDatabase(empty = true)
  TestDatabase db;

  @TempDir Path storageRoot;

  @Test
  void refusesToStartOnADatabaseThatIsNotSetUp() {
    assertThatThrownBy(() -> TestServer.start(db, storageRoot))
        .hasMessageContaining("bin/start-uc-server --upgrade-schema");
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
  }

  @Test
  void setsUpAnInMemoryDatabaseAtStart() throws ApiException {
    // It lasts only as long as the server, so no command can set it up before the server starts.
    Properties inMemory = db.hibernateProperties();
    inMemory.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    inMemory.setProperty("hibernate.connection.url", "jdbc:h2:mem:gate;DB_CLOSE_DELAY=-1");

    try (TestServer server = TestServer.start(db, inMemory, storageRoot)) {
      assertThat(
              new CatalogsApi(server.apiClient())
                  .listCatalogs(null, null, /* includeBrowse= */ false)
                  .getCatalogs())
          .isEmpty();
    }
  }

  @Test
  void hibernateModeRefusesASchemaNewerThanThisRelease() {
    // A newer release (here one with step 1000) upgraded the database.
    new SchemaMigrator(db.dataSource(), List.of("classpath:db/test-steps/ahead")).upgradeSchema();
    // A column that Hibernate's update would add back, to tell whether it ran.
    UpgradeTestUtils.execute(db, "ALTER TABLE uc_catalogs DROP COLUMN comment");
    SchemaSnapshot upgraded = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(() -> TestServer.startWithHibernateManagedSchema(db, storageRoot))
        .hasMessageContaining("The schema is newer than this release (version 1000");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(upgraded);
  }

  @Test
  void databaseHibernateSetUpCanSwitchBackToTheSteps() {
    try (TestServer server = TestServer.startWithHibernateManagedSchema(db, storageRoot)) {
      // Hibernate created the tables, and no schema step recorded anything.
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.UNVERSIONED_UC);
    }

    // Its schema is this release's, so every step is recorded without running.
    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.checkSchema(new PrintStream(new ByteArrayOutputStream()))).isZero();
    assertThat(migrator.upgradeSchema()).isEqualTo(migrator.steps().size());
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
    }
  }

  @Test
  void baselineDatabaseThatHibernateRanOnIsAdopted() {
    UpgradeTestUtils.createBaselineSchema(db);
    try (TestServer server = TestServer.startWithHibernateManagedSchema(db, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.UNVERSIONED_UC);
    }

    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThat(migrator.upgradeSchema()).isEqualTo(migrator.steps().size());
    assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
  }

  @Test
  void hibernateLeavesTheStepsHistoryAsItIs() {
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    String version = new SchemaMigrator(db.dataSource()).recordedVersion();

    try (TestServer server = TestServer.startWithHibernateManagedSchema(db, storageRoot)) {
      SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
      assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
      assertThat(migrator.recordedVersion()).isEqualTo(version);
    }
  }

  @Test
  void upgradeSchemaCommandSetsUpTheConfiguredDatabase(@TempDir Path conf) throws IOException {
    writeConfiguration(conf, "", db.jdbcUrl());
    // The command's own pool is larger than the server's, which the configuration may set small.
    Files.writeString(
        conf.resolve("hibernate.properties"),
        "\nhibernate.hikari.maximumPoolSize=1\nhibernate.hikari.connectionTimeout=1000\n",
        StandardOpenOption.APPEND);

    assertThat(SchemaMigrator.runCommand(true, conf, new PrintStream(new ByteArrayOutputStream())))
        .isZero();
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    // On H2 the check opens the file that now exists, with IFEXISTS=TRUE added to its URL.
    assertThat(SchemaMigrator.runCommand(false, conf, new PrintStream(new ByteArrayOutputStream())))
        .isZero();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "server.properties missing | | not found: run bin/start-uc-server",
        "hibernate.properties missing | | not found: run bin/start-uc-server",
        "in-memory database | jdbc:h2:mem:schema-command | is in memory",
        "unnamed in-memory database | jdbc:h2:. | is in memory",
        "Hibernate manages the schema | server.schema-management=hibernate"
            + " | Set server.schema-management=flyway",
        "Hibernate default schema | hibernate.default_schema=uc | hibernate.default_schema is not",
        "Hibernate default catalog | hibernate.default_catalog=uc | hibernate.default_catalog is"
            + " not",
      })
  void schemaCommandsRefuse(String setup, String setting, String message, @TempDir Path conf)
      throws IOException {
    boolean memoryUrl = setting != null && setting.startsWith("jdbc:");
    boolean hibernateSetting = setting != null && setting.startsWith("hibernate.");
    writeConfiguration(
        conf,
        memoryUrl || hibernateSetting || setting == null ? "" : setting,
        memoryUrl ? setting : db.jdbcUrl());
    if (hibernateSetting) {
      Files.writeString(
          conf.resolve("hibernate.properties"), "\n" + setting + "\n", StandardOpenOption.APPEND);
    }
    if (setup.endsWith("missing")) {
      Files.delete(conf.resolve(setup.split(" ")[0]));
    }

    for (boolean upgrade : List.of(false, true)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertThat(SchemaMigrator.runCommand(upgrade, conf, new PrintStream(out, true, UTF_8)))
          .isOne();
      assertThat(out.toString(UTF_8)).contains(message);
    }
    assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
  }

  @Test
  void checkSchemaDoesNotCreateAMissingH2File(@TempDir Path conf, @TempDir Path data)
      throws IOException {
    writeConfiguration(conf, "", "jdbc:h2:file:" + data.resolve("uc"));
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    assertThat(SchemaMigrator.runCommand(false, conf, new PrintStream(out, true, UTF_8))).isOne();

    assertThat(out.toString(UTF_8)).contains("is not found").contains("etc/conf");
    try (var files = Files.list(data)) {
      assertThat(files).isEmpty();
    }
  }

  /**
   * {@code server.properties} with {@code serverSetting}, and {@code hibernate.properties} for
   * {@code url}.
   */
  private void writeConfiguration(Path conf, String serverSetting, String url) throws IOException {
    Files.writeString(conf.resolve("server.properties"), serverSetting + "\n");
    Properties hibernate = db.hibernateProperties();
    hibernate.setProperty("hibernate.connection.url", url);
    if (url.startsWith("jdbc:h2:")) {
      hibernate.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
    }
    if (url.startsWith("jdbc:h2:mem:")) {
      // Without credentials, as the server's default in-memory database: other tests in this JVM
      // open the same one that way.
      hibernate.remove("hibernate.connection.username");
      hibernate.remove("hibernate.connection.password");
    }
    try (OutputStream out = Files.newOutputStream(conf.resolve("hibernate.properties"))) {
      hibernate.store(out, null);
    }
  }
}
