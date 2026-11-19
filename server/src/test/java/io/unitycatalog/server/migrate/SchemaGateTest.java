package io.unitycatalog.server.migrate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.zaxxer.hikari.HikariDataSource;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.sql.DataSource;
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
        .hasMessageContaining("The database has no Unity Catalog schema yet")
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

  /** Also when the newer step failed, which on H2 and MySQL can leave part of its change. */
  @ParameterizedTest
  @CsvSource({"false", "true"})
  void hibernateModeRefusesASchemaNewerThanThisRelease(boolean newerStepFailed) {
    // A newer release (here one with step 1000) upgraded the database.
    new SchemaMigrator(db.dataSource(), List.of("classpath:db/test-steps/ahead")).upgradeSchema();
    if (newerStepFailed) {
      String q = DatabaseKind.of(db.dataSource()) == DatabaseKind.MYSQL ? "`" : "\"";
      UpgradeTestUtils.execute(
          db,
          ("UPDATE %1$suc_schema_history%1$s SET %1$ssuccess%1$s = false"
                  + " WHERE %1$sversion%1$s = '1000'")
              .formatted(q));
    }
    // A column that Hibernate's update would add back, to tell whether it ran.
    UpgradeTestUtils.execute(db, "ALTER TABLE uc_catalogs DROP COLUMN comment");
    SchemaSnapshot upgraded = SchemaSnapshot.of(db.dataSource());

    assertThatThrownBy(() -> TestServer.startWithHibernateManagedSchema(db, storageRoot))
        .hasMessageContaining("The schema is newer than this release (version 1000");
    assertThat(SchemaSnapshot.of(db.dataSource())).isEqualTo(upgraded);
  }

  @Test
  void hibernateModeFailsWhenTheUserCannotReadTheSchemaHistory() throws Exception {
    // Only MySQL hides a table the user has no privilege on.
    assumeTrue(DatabaseKind.of(db.dataSource()) == DatabaseKind.MYSQL);
    new SchemaMigrator(db.dataSource(), List.of("classpath:db/test-steps/ahead")).upgradeSchema();
    List<String> tables = new ArrayList<>();
    String database;
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      try (ResultSet rs = statement.executeQuery("SELECT DATABASE()")) {
        rs.next();
        database = rs.getString(1);
      }
      try (ResultSet rs = statement.executeQuery("SHOW TABLES")) {
        while (rs.next()) {
          if (!rs.getString(1).equals("uc_schema_history")) {
            tables.add(rs.getString(1));
          }
        }
      }
      statement.execute("CREATE USER 'restricted'@'%' IDENTIFIED BY 'restricted'");
      for (String table : tables) {
        statement.execute(
            "GRANT SELECT, INSERT, UPDATE, DELETE, ALTER, CREATE, INDEX ON `%s`.`%s` TO 'restricted'@'%%'"
                .formatted(database, table));
      }
    }
    Properties restricted = db.hibernateProperties();
    restricted.setProperty("hibernate.connection.username", "restricted");
    restricted.setProperty("hibernate.connection.password", "restricted");

    // MySQL hides the table from this user, which must not read as "no history".
    assertThatThrownBy(() -> db.openHibernateConfigurator(restricted, true).close())
        .hasMessageContaining("Could not read the schema history uc_schema_history");
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("DROP USER 'restricted'@'%'");
    }
  }

  @Test
  void hibernateModeStartsInASchemaWhoseNameHasAQuote() throws Exception {
    DatabaseKind kind = DatabaseKind.of(db.dataSource());
    assumeTrue(kind == DatabaseKind.POSTGRESQL || kind == DatabaseKind.MYSQL);
    String url = db.jdbcUrl();
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      if (kind == DatabaseKind.POSTGRESQL) {
        statement.execute("CREATE SCHEMA \"team\"\"blue\"");
        url += "?currentSchema=%22team%22%22blue%22";
      } else {
        statement.execute("CREATE DATABASE `team``blue`");
        url = url.substring(0, url.lastIndexOf('/') + 1) + "team%60blue";
      }
    }
    Properties quoted = db.hibernateProperties();
    quoted.setProperty("hibernate.connection.url", url);

    // An empty schema has no history to read, which must not depend on how its name is spelled.
    db.openHibernateConfigurator(quoted, true).close();
    if (kind == DatabaseKind.MYSQL) {
      try (Connection connection = db.dataSource().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("DROP DATABASE `team``blue`");
      }
    }
  }

  @Test
  void hibernateModeRefusesANewerSchemaInALaterSchemaOnTheSearchPath() throws Exception {
    assumeTrue(DatabaseKind.of(db.dataSource()) == DatabaseKind.POSTGRESQL);
    try (Connection connection = db.dataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE SCHEMA empty_s");
      statement.execute("CREATE SCHEMA live");
    }
    Properties live = db.hibernateProperties();
    live.setProperty("hibernate.connection.url", db.jdbcUrl() + "?currentSchema=live");
    // A newer release (here one with step 1000) upgraded the schema "live".
    try (HikariDataSource pool = new HikariDataSource(HibernateConfigurator.hikariConfig(live))) {
      new SchemaMigrator(pool, List.of("classpath:db/test-steps/ahead")).upgradeSchema();
    }
    Properties both = db.hibernateProperties();
    both.setProperty("hibernate.connection.url", db.jdbcUrl() + "?currentSchema=empty_s,live");

    // The first schema is empty, but the search path reaches the newer schema behind it.
    assertThatThrownBy(() -> db.openHibernateConfigurator(both, true).close())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Unity Catalog tables are in schema live");
  }

  @Test
  void hibernateModeChecksWithAPoolOfOne() {
    new SchemaMigrator(db.dataSource(), List.of("classpath:db/test-steps/ahead")).upgradeSchema();
    Properties onePool = db.hibernateProperties();
    onePool.setProperty("hibernate.hikari.maximumPoolSize", "1");
    onePool.setProperty("hibernate.hikari.connectionTimeout", "1000");

    assertThatThrownBy(() -> db.openHibernateConfigurator(onePool, true).close())
        .hasMessageContaining("The schema is newer than this release (version 1000");
  }

  @Test
  void hibernateModeFailsWhenTheSchemaHistoryCannotBeRead() {
    new SchemaMigrator(db.dataSource()).upgradeSchema();
    String q = DatabaseKind.of(db.dataSource()) == DatabaseKind.MYSQL ? "`" : "\"";
    UpgradeTestUtils.execute(
        db,
        "ALTER TABLE %1$suc_schema_history%1$s RENAME COLUMN %1$sversion%1$s TO %1$sversion_x%1$s"
            .formatted(q));

    assertThatThrownBy(() -> TestServer.startWithHibernateManagedSchema(db, storageRoot))
        .hasMessageContaining("Error while retrieving the list of applied migrations")
        .hasMessageContaining("uc_schema_history");
  }

  /**
   * Hibernate reads JVM system properties when its Environment class loads, so this runs in a JVM
   * of its own, started with {@code -Dhibernate.default_schema=ucx}.
   */
  @Test
  void hibernateModeRefusesADefaultSchemaFromASystemProperty(@TempDir Path dir) throws Exception {
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dhibernate.default_schema=ucx",
                "-cp",
                System.getProperty("java.class.path"),
                ForkedDefaultSchema.class.getName(),
                dir.resolve("uc").toString())
            .redirectErrorStream(true)
            .start();
    String output = new String(process.getInputStream().readAllBytes(), UTF_8);

    assertThat(process.waitFor()).as(output).isZero();
  }

  /** Exits with 0 when Hibernate mode refuses the default schema that the system property sets. */
  public static final class ForkedDefaultSchema {
    public static void main(String[] args) {
      Properties properties = new Properties();
      properties.setProperty("hibernate.connection.driver_class", "org.h2.Driver");
      properties.setProperty("hibernate.connection.url", "jdbc:h2:file:" + args[0]);
      properties.setProperty("hibernate.connection.user", "sa");
      try (HibernateConfigurator configurator = new HibernateConfigurator(properties, true)) {
        System.out.println("Hibernate mode started with a default schema");
        System.exit(1);
      } catch (IllegalArgumentException e) {
        System.exit(e.getMessage().contains("hibernate.default_schema is not supported") ? 0 : 2);
      }
    }
  }

  @Test
  void hibernateModeChecksNothingOnADatabaseTheStepsDontSupport() {
    // A database product the schema steps don't support, which Hibernate mode can still run on.
    DataSource otherProduct =
        UpgradeTestUtils.reportingProduct(db.dataSource(), "Some other database");

    SchemaMigrator.refuseNewerSchema(otherProduct);
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

  /**
   * The server sets up an in-memory database itself, so an upgrade has nothing to do and succeeds,
   * and a check has nothing to report.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "in-memory database | jdbc:h2:mem:schema-command",
        "unnamed in-memory database | jdbc:h2:.",
        "in-memory file system | jdbc:h2:memFS:schema-command",
        "compressed in-memory file system | jdbc:h2:memLZF:schema-command",
        "NIO in-memory file system | jdbc:h2:nioMemFS:schema-command",
        "NIO compressed in-memory file system | jdbc:h2:nioMemLZF:schema-command",
      })
  void schemaCommandsLeaveAnInMemoryDatabaseToTheServer(
      String setup, String url, @TempDir Path conf) throws IOException {
    writeConfiguration(conf, "", url);

    for (boolean upgrade : List.of(false, true)) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      assertThat(SchemaMigrator.runCommand(upgrade, conf, new PrintStream(out, true, UTF_8)))
          .isEqualTo(upgrade ? 0 : 1);
      assertThat(out.toString(UTF_8))
          .contains("The database is in memory: the server sets it up itself when it starts")
          .contains(upgrade ? "nothing to upgrade" : "nothing to check");
    }
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "server.properties missing | | not found: run bin/start-uc-server",
        "hibernate.properties missing | | not found: run bin/start-uc-server",
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

  /**
   * The second column is a Hibernate property that the configuration adds to the URL: H2 reads a
   * data source property as a setting of the URL too, in any case. {@code \;} in a setting's value
   * is H2's escaped semicolon, not the end of the setting.
   */
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      quoteCharacter = '"',
      value = {
        "jdbc:h2:file:{file} | ",
        "jdbc:h2:file:{file};IFEXISTS=FALSE | ",
        "jdbc:wrapper:h2:file:{file} | ",
        "jdbc:h2:file:{file};INIT=SET @x 'a\\;IFEXISTS=FALSE' | ",
        "jdbc:h2:file:{file} | hibernate.hikari.dataSource.IFEXISTS=FALSE",
        "jdbc:h2:file:{file} | hibernate.hikari.dataSource.ifexists=false",
      })
  void checkSchemaDoesNotCreateAMissingH2File(
      String url, String property, @TempDir Path conf, @TempDir Path data) throws IOException {
    writeConfiguration(conf, "", url.replace("{file}", data.resolve("uc").toString()));
    if (property != null) {
      Files.writeString(
          conf.resolve("hibernate.properties"), "\n" + property + "\n", StandardOpenOption.APPEND);
    }
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
    if (url.startsWith(SchemaMigratorTest.WrappingDriver.PREFIX)) {
      hibernate.setProperty(
          "hibernate.connection.driver_class", SchemaMigratorTest.WrappingDriver.class.getName());
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
