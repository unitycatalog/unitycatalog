package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

/**
 * Each release of {@link OldServer#releaseChain()}, oldest first, writes its copy of the {@link
 * DataSet} to one database; the current build's schema upgrade then brings it to a new install's
 * schema, and the current server reads every copy back. Only the current server reads them: a
 * release in between would only re-check releases that already shipped.
 *
 * <p>It builds and runs old servers' jars, so it runs only with {@code UC_TEST_UPGRADES=true}.
 */
public class RealUpgradeTest {

  /** The repair SQL for a database that a release before 0.7.0 set up, per database. */
  private static final String REPAIR = "db/repair-0.7.0/<database>.sql";

  @TempDatabase(empty = true)
  TestDatabase db;

  @Test
  @EnabledIfEnvironmentVariable(named = "UC_TEST_UPGRADES", matches = "true")
  void upgradesAndKeepsTheData(@TempDir Path workDir, @TempDir Path storageRoot) {
    List<String> chain = OldServer.releaseChain();
    assertThat(chain).as("no release tags found: git fetch --tags").isNotEmpty();
    List<DataSet> copies = new ArrayList<>();
    for (String release : chain) {
      DataSet copy = new DataSet(release, storageRoot);
      OldServer.run(release, db, workDir, storageRoot, copy::write);
      copies.add(copy);
    }

    new SchemaMigrator(db.dataSource()).upgradeSchema();
    try (TestServer server = TestServer.start(db, storageRoot)) {
      copies.forEach(copy -> copy.assertReadBack(server.apiClient()));
    }
  }

  /**
   * A database that release 0.4.0 set up and 0.7.0 then upgraded with Hibernate differs from
   * 0.7.0's own schema, so the schema upgrade refuses it and names the differences. (On such a
   * database 0.7.0 can't store a function with every parameter type.) The repair SQL that
   * docs/server/upgrades.md gives, as this test's copy of it ({@value #REPAIR}), removes them; the
   * upgrade then keeps what both releases wrote, and the current server stores and reads the whole
   * data set.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "UC_TEST_UPGRADES", matches = "true")
  void upgradesAnOlderDatabaseOnceItIsRepaired(@TempDir Path workDir, @TempDir Path storageRoot)
      throws ApiException {
    for (String release : List.of("v0.4.0", "v0.7.0")) {
      OldServer.run(release, db, workDir, storageRoot, api -> createCatalog(api, release));
    }

    SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
    assertThatThrownBy(migrator::upgradeSchema)
        .hasMessageContaining("differs from version 1 in:")
        .hasMessageContaining("unexpected check uc_functions ")
        .hasMessageContaining("unexpected check uc_function_params ");
    repair();
    migrator.upgradeSchema();

    try (TestServer server = TestServer.start(db, storageRoot)) {
      for (String release : List.of("v0.4.0", "v0.7.0")) {
        String name = catalogOf(release);
        assertThat(new CatalogsApi(server.apiClient()).getCatalog(name, false).getName())
            .isEqualTo(name);
      }
      DataSet data = new DataSet("HEAD", storageRoot);
      data.write(server.apiClient());
      data.assertReadBack(server.apiClient());
    }
  }

  /**
   * Runs the repair SQL as an operator does: each {@code -- <name>: query} line gives the query
   * that finds the name to put in place of {@code <name>} in the statements.
   */
  private void repair() {
    String sql =
        UpgradeTestUtils.resource(
            REPAIR.replace("<database>", DatabaseKind.of(db.dataSource()).folder()));
    Matcher lookup = Pattern.compile("(?m)^-- (<[^>]+>): (.+);$").matcher(sql);
    while (lookup.find()) {
      String name;
      try (Connection connection = db.dataSource().getConnection();
          Statement statement = connection.createStatement();
          ResultSet rs = statement.executeQuery(lookup.group(2))) {
        assertThat(rs.next()).as(lookup.group(1)).isTrue();
        name = rs.getString(1);
      } catch (SQLException e) {
        throw new IllegalStateException(lookup.group(2), e);
      }
      sql = sql.replace(lookup.group(1), name);
    }
    UpgradeTestUtils.executeStatements(db, sql);
  }

  private static void createCatalog(ApiClient api, String release) {
    try {
      new CatalogsApi(api).createCatalog(new CreateCatalog().name(catalogOf(release)));
    } catch (ApiException e) {
      throw new IllegalStateException(e);
    }
  }

  private static String catalogOf(String release) {
    return "older_catalog_" + release.replaceAll("\\W", "_");
  }

  /**
   * This commit's shaded jar sets up an empty database with {@code --upgrade-schema}, then serves
   * it, as a packaged deployment runs it.
   */
  @Test
  @EnabledIfEnvironmentVariable(named = "UC_TEST_UPGRADES", matches = "true")
  void packagedServerSetsUpTheSchema(@TempDir Path workDir, @TempDir Path storageRoot) {
    DataSet data = new DataSet("HEAD", storageRoot);
    OldServer.run(
        "HEAD",
        db,
        workDir,
        storageRoot,
        api -> {
          data.write(api);
          data.assertReadBack(api);
        });
    assertThat(SchemaSnapshot.of(db.dataSource()))
        .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
  }
}
