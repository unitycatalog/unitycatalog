package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.migrate.SchemaMigrator.SchemaState;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SchemaGateTest {

  /** A test-only step 1000: the next release's step for a current database. */
  private static final List<String> AHEAD = List.of("classpath:db/test-steps/ahead");

  @TempDatabase(empty = true)
  TestDatabase db;

  @TempDir Path storageRoot;

  @Test
  void setsUpAnEmptyDatabaseOnFirstStart() {
    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void setsUpTheSchemaWithASmallConnectionPool() {
    // Two connections run the server (Casbin keeps one), but not the migration (its lock plus
    // Flyway's two on PostgreSQL); the migration has its own pool. A regression fails within the
    // short connection timeout instead of Hikari's 30 seconds.
    Properties smallPool = db.hibernateProperties();
    smallPool.setProperty("hibernate.hikari.maximumPoolSize", "2");
    smallPool.setProperty("hibernate.hikari.connectionTimeout", "1000");

    try (TestServer server = TestServer.start(db, smallPool, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void upgradesA07Database() {
    UpgradeTestUtils.create07Schema(db);

    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
    }
  }

  @Test
  void refusesAConfiguratorThatLetsHibernateChangeTheSchema() {
    Properties update = db.hibernateProperties();
    update.setProperty("hibernate.hbm2ddl.auto", "update");

    assertThatThrownBy(() -> TestServer.start(db, update, storageRoot))
        .hasMessageContaining("hibernate.hbm2ddl.auto=update is not supported");
  }

  @Test
  void refusesASchemaNewerThanThisRelease() {
    new SchemaMigrator(db.dataSource(), AHEAD).migrate();

    assertThatThrownBy(() -> TestServer.start(db, storageRoot))
        .hasMessageContaining("this release ends at");
  }
}
