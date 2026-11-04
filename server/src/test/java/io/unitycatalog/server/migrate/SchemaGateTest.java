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
    // Two connections run the server, but not the migration; the migration has its own pool. A
    // regression fails within the short connection timeout instead of Hikari's 30 seconds.
    Properties smallPool = db.hibernateProperties();
    smallPool.setProperty("hibernate.hikari.maximumPoolSize", "2");
    smallPool.setProperty("hibernate.hikari.connectionTimeout", "1000");

    try (TestServer server = TestServer.start(db, smallPool, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void upgradesABaselineDatabase() {
    UpgradeTestUtils.createBaselineSchema(db);

    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(SchemaSnapshot.of(db.dataSource()))
          .isEqualTo(UpgradeTestUtils.freshInstallSnapshot());
    }
  }

  @Test
  void hibernateManagesTheSchemaUntilTheStepsTakeItBack() {
    try (TestServer server = TestServer.startWithHibernateManagedSchema(db, storageRoot)) {
      // Hibernate created the tables, and no schema step recorded anything.
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.UNVERSIONED_UC);
    }

    // Back to the schema steps: the database is adopted and upgraded.
    try (TestServer server = TestServer.start(db, storageRoot)) {
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.CURRENT);
    }
  }

  @Test
  void hibernateLeavesTheStepsHistoryAsItIs() {
    new SchemaMigrator(db.dataSource()).migrate();
    String version = new SchemaMigrator(db.dataSource()).currentVersion();

    try (TestServer server = TestServer.startWithHibernateManagedSchema(db, storageRoot)) {
      SchemaMigrator migrator = new SchemaMigrator(db.dataSource());
      assertThat(migrator.state()).isEqualTo(SchemaState.CURRENT);
      assertThat(migrator.currentVersion()).isEqualTo(version);
    }
  }

  @Test
  void refusesAConfiguratorThatLetsHibernateChangeTheSchema() {
    for (String setting :
        List.of(
            "hibernate.hbm2ddl.auto",
            "hibernate.hbm2ddl.auto.orm",
            "jakarta.persistence.schema-generation.database.action",
            "javax.persistence.schema-generation.database.action")) {
      Properties create = db.hibernateProperties();
      create.setProperty(setting, "create");

      assertThatThrownBy(() -> TestServer.start(db, create, storageRoot))
          .hasMessageContaining(setting + "=create is not supported");
      // Refused before Hibernate created any table.
      assertThat(new SchemaMigrator(db.dataSource()).state()).isEqualTo(SchemaState.EMPTY);
    }
  }
}
