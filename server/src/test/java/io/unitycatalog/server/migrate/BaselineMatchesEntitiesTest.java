package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.migrate.SchemaMigrator.DatabaseKind;
import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import jakarta.persistence.metamodel.EntityType;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.JdbcSettings;
import org.hibernate.cfg.SchemaToolingSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * The steps build the schema Hibernate builds from the entities, so Hibernate's update at server
 * start changes nothing on a database that the steps built. Runs with {@code
 * UC_TEST_MIGRATION_CHECKS=true}.
 */
@EnabledIfEnvironmentVariable(named = "UC_TEST_MIGRATION_CHECKS", matches = "true")
public class BaselineMatchesEntitiesTest {

  @TempDatabase(empty = true)
  TestDatabase fromSteps;

  @TempDatabase(empty = true)
  TestDatabase fromEntities;

  @Test
  void stepsBuildTheEntitySchema() {
    new SchemaMigrator(fromSteps.dataSource()).upgradeSchema();
    // Hibernate builds the second from the server's entities, with the settings below.
    Configuration entities = new Configuration();
    fromEntities.sessionFactory().getMetamodel().getEntities().stream()
        .map(EntityType::getJavaType)
        .forEach(entities::addAnnotatedClass);
    // "update", as the baseline release ran: unlike "create", it names the unique constraints.
    entities.setProperty(SchemaToolingSettings.HBM2DDL_AUTO, "update");
    // MySQL rejects one index of the entities (key too long); the baseline schema never had it. The
    // other databases stop on the first entity DDL error.
    entities.setProperty(
        SchemaToolingSettings.HBM2DDL_HALT_ON_ERROR,
        String.valueOf(DatabaseKind.of(fromEntities.dataSource()) != DatabaseKind.MYSQL));
    entities
        .getProperties()
        .put(JdbcSettings.JAKARTA_NON_JTA_DATASOURCE, fromEntities.dataSource());
    entities.buildSessionFactory().close();
    // On a mismatch, AssertJ also lists the missing and unexpected entries.
    assertThat(SchemaSnapshot.of(fromSteps.dataSource()).entries())
        .containsExactlyElementsOf(SchemaSnapshot.of(fromEntities.dataSource()).entries());
  }
}
