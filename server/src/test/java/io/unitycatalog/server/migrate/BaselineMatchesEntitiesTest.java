package io.unitycatalog.server.migrate;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.testdatabase.TempDatabase;
import io.unitycatalog.server.testdatabase.TestDatabase;
import java.util.Properties;
import org.junit.jupiter.api.Test;

public class BaselineMatchesEntitiesTest {

  @TempDatabase(empty = true)
  TestDatabase fromSteps;

  @TempDatabase(empty = true)
  TestDatabase fromEntities;

  @Test
  void stepsBuildTheEntitySchema() {
    new SchemaMigrator(fromSteps.dataSource(), fromSteps.kind()).migrate();
    Properties create = fromEntities.hibernateProperties();
    // "update", as the baseline release ran: unlike "create", it names the unique constraints.
    create.setProperty("hibernate.hbm2ddl.auto", "update");
    // MySQL rejects one index of the entities (key too long); the baseline schema never had it.
    create.setProperty("hibernate.hbm2ddl.halt_on_error", "false");
    fromEntities.openHibernateConfigurator(create); // the entities build the second
    assertThat(SchemaSnapshot.of(fromSteps.dataSource()))
        .isEqualTo(SchemaSnapshot.of(fromEntities.dataSource()));
  }
}
