package io.unitycatalog.server.migrate;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/** Applies the schema steps of {@code db/migration/<database>} to a database. */
public final class Migrator {

  private final Flyway flyway;

  /** Does not own {@code dataSource}. */
  public Migrator(DataSource dataSource, DatabaseKind kind) {
    this.flyway =
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration/" + kind.folder())
            .load();
  }

  public void migrate() {
    flyway.migrate();
  }
}
