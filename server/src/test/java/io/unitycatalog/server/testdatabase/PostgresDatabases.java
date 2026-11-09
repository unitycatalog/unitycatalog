package io.unitycatalog.server.testdatabase;

import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

/** PostgreSQL test databases; each is {@code CREATE DATABASE … TEMPLATE uc_template}. */
final class PostgresDatabases extends TemplateDatabases {

  private static PostgresDatabases instance;

  private PostgresDatabases(SupportedDatabase database) {
    super(database);
  }

  /** The container for the selected PostgreSQL database, started on first use. */
  static synchronized PostgresDatabases get() {
    if (instance == null) {
      PostgresDatabases created = new PostgresDatabases(SupportedDatabase.selected());
      created.start();
      instance = created;
    }
    return instance;
  }

  @Override
  JdbcDatabaseContainer<?> newContainer(String image) {
    return new PostgreSQLContainer<>(image)
        // Test-only speed settings (data is thrown away):
        .withCommand(
            "postgres",
            // Do not wait for writes to reach the disk.
            "-c",
            "fsync=off",
            // Report a commit before its WAL record is on disk.
            "-c",
            "synchronous_commit=off",
            // No full page images in the WAL (they only guard against torn pages on a crash).
            "-c",
            "full_page_writes=off");
  }

  @Override
  String dataDirectory() {
    return "/var/lib/postgresql";
  }

  @Override
  String driver() {
    return "org.postgresql.Driver";
  }

  @Override
  String jdbcUrl(String name) {
    return "jdbc:postgresql://"
        + container.getHost()
        + ":"
        + container.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT)
        + "/"
        + name;
  }

  @Override
  void createDatabase(String name) {
    execute("CREATE DATABASE " + name);
  }

  @Override
  void cloneTemplate(String name) {
    execute("CREATE DATABASE " + name + " TEMPLATE " + TEMPLATE);
  }

  @Override
  String dropSql(String name) {
    return "DROP DATABASE " + name + " WITH (FORCE)";
  }
}
