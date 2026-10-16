package io.unitycatalog.server.testdatabase;

import java.util.Arrays;
import java.util.Optional;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;

/**
 * The databases the test suite runs on, selected with {@value #ENV} (unset means {@link #H2}). This
 * enum is the only list: sbt aliases and the CI matrix are checked against it.
 */
public enum SupportedDatabase {
  H2(null),
  /** postgres:18-alpine (PostgreSQL 18.6), pinned by digest. */
  POSTGRES_18("postgres@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873");

  public static final String ENV = "UC_TEST_DATABASE";

  private final String image;

  SupportedDatabase(String image) {
    this.image = image;
  }

  public static SupportedDatabase selected() {
    return parse(System.getenv(ENV));
  }

  static SupportedDatabase parse(String value) {
    if (value == null || value.isEmpty()) {
      return H2;
    }
    return Arrays.stream(values())
        .filter(database -> database.name().equals(value))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    ENV + "=" + value + " is not one of " + Arrays.toString(values())));
  }

  /** The container image for a database that runs in Docker; empty for in-process H2. */
  public Optional<String> image() {
    return Optional.ofNullable(image);
  }

  /** Whether {@code dialect}, resolved from a live connection, is this database. */
  public boolean matches(Dialect dialect) {
    return switch (this) {
      case H2 -> dialect instanceof H2Dialect;
      case POSTGRES_18 ->
          dialect instanceof PostgreSQLDialect
              && dialect.getVersion().getDatabaseMajorVersion() == 18;
    };
  }

  static String describe(Dialect dialect) {
    DatabaseVersion version = dialect.getVersion();
    String family = dialect.getClass().getSimpleName().replace("Dialect", "");
    return family
        + " "
        + version.getDatabaseMajorVersion()
        + "."
        + version.getDatabaseMinorVersion()
        + "."
        + version.getDatabaseMicroVersion();
  }
}
