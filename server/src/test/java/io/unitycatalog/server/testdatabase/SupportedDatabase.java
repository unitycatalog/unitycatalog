package io.unitycatalog.server.testdatabase;

import java.util.Arrays;
import java.util.Optional;

/**
 * The databases the test suite runs on, selected with {@value #ENV} (unset means {@link #H2}). This
 * enum is the only list.
 */
public enum SupportedDatabase {
  H2(null),
  // postgres:18-alpine
  POSTGRES_18("postgres@sha256:898c60cb390d1085bc97bdd6a1ae968757421df394b071848b52b1c09c89d6a5"),
  // mysql:8.4
  MYSQL_8("mysql@sha256:76f0358884cb92d9a086d24e5671c38048d3fef40f2588703d73eb55392d6b69");

  public static final String ENV = "UC_TEST_DATABASE";

  private final String image;

  SupportedDatabase(String image) {
    this.image = image;
  }

  public static SupportedDatabase selected() {
    return parse(System.getenv(ENV));
  }

  private static SupportedDatabase parse(String value) {
    if (value == null) {
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
}
