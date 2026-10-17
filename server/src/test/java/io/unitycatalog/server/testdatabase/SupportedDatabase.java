package io.unitycatalog.server.testdatabase;

import java.util.Arrays;
import java.util.Optional;

/**
 * The databases the test suite runs on, selected with {@value #ENV} (unset means {@link #H2}). This
 * enum is the only list.
 */
public enum SupportedDatabase {
  H2(null),
  POSTGRES_18("postgres:18-alpine");

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
