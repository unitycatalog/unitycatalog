package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** A failed container start or schema build is reported as itself and not retried. */
class PostgresDatabasesFailureTest {

  @Test
  void cachedFailureKeepsTheFirstMessage() {
    RuntimeException first =
        new IllegalStateException("POSTGRES_18 selected but its container could not start: boom");
    PostgresDatabases failed = PostgresDatabases.failed(SupportedDatabase.POSTGRES_18, first);
    assertThatThrownBy(() -> failed.newDatabase("uc_x"))
        .hasMessage(
            "POSTGRES_18 selected but its container could not start: boom (cached; not retried)")
        .hasCause(first);
  }
}
