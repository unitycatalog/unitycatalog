package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.junit.jupiter.api.Test;

class SelectedDatabaseCheckTest {

  private static final H2Dialect H2 = new H2Dialect(DatabaseVersion.make(2, 2, 224));
  private static final PostgreSQLDialect PG18 = new PostgreSQLDialect(DatabaseVersion.make(18, 6));

  @Test
  void acceptsTheSelectedDatabase() {
    assertThatCode(
            () ->
                SelectedDatabaseCheck.verify(SupportedDatabase.POSTGRES_18, PG18, Optional.empty()))
        .doesNotThrowAnyException();
  }

  @Test
  void rejectsAnotherDatabaseWithTheFix() {
    assertThatThrownBy(
            () -> SelectedDatabaseCheck.verify(SupportedDatabase.POSTGRES_18, H2, Optional.empty()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "SelectedDatabaseCheck: expected POSTGRES_18, connected to H2 2.2.224. If this test"
                + " intentionally uses its own database, annotate it with"
                + " @UsesOwnDatabase(reason = \"…\").");
  }

  @Test
  void allowsAClassThatUsesItsOwnDatabase() {
    assertThatCode(
            () ->
                SelectedDatabaseCheck.verify(
                    SupportedDatabase.POSTGRES_18, H2, Optional.of("X: tests H2 itself")))
        .doesNotThrowAnyException();
  }
}
