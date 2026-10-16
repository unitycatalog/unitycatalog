package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
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

  @Test
  void integrateRejectsSessionFactoryOnAnotherDatabase() {
    SupportedDatabase selected = SupportedDatabase.selected();
    Dialect other = selected == SupportedDatabase.H2 ? PG18 : H2;
    SessionFactoryImplementor factory = mock(SessionFactoryImplementor.class, RETURNS_DEEP_STUBS);
    when(factory.getJdbcServices().getDialect()).thenReturn(other);
    assertThatThrownBy(() -> new SelectedDatabaseCheck().integrate(null, null, factory))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("SelectedDatabaseCheck: expected " + selected);
  }
}
