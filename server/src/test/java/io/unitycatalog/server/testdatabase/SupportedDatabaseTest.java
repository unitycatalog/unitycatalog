package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hibernate.dialect.DatabaseVersion;
import org.hibernate.dialect.H2Dialect;
import org.hibernate.dialect.PostgreSQLDialect;
import org.junit.jupiter.api.Test;

class SupportedDatabaseTest {

  @Test
  void unsetOrEmptyMeansH2() {
    assertThat(SupportedDatabase.parse(null)).isEqualTo(SupportedDatabase.H2);
    assertThat(SupportedDatabase.parse("")).isEqualTo(SupportedDatabase.H2);
  }

  @Test
  void parsesExactConstantNames() {
    assertThat(SupportedDatabase.parse("H2")).isEqualTo(SupportedDatabase.H2);
    assertThat(SupportedDatabase.parse("POSTGRES_18")).isEqualTo(SupportedDatabase.POSTGRES_18);
  }

  @Test
  void unknownValueListsTheSupportedValues() {
    assertThatThrownBy(() -> SupportedDatabase.parse("postgres"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("UC_TEST_DATABASE=postgres is not one of [H2, POSTGRES_18]");
  }

  @Test
  void matchesFamilyAndMajorVersion() {
    var h2 = new H2Dialect(DatabaseVersion.make(2, 2, 224));
    var pg18 = new PostgreSQLDialect(DatabaseVersion.make(18, 6));
    var pg16 = new PostgreSQLDialect(DatabaseVersion.make(16, 15));
    assertThat(SupportedDatabase.H2.matches(h2)).isTrue();
    assertThat(SupportedDatabase.H2.matches(pg18)).isFalse();
    assertThat(SupportedDatabase.POSTGRES_18.matches(pg18)).isTrue();
    assertThat(SupportedDatabase.POSTGRES_18.matches(pg16)).isFalse();
    assertThat(SupportedDatabase.POSTGRES_18.matches(h2)).isFalse();
  }

  @Test
  void describesDialectAsFamilyAndVersion() {
    assertThat(SupportedDatabase.describe(new H2Dialect(DatabaseVersion.make(2, 2, 224))))
        .isEqualTo("H2 2.2.224");
    assertThat(SupportedDatabase.describe(new PostgreSQLDialect(DatabaseVersion.make(18, 6))))
        .isEqualTo("PostgreSQL 18.6.0");
  }

  @Test
  void onlyPostgresHasAnImage() {
    assertThat(SupportedDatabase.H2.image()).isEmpty();
    assertThat(SupportedDatabase.POSTGRES_18.image())
        .contains(
            "postgres@sha256:77f585114c32fbca283dc835b0596f4e52b51b4c6662d7810b2f4084f60a1873");
  }
}
