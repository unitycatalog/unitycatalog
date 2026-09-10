package io.unitycatalog.server.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestDatabaseUtilsTest {

  @Test
  public void testParseDatabaseTypeDefaultsToH2() {
    assertThat(TestDatabaseUtils.parseDatabaseType(null))
        .isEqualTo(TestDatabaseUtils.DatabaseType.H2);
    assertThat(TestDatabaseUtils.parseDatabaseType(""))
        .isEqualTo(TestDatabaseUtils.DatabaseType.H2);
  }

  @Test
  public void testParseDatabaseTypeAcceptsSupportedDatabases() {
    assertThat(TestDatabaseUtils.parseDatabaseType("h2"))
        .isEqualTo(TestDatabaseUtils.DatabaseType.H2);
    assertThat(TestDatabaseUtils.parseDatabaseType("PostgreSQL"))
        .isEqualTo(TestDatabaseUtils.DatabaseType.POSTGRESQL);
    assertThat(TestDatabaseUtils.parseDatabaseType("mysql"))
        .isEqualTo(TestDatabaseUtils.DatabaseType.MYSQL);
  }

  @Test
  public void testParseDatabaseTypeRejectsUnsupportedDatabase() {
    assertThatThrownBy(() -> TestDatabaseUtils.parseDatabaseType("postgres"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("UC_TEST_DATABASE must be one of: h2, postgresql, mysql")
        .hasMessageContaining("postgres");
  }
}
