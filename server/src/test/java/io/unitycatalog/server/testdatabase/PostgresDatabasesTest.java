package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.utils.ServerProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

@EnabledIfEnvironmentVariable(named = SupportedDatabase.ENV, matches = "POSTGRES_.*")
class PostgresDatabasesTest {

  private static final ServerProperties SERVER = new ServerProperties(new Properties());

  @Test
  void eachTestGetsAnEmptyCopyOfTheSchema() throws Exception {
    try (TestDatabase first = TestDatabases.newDatabase();
        TestDatabase second = TestDatabases.newDatabase()) {
      try (Connection c = connect(first.hibernateProperties(SERVER))) {
        c.createStatement()
            .execute(
                "insert into uc_catalogs (id, name) values (gen_random_uuid(), 'only_in_first')");
        c.commit();
      }
      try (Connection c = connect(second.hibernateProperties(SERVER));
          ResultSet rs = c.createStatement().executeQuery("select count(*) from uc_catalogs")) {
        rs.next();
        assertThat(rs.getInt(1)).isZero();
      }
    }
  }

  @Test
  void closeDropsTheDatabase() {
    TestDatabase database = TestDatabases.newDatabase();
    Properties p = database.hibernateProperties(SERVER);
    database.close();
    assertThatThrownBy(() -> connect(p).close()).hasMessageContaining("does not exist");
  }

  @Test
  void templateFailureIsCachedAndRethrown() {
    RuntimeException first = new IllegalStateException("boom");
    PostgresDatabases failed = PostgresDatabases.failed(SupportedDatabase.POSTGRES_18, first);
    assertThatThrownBy(() -> failed.newDatabase("uc_x"))
        .hasMessage("POSTGRES_18 schema build failed earlier; see the first failure")
        .hasCause(first);
  }

  private static Connection connect(Properties p) throws Exception {
    Connection c =
        DriverManager.getConnection(
            p.getProperty("hibernate.connection.url"),
            p.getProperty("hibernate.connection.username"),
            p.getProperty("hibernate.connection.password"));
    c.setAutoCommit(false);
    return c;
  }
}
