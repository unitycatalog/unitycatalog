package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import io.unitycatalog.server.utils.ServerProperties;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.sql.DriverManager;
import java.util.Properties;
import org.hibernate.cfg.Configuration;
import org.junit.jupiter.api.Test;

class TestDatabasesTest {

  private static ServerProperties testServerProperties() {
    Properties properties = new Properties();
    properties.setProperty("server.env", "test");
    return new ServerProperties(properties);
  }

  @Test
  void selectedDatabaseCheckRunsOnEverySessionFactory() {
    int before = SelectedDatabaseCheck.checkCount();
    try (TestDatabase database = TestDatabases.newDatabase()) {
      database.openHibernateConfigurator(testServerProperties());
    }
    assertThat(SelectedDatabaseCheck.checkCount()).isGreaterThan(before);
  }

  @Test
  void eachDatabaseIsSeparate() {
    try (TestDatabase first = TestDatabases.newDatabase();
        TestDatabase second = TestDatabases.newDatabase()) {
      assertThat(first.jdbcUrl()).isNotEqualTo(second.jdbcUrl());
    }
  }

  @Test
  void overridesTheTestEnvironmentAndHaltsOnSchemaErrors() {
    try (TestDatabase database = TestDatabases.newDatabase()) {
      Properties properties = database.hibernateProperties(testServerProperties());
      assertThat(properties.getProperty("hibernate.connection.url")).isEqualTo(database.jdbcUrl());
      assertThat(properties.getProperty("hibernate.hbm2ddl.halt_on_error")).isEqualTo("true");
    }
  }

  @Test
  void closeReleasesOpenedConfigurators() {
    HibernateConfigurator configurator;
    try (TestDatabase database = TestDatabases.newDatabase()) {
      configurator = database.openHibernateConfigurator(testServerProperties());
      assertThat(configurator.getSessionFactory().isOpen()).isTrue();
    }
    assertThat(configurator.getSessionFactory().isOpen()).isFalse();
    assertThat(configurator.getDataSource().isClosed()).isTrue();
  }

  /** An entity whose column type no database accepts. */
  @Entity(name = "BrokenEntity")
  static class BrokenEntity {
    @Id Long id;

    @Column(columnDefinition = "not_a_real_type")
    String value;
  }

  @Test
  void failingSchemaStatementStopsTheBuild() {
    assumeTrue(
        SupportedDatabase.selected() == SupportedDatabase.H2, "checks the H2 build directly");
    try (TestDatabase database = TestDatabases.newDatabase()) {
      Properties properties = database.hibernateProperties(testServerProperties());
      Configuration configuration =
          new Configuration().setProperties(properties).addAnnotatedClass(BrokenEntity.class);
      assertThatThrownBy(configuration::buildSessionFactory)
          .hasStackTraceContaining("not_a_real_type");
    }
  }

  @Test
  void closeShutsDownTheH2Database() throws Exception {
    assumeTrue(
        SupportedDatabase.selected() == SupportedDatabase.H2, "checks the H2 release directly");
    TestDatabase database = TestDatabases.newDatabase();
    String url = database.jdbcUrl();
    DriverManager.getConnection(url, "sa", "").close();
    database.close();
    assertThatThrownBy(() -> DriverManager.getConnection(url + ";IFEXISTS=TRUE", "sa", "").close())
        .hasMessageContaining("not found");
    // Releasing a database that is gone, or was never created, leaves no database behind.
    database.close();
    assertThatThrownBy(() -> DriverManager.getConnection(url + ";IFEXISTS=TRUE", "sa", "").close())
        .hasMessageContaining("not found");
  }
}
