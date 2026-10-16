package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A configurator that fails to build must still release the database in tearDown. */
class BaseServerTestSetupFailureTest extends BaseServerTest {

  @Override
  protected void setUpHibernateProperties(Properties hibernateProperties) {
    hibernateProperties.setProperty("hibernate.connection.url", "jdbc:not-a-driver:x");
  }

  // JUnit does not carry @BeforeEach onto an overriding method, so repeat it.
  @BeforeEach
  @Override
  public void setUp() {
    try {
      super.setUp();
    } catch (RuntimeException expected) {
      // The configurator build fails; tearDown must still release testDatabase.
    }
  }

  @Test
  void tearDownReleasesTheDatabaseAfterFailedSetUp() {
    assertThat(unityCatalogServer).isNull();
    assertThat(testDatabase).isNotNull();
    tearDown();
    assertThat(testDatabase).isNull();
  }
}
