package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.persist.utils.FileOperations;
import io.unitycatalog.server.persist.utils.HibernateConfigurator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A server build that fails after the configurator exists must still close its pool. */
class BaseServerTestBuildFailureTest extends BaseServerTest {

  @Override
  protected FileOperations decorateFileOperations(FileOperations fileOperations) {
    throw new IllegalStateException("server build fails after the configurator is built");
  }

  // JUnit does not carry @BeforeEach onto an overriding method, so repeat it.
  @BeforeEach
  @Override
  public void setUp() {
    try {
      super.setUp();
    } catch (IllegalStateException expected) {
      // The server build fails; tearDown must still close the configurator.
    }
  }

  @Test
  void tearDownClosesTheConfiguratorAfterFailedServerBuild() {
    assertThat(unityCatalogServer).isNull();
    HibernateConfigurator configurator = hibernateConfigurator;
    assertThat(configurator).isNotNull();
    tearDown();
    assertThat(configurator.getDataSource().isClosed()).isTrue();
    assertThat(testDatabase).isNull();
  }
}
