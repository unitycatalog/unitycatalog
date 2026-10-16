package io.unitycatalog.server.testdatabase;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.server.base.BaseServerTest;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;

/** A failing cleanup query must still stop the server and release the database. */
class BaseServerTestCleanupFailureTest extends BaseServerTest {

  @Test
  void tearDownClosesServerAndDatabaseWhenCleanupFails() {
    try (Session session = hibernateConfigurator.getSessionFactory().openSession()) {
      session.beginTransaction();
      // tearDown's first cleanup query deletes from this table.
      session.createNativeMutationQuery("drop table uc_function_params").executeUpdate();
      session.getTransaction().commit();
    }
    assertThatThrownBy(this::tearDown).isNotNull();
    assertThat(unityCatalogServer).isNull();
    assertThat(testDatabase).isNull();
  }
}
