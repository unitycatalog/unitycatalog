package io.unitycatalog.docker.tests;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.docker.tests.support.TenantBootstrap;
import io.unitycatalog.docker.tests.support.UcOperations;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Bootstrap a tenant (credential, external locations, catalog, user, grants) and verify catalog
 * access via token exchange.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BootstrapTenantTest extends DockerIntegrationTestBase {

  private static final String TENANT_ID = "BOOTSTRAPJAVA";
  private static final String CATALOG = "catbootstrapjava";
  private static final String USER_EMAIL = "bootstrapjava@example.com";
  private static final String CREDENTIAL = "cred_bootstrapjava";
  private static final String EL = "el_catbootstrapjava";
  private static final String DATA_EL = "el_catbootstrapjava_data";

  private String userToken;

  @AfterAll
  void cleanup() throws Exception {
    deleteTenant(CATALOG, EL, DATA_EL, CREDENTIAL);
  }

  @Test
  @Order(1)
  void bootstrapTenant() throws Exception {
    deleteTenant(CATALOG, EL, DATA_EL, CREDENTIAL);
    bootstrapTenant(tenantSpec(TENANT_ID, CATALOG, USER_EMAIL, CREDENTIAL, EL, DATA_EL));
  }

  @Test
  @Order(2)
  void catalogUserCanExchangeTokenAndSeeCatalog() throws Exception {
    userToken = userToken(USER_EMAIL, null);
    assertThat(userToken).isNotBlank();
    new UcOperations(SERVER, userToken).assertCatalogVisible(CATALOG);
  }
}
