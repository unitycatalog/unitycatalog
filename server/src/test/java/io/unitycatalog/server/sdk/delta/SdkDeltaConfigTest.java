package io.unitycatalog.server.sdk.delta;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.ConfigurationApi;
import io.unitycatalog.client.delta.model.CatalogConfig;
import io.unitycatalog.client.delta.model.ErrorType;
import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.base.catalog.CatalogOperations;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SdkDeltaConfigTest extends BaseServerTest {

  protected CatalogOperations catalogOperations;
  private ConfigurationApi configApi;

  @BeforeEach
  public void setUp() {
    super.setUp();
    catalogOperations = new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
    configApi = new ConfigurationApi(TestUtils.createApiClient(serverConfig));
    cleanUp();
    createCatalog();
  }

  private void cleanUp() {
    try {
      catalogOperations.deleteCatalog(TestUtils.CATALOG_NAME, Optional.of(true));
    } catch (Exception e) {
      // Ignore
    }
  }

  private void createCatalog() {
    try {
      catalogOperations.createCatalog(
          new CreateCatalog().name(TestUtils.CATALOG_NAME).comment("test"));
    } catch (ApiException e) {
      throw new RuntimeException("Failed to create test catalog", e);
    }
  }

  @Test
  public void testGetConfig() throws Exception {
    // Success: valid catalog returns endpoints and protocol version
    CatalogConfig config = configApi.getConfig(TestUtils.CATALOG_NAME, "1.0");
    assertThat(config.getEndpoints()).isNotEmpty();
    assertThat(config.getProtocolVersion()).isEqualTo("1.0");

    // Error: missing catalog returns 400 with InvalidParameterValueException
    TestUtils.assertDeltaApiException(
        () -> configApi.getConfig("", "1.0"),
        ErrorType.INVALID_PARAMETER_VALUE_EXCEPTION,
        "catalog");

    // Error: nonexistent catalog returns 404 with NoSuchCatalogException
    TestUtils.assertDeltaApiException(
        () -> configApi.getConfig("nonexistent", "1.0"),
        ErrorType.NO_SUCH_CATALOG_EXCEPTION,
        "nonexistent");
  }
}
