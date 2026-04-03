package io.unitycatalog.server.sdk.access.delta;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.ConfigurationApi;
import io.unitycatalog.client.delta.model.CatalogConfig;
import io.unitycatalog.client.model.SecurableType;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.sdk.access.SdkAccessControlBaseCRUDTest;
import io.unitycatalog.server.sdk.catalog.SdkCatalogOperations;
import io.unitycatalog.server.utils.TestUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

/** Access control tests for the Delta REST Catalog /config endpoint. */
public class DeltaConfigAccessControlTest extends SdkAccessControlBaseCRUDTest {

  @Override
  protected SdkCatalogOperations createCatalogOperations(ServerConfig serverConfig) {
    return new SdkCatalogOperations(TestUtils.createApiClient(serverConfig));
  }

  @Test
  @SneakyThrows
  public void testGetConfigAccessControl() {
    // Unauthenticated: no token returns 401
    ServerConfig noAuthConfig = new ServerConfig(serverConfig.getServerUrl(), null);
    ConfigurationApi noAuthApi = new ConfigurationApi(TestUtils.createApiClient(noAuthConfig));
    assertThatThrownBy(() -> noAuthApi.getConfig(TestUtils.CATALOG_NAME, "1.0"))
        .isInstanceOf(ApiException.class)
        .satisfies(e -> assertThat(((ApiException) e).getCode()).isEqualTo(401));

    // Unauthorized: valid user without permissions returns 403
    createCommonTestUsers();
    ServerConfig regularConfig = createTestUserServerConfig(REGULAR_1);
    ConfigurationApi regularApi = new ConfigurationApi(TestUtils.createApiClient(regularConfig));
    assertThatThrownBy(() -> regularApi.getConfig(TestUtils.CATALOG_NAME, "1.0"))
        .isInstanceOf(ApiException.class)
        .satisfies(e -> assertThat(((ApiException) e).getCode()).isEqualTo(403));

    // Authorized: user with USE_CATALOG returns 200
    ServerConfig principalConfig = createTestUserServerConfig(PRINCIPAL_1);
    ConfigurationApi principalApi =
        new ConfigurationApi(TestUtils.createApiClient(principalConfig));
    grantPermissions(
        PRINCIPAL_1, SecurableType.CATALOG, TestUtils.CATALOG_NAME, Privileges.USE_CATALOG);
    CatalogConfig config = principalApi.getConfig(TestUtils.CATALOG_NAME, "1.0");
    assertThat(config.getEndpoints()).isNotEmpty();
    assertThat(config.getProtocolVersion()).isEqualTo("1.0");
  }
}
