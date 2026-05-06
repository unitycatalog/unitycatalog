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
import java.net.http.HttpResponse;
import java.util.Optional;
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

    // Cross-channel probe: the attacker omits the `catalog` query parameter and moves the field
    // into a JSON body instead. The body must not be treated as a fallback source for a
    // query-param-driven authz decision. Today the request fails with 404 (Armeria rejects the
    // missing required query param before authz runs); if a future framework change routes the
    // request past Armeria's value resolver, authz would see #catalog=null and must still deny
    // with 403. Any 4xx plus an empty config body is the security invariant we guard.
    HttpResponse<String> rawWithBodyNoQuery =
        TestUtils.sendRawGet(
            regularConfig,
            "/api/2.1/unity-catalog/delta/v1/config",
            Optional.of("{\"catalog\":\"" + TestUtils.CATALOG_NAME + "\"}"));
    assertThat(rawWithBodyNoQuery.statusCode())
        .as("body must not act as a fallback source for a query-param-driven authz decision")
        .isBetween(400, 499);
    assertThat(rawWithBodyNoQuery.body())
        .as("response must not leak the catalog config to an unauthorized caller")
        .doesNotContain("\"endpoints\"");

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
