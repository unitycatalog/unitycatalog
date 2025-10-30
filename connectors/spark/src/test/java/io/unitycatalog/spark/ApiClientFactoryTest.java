package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class ApiClientFactoryTest extends BaseSparkIntegrationTest {
  @Test
  public void testApiClientBaseUri() {
    String token = "";

    URI uriDatabricks = URI.create("https://adb-1234567890123456.3.azuredatabricks.net");
    ApiClient apiClientDatabricks = ApiClientFactory.createApiClient(uriDatabricks, token);
    assertThat(apiClientDatabricks.getBaseUri())
        .isEqualTo("https://adb-1234567890123456.3.azuredatabricks.net/api/2.1/unity-catalog");

    URI uriMicrosoftFabric = URI.create("https://onelake.table.fabric.microsoft.com/delta/workspace/test.lakehouse");
    ApiClient apiClientFabric = ApiClientFactory
        .createApiClient(uriMicrosoftFabric, token);
    assertThat(apiClientFabric.getBaseUri())
        .isEqualTo("https://onelake.table.fabric.microsoft.com/delta/workspace/test.lakehouse/api/2.1/unity-catalog");
  }
}
