package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import java.net.URI;
import org.junit.jupiter.api.Test;

public class ApiClientFactoryTest extends BaseSparkIntegrationTest {
  @Test
  public void testApiClientBaseUri() {
    ApiClientConf clientConf = new ApiClientConf();
    String token = "";
    URI uriNoSuffix = URI.create("https://localhost:8080");
    ApiClient apiClientNoSuffix = ApiClientFactory.createApiClient(clientConf, uriNoSuffix, token);
    assertThat(apiClientNoSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/api/2.1/unity-catalog");

    URI uriWithSuffix = URI.create("https://localhost:8080/path/to/uc/api");
    ApiClient apiClientWithSuffix =
        ApiClientFactory.createApiClient(clientConf, uriWithSuffix, token);
    assertThat(apiClientWithSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/path/to/uc/api/api/2.1/unity-catalog");
  }
}
