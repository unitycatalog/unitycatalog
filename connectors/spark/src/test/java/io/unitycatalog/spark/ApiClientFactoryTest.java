package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.cli.utils.VersionUtils;
import io.unitycatalog.client.ApiClient;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class ApiClientFactoryTest {
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

  @Test
  public void testUserAgent() {
    URI testUri = URI.create("https://localhost:8080/api/test");

    // Test User-Agent with custom info and Authorization with token
    Map<String, String> customInfo =
        Map.of(
            "SparkVersion", "3.5.0",
            "DeltaVersion", "3.0.0");

    HttpRequest request1 = buildRequest(testUri, "test-token", customInfo);
    assertThat(request1.headers().firstValue("Authorization"))
        .isPresent()
        .hasValue("Bearer test-token");
    assertThat(request1.headers().firstValue("User-Agent"))
        .isPresent()
        .get()
        .asString()
        .contains("UnityCatalog-Java-Client/" + VersionUtils.VERSION)
        .contains("SparkVersion/3.5.0")
        .contains("DeltaVersion/3.0.0");

    // Test User-Agent with only SDK version when custom info is empty
    HttpRequest request2 = buildRequest(testUri, "token", new HashMap<>());
    assertThat(request2.headers().firstValue("User-Agent"))
        .isPresent()
        .hasValue("UnityCatalog-Java-Client/" + VersionUtils.VERSION);

    // User-Agent with multiple custom entries
    Map<String, String> multipleInfo =
        Map.of(
            "SparkVersion", "3.5.0",
            "DeltaVersion", "3.0.0",
            "HadoopVersion", "3.3.4");
    HttpRequest request3 = buildRequest(testUri, "token", multipleInfo);
    String userAgent = request3.headers().firstValue("User-Agent").orElse("");
    assertThat(userAgent)
        .contains("UnityCatalog-Java-Client/" + VersionUtils.VERSION)
        .contains("SparkVersion/3.5.0")
        .contains("DeltaVersion/3.0.0")
        .contains("HadoopVersion/3.3.4");
    assertThat(userAgent.split(" ").length).isGreaterThanOrEqualTo(4);

    // Integration with createApiClient using default VERSIONS
    ApiClientConf clientConf = new ApiClientConf();
    ApiClient apiClient =
        ApiClientFactory.createApiClient(clientConf, URI.create("https://localhost:8080"), "token");
    assertThat(apiClient).isNotNull();

    HttpRequest request4 = buildRequest(testUri, "token", ApiClientFactory.VERSIONS);
    String defaultUserAgent = request4.headers().firstValue("User-Agent").orElse("");
    assertThat(defaultUserAgent).contains("UnityCatalog-Java-Client/" + VersionUtils.VERSION);
    assertThat(defaultUserAgent).containsAnyOf("SparkVersion/", "DeltaVersion/");
  }

  private HttpRequest buildRequest(URI uri, String token, Map<String, String> userAgentInfo) {
    HttpRequest.Builder builder = HttpRequest.newBuilder().uri(uri);
    ApiClientFactory.UCHttpInterceptor interceptor =
        new ApiClientFactory.UCHttpInterceptor(token, userAgentInfo);
    interceptor.accept(builder);
    return builder.GET().build();
  }
}
