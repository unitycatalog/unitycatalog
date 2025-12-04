package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientConf;
import io.unitycatalog.client.auth.FixedUCTokenProvider;
import java.net.URI;
import org.junit.jupiter.api.Test;

/** Test class for ApiClientFactory to verify User-Agent configuration and client setup. */
public class ApiClientFactoryTest {

  @Test
  public void testUserAgentContainsSparkAndDelta() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(clientConf, uri, null);

    String userAgent = client.getUserAgent();

    // Verify the user agent contains the base Unity Catalog client
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Get expected Spark version
    String expectedSparkVersion = org.apache.spark.package$.MODULE$.SPARK_VERSION();
    assertThat(userAgent).contains("Spark/" + expectedSparkVersion);

    // Get expected Delta version and verify
    String expectedDeltaVersion = io.delta.package$.MODULE$.VERSION();
    assertThat(expectedDeltaVersion).isNotNull();
    assertThat(userAgent).contains("Delta/" + expectedDeltaVersion);
  }

  @Test
  public void testUserAgentWithToken() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("http://localhost:8080");
    String token = "test-token-12345";
    ApiClient client = createApiClient(clientConf, uri, token);

    String userAgent = client.getUserAgent();

    // Verify user agent is set correctly even with authentication
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("Spark");
    assertThat(userAgent).contains("Delta");

    // Verify that the token is not part of the user agent
    assertThat(userAgent).doesNotContain(token);
  }

  @Test
  public void testUserAgentFormat() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(clientConf, uri, null);

    String userAgent = client.getUserAgent();

    // Verify the format follows RFC 7231: product/version [product/version ...]
    String[] parts = userAgent.split(" ");
    assertThat(parts.length).isGreaterThanOrEqualTo(2); // At least UC client and Spark

    // First part should be UnityCatalog-Java-Client/version
    assertThat(parts[0]).matches("UnityCatalog-Java-Client/.*");

    // Verify Spark is present (required)
    boolean hasSparkOrVersion = false;
    for (int i = 1; i < parts.length; i++) {
      if (parts[i].startsWith("Spark")) {
        hasSparkOrVersion = true;
        break;
      }
    }
    assertThat(hasSparkOrVersion).isTrue();
  }

  @Test
  public void testClientConfiguration() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("https://example.com:8443");
    ApiClient client = ApiClientFactory.createApiClient(clientConf, uri, null);

    // Verify the client is configured with the correct URI components
    assertThat(client.getBaseUri()).contains("https://example.com:8443");

    // Verify user agent is still set with at least Spark
    assertThat(client.getUserAgent()).startsWith("UnityCatalog-Java-Client/");
    assertThat(client.getUserAgent()).contains("Spark");
  }

  @Test
  public void testApiClientBaseUri() {
    ApiClientConf clientConf = new ApiClientConf();
    String token = "";
    URI uriNoSuffix = URI.create("https://localhost:8080");
    ApiClient apiClientNoSuffix = createApiClient(clientConf, uriNoSuffix, token);
    assertThat(apiClientNoSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/api/2.1/unity-catalog");

    URI uriWithSuffix = URI.create("https://localhost:8080/path/to/uc/api");
    ApiClient apiClientWithSuffix = createApiClient(clientConf, uriWithSuffix, token);
    assertThat(apiClientWithSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/path/to/uc/api/api/2.1/unity-catalog");
  }

  public static ApiClient createApiClient(ApiClientConf conf, URI uri, String token) {
    return ApiClientFactory.createApiClient(conf, uri, new FixedUCTokenProvider(token));
  }
}
