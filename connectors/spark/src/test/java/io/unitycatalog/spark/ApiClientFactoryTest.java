package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.ApiClient;
import java.net.URI;
import org.junit.jupiter.api.Test;

/**
 * Test class for ApiClientFactory to verify User-Agent configuration and client setup.
 */
public class ApiClientFactoryTest {

  @Test
  public void testUserAgentContainsSpark() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(clientConf, uri, null);

    String userAgent = client.getUserAgent();

    // Verify the user agent contains the base Unity Catalog client
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify the user agent contains Spark (required)
    assertThat(userAgent).contains("Spark");

    // Extract and verify Spark version (version may be empty but Spark must be present)
    boolean foundSpark = false;
    String[] parts = userAgent.split(" ");
    for (String part : parts) {
      if (part.startsWith("Spark")) {
        foundSpark = true;
        break;
      }
    }
    assertThat(foundSpark).as("Spark must be present in User-Agent").isTrue();

    // Delta is optional - verify it if present
    boolean foundDelta = false;
    for (String part : parts) {
      if (part.startsWith("Delta")) {
        foundDelta = true;
        break;
      }
    }
    // Note: Delta may or may not be present depending on classpath
  }

  @Test
  public void testUserAgentWithToken() throws Exception {
    ApiClientConf clientConf = new ApiClientConf();
    URI uri = new URI("http://localhost:8080");
    String token = "test-token-12345";
    ApiClient client = ApiClientFactory.createApiClient(clientConf, uri, token);

    String userAgent = client.getUserAgent();

    // Verify user agent is set correctly even with authentication
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify Spark is present (required)
    assertThat(userAgent).contains("Spark");

    // Delta is optional
    // assertThat(userAgent).contains("Delta"); // May or may not be present

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

    // Delta is optional - we don't assert on it
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
    // Delta is optional
  }

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
