package io.unitycatalog.spark;

import io.unitycatalog.client.ApiClient;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for ApiClientFactory to verify User-Agent configuration.
 */
public class ApiClientFactoryTest {

  @Test
  public void testUserAgentContainsSparkAndDelta() throws Exception {
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    String userAgent = client.getUserAgent();

    // Verify the user agent contains the base Unity Catalog client
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify the user agent contains Spark (required)
    assertThat(userAgent).contains("Spark");

    // Verify the user agent contains Delta (required)
    assertThat(userAgent).contains("Delta");

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

    // Extract and verify Delta (version may be empty but Delta must be present)
    boolean foundDelta = false;
    for (String part : parts) {
      if (part.startsWith("Delta")) {
        foundDelta = true;
        break;
      }
    }
    assertThat(foundDelta).as("Delta must be present in User-Agent").isTrue();
  }

  @Test
  public void testUserAgentWithToken() throws Exception {
    URI uri = new URI("http://localhost:8080");
    String token = "test-token-12345";
    ApiClient client = ApiClientFactory.createApiClient(uri, token);

    String userAgent = client.getUserAgent();

    // Verify user agent is set correctly even with authentication
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify both Spark and Delta are present
    assertThat(userAgent).contains("Spark");
    assertThat(userAgent).contains("Delta");

    // Verify that the token is not part of the user agent
    assertThat(userAgent).doesNotContain(token);
  }

  @Test
  public void testUserAgentFormat() throws Exception {
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    String userAgent = client.getUserAgent();

    // Verify the format follows RFC 7231: product/version [product/version ...]
    String[] parts = userAgent.split(" ");
    assertThat(parts.length).isGreaterThanOrEqualTo(3); // UC client, Spark, and Delta

    // First part should be UnityCatalog-Java-Client/version
    assertThat(parts[0]).matches("UnityCatalog-Java-Client/.*");

    // Verify Spark and Delta are present
    boolean hasSparkOrVersion = false;
    boolean hasDeltaOrVersion = false;
    for (int i = 1; i < parts.length; i++) {
      if (parts[i].startsWith("Spark")) {
        hasSparkOrVersion = true;
      }
      if (parts[i].startsWith("Delta")) {
        hasDeltaOrVersion = true;
      }
    }
    assertThat(hasSparkOrVersion).isTrue();
    assertThat(hasDeltaOrVersion).isTrue();
  }

  @Test
  public void testClientConfiguration() throws Exception {
    URI uri = new URI("https://example.com:8443");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    // Verify the client is configured with the correct URI components
    assertThat(client.getBaseUri()).contains("https://example.com:8443");

    // Verify user agent is still set with Spark and Delta
    assertThat(client.getUserAgent()).startsWith("UnityCatalog-Java-Client/");
    assertThat(client.getUserAgent()).contains("Spark");
    assertThat(client.getUserAgent()).contains("Delta");
  }
}
