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
  public void testUserAgentContainsSparkVersion() throws Exception {
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    String userAgent = client.getUserAgent();
    System.out.println("User-Agent: " + userAgent);

    // Verify the user agent contains the base Unity Catalog client
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify the user agent contains Spark version
    assertThat(userAgent).contains("Spark/");

    // Extract and verify Spark version is not empty
    String[] parts = userAgent.split(" ");
    boolean foundSpark = false;
    for (String part : parts) {
      if (part.startsWith("Spark/")) {
        String version = part.substring("Spark/".length());
        assertThat(version).isNotEmpty();
        foundSpark = true;
        System.out.println("Found Spark version: " + version);
        break;
      }
    }
    assertThat(foundSpark).isTrue();
  }

  @Test
  public void testUserAgentMayContainDeltaVersion() throws Exception {
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    String userAgent = client.getUserAgent();
    System.out.println("User-Agent: " + userAgent);

    // Delta version is optional (depends on classpath)
    // If Delta is available, verify the version is present
    if (userAgent.contains("Delta/")) {
      String[] parts = userAgent.split(" ");
      boolean foundDelta = false;
      for (String part : parts) {
        if (part.startsWith("Delta/")) {
          String version = part.substring("Delta/".length());
          assertThat(version).isNotEmpty();
          foundDelta = true;
          System.out.println("Found Delta version: " + version);
          break;
        }
      }
      assertThat(foundDelta).isTrue();
    } else {
      System.out.println("Delta not available in classpath (expected in some environments)");
    }
  }

  @Test
  public void testUserAgentWithToken() throws Exception {
    URI uri = new URI("http://localhost:8080");
    String token = "test-token-12345";
    ApiClient client = ApiClientFactory.createApiClient(uri, token);

    String userAgent = client.getUserAgent();
    System.out.println("User-Agent with token: " + userAgent);

    // Verify user agent is set correctly even with authentication
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("Spark/");

    // Verify that the token is not part of the user agent
    assertThat(userAgent).doesNotContain(token);
  }

  @Test
  public void testUserAgentFormat() throws Exception {
    URI uri = new URI("http://localhost:8080");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    String userAgent = client.getUserAgent();
    System.out.println("User-Agent format check: " + userAgent);

    // Verify the format follows RFC 7231: product/version [product/version ...]
    String[] parts = userAgent.split(" ");
    assertThat(parts.length).isGreaterThanOrEqualTo(2); // At least UC client and Spark

    // First part should be UnityCatalog-Java-Client/version
    assertThat(parts[0]).matches("UnityCatalog-Java-Client/.*");

    // At least one more part should be Spark/version
    boolean hasSparkVersion = false;
    for (int i = 1; i < parts.length; i++) {
      if (parts[i].startsWith("Spark/")) {
        assertThat(parts[i]).matches("Spark/.*");
        hasSparkVersion = true;
        break;
      }
    }
    assertThat(hasSparkVersion).isTrue();
  }

  @Test
  public void testClientConfiguration() throws Exception {
    URI uri = new URI("https://example.com:8443");
    ApiClient client = ApiClientFactory.createApiClient(uri, null);

    // Verify the client is configured with the correct URI components
    assertThat(client.getBaseUri()).contains("https://example.com:8443");

    // Verify user agent is still set
    assertThat(client.getUserAgent()).startsWith("UnityCatalog-Java-Client/");
  }
}

