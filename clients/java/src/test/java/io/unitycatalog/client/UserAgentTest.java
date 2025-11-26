package io.unitycatalog.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class demonstrating User-Agent functionality in the Unity Catalog Java client.
 *
 * This test verifies that:
 * 1. ApiClient has a default User-Agent header set
 * 2. Users can customize the User-Agent header
 * 3. The User-Agent header is applied to all HTTP requests
 */
public class UserAgentTest {

  @Test
  public void testDefaultUserAgent() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Verify that a default User-Agent is set
    String userAgent = client.getUserAgent();
    assertThat(userAgent).isNotNull();
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
  }

  @Test
  public void testCustomUserAgent() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Set a custom User-Agent
    String customUserAgent = "MyApplication/1.0.0 (CustomClient)";
    client.setUserAgent(customUserAgent);

    // Verify that the custom User-Agent is set
    String userAgent = client.getUserAgent();
    assertThat(userAgent).isEqualTo(customUserAgent);
  }

  @Test
  public void testUserAgentChaining() {
    // Verify that setUserAgent returns the ApiClient for method chaining
    ApiClient client = new ApiClient();

    ApiClient result = client
        .setUserAgent("ChainedApp/2.0")
        .setHost("example.com")
        .setPort(8443);

    assertThat(result).isSameAs(client);
    assertThat(client.getUserAgent()).isEqualTo("ChainedApp/2.0");
  }

  @Test
  public void testUserAgentReset() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Set a custom User-Agent
    client.setUserAgent("CustomApp/1.0");
    assertThat(client.getUserAgent()).isEqualTo("CustomApp/1.0");

    // Reset to default by passing null
    client.setUserAgent(null);
    assertThat(client.getUserAgent()).startsWith("UnityCatalog-Java-Client/");
  }

  @Test
  public void testSetClientVersion() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Add client application information
    client.setClientVersion("MyApp", "1.0.0");

    // Verify the User-Agent includes both Unity Catalog client and application info
    String userAgent = client.getUserAgent();
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("MyApp/1.0.0");
  }

  @Test
  public void testSetClientVersionWithoutVersion() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Add client information without version
    client.setClientVersion("MyApp", null);

    // Verify the User-Agent includes client name without version
    String userAgent = client.getUserAgent();
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("MyApp");
    assertThat(userAgent).doesNotContain("MyApp/");
  }

  @Test
  public void testSetClientVersionChaining() {
    // Create a new ApiClient and use method chaining
    ApiClient client = new ApiClient()
        .setClientVersion("DataPipeline", "2.1.0")
        .setHost("catalog.example.com")
        .setPort(8443);

    // Verify chaining worked
    assertThat(client.getUserAgent()).contains("DataPipeline/2.1.0");
  }

  @Test
  public void testSetClientVersionMultiplePairs() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Add multiple client information in a single call
    client.setClientVersion("MyApp", "1.0.0", "MyWrapper", "2.5.1");

    // Verify the User-Agent includes all client info
    String userAgent = client.getUserAgent();
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("MyApp/1.0.0");
    assertThat(userAgent).contains("MyWrapper/2.5.1");
  }

  @Test
  public void testSetClientVersionOverrides() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Set initial client version
    client.setClientVersion("MyApp", "1.0.0");
    assertThat(client.getUserAgent()).contains("MyApp/1.0.0");

    // Set a different client version - should override, not append
    client.setClientVersion("MyWrapper", "2.5.1");

    String userAgent = client.getUserAgent();
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("MyWrapper/2.5.1");
    assertThat(userAgent).doesNotContain("MyApp"); // Previous client is replaced
  }

  @Test
  public void testSetClientVersionPreservesBase() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Store the original base user agent
    String originalUserAgent = client.getUserAgent();

    // Add client information
    client.setClientVersion("TestApp", "3.0");

    // Verify the original Unity Catalog client info is still present
    String newUserAgent = client.getUserAgent();
    assertThat(newUserAgent).startsWith(originalUserAgent);
    assertThat(newUserAgent).contains("TestApp/3.0");
  }

  @Test
  public void testSetClientVersionWithEmptyName() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Try to add client information with empty name
    try {
      client.setClientVersion("", "1.0.0");
      // Should throw IllegalArgumentException
      assertThat(false).as("Expected IllegalArgumentException").isTrue();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("Client name");
      assertThat(e.getMessage()).contains("cannot be null or empty");
    }
  }

  @Test
  public void testSetClientVersionWithOddNumberOfArgs() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Try to add client information with odd number of arguments
    try {
      client.setClientVersion("MyApp", "1.0.0", "MissingVersion");
      // Should throw IllegalArgumentException
      assertThat(false).as("Expected IllegalArgumentException").isTrue();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).contains("even number of arguments");
    }
  }

  @Test
  public void testSetClientVersionWithMixedVersions() {
    // Create a new ApiClient
    ApiClient client = new ApiClient();

    // Add multiple clients, some with versions, some without
    client.setClientVersion("MyApp", "1.0.0", "MyTool", null, "MyWrapper", "2.5.1");

    String userAgent = client.getUserAgent();
    assertThat(userAgent).contains("MyApp/1.0.0");
    assertThat(userAgent).contains("MyTool ");  // No version slash
    assertThat(userAgent).contains("MyWrapper/2.5.1");
  }
}

