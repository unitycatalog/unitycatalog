package io.unitycatalog.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.auth.TokenProviderUtils;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test class for User-Agent functionality in the Unity Catalog Java client.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>ApiClientBuilder sets a default User-Agent header
 *   <li>Users can customize the User-Agent header through addAppVersion
 *   <li>The User-Agent header is properly formatted and applied to HTTP requests
 *   <li>Multiple application versions can be added and are properly formatted
 * </ul>
 */
public class UserAgentTest {

  private static final String TEST_URI = "http://localhost:8080";
  private static final String TEST_TOKEN = "test-token";
  private static final TokenProvider TOKEN_PROVIDER = TokenProviderUtils.create(TEST_TOKEN);

  /**
   * Helper method to extract the User-Agent header value from an ApiClient's request interceptor.
   *
   * @param client the ApiClient to extract the User-Agent from
   * @return the User-Agent header value
   */
  private String extractUserAgent(ApiClient client) {
    HttpRequest.Builder mockRequestBuilder = mock(HttpRequest.Builder.class);
    client.getRequestInterceptor().accept(mockRequestBuilder);

    ArgumentCaptor<String> headerNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> headerValueCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockRequestBuilder, atLeastOnce())
        .header(headerNameCaptor.capture(), headerValueCaptor.capture());

    assertThat(headerNameCaptor.getAllValues()).contains("User-Agent");
    int userAgentIndex = headerNameCaptor.getAllValues().indexOf("User-Agent");
    return headerValueCaptor.getAllValues().get(userAgentIndex);
  }

  @Test
  public void testDefaultUserAgent() {
    ApiClient client =
        ApiClientBuilder.create().uri(TEST_URI).tokenProvider(TOKEN_PROVIDER).build();

    String userAgent = extractUserAgent(client);
    assertThat(userAgent).contains("UnityCatalog-Java-Client");
    assertThat(userAgent).contains(VersionUtils.VERSION);
  }

  @Test
  public void testCustomUserAgentWithSingleApp() {
    ApiClient client =
        ApiClientBuilder.create()
            .uri(TEST_URI)
            .tokenProvider(TOKEN_PROVIDER)
            .addAppVersion("MyApp", "1.0.0")
            .build();

    String userAgent = extractUserAgent(client);
    assertThat(userAgent).contains("UnityCatalog-Java-Client", "MyApp", "1.0.0");
  }

  @Test
  public void testCustomUserAgentWithMultipleApps() {
    ApiClient client =
        ApiClientBuilder.create()
            .uri(TEST_URI)
            .tokenProvider(TOKEN_PROVIDER)
            .addAppVersion("MyApp", "1.0.0")
            .addAppVersion("MyWrapper", "2.5.1")
            .addAppVersion("Java", "17")
            .build();

    String userAgent = extractUserAgent(client);
    assertThat(userAgent)
        .contains("UnityCatalog-Java-Client", "MyApp", "1.0.0", "MyWrapper", "2.5.1", "Java", "17");
  }

  @Test
  public void testUserAgentChaining() {
    ApiClientBuilder builder = ApiClientBuilder.create();
    ApiClientBuilder result =
        builder
            .uri(TEST_URI)
            .tokenProvider(TOKEN_PROVIDER)
            .addAppVersion("ChainedApp", "2.0")
            .addAppVersion("Framework", "Spring");

    assertThat(result).isSameAs(builder);

    ApiClient client = result.build();
    String userAgent = extractUserAgent(client);
    assertThat(userAgent).contains("ChainedApp", "2.0", "Framework", "Spring");
  }

  @Test
  public void testAddAppVersionValidation() {
    // Test validation: null name
    assertThatThrownBy(() -> ApiClientBuilder.create().addAppVersion(null, "1.0.0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("App name cannot be null or empty");

    // Test validation: empty name
    assertThatThrownBy(() -> ApiClientBuilder.create().addAppVersion("", "1.0.0"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("App name cannot be null or empty");

    // Test validation: null version
    assertThatThrownBy(() -> ApiClientBuilder.create().addAppVersion("MyApp", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("App version cannot be null or empty");

    // Test validation: empty version
    assertThatThrownBy(() -> ApiClientBuilder.create().addAppVersion("MyApp", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("App version cannot be null or empty");
  }

  @Test
  public void testAppVersionOrdering() {
    ApiClient client =
        ApiClientBuilder.create()
            .uri(TEST_URI)
            .tokenProvider(TOKEN_PROVIDER)
            .addAppVersion("FirstApp", "1.0")
            .addAppVersion("SecondApp", "2.0")
            .addAppVersion("ThirdApp", "3.0")
            .build();

    String userAgent = extractUserAgent(client);

    // Verify ordering - UnityCatalog client should appear first
    int ucIndex = userAgent.indexOf("UnityCatalog-Java-Client");
    int firstAppIndex = userAgent.indexOf("FirstApp");
    int secondAppIndex = userAgent.indexOf("SecondApp");
    int thirdAppIndex = userAgent.indexOf("ThirdApp");

    assertThat(ucIndex).isLessThan(firstAppIndex);
    assertThat(firstAppIndex).isLessThan(secondAppIndex);
    assertThat(secondAppIndex).isLessThan(thirdAppIndex);
  }

  @Test
  public void testDuplicateAppNameOverrides() {
    ApiClient client =
        ApiClientBuilder.create()
            .uri(TEST_URI)
            .tokenProvider(TOKEN_PROVIDER)
            .addAppVersion("MyApp", "1.0.0")
            .addAppVersion("MyApp", "2.0.0") // Should override the previous version
            .build();

    String userAgent = extractUserAgent(client);

    // Verify only the latest version is present
    assertThat(userAgent).contains("MyApp", "2.0.0");
    assertThat(userAgent).doesNotContain("1.0.0");
  }
}
