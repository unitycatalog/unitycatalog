package io.unitycatalog.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.auth.TokenProviderUtils;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Unit tests for {@link ApiClientBuilder}.
 *
 * <p>This test class verifies the builder pattern functionality, configuration validation, and
 * proper setup of ApiClient instances.
 */
public class ApiClientBuilderTest {

  private static final String TEST_URI = "http://localhost:8080";
  private static final String TEST_TOKEN = "test-token";

  @Test
  public void testBasicBuildAndUriConfiguration() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);

    // Test basic build with minimal configuration
    ApiClient client1 =
        ApiClientBuilder.create().uri(TEST_URI).tokenProvider(tokenProvider).build();
    assertThat(client1).isNotNull();
    assertThat(client1.getRequestInterceptor()).isNotNull();

    // Test URI with String parameter (different host and port)
    ApiClient client2 =
        ApiClientBuilder.create()
            .uri("https://example.com:9090")
            .tokenProvider(tokenProvider)
            .build();
    assertThat(client2).isNotNull();

    // Test URI with URI parameter
    ApiClient client3 =
        ApiClientBuilder.create()
            .uri(URI.create("https://catalog.example.com:8443"))
            .tokenProvider(tokenProvider)
            .build();
    assertThat(client3).isNotNull();

    // Test URI with custom path
    ApiClient client4 =
        ApiClientBuilder.create()
            .uri("http://localhost:8080/custom/path")
            .tokenProvider(tokenProvider)
            .build();
    assertThat(client4).isNotNull();

    // Test URI without explicit port
    ApiClient client5 =
        ApiClientBuilder.create().uri("https://example.com").tokenProvider(tokenProvider).build();
    assertThat(client5).isNotNull();

    // Verify multiple builds produce different instances
    ApiClientBuilder builder = ApiClientBuilder.create().uri(TEST_URI).tokenProvider(tokenProvider);
    assertThat(builder.build()).isNotSameAs(builder.build());
  }

  @Test
  public void testTokenProviderConfiguration() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);
    ApiClient client = ApiClientBuilder.create().uri(TEST_URI).tokenProvider(tokenProvider).build();

    assertThat(client.getRequestInterceptor()).isNotNull();

    // Verify Authorization header is set correctly
    HttpRequest.Builder mockRequestBuilder = mock(HttpRequest.Builder.class);
    client.getRequestInterceptor().accept(mockRequestBuilder);

    ArgumentCaptor<String> headerNameCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> headerValueCaptor = ArgumentCaptor.forClass(String.class);
    verify(mockRequestBuilder, org.mockito.Mockito.atLeastOnce())
        .header(headerNameCaptor.capture(), headerValueCaptor.capture());

    assertThat(headerNameCaptor.getAllValues()).contains("Authorization");
    int authIndex = headerNameCaptor.getAllValues().indexOf("Authorization");
    assertThat(headerValueCaptor.getAllValues().get(authIndex)).isEqualTo("Bearer " + TEST_TOKEN);
  }

  @Test
  public void testAddAppVersionConfiguration() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);

    // Test that addAppVersion method works and returns builder for chaining
    ApiClientBuilder builder = ApiClientBuilder.create();
    ApiClientBuilder result =
        builder.uri(TEST_URI).tokenProvider(tokenProvider).addAppVersion("MyApp", "1.0.0");

    assertThat(result).isSameAs(builder);

    // Verify client builds successfully with app versions
    ApiClient client = result.addAppVersion("App2", "2.0").build();
    assertThat(client).isNotNull();
    assertThat(client.getRequestInterceptor()).isNotNull();

    // Note: Detailed User-Agent testing is in UserAgentTest
  }

  @Test
  public void testRetryPolicyConfiguration() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);

    // Test with custom retry policy
    RetryPolicy customRetryPolicy = JitterDelayRetryPolicy.builder().maxAttempts(10).build();
    ApiClient client1 =
        ApiClientBuilder.create()
            .uri(TEST_URI)
            .tokenProvider(tokenProvider)
            .retryPolicy(customRetryPolicy)
            .build();
    assertThat(client1).isNotNull();

    // Test with default retry policy (no explicit setting)
    ApiClient client2 =
        ApiClientBuilder.create().uri(TEST_URI).tokenProvider(tokenProvider).build();
    assertThat(client2).isNotNull();
  }

  @Test
  public void testValidationFailures() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);

    // Test missing URI
    assertThatThrownBy(() -> ApiClientBuilder.create().tokenProvider(tokenProvider).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("unitycatalog uri cannot be null");

    // Test missing token provider
    assertThatThrownBy(() -> ApiClientBuilder.create().uri(TEST_URI).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("unitycatalog token provider cannot be null");

    // Test null URI
    assertThatThrownBy(
            () -> ApiClientBuilder.create().uri((String) null).tokenProvider(tokenProvider).build())
        .isInstanceOf(NullPointerException.class);

    // Test null token provider
    assertThatThrownBy(() -> ApiClientBuilder.create().uri(TEST_URI).tokenProvider(null).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("unitycatalog token provider cannot be null");
  }

  @Test
  public void testCompleteConfiguration() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);
    RetryPolicy retryPolicy = JitterDelayRetryPolicy.builder().maxAttempts(3).build();

    // Test that all builder components work together
    ApiClient client =
        ApiClientBuilder.create()
            .uri("https://unity-catalog.example.com:8443/api")
            .tokenProvider(tokenProvider)
            .addAppVersion("TestApp", "3.0.0")
            .retryPolicy(retryPolicy)
            .build();

    assertThat(client).isNotNull();
    assertThat(client.getRequestInterceptor()).isNotNull();

    // Verify base URI construction
    assertThat(client.getBaseUri())
        .isEqualTo("https://unity-catalog.example.com:8443/api/api/2.1/unity-catalog");

    // Note: Detailed User-Agent and token testing is in dedicated test classes
  }

  @Test
  public void testBaseUriConstruction() {
    TokenProvider tokenProvider = TokenProviderUtils.create(TEST_TOKEN);

    // Test base URI without path suffix
    URI uriNoSuffix = URI.create("https://localhost:8080");
    ApiClient clientNoSuffix =
        ApiClientBuilder.create().uri(uriNoSuffix).tokenProvider(tokenProvider).build();
    assertThat(clientNoSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/api/2.1/unity-catalog");

    // Test base URI with path suffix
    URI uriWithSuffix = URI.create("https://localhost:8080/path/to/uc");
    ApiClient clientWithSuffix =
        ApiClientBuilder.create().uri(uriWithSuffix).tokenProvider(tokenProvider).build();
    assertThat(clientWithSuffix.getBaseUri())
        .isEqualTo("https://localhost:8080/path/to/uc/api/2.1/unity-catalog");
  }
}
