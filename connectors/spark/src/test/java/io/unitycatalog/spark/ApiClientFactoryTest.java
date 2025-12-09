package io.unitycatalog.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.net.http.HttpRequest;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test class for ApiClientFactory to verify Spark-specific functionality.
 *
 * <p>This test class focuses on Spark/Delta version metadata injection into the User-Agent. For
 * general ApiClient configuration tests, see {@link io.unitycatalog.client.ApiClientBuilderTest}.
 */
public class ApiClientFactoryTest {
  private static final TokenProvider UC_TOKEN_PROVIDER = TokenProvider.create("token");
  private static final RetryPolicy RETRY_POLICY = JitterDelayRetryPolicy.builder().build();
  private static final URI TEST_URI = URI.create("http://localhost:8080");

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
  public void testSparkAndDeltaVersionsInUserAgent() {
    ApiClient client = ApiClientFactory.createApiClient(RETRY_POLICY, TEST_URI, UC_TOKEN_PROVIDER);
    String userAgent = extractUserAgent(client);

    // Verify the base Unity Catalog client is present
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify Spark version is included
    String expectedSparkVersion = org.apache.spark.package$.MODULE$.SPARK_VERSION();
    assertThat(userAgent).contains("Spark/" + expectedSparkVersion);

    // Verify Delta version is included
    String expectedDeltaVersion = io.delta.package$.MODULE$.VERSION();
    assertThat(expectedDeltaVersion).isNotNull();
    assertThat(userAgent).contains("Delta/" + expectedDeltaVersion);
  }

  @Test
  public void testTokenNotLeakedInUserAgent() {
    String sensitiveToken = "test-token-12345";
    TokenProvider tokenProvider = TokenProvider.create(sensitiveToken);
    ApiClient client = ApiClientFactory.createApiClient(RETRY_POLICY, TEST_URI, tokenProvider);

    String userAgent = extractUserAgent(client);

    // Verify Spark/Delta versions are present
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("Spark");
    assertThat(userAgent).contains("Delta");

    // Critical: Verify the token is not leaked in the user agent
    assertThat(userAgent).doesNotContain(sensitiveToken);
  }

  @Test
  public void testUserAgentFormat() {
    ApiClient client = ApiClientFactory.createApiClient(RETRY_POLICY, TEST_URI, UC_TOKEN_PROVIDER);
    String userAgent = extractUserAgent(client);

    // Verify format: project/version,[project/version,...]
    String[] parts = userAgent.split(" ");
    assertThat(parts.length).isGreaterThanOrEqualTo(3); // UC client, Spark, and Delta

    // First part should be UnityCatalog-Java-Client/version
    assertThat(parts[0]).matches("UnityCatalog-Java-Client/.*");

    // Verify Spark and Delta are present
    boolean hasSparkVersion = false;
    boolean hasDeltaVersion = false;
    for (String part : parts) {
      if (part.startsWith("Spark/")) hasSparkVersion = true;
      if (part.startsWith("Delta/")) hasDeltaVersion = true;
    }
    assertThat(hasSparkVersion).isTrue();
    assertThat(hasDeltaVersion).isTrue();
  }
}
