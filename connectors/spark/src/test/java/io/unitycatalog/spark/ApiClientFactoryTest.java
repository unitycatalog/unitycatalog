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
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Test class for ApiClientFactory to verify Spark-specific functionality.
 *
 * <p>This test class focuses on Spark/Delta/Java/Scala version metadata injection into the
 * User-Agent. For general ApiClient configuration tests, see {@link
 * io.unitycatalog.client.ApiClientBuilderTest}.
 */
public class ApiClientFactoryTest {
  private static final TokenProvider UC_TOKEN_PROVIDER = createStaticTokenProvider("token");
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
  public void testAllVersionsInUserAgent() {
    ApiClient client = ApiClientFactory.createApiClient(RETRY_POLICY, TEST_URI, UC_TOKEN_PROVIDER);
    String userAgent = extractUserAgent(client);

    // Verify the base Unity Catalog client is present
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");

    // Verify Java version is included
    String expectedJavaVersion = System.getProperty("java.version");
    assertThat(expectedJavaVersion).isNotNull();
    assertThat(userAgent).contains("Java/" + expectedJavaVersion);

    // Verify Scala version is included
    String expectedScalaVersion = scala.util.Properties.versionNumberString();
    assertThat(expectedScalaVersion).isNotNull();
    assertThat(userAgent).contains("Scala/" + expectedScalaVersion);

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
    TokenProvider tokenProvider = createStaticTokenProvider(sensitiveToken);
    ApiClient client = ApiClientFactory.createApiClient(RETRY_POLICY, TEST_URI, tokenProvider);

    String userAgent = extractUserAgent(client);

    // Verify Java/Scala/Spark/Delta versions are present
    assertThat(userAgent).startsWith("UnityCatalog-Java-Client/");
    assertThat(userAgent).contains("Java");
    assertThat(userAgent).contains("Scala");
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
    assertThat(parts.length).isGreaterThanOrEqualTo(5); // UC client, Java, Scala, Spark, and Delta

    // First part should be UnityCatalog-Java-Client/version
    assertThat(parts[0]).matches("UnityCatalog-Java-Client/.*");

    // Verify all expected versions are present in the correct order
    int sparkIndex = -1;
    int deltaIndex = -1;
    int javaIndex = -1;
    int scalaIndex = -1;

    for (int i = 0; i < parts.length; i++) {
      if (parts[i].startsWith("Spark/")) sparkIndex = i;
      if (parts[i].startsWith("Delta/")) deltaIndex = i;
      if (parts[i].startsWith("Java/")) javaIndex = i;
      if (parts[i].startsWith("Scala/")) scalaIndex = i;
    }

    // Verify all versions are present
    assertThat(sparkIndex).isGreaterThan(0);
    assertThat(deltaIndex).isGreaterThan(0);
    assertThat(javaIndex).isGreaterThan(0);
    assertThat(scalaIndex).isGreaterThan(0);

    // Verify ordering: UC (position 0) < Spark < Delta < Java < Scala
    assertThat(sparkIndex).isLessThan(deltaIndex);
    assertThat(deltaIndex).isLessThan(javaIndex);
    assertThat(javaIndex).isLessThan(scalaIndex);
  }

  private static TokenProvider createStaticTokenProvider(String token) {
    return TokenProvider.create(Map.of("type", "static", "token", token));
  }
}
