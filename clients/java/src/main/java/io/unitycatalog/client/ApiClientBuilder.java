package io.unitycatalog.client;

import com.google.common.base.Preconditions;
import io.unitycatalog.client.auth.UCTokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;

/**
 * Builder to create configured {@link ApiClient} instances for Unity Catalog operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ApiClient client = ApiClientBuilder.create()
 *     .url("http://localhost:8080")
 *     .ucTokenProvider(UCTokenProvider.builder().token("my-token").build())
 *     .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(5).build())
 *     .clientVersion("MyApp", "1.0.0", "Java", "11")
 *     .build();
 * }</pre>
 *
 * @see ApiClient
 * @see UCTokenProvider
 * @see RetryPolicy
 */
public class ApiClientBuilder {
  private static final String BASE_PATH = "/api/2.1/unity-catalog";

  private URI url = null;
  private UCTokenProvider ucTokenProvider = null;
  private String[] nameVersionPairs = null;
  private RetryPolicy retryPolicy = JitterDelayRetryPolicy.builder().build();

  /**
   * Creates a new instance of {@link ApiClientBuilder}.
   *
   * @return a new {@link ApiClientBuilder} instance
   */
  public static ApiClientBuilder create() {
    return new ApiClientBuilder();
  }

  /**
   * Sets the Unity Catalog server URL.
   *
   * <p>The base path {@code /api/2.1/unity-catalog} will be automatically appended.
   *
   * @param url the Unity Catalog server URL, must not be null
   * @return this builder instance for method chaining
   */
  public ApiClientBuilder url(URI url) {
    this.url = url;
    return this;
  }

  /**
   * Sets the Unity Catalog server URL from a string.
   *
   * @param url the Unity Catalog server URL, must not be null
   * @return this builder instance for method chaining
   * @see #url(URI)
   */
  public ApiClientBuilder url(String url) {
    return url(URI.create(url));
  }

  /**
   * Sets the token provider for authentication.
   *
   * <p>The token will be included in the Authorization header as a Bearer token.
   *
   * @param ucTokenProvider the token provider implementation, must not be null
   * @return this builder instance for method chaining
   * @see UCTokenProvider
   */
  public ApiClientBuilder ucTokenProvider(UCTokenProvider ucTokenProvider) {
    this.ucTokenProvider = ucTokenProvider;
    return this;
  }

  /**
   * Sets client version metadata as name-version pairs.
   *
   * <p>Arguments must be provided in alternating name-version order with an even count, e.g.,
   * {@code "MyApp1", "0.1.0", "MyApp2", "1.0.2"}.
   *
   * @param nameVersionPairs the metadata as alternating name-version pairs
   * @return this builder instance for method chaining
   * @throws IllegalArgumentException if an odd number of arguments is provided
   */
  public ApiClientBuilder clientVersion(String... nameVersionPairs) {
    Preconditions.checkArgument(
        nameVersionPairs.length % 2 == 0,
        "Must provide an even number of arguments for the name-value pairs.");
    this.nameVersionPairs = nameVersionPairs;
    return this;
  }

  /**
   * Sets a custom retry policy for handling transient failures.
   *
   * <p>Defaults to {@link JitterDelayRetryPolicy} if not specified.
   *
   * @param retryPolicy the retry policy implementation, must not be null
   * @return this builder instance for method chaining
   * @see RetryPolicy
   * @see JitterDelayRetryPolicy
   */
  public ApiClientBuilder retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  /**
   * Builds and returns a configured {@link ApiClient} instance.
   *
   * @return a configured {@link ApiClient} instance
   * @throws NullPointerException if {@code url} or {@code ucTokenProvider} is null
   */
  public ApiClient build() {
    // Set the scheme, host, port and base path, for the unity catalog client.
    Preconditions.checkNotNull(url, "The unitycatalog url cannot be null");
    ApiClient apiClient = new RetryingApiClient(retryPolicy);
    apiClient.setScheme(url.getScheme());
    apiClient.setHost(url.getHost());
    apiClient.setPort(url.getPort());
    apiClient.setBasePath(url.getPath() + BASE_PATH);

    // Set the unity catalog token provider.
    Preconditions.checkNotNull(ucTokenProvider, "The unitycatalog token provider cannot be null");
    apiClient.setRequestInterceptor(
        request -> request.header("Authorization", "Bearer " + ucTokenProvider.accessToken()));

    // Set the name and version pairs.
    if (nameVersionPairs != null && nameVersionPairs.length > 0) {
      apiClient.setClientVersion(nameVersionPairs);
    }

    return apiClient;
  }
}
