package io.unitycatalog.client;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.internal.RetryingApiClient;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Builder to create configured {@link ApiClient} instances for Unity Catalog operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ApiClient client = ApiClientBuilder.create()
 *     .url("http://localhost:8080")
 *     .tokenProvider(TokenProvider.create("my-token"))
 *     .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(5).build())
 *     .addAppVersion("MyApp", "1.0.0", "Java", "11")
 *     .build();
 * }</pre>
 *
 * @see ApiClient
 * @see TokenProvider
 * @see RetryPolicy
 */
public class ApiClientBuilder {
  private static final String BASE_PATH = "/api/2.1/unity-catalog";

  private URI uri = null;
  private TokenProvider tokenProvider = null;
  private final List<String> nameVersionPairs = new ArrayList<>();
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
   * Sets the Unity Catalog server URI.
   *
   * <p>The base path {@code /api/2.1/unity-catalog} will be automatically appended.
   *
   * @param uri the Unity Catalog server URI, must not be null
   * @return this builder instance for method chaining
   */
  public ApiClientBuilder uri(URI uri) {
    this.uri = uri;
    return this;
  }

  /**
   * Sets the Unity Catalog server URI from a string.
   *
   * @param uri the Unity Catalog server URI, must not be null
   * @return this builder instance for method chaining
   * @see #uri(URI)
   */
  public ApiClientBuilder uri(String uri) {
    return uri(URI.create(uri));
  }

  /**
   * Sets the token provider for authentication.
   *
   * <p>The token will be included in the Authorization header as a Bearer token.
   *
   * @param tokenProvider the token provider implementation, must not be null
   * @return this builder instance for method chaining
   * @see TokenProvider
   */
  public ApiClientBuilder tokenProvider(TokenProvider tokenProvider) {
    this.tokenProvider = tokenProvider;
    return this;
  }

  /**
   * Sets application version metadata as name-version pairs.
   *
   * <p>Arguments must be provided in alternating name-version order with an even count, e.g.,
   * {@code "MyApp1", "0.1.0", "MyApp2", "1.0.2"}.
   *
   * @param nameVersionPairs the metadata as alternating name-version pairs
   * @return this builder instance for method chaining
   * @throws IllegalArgumentException if an odd number of arguments is provided
   */
  public ApiClientBuilder addAppVersion(String... nameVersionPairs) {
    Preconditions.checkArgument(
        nameVersionPairs.length % 2 == 0,
        "Must provide an even number of arguments for the name-version pairs.");
    Collections.addAll(this.nameVersionPairs, nameVersionPairs);
    return this;
  }

  /**
   * Sets a custom request retry policy for handling transient failures.
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
   * @throws NullPointerException if {@code uri} or {@code tokenProvider} is null
   */
  public ApiClient build() {
    // Set the scheme, host, port and base path, for the unity catalog client.
    Preconditions.checkNotNull(uri, "The unitycatalog uri cannot be null");
    ApiClient apiClient = new RetryingApiClient(retryPolicy);
    apiClient.setScheme(uri.getScheme());
    apiClient.setHost(uri.getHost());
    apiClient.setPort(uri.getPort());
    apiClient.setBasePath(uri.getPath() + BASE_PATH);

    // Set the unity catalog token provider.
    Preconditions.checkNotNull(tokenProvider, "The unitycatalog token provider cannot be null");
    apiClient.setRequestInterceptor(
        request -> request.header("Authorization", "Bearer " + tokenProvider.accessToken()));

    // Set the name and version pairs.
    if (!nameVersionPairs.isEmpty()) {
      apiClient.setClientVersion(nameVersionPairs.toArray(new String[0]));
    }

    return apiClient;
  }
}
