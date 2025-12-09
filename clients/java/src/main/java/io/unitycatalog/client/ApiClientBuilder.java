package io.unitycatalog.client;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.internal.RetryingApiClient;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.client.retry.RetryPolicy;
import java.net.URI;
import java.net.http.HttpRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Builder to create configured {@link ApiClient} instances for Unity Catalog operations.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * ApiClient client = ApiClientBuilder.create()
 *     .uri("http://localhost:8080")
 *     .tokenProvider(TokenProvider.create("my-token"))
 *     .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(5).build())
 *     .addAppVersion("MyApp", "1.0.0")
 *     .build();
 * }</pre>
 *
 * @see ApiClient
 * @see TokenProvider
 * @see RetryPolicy
 */
public class ApiClientBuilder {
  private static final String DEFAULT_APP_NAME = "UnityCatalog-Java-Client";
  private static final String BASE_PATH = "/api/2.1/unity-catalog";

  private URI uri = null;
  private TokenProvider tokenProvider = null;
  private final Map<String, String> appVersionMap = new LinkedHashMap<>();
  private RetryPolicy retryPolicy = JitterDelayRetryPolicy.builder().build();
  private Consumer<HttpRequest.Builder> requestInterceptor = t -> {};

  /**
   * Creates a new instance of {@link ApiClientBuilder}.
   *
   * @return a new {@link ApiClientBuilder} instance
   */
  public static ApiClientBuilder create() {
    return new ApiClientBuilder();
  }

  private ApiClientBuilder() {
    // Add the default app name and version.
    appVersionMap.put(DEFAULT_APP_NAME, VersionUtils.VERSION);
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
   * {@code "MyApp1", "0.1.0"}.
   *
   * @param name the application name.
   * @param version the application version.
   * @return this builder instance for method chaining
   * @throws IllegalArgumentException if any argument is null or empty.
   */
  public ApiClientBuilder addAppVersion(String name, String version) {
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "App name cannot be null or empty");
    Preconditions.checkArgument(
        version != null && !version.isEmpty(), "App version cannot be null or empty");
    appVersionMap.put(name, version);
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
   * Adds a custom request interceptor to modify HTTP requests before they are sent.
   *
   * <p>Request interceptors can be used to add custom headers, modify request properties, or
   * perform logging. Multiple interceptors can be added and will be executed in the order they were
   * registered.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * ApiClientBuilder builder = ApiClientBuilder.create()
   *     .uri("http://localhost:8080")
   *     .tokenProvider(TokenProvider.create("my-token"))
   *     .addRequestInterceptor(builder -> builder.header("X-Custom-Header", "custom-value"));
   * }</pre>
   *
   * <p><b>Warning:</b> If you call {@code setRequestInterceptor} directly on the built {@link
   * ApiClient}, it will replace all built-in interceptors from this builder, including the
   * User-Agent and Authorization headers, which may cause authentication to fail. Always use {@link
   * #addRequestInterceptor(Consumer)} instead to preserve the built-in interceptors.
   *
   * @param interceptor a consumer that accepts an {@link HttpRequest.Builder} to modify the
   *     request, must not be null
   * @return this builder instance for method chaining
   */
  public ApiClientBuilder addRequestInterceptor(Consumer<HttpRequest.Builder> interceptor) {
    Consumer<HttpRequest.Builder> oldInterceptor = this.requestInterceptor;
    this.requestInterceptor =
        builder -> {
          oldInterceptor.accept(builder);
          interceptor.accept(builder);
        };
    return this;
  }

  /**
   * Builds and returns a configured {@link ApiClient} instance.
   *
   * @return a configured {@link ApiClient} instance
   * @throws NullPointerException if {@code uri} or {@code tokenProvider} is null
   */
  public ApiClient build() {
    // Set the scheme, host, port and base path, for the Api client.
    Preconditions.checkNotNull(uri, "The unitycatalog uri cannot be null");
    ApiClient apiClient = new RetryingApiClient(retryPolicy);
    apiClient.setScheme(uri.getScheme());
    apiClient.setHost(uri.getHost());
    apiClient.setPort(uri.getPort());
    apiClient.setBasePath(uri.getPath() + BASE_PATH);

    // Add the User-Agent request interceptor.
    if (!appVersionMap.isEmpty()) {
      String userAgent =
          appVersionMap.entrySet().stream()
              .map(e -> String.format("%s/%s", e.getKey(), e.getValue()))
              .collect(Collectors.joining(" "));
      addRequestInterceptor(builder -> builder.header("User-Agent", userAgent));
    }

    // Add the client's token interceptor.
    Preconditions.checkNotNull(tokenProvider, "The unitycatalog token provider cannot be null");
    addRequestInterceptor(
        builder -> builder.header("Authorization", "Bearer " + tokenProvider.accessToken()));

    // Set the composed request interceptor on the API client.
    apiClient.setRequestInterceptor(requestInterceptor);

    return apiClient;
  }
}
