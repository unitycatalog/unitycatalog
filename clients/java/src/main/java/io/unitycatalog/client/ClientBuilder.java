package io.unitycatalog.client;

import io.unitycatalog.client.auth.UCTokenProvider;
import java.net.URI;

public class ClientBuilder {
  public static final String BASE_PATH = "/api/2.1/unity-catalog";

  private URI url = null;
  private UCTokenProvider ucTokenProvider = null;
  private String[] nameValuePairs = null;
  // TODO we need to use this retry policy to build a retryable http client.
  private RetryPolicy retryPolicy = RetryPolicy.newBuilder().build();

  public static ClientBuilder create() {
    return new ClientBuilder();
  }

  public ClientBuilder url(URI url) {
    this.url = url;
    return this;
  }

  public ClientBuilder ucTokenProvider(UCTokenProvider ucTokenProvider) {
    this.ucTokenProvider = ucTokenProvider;
    return this;
  }

  public ClientBuilder clientVersion(String... nameValuePairs) {
    this.nameValuePairs = nameValuePairs;
    return this;
  }

  public ClientBuilder retryPolicy(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
    return this;
  }

  public ApiClient build() {
    // Set the scheme, host, port and base path, for the unity catalog client.
    Preconditions.checkNotNull(url, "The unitycatalog url cannot be null");
    ApiClient apiClient = new ApiClient();
    apiClient.setHost(url.getHost());
    apiClient.setPort(url.getPort());
    apiClient.setBasePath(url.getPath() + BASE_PATH);

    // Set the unity catalog token provider.
    Preconditions.checkNotNull(ucTokenProvider, "The UC token provider cannot be null");
    apiClient.setRequestInterceptor(
        request -> request.header("Authorization", "Bearer " + ucTokenProvider.accessToken())
    );

    // Set the name and version pairs.
    if (nameValuePairs != null) {
      apiClient.setClientVersion(nameValuePairs);
    }

    return apiClient;
  }
}
