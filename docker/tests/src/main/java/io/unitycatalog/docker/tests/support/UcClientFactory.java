package io.unitycatalog.docker.tests.support;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.auth.TokenProvider;
import java.net.URI;

public final class UcClientFactory {

  private UcClientFactory() {}

  public static ApiClient catalogClient(String serverUrl, String token) {
    return ApiClientBuilder.create()
        .uri(URI.create(serverUrl))
        .tokenProvider(TokenProvider.create(java.util.Map.of("type", "static", "token", token)))
        .build();
  }

  public static io.unitycatalog.control.ApiClient controlClient(
      String serverUrl, String adminToken) {
    io.unitycatalog.control.ApiClient client = new io.unitycatalog.control.ApiClient();
    client.updateBaseUri(serverUrl + "/api/1.0/unity-control");
    client.setRequestInterceptor(
        request -> request.header("Authorization", "Bearer " + adminToken));
    return client;
  }
}
