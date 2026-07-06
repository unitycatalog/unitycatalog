package io.unitycatalog.docker.tests.support;

import cloud.celonis.internal.client.async.AuthenticationFilter;
import cloud.celonis.oauth.client.OAuthClientApi;
import cloud.celonis.oauth.client.OAuthClientApiImpl;
import cloud.celonis.oauth.internal.client.generated.async.ApiClient;
import cloud.celonis.oauth.internal.client.generated.async.OAuthClientsInternalAsyncApi;
import cloud.celonis.security.auth.client.AuthenticationHeadersProvider;
import cloud.celonis.security.auth.token.SystemTokenCreator;
import cloud.celonis.security.config.JwtConfig;
import cloud.celonis.security.config.JwtConfigurationProperties;
import org.springframework.web.reactive.function.client.WebClient;

/** Factory for the Celonis OAuth server internal client ({@code cloud-oauth-server-internal-client}). */
public final class CelonisOAuthClients {

  private static volatile OAuthClientApi client;

  private CelonisOAuthClients() {}

  public static OAuthClientApi client() {
    if (client == null) {
      synchronized (CelonisOAuthClients.class) {
        if (client == null) {
          client = createClient();
        }
      }
    }
    return client;
  }

  private static OAuthClientApi createClient() {
    JwtConfigurationProperties jwtProperties =
        JwtConfigurationProperties.withSecretOnly(CelonisOAuthTestConstants.OAUTH_INTERNAL_JWT_SECRET);
    JwtConfig jwtConfig = new JwtConfig(jwtProperties);
    SystemTokenCreator tokenCreator = new SystemTokenCreator(jwtConfig);
    AuthenticationHeadersProvider headersProvider = new AuthenticationHeadersProvider(tokenCreator);
    AuthenticationFilter authFilter = new AuthenticationFilter(headersProvider);

    WebClient webClient = OAuthHttp.webClientBuilder().filter(authFilter).build();

    ApiClient apiClient = new ApiClient(webClient);
    apiClient.setBasePath(CelonisOAuthTestConstants.oauthBaseUrl());

    OAuthClientsInternalAsyncApi internalApi = new OAuthClientsInternalAsyncApi(apiClient);
    return new OAuthClientApiImpl(internalApi);
  }
}
