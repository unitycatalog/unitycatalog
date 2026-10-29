package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.auth0.jwt.JWT;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.retry.JitterDelayRetryPolicy;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end check that the Java client's OAuth token provider can authenticate against a UC server
 * with authorization enabled when {@code oauth.exchangeForUcToken} is set.
 */
public class OAuthTokenExchangeClientTest extends BaseAuthCRUDTest {

  private static final String OAUTH_CLIENT_ID = "oauth-client-uuid";
  private static final String SERVICE_EMAIL = "service-principal@example.com";

  private HttpServer idpTokenServer;
  private String idpTokenUri;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    createEnabledUser(SERVICE_EMAIL, OAUTH_CLIENT_ID);
    startIdpTokenServer();
  }

  @AfterEach
  @Override
  public void tearDown() {
    if (idpTokenServer != null) {
      idpTokenServer.stop(0);
    }
    super.tearDown();
  }

  @Test
  public void testExchangedTokenIsAcceptedByServer() throws ApiException {
    TokenProvider provider = TokenProvider.create(oauthConfigs(/* exchangeForUcToken= */ true));

    assertThat(JWT.decode(provider.accessToken()).getClaim("sub").asString())
        .isEqualTo(SERVICE_EMAIL);
    assertThat(
            catalogsApi(provider)
                .listCatalogs(null, null, /* includeBrowse= */ false)
                .getCatalogs())
        .isNullOrEmpty();
  }

  @Test
  public void testIdentityProviderTokenIsRejectedWithoutExchange() {
    TokenProvider provider = TokenProvider.create(oauthConfigs(/* exchangeForUcToken= */ false));

    assertThatThrownBy(
            () -> catalogsApi(provider).listCatalogs(null, null, /* includeBrowse= */ false))
        .isInstanceOf(ApiException.class)
        .hasMessageContaining("403");
  }

  private Map<String, String> oauthConfigs(boolean exchangeForUcToken) {
    Map<String, String> configs = new HashMap<>();
    configs.put("type", "oauth");
    configs.put("catalogUri", serverConfig.getServerUrl());
    configs.put("oauth.uri", idpTokenUri);
    configs.put("oauth.clientId", OAUTH_CLIENT_ID);
    configs.put("oauth.clientSecret", "client-secret");
    configs.put("oauth.exchangeForUcToken", String.valueOf(exchangeForUcToken));
    return configs;
  }

  private CatalogsApi catalogsApi(TokenProvider provider) {
    return new CatalogsApi(
        ApiClientBuilder.create()
            .uri(serverConfig.getServerUrl())
            .tokenProvider(provider)
            .retryPolicy(JitterDelayRetryPolicy.builder().maxAttempts(1).build())
            .build());
  }

  /** Serves client credentials tokens signed by the test issuer the server trusts. */
  private void startIdpTokenServer() {
    try {
      idpTokenServer = HttpServer.create(new InetSocketAddress(0), 0);
    } catch (Exception e) {
      throw new RuntimeException("Failed to start identity provider token server", e);
    }
    idpTokenServer.createContext(
        "/token",
        exchange -> {
          String accessToken =
              JWT.create()
                  .withClaim("azp", OAUTH_CLIENT_ID)
                  .withAudience(TEST_AUDIENCE)
                  .withIssuer(testIssuer)
                  .withIssuedAt(new Date())
                  .withExpiresAt(new Date(System.currentTimeMillis() + 3_600_000))
                  .withKeyId(testIssuerKeyId)
                  .withJWTId(UUID.randomUUID().toString())
                  .sign(testIssuerAlgorithm);
          byte[] body =
              String.format(
                      "{\"access_token\":\"%s\",\"token_type\":\"Bearer\",\"expires_in\":3600}",
                      accessToken)
                  .getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    idpTokenServer.start();
    idpTokenUri = "http://localhost:" + idpTokenServer.getAddress().getPort() + "/token";
  }
}
