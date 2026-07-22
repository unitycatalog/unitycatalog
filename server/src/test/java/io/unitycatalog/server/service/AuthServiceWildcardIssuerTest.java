package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Integration tests for wildcard issuer patterns in server.allowed-issuers. */
public class AuthServiceWildcardIssuerTest extends BaseAuthCRUDTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private WebClient client;

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    serverProperties.setProperty("server.allowed-issuers", "http://localhost:*");
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    client = WebClient.builder(serverConfig.getServerUrl()).build();
  }

  @Test
  public void testTokenExchangeAcceptsWildcardIssuerMatch() throws IOException {
    String token =
        createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    assertThat(response.status()).isEqualTo(HttpStatus.OK);
    JsonNode body = MAPPER.readTree(response.contentUtf8());
    assertThat(body.has("access_token")).isTrue();
  }

  @Test
  public void testTokenExchangeRejectsIssuerOutsideAllowlist() {
    String token =
        createIdentityToken(
            "https://evil.com", TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

    AggregatedHttpResponse response = exchangeToken(token);

    assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(response.contentUtf8()).contains("Invalid issuer");
  }

  private String createIdentityToken(
      String issuer, String audience, Algorithm algorithm, String keyId) {
    var builder =
        JWT.create()
            .withSubject("admin")
            .withIssuer(issuer)
            .withIssuedAt(new Date())
            .withKeyId(keyId)
            .withJWTId(UUID.randomUUID().toString());
    if (audience != null) {
      builder.withAudience(audience);
    }
    return builder.sign(algorithm);
  }

  private AggregatedHttpResponse exchangeToken(String identityToken) {
    String formBody =
        "grant_type=urn:ietf:params:oauth:grant-type:token-exchange"
            + "&requested_token_type=urn:ietf:params:oauth:token-type:access_token"
            + "&subject_token_type=urn:ietf:params:oauth:token-type:id_token"
            + "&subject_token="
            + identityToken;

    RequestHeaders headers =
        RequestHeaders.builder()
            .method(HttpMethod.POST)
            .path(TOKEN_ENDPOINT)
            .contentType(MediaType.FORM_DATA)
            .build();

    return client.execute(headers, HttpData.ofUtf8(formBody)).aggregate().join();
  }
}
