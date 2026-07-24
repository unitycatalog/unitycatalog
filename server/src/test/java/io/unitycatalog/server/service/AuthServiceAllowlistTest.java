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
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.WildcardAllowlist;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Integration tests for issuer/audience allowlist matching during token exchange. */
public class AuthServiceAllowlistTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  abstract static class TokenExchangeTestBase extends BaseAuthCRUDTest {

    protected WebClient client;

    @BeforeEach
    @Override
    public void setUp() {
      super.setUp();
      client = WebClient.builder(serverConfig.getServerUrl()).build();
    }

    protected String createIdentityToken(
        String issuer, String audience, Algorithm algorithm, String keyId) {
      return createIdentityTokenForSubject(issuer, audience, "admin", algorithm, keyId);
    }

    protected String createIdentityTokenForSubject(
        String issuer, String audience, String subject, Algorithm algorithm, String keyId) {
      var builder =
          JWT.create()
              .withSubject(subject)
              .withIssuer(issuer)
              .withIssuedAt(new Date())
              .withKeyId(keyId)
              .withJWTId(UUID.randomUUID().toString());
      if (audience != null) {
        builder.withAudience(audience);
      }
      return builder.sign(algorithm);
    }

    protected AggregatedHttpResponse exchangeToken(String identityToken) {
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

  @Nested
  class WildcardIssuer extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(
          ServerProperties.Property.ALLOWED_ISSUERS.key, "http://localhost:*");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse accepted =
          exchangeToken(
              createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));
      assertThat(accepted.status()).isEqualTo(HttpStatus.OK);
      JsonNode body = MAPPER.readTree(accepted.contentUtf8());
      assertThat(body.has("access_token")).isTrue();

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityToken(
                  "https://evil.com", TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
      assertThat(rejected.contentUtf8()).contains("Invalid issuer");
    }
  }

  @Nested
  class WildcardAudiencePattern extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(
          ServerProperties.Property.AUDIENCES.key, "unity-catalog-local,https://*.dev.example.com");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse exactAudience =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "unity-catalog-local", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(exactAudience.status()).isEqualTo(HttpStatus.OK);
      JsonNode exactBody = MAPPER.readTree(exactAudience.contentUtf8());
      assertThat(exactBody.has("access_token")).isTrue();

      AggregatedHttpResponse wildcardAudience =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "https://dev.dev.example.com", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(wildcardAudience.status()).isEqualTo(HttpStatus.OK);

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "dynamic-oauth-client-uuid", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
      assertThat(rejected.contentUtf8()).contains("Invalid audience");
    }
  }

  @Nested
  class WildcardAudienceDisabled extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.AUDIENCES.key, "*");
    }

    @Test
    public void testTokenExchange() throws IOException {
      AggregatedHttpResponse accepted =
          exchangeToken(
              createIdentityToken(
                  testIssuer, "dynamic-oauth-client-uuid", testIssuerAlgorithm, testIssuerKeyId));
      assertThat(accepted.status()).isEqualTo(HttpStatus.OK);
      JsonNode body = MAPPER.readTree(accepted.contentUtf8());
      assertThat(body.has("access_token")).isTrue();
      assertThat(body.get("access_token").asText()).isNotEmpty();

      AggregatedHttpResponse rejected =
          exchangeToken(
              createIdentityTokenForSubject(
                  testIssuer,
                  "any-audience",
                  "unknown-user@example.com",
                  testIssuerAlgorithm,
                  testIssuerKeyId));
      assertThat(rejected.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(rejected.contentUtf8()).contains("User not allowed");
    }
  }

  @Nested
  class MissingAllowedIssuers extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.ALLOWED_ISSUERS.key, "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAllowedIssuers() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains(
              "No "
                  + WildcardAllowlist.ALLOWED_ISSUERS_NAME
                  + " configured. Set "
                  + ServerProperties.Property.ALLOWED_ISSUERS.key
                  + " in server.properties");
    }
  }

  @Nested
  class MissingAudiences extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(ServerProperties.Property.AUDIENCES.key, "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAudiences() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains(
              "No "
                  + WildcardAllowlist.AUDIENCES_NAME
                  + " configured. Set "
                  + ServerProperties.Property.AUDIENCES.key
                  + " in server.properties");
    }
  }
}
