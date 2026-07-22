package io.unitycatalog.server.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import io.unitycatalog.server.base.auth.BaseAuthCRUDTest;
import io.unitycatalog.server.utils.WildcardAllowlist;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Integration tests for issuer/audience allowlist configuration during token exchange. */
public class AuthServiceAllowlistConfigurationTest {

  private static final String TOKEN_ENDPOINT = "/api/1.0/unity-control/auth/tokens";

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
      return JWT.create()
          .withSubject("admin")
          .withIssuer(issuer)
          .withAudience(audience)
          .withIssuedAt(new Date())
          .withKeyId(keyId)
          .withJWTId(UUID.randomUUID().toString())
          .sign(algorithm);
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
  class MissingAllowedIssuers extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(WildcardAllowlist.ALLOWED_ISSUERS_PROPERTY, "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAllowedIssuers() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains(
              "No allowed issuers configured. Set server.allowed-issuers in server.properties");
    }
  }

  @Nested
  class MissingAudiences extends TokenExchangeTestBase {

    @Override
    protected void setUpProperties() {
      super.setUpProperties();
      serverProperties.setProperty(WildcardAllowlist.AUDIENCES_PROPERTY, "");
    }

    @Test
    public void testTokenExchangeRejectsMissingAudiences() {
      String token =
          createIdentityToken(testIssuer, TEST_AUDIENCE, testIssuerAlgorithm, testIssuerKeyId);

      AggregatedHttpResponse response = exchangeToken(token);

      assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
      assertThat(response.contentUtf8())
          .contains("No audiences configured. Set server.audiences in server.properties");
    }
  }
}
