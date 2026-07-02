package io.unitycatalog.docker.tests.support;

import cloud.celonis.oauth.client.OAuthClientApi;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.OAuthTokenExchangeForm;
import io.unitycatalog.control.model.TokenType;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;

public final class AuthSupport {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private AuthSupport() {}

  public static String deriveOAuthUsername(String email) {
    return emailLocalPart(email).toLowerCase(Locale.ROOT);
  }

  public static String deriveDefaultPassword(String email) {
    String localPart = emailLocalPart(email).toLowerCase(Locale.ROOT);
    if (localPart.length() <= 1) {
      return capitalizeFirst(localPart);
    }
    String head = localPart.substring(0, localPart.length() - 1);
    String lastUpper =
        localPart.substring(localPart.length() - 1).toUpperCase(Locale.ROOT);
    return head + lastUpper;
  }

  public static String ucTokenForUser(String serverUrl, String email, String password)
      throws IOException, InterruptedException {
    String idToken = obtainIdToken(email);
    return exchangeUcAccessToken(serverUrl, idToken);
  }

  /** Verify Celonis OAuth is reachable and the configured client can mint user tokens. */
  public static void verifyOAuthLogin(String email, String password)
      throws IOException, InterruptedException {
    obtainIdToken(email);
  }

  private static String obtainIdToken(String email) throws IOException, InterruptedException {
    verifyOAuthClientRegistered();
    return CelonisOAuthAuthorizationCodeFlow.fetchIdToken(email);
  }

  private static void verifyOAuthClientRegistered() {
    OAuthClientApi oauthClient = CelonisOAuthClients.client();
    oauthClient.findOne(CelonisOAuthTestConstants.OAUTH_CLIENT_ID, true);
  }

  private static String exchangeUcAccessToken(String serverUrl, String idToken)
      throws IOException, InterruptedException {
    URI endpoint = URI.create(serverUrl + "/api/1.0/unity-control/auth/tokens");
    String body =
        new OAuthTokenExchangeForm()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .requestedTokenType(TokenType.ACCESS_TOKEN)
            .subjectTokenType(TokenType.ID_TOKEN)
            .subjectToken(idToken)
            .toUrlQueryString();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    HttpResponse<String> response =
        HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException("UC token exchange failed: " + response.body());
    }
    Map<String, String> tokenResponse =
        MAPPER.readValue(response.body(), new TypeReference<Map<String, String>>() {});
    String accessToken = tokenResponse.get("access_token");
    if (accessToken == null || accessToken.isBlank()) {
      throw new IllegalStateException("UC token exchange response missing access_token");
    }
    return accessToken;
  }

  private static String emailLocalPart(String email) {
    int at = email.indexOf('@');
    return at >= 0 ? email.substring(0, at) : email;
  }

  private static String capitalizeFirst(String value) {
    if (value == null || value.isEmpty()) {
      return value;
    }
    return Character.toUpperCase(value.charAt(0)) + value.substring(1);
  }
}
