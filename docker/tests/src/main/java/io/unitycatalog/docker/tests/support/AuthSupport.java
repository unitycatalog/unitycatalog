package io.unitycatalog.docker.tests.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.control.model.GrantType;
import io.unitycatalog.control.model.OAuthTokenExchangeForm;
import io.unitycatalog.control.model.TokenType;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public final class AuthSupport {

  private static final String KC_CLIENT_ID = "unity-catalog-local";
  private static final String KC_CLIENT_SECRET = "unity-catalog-local-secret";
  private static final String KC_TOKEN_PATH =
      "/realms/unity-catalog/protocol/openid-connect/token";

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();

  private AuthSupport() {}

  public static String deriveKeycloakUsername(String email) {
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
    String kcUsername = deriveKeycloakUsername(email);
    String kcPassword =
        password != null && !password.isBlank() ? password : deriveDefaultPassword(email);
    String idToken = fetchKeycloakIdToken(kcUsername, kcPassword);
    return exchangeUcAccessToken(serverUrl, idToken);
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

  private static String fetchKeycloakIdToken(String username, String password)
      throws IOException, InterruptedException {
    URI tokenUri = URI.create("http://localhost:9090" + KC_TOKEN_PATH);
    String form =
        formBody(
            Map.of(
                "grant_type", "password",
                "client_id", KC_CLIENT_ID,
                "client_secret", KC_CLIENT_SECRET,
                "username", username,
                "password", password,
                "scope", "openid"));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokenUri)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();

    HttpResponse<String> response =
        HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException("Keycloak password grant failed: " + response.body());
    }
    Map<String, String> tokenResponse =
        MAPPER.readValue(response.body(), new TypeReference<Map<String, String>>() {});
    String idToken = tokenResponse.get("id_token");
    if (idToken == null || idToken.isBlank()) {
      throw new IllegalStateException("Keycloak response missing id_token");
    }
    return idToken;
  }

  private static String formBody(Map<String, String> fields) {
    return fields.entrySet().stream()
        .map(
            e ->
                URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8)
                    + "="
                    + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
        .collect(Collectors.joining("&"));
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
