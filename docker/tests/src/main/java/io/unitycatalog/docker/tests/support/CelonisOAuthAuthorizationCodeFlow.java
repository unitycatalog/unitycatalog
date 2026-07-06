package io.unitycatalog.docker.tests.support;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Public OAuth2 authorization-code flow for user {@code id_token}s.
 *
 * <p>The {@code cloud-oauth-server-internal-client} library covers internal admin APIs (client CRUD,
 * impersonation, etc.) but not the browserless authorize/token endpoints, so those remain direct
 * HTTP calls.
 */
public final class CelonisOAuthAuthorizationCodeFlow {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = OAuthHttp.createHttpClient();

  private CelonisOAuthAuthorizationCodeFlow() {}

  public static String fetchIdToken(String email) throws IOException, InterruptedException {
    String sessionJwt = createInternalSessionJwt(email);
    String code = requestAuthorizationCode(sessionJwt);
    return exchangeAuthorizationCode(code);
  }

  private static String requestAuthorizationCode(String sessionJwt)
      throws IOException, InterruptedException {
    String query =
        formBody(
            Map.of(
                "client_id", CelonisOAuthTestConstants.OAUTH_CLIENT_ID,
                "response_type", "code",
                "scope", "openid email",
                "redirect_uri", CelonisOAuthTestConstants.OAUTH_REDIRECT_URI,
                "state", "unitycatalog-docker-tests"));
    URI authorizeUri =
        URI.create(CelonisOAuthTestConstants.oauthBaseUrl() + "/oauth2/authorize?" + query);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(authorizeUri)
            .header("Authorization", "Bearer " + sessionJwt)
            .GET()
            .build();

    HttpResponse<String> response =
        HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    int status = response.statusCode();
    if (status != 302 && status != 303) {
      throw new IllegalStateException(
          "OAuth authorize failed (" + response.statusCode() + "): " + response.body());
    }

    String location = response.headers().firstValue("Location").orElse("");
    if (location.isBlank()) {
      throw new IllegalStateException("OAuth authorize response missing Location header");
    }
    URI redirect = URI.create(location);
    String queryString = redirect.getQuery();
    if (queryString == null || !queryString.contains("code=")) {
      throw new IllegalStateException("OAuth authorize redirect missing code: " + location);
    }
    for (String part : queryString.split("&")) {
      if (part.startsWith("code=")) {
        return part.substring("code=".length());
      }
    }
    throw new IllegalStateException("OAuth authorize redirect missing code: " + location);
  }

  private static String exchangeAuthorizationCode(String code)
      throws IOException, InterruptedException {
    URI tokenUri = URI.create(CelonisOAuthTestConstants.oauthBaseUrl() + "/oauth2/token");
    String form =
        formBody(
            Map.of(
                "grant_type", "authorization_code",
                "code", code,
                "redirect_uri", CelonisOAuthTestConstants.OAUTH_REDIRECT_URI,
                "client_id", CelonisOAuthTestConstants.OAUTH_CLIENT_ID,
                "client_secret", CelonisOAuthTestConstants.OAUTH_CLIENT_SECRET));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokenUri)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(form))
            .build();

    HttpResponse<String> response =
        HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException("OAuth token exchange failed: " + response.body());
    }
    Map<String, String> tokenResponse =
        MAPPER.readValue(response.body(), new TypeReference<Map<String, String>>() {});
    String idToken = tokenResponse.get("id_token");
    if (idToken == null || idToken.isBlank()) {
      throw new IllegalStateException("OAuth response missing id_token");
    }
    return idToken;
  }

  private static String createInternalSessionJwt(String email) throws IOException {
    Map<String, Object> team =
        Map.of(
            "id", CelonisOAuthTestConstants.oauthTeamId(),
            "domain", CelonisOAuthTestConstants.oauthTeamDomain(),
            "role", 1,
            "featureKeys", List.of(),
            "groups", List.of());
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("sub", UUID.randomUUID().toString());
    payload.put("subject_token_type", "session");
    payload.put("teams", List.of(team));
    payload.put("name", "User " + capitalizeFirst(emailLocalPart(email)));
    payload.put("email", email);
    payload.put("azp", CelonisOAuthTestConstants.OAUTH_CLIENT_ID);
    payload.put("scopes", List.of());

    return signHs256Jwt(payload, CelonisOAuthTestConstants.OAUTH_INTERNAL_JWT_SECRET);
  }

  private static String signHs256Jwt(Map<String, Object> payload, String secret)
      throws IOException {
    Map<String, Object> header = Map.of("alg", "HS256", "typ", "JWT");
    String encodedHeader = base64Url(MAPPER.writeValueAsBytes(header));
    String encodedPayload = base64Url(MAPPER.writeValueAsBytes(payload));
    String signingInput = encodedHeader + "." + encodedPayload;
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
      String signature = base64Url(mac.doFinal(signingInput.getBytes(StandardCharsets.UTF_8)));
      return signingInput + "." + signature;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to sign internal session JWT", e);
    }
  }

  private static String base64Url(byte[] value) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
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
