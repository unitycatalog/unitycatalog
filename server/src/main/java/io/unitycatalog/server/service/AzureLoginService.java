package io.unitycatalog.server.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import com.linecorp.armeria.server.annotation.RequestObject;
import io.unitycatalog.control.model.AzureLoginStartRequest;
import io.unitycatalog.control.model.AzureLoginStartResponse;
import io.unitycatalog.control.model.OAuthTokenExchangeInfo;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Azure OAuth2 Login Service - API equivalent of CLI auth login.
 *
 * <p>Provides server-side Azure AD OAuth2 flow for users not yet in Unity Catalog database. This
 * eliminates the need for CLI browser automation and enables programmatic authentication.
 */
@ProducesJson
public class AzureLoginService {

  private static final Logger LOGGER = LoggerFactory.getLogger(AzureLoginService.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Session storage for OAuth2 flow state
  private static final Map<String, LoginSession> LOGIN_SESSIONS = new ConcurrentHashMap<>();
  private static final int SESSION_EXPIRY_SECONDS = 300; // 5 minutes

  private final ServerProperties serverProperties;
  private final BootstrapTokenExchangeService bootstrapService;
  private final HttpClient httpClient;

  public AzureLoginService(
      SecurityContext securityContext,
      ServerProperties serverProperties,
      BootstrapTokenExchangeService bootstrapService) {
    this.serverProperties = serverProperties;
    this.bootstrapService = bootstrapService;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  /**
   * Start Azure AD OAuth2 login flow.
   *
   * <p>Generates authorization URL and session for completing the login.
   */
  @Post("/start")
  public AzureLoginStartResponse startAzureLogin(@RequestObject AzureLoginStartRequest request) {
    LOGGER.debug("Starting Azure login flow: {}", request);

    // Validate Azure AD configuration
    validateAzureConfiguration();

    // Generate session
    String sessionId = generateSessionId();
    String state = generateState();

    // Encode session ID in state parameter so Azure sends it back
    String stateWithSession = state + ":" + sessionId;

    // Azure must redirect to our server callback, not the client's redirect URI
    String serverCallbackUri =
        "http://localhost:8080/api/1.0/unity-control/auth/azure-login/callback";
    String clientRedirectUri =
        request.getRedirectUri() != null
            ? request.getRedirectUri()
            : "http://localhost:3000/callback";

    // Build authorization URL pointing to our server callback
    String scope = request.getScope() != null ? request.getScope() : "openid profile email";
    String authorizationUrl = buildAuthorizationUrl(serverCallbackUri, scope, stateWithSession);

    // Store session with both URIs
    LoginSession session =
        new LoginSession(sessionId, state, clientRedirectUri, System.currentTimeMillis());
    LOGIN_SESSIONS.put(sessionId, session);

    // Clean up expired sessions
    cleanupExpiredSessions();

    LOGGER.info("Azure login flow started: sessionId={}, state={}", sessionId, state);
    LOGGER.info("LOGIN_SESSIONS size after storing: {}", LOGIN_SESSIONS.size());
    LOGGER.info("LOGIN_SESSIONS contains sessionId: {}", LOGIN_SESSIONS.containsKey(sessionId));

    return new AzureLoginStartResponse()
        .authorizationUrl(authorizationUrl)
        .sessionId(sessionId)
        .expiresIn(SESSION_EXPIRY_SECONDS)
        .state(state);
  }

  /**
   * Complete Azure AD OAuth2 login flow.
   *
   * <p>Exchanges authorization code for Azure ID token, then for Unity Catalog token.
   */
  @Get("/callback")
  public OAuthTokenExchangeInfo completeAzureLogin(
      @Param("code") String code, @Param("state") String stateWithSession) {

    // Extract session ID from state parameter
    String[] parts = stateWithSession.split(":", 2);
    if (parts.length != 2) {
      LOGGER.error("Invalid state parameter format: {}", stateWithSession);
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid state parameter format");
    }

    String state = parts[0];
    String sessionId = parts[1];

    LOGGER.error(
        "DEBUG: Looking for sessionId: '{}' in LOGIN_SESSIONS (size={})",
        sessionId,
        LOGIN_SESSIONS.size());
    LOGGER.error("DEBUG: Available session IDs: {}", LOGIN_SESSIONS.keySet());

    LOGGER.debug("Completing Azure login flow: sessionId={}", sessionId);
    LOGGER.info("LOGIN_SESSIONS size before lookup: {}", LOGIN_SESSIONS.size());
    LOGGER.info("Looking for sessionId: {}", sessionId);
    LOGGER.info("Available session IDs: {}", LOGIN_SESSIONS.keySet());

    // Validate session
    LoginSession session = LOGIN_SESSIONS.remove(sessionId);
    if (session == null) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Invalid or expired session ID");
    }

    if (session.isExpired()) {
      throw new BaseException(ErrorCode.INVALID_ARGUMENT, "Session has expired");
    }

    if (!session.getState().equals(state)) {
      throw new BaseException(
          ErrorCode.INVALID_ARGUMENT, "Invalid state parameter - CSRF check failed");
    }

    try {
      // Exchange authorization code for Azure tokens using the server callback URI
      String serverCallbackUri =
          "http://localhost:8080/api/1.0/unity-control/auth/azure-login/callback";
      String azureIdToken = exchangeCodeForToken(code, serverCallbackUri);

      // Return the Azure ID token (not a UC token)
      return new OAuthTokenExchangeInfo()
          .accessToken(azureIdToken)
          .tokenType(io.unitycatalog.control.model.AccessTokenType.BEARER)
          .expiresIn(3600L); // Azure tokens typically expire in 1 hour

    } catch (Exception e) {
      LOGGER.error("Failed to complete Azure login: sessionId={}", sessionId, e);
      throw new BaseException(
          ErrorCode.INTERNAL, "Failed to complete Azure login: " + e.getMessage());
    }
  }

  private void validateAzureConfiguration() {
    String authUrl = serverProperties.getProperty("server.authorization-url");
    String tokenUrl = serverProperties.getProperty("server.token-url");
    String clientId = serverProperties.getProperty("server.client-id");
    String clientSecret = serverProperties.getProperty("server.client-secret");

    if (authUrl == null || tokenUrl == null || clientId == null || clientSecret == null) {
      throw new BaseException(
          ErrorCode.FAILED_PRECONDITION,
          "Azure AD configuration incomplete - missing required properties");
    }
  }

  private String buildAuthorizationUrl(String redirectUri, String scope, String state) {
    String authUrl = serverProperties.getProperty("server.authorization-url");
    String clientId = serverProperties.getProperty("server.client-id");

    return String.format(
        "%s?response_type=code&client_id=%s&redirect_uri=%s&scope=%s&state=%s",
        authUrl,
        clientId,
        java.net.URLEncoder.encode(redirectUri, java.nio.charset.StandardCharsets.UTF_8),
        java.net.URLEncoder.encode(scope, java.nio.charset.StandardCharsets.UTF_8),
        state);
  }

  private String exchangeCodeForToken(String authorizationCode, String redirectUri)
      throws Exception {
    String tokenUrl = serverProperties.getProperty("server.token-url");
    String clientId = serverProperties.getProperty("server.client-id");
    String clientSecret = serverProperties.getProperty("server.client-secret");

    // Build token exchange request
    String requestBody =
        String.format(
            "grant_type=authorization_code&code=%s&redirect_uri=%s",
            java.net.URLEncoder.encode(authorizationCode, java.nio.charset.StandardCharsets.UTF_8),
            java.net.URLEncoder.encode(redirectUri, java.nio.charset.StandardCharsets.UTF_8));

    // Prepare authentication header
    String auth = clientId + ":" + clientSecret;
    String authHeader = "Basic " + Base64.getEncoder().encodeToString(auth.getBytes());

    // Make token exchange request
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Authorization", authHeader)
            .POST(HttpRequest.BodyPublishers.ofString(requestBody))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != HttpURLConnection.HTTP_OK) {
      throw new BaseException(
          ErrorCode.INTERNAL, "Azure token exchange failed: " + response.body());
    }

    // Parse response to get ID token
    Map<String, Object> tokenResponse =
        OBJECT_MAPPER.readValue(response.body(), new TypeReference<>() {});
    String idToken = (String) tokenResponse.get("id_token");

    if (idToken == null) {
      throw new BaseException(ErrorCode.INTERNAL, "No ID token in Azure response");
    }

    return idToken;
  }

  private String generateSessionId() {
    byte[] bytes = new byte[16];
    new SecureRandom().nextBytes(bytes);
    return "sess_" + Hex.encodeHexString(bytes);
  }

  private String generateState() {
    byte[] bytes = new byte[16];
    new SecureRandom().nextBytes(bytes);
    return Hex.encodeHexString(bytes);
  }

  private void cleanupExpiredSessions() {
    long now = System.currentTimeMillis();
    LOGIN_SESSIONS.entrySet().removeIf(entry -> entry.getValue().isExpired(now));
  }

  /** Session data for OAuth2 flow. */
  private static class LoginSession {
    private final String sessionId;
    private final String state;
    private final String redirectUri;
    private final long createdAt;

    public LoginSession(String sessionId, String state, String redirectUri, long createdAt) {
      this.sessionId = sessionId;
      this.state = state;
      this.redirectUri = redirectUri;
      this.createdAt = createdAt;
    }

    public String getSessionId() {
      return sessionId;
    }

    public String getState() {
      return state;
    }

    public String getRedirectUri() {
      return redirectUri;
    }

    public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
    }

    public boolean isExpired(long currentTime) {
      return (currentTime - createdAt) > (SESSION_EXPIRY_SECONDS * 1000L);
    }
  }
}
