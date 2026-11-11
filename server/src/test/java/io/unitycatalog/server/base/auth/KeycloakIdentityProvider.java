package io.unitycatalog.server.base.auth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.QueryParams;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import java.time.Duration;
import java.util.Map;
import lombok.SneakyThrows;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A real Keycloak for {@code external} authentication mode tests: one container per test JVM,
 * started on first use like the test databases ({@code PostgresDatabases.get}); Testcontainers'
 * reaper removes it when the JVM exits. Its realms are created once through Keycloak's admin REST
 * API:
 *
 * <ul>
 *   <li>{@value #REALM}: the user {@value #PERSON_EMAIL} and these clients, each with {@value
 *       #AUDIENCE} as an audience unless noted:
 *       <ul>
 *         <li>{@value #PERSON_CLIENT}: public, password grant, for a person's token
 *         <li>{@value #MACHINE_CLIENT}: client credentials, with an {@code email} claim of {@value
 *             #MACHINE_EMAIL}
 *         <li>{@value #OTHER_AUDIENCE_CLIENT}: client credentials, without the audience
 *         <li>{@value #SHORT_LIVED_CLIENT}: client credentials, tokens valid for one second
 *       </ul>
 *   <li>{@value #OTHER_REALM}: another issuer, with {@value #MACHINE_CLIENT} only
 * </ul>
 */
public final class KeycloakIdentityProvider {

  private static final DockerImageName IMAGE =
      DockerImageName.parse("quay.io/keycloak/keycloak:26.4");
  private static final int PORT = 8080;
  private static final String ADMIN = "admin";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String REALM = "uc";
  public static final String OTHER_REALM = "other";
  public static final String AUDIENCE = "unity-catalog";
  public static final String PERSON_CLIENT = "uc-cli";
  public static final String PERSON_EMAIL = "alice@example.com";
  public static final String MACHINE_CLIENT = "uc-etl";
  public static final String MACHINE_EMAIL = "etl-bot@example.com";
  public static final String OTHER_AUDIENCE_CLIENT = "uc-other-audience";
  public static final String SHORT_LIVED_CLIENT = "uc-short-lived";
  private static final String PERSON_USERNAME = "alice";
  private static final String PERSON_PASSWORD = "alice-password";
  private static final String CLIENT_SECRET = "test-client-secret";

  private static final String AUDIENCE_MAPPER =
      """
      {"name":"audience","protocol":"openid-connect","protocolMapper":"oidc-audience-mapper",
       "config":{"included.custom.audience":"%s","access.token.claim":"true",
                 "id.token.claim":"false"}}\
      """
          .formatted(AUDIENCE);
  private static final String MACHINE_EMAIL_MAPPER =
      """
      {"name":"email","protocol":"openid-connect","protocolMapper":"oidc-hardcoded-claim-mapper",
       "config":{"claim.name":"email","claim.value":"%s","jsonType.label":"String",
                 "access.token.claim":"true"}}\
      """
          .formatted(MACHINE_EMAIL);

  private static KeycloakIdentityProvider instance;

  private final GenericContainer<?> container;
  private final String baseUrl;
  private final WebClient client;

  private KeycloakIdentityProvider() {
    container =
        new GenericContainer<>(IMAGE)
            .withExposedPorts(PORT)
            .withEnv("KC_BOOTSTRAP_ADMIN_USERNAME", ADMIN)
            .withEnv("KC_BOOTSTRAP_ADMIN_PASSWORD", ADMIN)
            .withCommand("start-dev")
            .waitingFor(
                Wait.forHttp("/realms/master")
                    .forPort(PORT)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    container.start();
    // Keycloak names the issuer after the URL a token is requested from; the server under test
    // fetches discovery and keys from the same URL.
    baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(PORT);
    client = WebClient.of(baseUrl);
    createRealms();
  }

  /** The Keycloak for this test JVM, started on first use. */
  public static synchronized KeycloakIdentityProvider get() {
    if (instance == null) {
      instance = new KeycloakIdentityProvider();
    }
    return instance;
  }

  /** The issuer ({@code iss}) of {@code realm}'s tokens. */
  public String issuer(String realm) {
    return baseUrl + "/realms/" + realm;
  }

  /** An access token for {@value #PERSON_EMAIL}, from the password grant. */
  public String personToken() {
    return token(
        REALM,
        Map.of(
            "grant_type", "password",
            "client_id", PERSON_CLIENT,
            "username", PERSON_USERNAME,
            "password", PERSON_PASSWORD));
  }

  /** An access token for {@code clientId}'s service account, from client credentials. */
  public String clientCredentialsToken(String realm, String clientId) {
    return token(
        realm,
        Map.of(
            "grant_type", "client_credentials",
            "client_id", clientId,
            "client_secret", CLIENT_SECRET));
  }

  @SneakyThrows
  private String token(String realm, Map<String, String> form) {
    AggregatedHttpResponse response =
        post(
            "/realms/" + realm + "/protocol/openid-connect/token",
            null,
            MediaType.FORM_DATA,
            QueryParams.builder().add(form.entrySet()).toQueryString());
    return MAPPER.readTree(response.contentUtf8()).get("access_token").asText();
  }

  private void createRealms() {
    String adminToken =
        token(
            "master",
            Map.of(
                "grant_type",
                "password",
                "client_id",
                "admin-cli",
                "username",
                ADMIN,
                "password",
                ADMIN));
    String person =
        """
        {"clientId":"%s","publicClient":true,"directAccessGrantsEnabled":true,
         "standardFlowEnabled":false,"protocolMappers":[%s]}\
        """
            .formatted(PERSON_CLIENT, AUDIENCE_MAPPER);
    String user =
        """
        {"username":"%s","email":"%s","emailVerified":true,"firstName":"Alice",
         "lastName":"Example","enabled":true,
         "credentials":[{"type":"password","value":"%s","temporary":false}]}\
        """
            .formatted(PERSON_USERNAME, PERSON_EMAIL, PERSON_PASSWORD);
    createRealm(
        adminToken,
        REALM,
        String.join(
            ",",
            person,
            serviceAccountClient(MACHINE_CLIENT, "{}", AUDIENCE_MAPPER, MACHINE_EMAIL_MAPPER),
            serviceAccountClient(OTHER_AUDIENCE_CLIENT, "{}", MACHINE_EMAIL_MAPPER),
            serviceAccountClient(
                SHORT_LIVED_CLIENT,
                "{\"access.token.lifespan\":\"1\"}",
                AUDIENCE_MAPPER,
                MACHINE_EMAIL_MAPPER)),
        user);
    createRealm(
        adminToken,
        OTHER_REALM,
        serviceAccountClient(MACHINE_CLIENT, "{}", AUDIENCE_MAPPER, MACHINE_EMAIL_MAPPER),
        "");
  }

  private static String serviceAccountClient(
      String clientId, String attributes, String... mappers) {
    return """
    {"clientId":"%s","publicClient":false,"secret":"%s","serviceAccountsEnabled":true,
     "standardFlowEnabled":false,"attributes":%s,"protocolMappers":[%s]}\
    """
        .formatted(clientId, CLIENT_SECRET, attributes, String.join(",", mappers));
  }

  private void createRealm(String adminToken, String realm, String clients, String users) {
    String body =
        """
        {"realm":"%s","enabled":true,"clients":[%s],"users":[%s]}\
        """
            .formatted(realm, clients, users);
    post("/admin/realms", adminToken, MediaType.JSON, body);
  }

  private AggregatedHttpResponse post(
      String path, String bearerToken, MediaType contentType, String body) {
    RequestHeadersBuilder headers =
        RequestHeaders.builder(HttpMethod.POST, path).contentType(contentType);
    if (bearerToken != null) {
      headers.add(HttpHeaderNames.AUTHORIZATION, "Bearer " + bearerToken);
    }
    AggregatedHttpResponse response =
        client.execute(headers.build(), HttpData.ofUtf8(body)).aggregate().join();
    if (!response.status().isSuccess()) {
      throw new IllegalStateException(
          "Keycloak " + path + " returned " + response.status() + ": " + response.contentUtf8());
    }
    return response;
  }
}
