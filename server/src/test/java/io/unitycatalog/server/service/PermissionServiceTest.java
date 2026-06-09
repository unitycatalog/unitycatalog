package io.unitycatalog.server.service;

import static io.unitycatalog.server.security.SecurityContext.Issuers.INTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.security.SecurityConfiguration;
import io.unitycatalog.server.security.SecurityContext;
import io.unitycatalog.server.utils.ServerProperties.Property;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Integration tests for {@link PermissionService}.
 *
 * <p>These tests follow the same approach as {@code AuthServiceTest}: a real Unity Catalog server
 * is booted (via {@link BaseServerTest}) and the permission endpoints are exercised end-to-end with
 * a raw Armeria {@link WebClient}. Unlike a Mockito unit test, this also covers the Armeria
 * authentication ({@code AuthDecorator}) and authorization ({@code UnityAccessDecorator})
 * decorators that wrap the service when {@code server.authorization=enable}.
 *
 * <p>The suite runs in two modes via the concrete subclasses below:
 *
 * <ul>
 *   <li>{@link PermissionServiceWithAuthorizationTest} — authorization enabled, so the
 *       {@code @AuthorizeExpression} rules and token authentication are enforced.
 *   <li>{@link PermissionServiceWithoutAuthorizationTest} — authorization disabled (the server
 *       default), so the decorators are not installed and an {@code AllowingAuthorizer} is used.
 * </ul>
 */
public abstract class PermissionServiceTest extends BaseServerTest {

  protected static final ObjectMapper MAPPER = new ObjectMapper();
  protected static final String BASE_PATH = "/api/2.1/unity-catalog/";
  protected static final String CONTROL_PATH = "/api/1.0/unity-control/";
  protected static final String CATALOGS_ENDPOINT = BASE_PATH + "catalogs";
  protected static final String SCIM_USERS_ENDPOINT = CONTROL_PATH + "scim2/Users";

  protected SecurityConfiguration securityConfiguration;
  protected SecurityContext securityContext;
  protected WebClient client;

  /** Whether the server under test should boot with {@code server.authorization=enable}. */
  protected abstract boolean authorizationEnabled();

  @Override
  protected void setUpProperties() {
    super.setUpProperties();
    if (authorizationEnabled()) {
      serverProperties.setProperty(Property.AUTHORIZATION_ENABLED.getKey(), "enable");
    }
  }

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();

    Path configurationFolder = Path.of("etc", "conf");
    securityConfiguration = new SecurityConfiguration(configurationFolder);
    securityContext =
        new SecurityContext(configurationFolder, securityConfiguration, "server", INTERNAL);

    client = WebClient.builder(serverConfig.getServerUrl()).build();
  }

  @AfterEach
  @Override
  public void tearDown() {
    // When authorization is enabled the JCasbin authorizer persists grants in the casbin_rule
    // table; clear it before the server (and its session factory) is shut down by super.tearDown().
    if (authorizationEnabled() && hibernateConfigurator != null) {
      SessionFactory sessionFactory = hibernateConfigurator.getSessionFactory();
      try (Session session = sessionFactory.openSession()) {
        Transaction tx = session.beginTransaction();
        session.createNativeMutationQuery("delete from casbin_rule").executeUpdate();
        tx.commit();
      }
    }
    System.clearProperty("server.authorization");
    super.tearDown();
  }

  // ---------------------------------------------------------------------------
  // Token helpers
  // ---------------------------------------------------------------------------

  /** Returns the bootstrap admin service token (admin is the metastore owner). */
  protected String adminToken() {
    return securityContext.createServiceToken();
  }

  /**
   * Mints an internally-issued access token for the given user, mirroring how the production
   * security context signs tokens (RSA512 with the keys from {@code etc/conf}).
   */
  @SneakyThrows
  protected String userToken(String email) {
    Algorithm algorithm = securityConfiguration.algorithmRSA();
    return JWT.create()
        .withSubject(email)
        .withIssuer(INTERNAL)
        .withIssuedAt(new Date())
        .withKeyId(securityConfiguration.getKeyId())
        .withJWTId(UUID.randomUUID().toString())
        .withClaim("email", email)
        .sign(algorithm);
  }

  // ---------------------------------------------------------------------------
  // Request helpers
  // ---------------------------------------------------------------------------

  private RequestHeaders headers(HttpMethod method, String path, String token) {
    RequestHeadersBuilder builder =
        RequestHeaders.builder().method(method).path(path).contentType(MediaType.JSON);
    if (token != null) {
      builder.add(HttpHeaderNames.AUTHORIZATION, "Bearer " + token);
    }
    return builder.build();
  }

  protected AggregatedHttpResponse send(HttpMethod method, String path, String token, String body) {
    if (body == null) {
      return client.execute(headers(method, path, token)).aggregate().join();
    }
    return client.execute(headers(method, path, token), HttpData.ofUtf8(body)).aggregate().join();
  }

  protected AggregatedHttpResponse getCatalogPermissions(String catalogName, String token) {
    return send(HttpMethod.GET, BASE_PATH + "permissions/catalog/" + catalogName, token, null);
  }

  protected AggregatedHttpResponse updateCatalogPermissions(
      String catalogName, String token, String body) {
    return send(HttpMethod.PATCH, BASE_PATH + "permissions/catalog/" + catalogName, token, body);
  }

  protected void createCatalog(String name, String token) {
    AggregatedHttpResponse response =
        send(
            HttpMethod.POST,
            CATALOGS_ENDPOINT,
            token,
            String.format("{\"name\":\"%s\",\"comment\":\"test catalog\"}", name));
    assertThat(response.status().isSuccess())
        .as("create catalog failed: %s %s", response.status(), response.contentUtf8())
        .isTrue();
  }

  protected void createUser(String email, String token) {
    AggregatedHttpResponse response =
        send(
            HttpMethod.POST,
            SCIM_USERS_ENDPOINT,
            token,
            String.format(
                "{\"displayName\":\"%s\",\"emails\":[{\"value\":\"%s\",\"primary\":true}]}",
                email.split("@")[0], email));
    assertThat(response.status().code())
        .as("create user failed: %s %s", response.status(), response.contentUtf8())
        .isEqualTo(201);
  }

  protected static String addPrivilegeBody(String principal, String privilege) {
    return String.format(
        "{\"changes\":[{\"principal\":\"%s\",\"add\":[\"%s\"],\"remove\":[]}]}",
        principal, privilege);
  }

  @SneakyThrows
  protected static JsonNode parse(AggregatedHttpResponse response) {
    return MAPPER.readTree(response.contentUtf8());
  }

  /** Returns the privilege strings assigned to {@code principal} in a PermissionsList response. */
  protected static List<String> privilegesFor(JsonNode permissionsList, String principal) {
    List<String> privileges = new ArrayList<>();
    JsonNode assignments = permissionsList.get("privilege_assignments");
    if (assignments == null) {
      return privileges;
    }
    for (JsonNode assignment : assignments) {
      if (principal.equals(assignment.get("principal").asText())) {
        assignment.get("privileges").forEach(node -> privileges.add(node.asText()));
      }
    }
    return privileges;
  }
}
