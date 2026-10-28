package io.unitycatalog.server.auth;

import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.CREATE_TABLE;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static io.unitycatalog.server.persist.model.Privileges.USE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.JsonUtils;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class OpaAuthorizerTest {
  private final UUID principal = UUID.randomUUID();
  private final UUID table = UUID.randomUUID();
  private final UUID schema = UUID.randomUUID();
  private final UUID catalog = UUID.randomUUID();
  private final JCasbinAuthorizer nativeAuthorizer = mock(JCasbinAuthorizer.class);
  private final SessionFactory sessionFactory = mock(SessionFactory.class);
  private final Session session = mock(Session.class);
  private final List<Request> requests = new CopyOnWriteArrayList<>();
  private HttpServer opa;
  private OpaAuthorizer authorizer;
  private int status = 200;
  private boolean stalled;
  private Function<JsonNode, String> reply = request -> "{\"result\":{\"allow\":true}}";

  private record Request(String method, String path, String contentType, JsonNode body) {}

  @BeforeEach
  void setUp() throws Exception {
    when(sessionFactory.openSession()).thenReturn(session);
    when(session.beginTransaction()).thenReturn(mock(Transaction.class));
    when(session.get(UserDAO.class, principal))
        .thenReturn(
            UserDAO.builder()
                .id(principal)
                .email("alice@example.com")
                .externalId("subject-123")
                .build());
    when(session.get(TableInfoDAO.class, table))
        .thenReturn(TableInfoDAO.builder().id(table).name("orders").schemaId(schema).build());
    when(session.get(SchemaInfoDAO.class, schema))
        .thenReturn(SchemaInfoDAO.builder().id(schema).name("reports").catalogId(catalog).build());
    when(session.get(CatalogInfoDAO.class, catalog))
        .thenReturn(CatalogInfoDAO.builder().id(catalog).name("analytics").build());
    opa = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    opa.createContext(
        "/",
        exchange -> {
          JsonNode request = JsonUtils.getInstance().readTree(exchange.getRequestBody());
          requests.add(
              new Request(
                  exchange.getRequestMethod(),
                  exchange.getRequestURI().getPath(),
                  exchange.getRequestHeaders().getFirst("Content-Type"),
                  request));
          if (stalled) {
            return;
          }
          byte[] body = reply.apply(request).getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.getResponseHeaders().set("Location", "/redirect-target");
          exchange.sendResponseHeaders(status, body.length);
          exchange.getResponseBody().write(body);
          exchange.close();
        });
    opa.start();
    authorizer = newAuthorizer(Duration.ofSeconds(2));
  }

  @AfterEach
  void tearDown() {
    opa.stop(0);
    authorizer.close();
  }

  private OpaAuthorizer newAuthorizer(Duration responseTimeout) {
    return new OpaAuthorizer(
        nativeAuthorizer,
        sessionFactory,
        "http://127.0.0.1:" + opa.getAddress().getPort() + "/v1/data/uc/authz",
        responseTimeout);
  }

  @Test
  void sendsOnePrivilegeWithMetadataAndUsesEachReply() throws Exception {
    assertThat(authorizer.authorize(principal, TABLE, table, SELECT)).isTrue();
    Request request = requests.get(0);
    assertThat(request.method()).isEqualTo("POST");
    assertThat(request.path()).isEqualTo("/v1/data/uc/authz");
    assertThat(request.contentType()).startsWith("application/json");
    assertThat(request.body())
        .isEqualTo(
            JsonUtils.getInstance()
                .readTree(
                    """
                    {"input": {
                      "principal": {"id": "%s", "email": "alice@example.com", "external_id": "subject-123"},
                      "privilege": "SELECT",
                      "resource": {"id": "%s", "type": "TABLE", "name": "orders", "parents": [
                        {"id": "%s", "type": "SCHEMA", "name": "reports"},
                        {"id": "%s", "type": "CATALOG", "name": "analytics"}
                      ]}
                    }}
                    """
                        .formatted(principal, table, schema, catalog)));

    allowOnly();
    assertThat(authorizer.authorize(principal, TABLE, table, SELECT)).isFalse();
    assertThat(requests).hasSize(2);
    verifyNoInteractions(nativeAuthorizer);
  }

  @Test
  void anyAndAllPreserveOrderingAndShortCircuit() {
    allowOnly(USE_SCHEMA, CREATE_TABLE);
    assertThat(authorizer.authorizeAny(principal, TABLE, table, OWNER, USE_SCHEMA, SELECT))
        .isTrue();
    assertPrivileges("OWNER", "USE_SCHEMA");

    assertThat(authorizer.authorizeAll(principal, TABLE, table, USE_SCHEMA, CREATE_TABLE)).isTrue();
    assertPrivileges("USE_SCHEMA", "CREATE_TABLE");

    assertThat(authorizer.authorizeAll(principal, TABLE, table, USE_SCHEMA, OWNER, CREATE_TABLE))
        .isFalse();
    assertPrivileges("USE_SCHEMA", "OWNER");

    assertThat(authorizer.authorizeAny(principal, TABLE, table, OWNER, SELECT)).isFalse();
    assertPrivileges("OWNER", "SELECT");

    assertThat(authorizer.authorizeAny(principal, TABLE, table)).isFalse();
    assertThat(authorizer.authorizeAll(principal, TABLE, table)).isTrue();
    assertThat(requests).isEmpty();
    verifyNoInteractions(nativeAuthorizer);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "not json",
        "null",
        "{}",
        "{\"result\":true}",
        "{\"result\":{}}",
        "{\"result\":{\"allow\":null}}",
        "{\"result\":{\"allow\":\"true\"}}",
        "{\"result\":{\"allow\":1}}",
        "{\"result\":{\"allow\":true}} {}"
      })
  void rejectsUnusableDecisions(String body) {
    reply = request -> body;
    unavailableDecision();
    assertThat(requests).hasSize(1);
  }

  @ParameterizedTest
  @ValueSource(ints = {301, 307, 403, 500})
  void rejectsHttpErrorsAndDoesNotFollowRedirects(int responseStatus) {
    status = responseStatus;
    assertThat(unavailableDecision()).hasMessage("OPA returned HTTP " + responseStatus);
    assertThat(requests).hasSize(1);
  }

  @Test
  void reportsConnectionFailures() {
    opa.stop(0);
    assertThat(unavailableDecision())
        .hasCauseInstanceOf(java.util.concurrent.CompletionException.class);
    assertThat(requests).isEmpty();
  }

  @Test
  void reportsResponseTimeouts() {
    stalled = true;
    authorizer = newAuthorizer(Duration.ofMillis(100));
    assertThat(unavailableDecision()).hasRootCauseInstanceOf(ResponseTimeoutException.class);
  }

  @Test
  void boundsResponseSize() {
    reply =
        request -> "{\"result\":{\"allow\":true},\"padding\":\"" + "x".repeat(65 * 1024) + "\"}";
    assertThat(unavailableDecision()).hasRootCauseInstanceOf(ContentTooLargeException.class);
  }

  @Test
  void metadataFailureStopsBeforeHttpAndDoesNotExposeDatabaseDetails() {
    when(session.get(UserDAO.class, principal))
        .thenThrow(new IllegalStateException("private-db-detail"));
    assertThat(unavailableDecision())
        .hasMessage("Could not get an authorization decision from OPA");
    assertThat(requests).isEmpty();
  }

  @Test
  void doesNotRefreshNativeDecisionsAndClosesNativeAuthorizer() {
    assertThat(authorizer.refreshAuthorizations(System.nanoTime())).isFalse();
    verifyNoInteractions(nativeAuthorizer);
    authorizer.close();
    verify(nativeAuthorizer).close();
    verifyNoMoreInteractions(nativeAuthorizer);
  }

  private BaseException unavailableDecision() {
    BaseException error =
        catchThrowableOfType(
            () -> authorizer.authorize(principal, TABLE, table, SELECT), BaseException.class);
    assertThat(error).isNotNull();
    assertThat(error.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
    verifyNoInteractions(nativeAuthorizer);
    return error;
  }

  private void allowOnly(Privileges... privileges) {
    Set<String> allowed = Arrays.stream(privileges).map(Enum::name).collect(Collectors.toSet());
    reply =
        request ->
            "{\"result\":{\"allow\":"
                + allowed.contains(request.path("input").path("privilege").asText())
                + "}}";
  }

  private void assertPrivileges(String... expected) {
    assertThat(requests)
        .extracting(request -> request.body().path("input").path("privilege").asText())
        .containsExactly(expected);
    requests.clear();
  }
}
