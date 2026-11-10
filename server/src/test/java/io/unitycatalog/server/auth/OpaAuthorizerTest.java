package io.unitycatalog.server.auth;

import static io.unitycatalog.server.model.SecurableType.TABLE;
import static io.unitycatalog.server.persist.model.Privileges.CREATE_TABLE;
import static io.unitycatalog.server.persist.model.Privileges.OWNER;
import static io.unitycatalog.server.persist.model.Privileges.SELECT;
import static io.unitycatalog.server.persist.model.Privileges.USE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.linecorp.armeria.client.ClientRequestContext;
import com.linecorp.armeria.client.ClientRequestContextCaptor;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.common.util.TimeoutMode;
import com.linecorp.armeria.server.RequestCancellationException;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.server.auth.decorator.KeyMapper;
import io.unitycatalog.server.auth.decorator.ResultFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessEvaluator;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.persist.dao.CatalogInfoDAO;
import io.unitycatalog.server.persist.dao.SchemaInfoDAO;
import io.unitycatalog.server.persist.dao.TableInfoDAO;
import io.unitycatalog.server.persist.dao.UserDAO;
import io.unitycatalog.server.persist.model.Privileges;
import io.unitycatalog.server.utils.JsonUtils;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.layout.PatternLayout;
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
  private Predicate<JsonNode> shouldStall = request -> false;
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
          if (shouldStall.test(request)) {
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
  @ValueSource(ints = {301, 307, 403, 404, 500})
  void rejectsHttpErrorsAndDoesNotFollowRedirects(int responseStatus) {
    status = responseStatus;
    assertThat(unavailableDecision())
        .hasMessage("OPA returned HTTP " + responseStatus)
        .hasNoCause();
    assertThat(requests).hasSize(1);
  }

  @Test
  void reportsConnectionFailures() {
    opa.stop(0);
    assertThat(unavailableDecision())
        .hasMessage("Could not get an authorization decision from OPA")
        .hasCauseInstanceOf(java.util.concurrent.CompletionException.class);
    assertThat(requests).isEmpty();
  }

  @Test
  void reportsResponseTimeouts() {
    shouldStall = request -> true;
    authorizer = newAuthorizer(Duration.ofMillis(100));
    assertThat(unavailableDecision()).hasRootCauseInstanceOf(ResponseTimeoutException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stopsOpaChecksWhenTheUcRequestIsCancelled(boolean timedOut) throws Exception {
    authorizer = newAuthorizer(Duration.ofSeconds(30));
    allowOnly(USE_SCHEMA);
    CountDownLatch pending = new CountDownLatch(1);
    shouldStall =
        request -> {
          if (request.path("input").path("privilege").asText().equals("SELECT")) {
            pending.countDown();
            return true;
          }
          return false;
        };
    ServiceRequestContext context = ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/"));
    Class<? extends Throwable> expectedCause =
        timedOut ? RequestTimeoutException.class : RequestCancellationException.class;
    List<ClientRequestContext> completed = new ArrayList<>();
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<BaseException> decision =
          executor.submit(
              () -> {
                try (SafeCloseable ignored = context.push();
                    ClientRequestContextCaptor captured = Clients.newContextCaptor()) {
                  for (int i = 0; i < 10; i++) {
                    assertThat(authorizer.authorize(principal, TABLE, table, USE_SCHEMA)).isTrue();
                  }
                  completed.addAll(captured.getAll());
                  // Let completion callbacks finish before cancelling the UC request.
                  for (ClientRequestContext call : completed) {
                    call.log().whenComplete().get(5, TimeUnit.SECONDS);
                    call.eventLoop().submit(() -> {}).get(5, TimeUnit.SECONDS);
                  }
                  requests.clear();
                  return catchThrowableOfType(
                      () ->
                          authorizer.authorizeAll(
                              principal, TABLE, table, USE_SCHEMA, SELECT, CREATE_TABLE),
                      BaseException.class);
                }
              });
      assertThat(pending.await(5, TimeUnit.SECONDS)).isTrue();
      if (timedOut) {
        context.setRequestTimeoutMillis(TimeoutMode.SET_FROM_START, 100);
      } else {
        context.cancel();
      }
      BaseException failure = decision.get(5, TimeUnit.SECONDS);
      context.whenRequestCancelled().get(5, TimeUnit.SECONDS);
      assertThat(failure).hasRootCauseInstanceOf(expectedCause);
      assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.UNAVAILABLE);
      assertPrivileges("USE_SCHEMA", "SELECT");
      assertThat(completed)
          .hasSize(10)
          .allSatisfy(call -> assertThat(call.cancellationCause()).isNull());

      try (SafeCloseable ignored = context.push()) {
        assertThat(unavailableDecision()).hasRootCauseInstanceOf(expectedCause);
      }
      assertThat(requests).isEmpty();
    } finally {
      context.cancel();
      opa.stop(0);
      executor.shutdownNow();
    }
  }

  @Test
  void boundsResponseSize() {
    reply =
        request -> "{\"result\":{\"allow\":true},\"padding\":\"" + "x".repeat(65 * 1024) + "\"}";
    assertThat(unavailableDecision()).hasRootCauseInstanceOf(ContentTooLargeException.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void metadataFailureStopsBeforeHttpAndDoesNotExposeDatabaseDetails(boolean openingSession)
      throws Exception {
    if (openingSession) {
      when(sessionFactory.openSession()).thenThrow(new IllegalStateException("private-db-detail"));
    } else {
      when(session.get(UserDAO.class, principal))
          .thenThrow(new IllegalStateException("private-db-detail"));
    }
    UnityAccessEvaluator evaluator = new UnityAccessEvaluator(authorizer, true);
    String expression = "#authorize(#principal, #table, SELECT)";
    List<TableInfo> items = new ArrayList<>(List.of(new TableInfo().tableId(table.toString())));
    ResultFilter filter =
        new ResultFilter(
            evaluator, principal, expression, "", Map.of(), Map.of(), mock(KeyMapper.class));
    for (Runnable check :
        List.<Runnable>of(
            () -> authorizer.authorize(principal, TABLE, table, SELECT),
            () -> evaluator.evaluate(principal, expression, Map.of(TABLE, table), Map.of()),
            () -> filter.filter(TABLE, items))) {
      BaseException failure = catchThrowableOfType(check::run, BaseException.class);
      assertThat(failure)
          .hasMessage("Could not build OPA authorization input")
          .hasRootCauseMessage("private-db-detail");
      assertThat(failure.getErrorCode()).isEqualTo(ErrorCode.INTERNAL);
    }
    assertThat(requests).isEmpty();
    assertThat(items).hasSize(1);
    verifyNoInteractions(nativeAuthorizer);
  }

  @ParameterizedTest
  @ValueSource(
      classes = {UserDAO.class, TableInfoDAO.class, SchemaInfoDAO.class, CatalogInfoDAO.class})
  void missingMetadataDeniesWithoutCallingOpa(Class<?> missingType) {
    when(session.get(eq(missingType), any())).thenReturn(null);

    assertThat(authorizer.authorize(principal, TABLE, table, SELECT)).isFalse();
    assertThat(authorizer.authorizeAny(principal, TABLE, table, OWNER, SELECT)).isFalse();
    assertThat(authorizer.authorizeAll(principal, TABLE, table, SELECT, OWNER)).isFalse();
    assertThat(requests).isEmpty();
    verifyNoInteractions(nativeAuthorizer);
  }

  @Test
  void missingMetadataLogsTheCheckAndMissingRecord() {
    when(session.get(CatalogInfoDAO.class, catalog)).thenReturn(null);
    StringWriter warnings = new StringWriter();
    WriterAppender appender =
        WriterAppender.newBuilder()
            .setName("OpaAuthorizerTest")
            .setLayout(PatternLayout.newBuilder().setPattern("%level %msg%n").build())
            .setTarget(warnings)
            .build();
    Logger logger = (Logger) LogManager.getLogger(OpaAuthorizer.class);
    Level previousLevel = logger.getLevel();
    appender.start();
    try {
      Configurator.setLevel(logger.getName(), Level.WARN);
      logger.addAppender(appender);
      assertThat(authorizer.authorize(principal, TABLE, table, SELECT)).isFalse();
      assertThat(requests).isEmpty();
      assertThat(warnings.toString())
          .contains(
              "WARN Authorization check denied",
              "principal=" + principal,
              "privilege=SELECT",
              "resource=table " + table,
              "CatalogInfoDAO " + catalog);
    } finally {
      logger.removeAppender(appender);
      Configurator.setLevel(logger.getName(), previousLevel);
      appender.stop();
    }
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
