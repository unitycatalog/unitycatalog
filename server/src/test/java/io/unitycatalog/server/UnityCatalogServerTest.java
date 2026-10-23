package io.unitycatalog.server;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static io.unitycatalog.server.utils.TestUtils.sendRawJsonPost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.prometheus.PrometheusExpositionService;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.AuthorizeKey;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.model.CatalogInfo;
import io.unitycatalog.server.model.CreateCatalog;
import io.unitycatalog.server.model.SecurableType;
import io.unitycatalog.server.observability.MetricsRegistries;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.net.ServerSocket;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class UnityCatalogServerTest extends BaseServerTest {

  @ParameterizedTest
  @ValueSource(ints = {0, -1, 65536, Integer.MAX_VALUE})
  public void rejectsOutOfRangeObservabilityPort(int observabilityPort) {
    assertThatThrownBy(() -> UnityCatalogServer.validateObservabilityPort(9000, observabilityPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 65535");
  }

  @ParameterizedTest
  @ValueSource(ints = {9000, 9001})
  public void rejectsObservabilityPortEqualToEitherApiPort(int observabilityPort) {
    assertThatThrownBy(() -> UnityCatalogServer.validateObservabilityPort(9000, observabilityPort))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Observability port");
  }

  @Test
  public void leavesObservabilityPortUnboundWhenDisabled() throws Exception {
    int observabilityPort = URI.create(observabilityServerConfig.getServerUrl()).getPort();
    try (ServerSocket unusedObservabilityPort = new ServerSocket(observabilityPort)) {
      assertThat(sendRawGet(serverConfig, "/").statusCode()).isEqualTo(200);
      for (String path : List.of("/livez", "/readyz", "/metrics")) {
        assertThat(sendRawGet(serverConfig, path).statusCode()).as(path).isEqualTo(404);
      }
    }
  }

  @Test
  public void responseAuthorizationRejectionIsRecorded() throws Exception {
    // Reuse the fixture's database and ports with a handler that deliberately omits filtering.
    unityCatalogServer.stop();
    int apiPort = URI.create(serverConfig.getServerUrl()).getPort();
    int observabilityPort = URI.create(observabilityServerConfig.getServerUrl()).getPort();
    ServerProperties properties = new ServerProperties(serverProperties);
    Repositories repositories =
        new Repositories(hibernateConfigurator.getSessionFactory(), properties);

    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(1).build();
    try {
      try (MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus();
          Server server =
              new ArmeriaServerBuilder(
                      apiPort, "/api/", "/control/", properties, blockingTaskExecutor)
                  .observabilityPort(observabilityPort)
                  .observabilityService(
                      "/metrics",
                      PrometheusExpositionService.of(metrics.registry().getPrometheusRegistry()))
                  .annotate("unfiltered", new UnfilteredResponseService())
                  .withSecurityDecorators(
                      new UnityAccessDecorator(new AllowingAuthorizer(), repositories, properties),
                      (delegate, ctx, req) -> delegate.serve(ctx, req))
                  .meterRegistry(metrics.registry())
                  .build()) {
        server.start().join();
        var response = sendRawGet(serverConfig, "/api/unfiltered");
        assertThat(response.statusCode()).isEqualTo(403);
        TestUtils.assertHttpApiException(
            response, ErrorCode.PERMISSION_DENIED, UnityAccessDecorator.ERR_AUTH_NOT_EXECUTED);
        assertThat(response.body()).doesNotContain("sensitive_data");
        TestUtils.assertHttpRequestMetric(
            serverConfig,
            observabilityServerConfig,
            "io.unitycatalog.server.UnityCatalogServerTest$UnfilteredResponseService",
            "unfilteredResponse",
            403,
            1.0);
        assertThat(
                sendRawGet(observabilityServerConfig, "/metrics")
                    .body()
                    .lines()
                    .filter(line -> line.startsWith("http_server_requests_total{"))
                    .toList())
            .hasSize(1);
      }
    } finally {
      blockingTaskExecutor.shutdown();
    }
  }

  public static class UnfilteredResponseService implements UnityCatalogRestService {
    @Get("")
    @AuthorizeExpression("true")
    @ResponseAuthorizeFilter
    public HttpResponse unfilteredResponse() {
      return HttpResponse.ofJson(Map.of("sensitive_data", "must-not-leak"));
    }
  }

  @Test
  public void authorizationPreconditionsApplyToRequestsAndResponseItems() throws Exception {
    unityCatalogServer.stop();
    int apiPort = URI.create(serverConfig.getServerUrl()).getPort();
    ServerProperties properties = new ServerProperties(serverProperties);
    properties.set(ServerProperties.Property.AUTHORIZATION_ENABLED, "enable");
    Repositories repositories =
        new Repositories(hibernateConfigurator.getSessionFactory(), properties);
    PreconditionService service = new PreconditionService();
    BlockingTaskExecutor blockingTaskExecutor =
        BlockingTaskExecutor.builder().numThreads(1).build();
    try {
      try (Server server =
          new ArmeriaServerBuilder(apiPort, "/api/", "/control/", properties, blockingTaskExecutor)
              .annotate("preconditions", service)
              .withSecurityDecorators(
                  new UnityAccessDecorator(new AllowingAuthorizer(), repositories, properties),
                  (delegate, ctx, req) -> delegate.serve(ctx, req))
              .build()) {
        server.start().join();
        for (String condition : List.of("allow", "also_allow", "deny")) {
          for (String permission : List.of("allow", "also_allow", "deny")) {
            boolean allowed = !condition.equals("deny") && !permission.equals("deny");
            int callsBefore = service.calls.get();
            var inline =
                sendRawGet(
                    serverConfig,
                    "/api/preconditions?name=" + condition + "&comment=" + permission);
            var payload =
                sendRawJsonPost(
                    serverConfig,
                    "/api/preconditions",
                    "{\"name\":\"" + condition + "\",\"comment\":\"" + permission + "\"}",
                    "application/json");
            for (var response : List.of(inline, payload)) {
              assertThat(response.statusCode()).isEqualTo(allowed ? 200 : 403);
              if (!allowed) {
                TestUtils.assertHttpApiException(
                    response, ErrorCode.PERMISSION_DENIED, "Access denied.");
              }
            }
            assertThat(service.calls.get()).isEqualTo(callsBefore + (allowed ? 2 : 0));
          }
        }
        TestUtils.assertHttpApiException(
            sendRawGet(serverConfig, "/api/preconditions/default-deny"),
            ErrorCode.PERMISSION_DENIED,
            "Access denied.");

        var filtered = sendRawGet(serverConfig, "/api/preconditions/list");
        assertThat(filtered.statusCode()).isEqualTo(200);
        assertThat(filtered.body())
            .contains(PreconditionService.VISIBLE_ID)
            .doesNotContain(PreconditionService.HIDDEN_ID);
      }
    } finally {
      blockingTaskExecutor.shutdown();
    }
  }

  public static class PreconditionService implements UnityCatalogRestService {
    private static final String VISIBLE_ID = "00000000-0000-0000-0000-000000000001";
    private static final String HIDDEN_ID = "00000000-0000-0000-0000-000000000002";
    private final AtomicInteger calls = new AtomicInteger();

    @Get("")
    @AuthorizeExpression(
        precondition = "#name == 'allow' || #name == 'also_allow'",
        value = "#comment == 'allow' || #comment == 'also_allow'")
    public HttpResponse inline(
        @Param("name") @AuthorizeKey(key = "name") String name,
        @Param("comment") @AuthorizeKey(key = "comment") String comment) {
      calls.incrementAndGet();
      return HttpResponse.ofJson(Map.of("allowed", true));
    }

    @Post("")
    @AuthorizeExpression(
        precondition = "#name == 'allow' || #name == 'also_allow'",
        value = "#comment == 'allow' || #comment == 'also_allow'")
    public HttpResponse payload(
        @AuthorizeKey(key = "name") @AuthorizeKey(key = "comment") CreateCatalog request) {
      calls.incrementAndGet();
      return HttpResponse.ofJson(Map.of("allowed", true));
    }

    @Get("/default-deny")
    @AuthorizeExpression(precondition = "#permit")
    public HttpResponse defaultDeny() {
      return HttpResponse.ofJson(Map.of("allowed", true));
    }

    @Get("/list")
    @AuthorizeExpression(
        precondition = "#catalog.toString() == '" + VISIBLE_ID + "'",
        value = "#permit")
    @ResponseAuthorizeFilter
    public HttpResponse list() {
      List<CatalogInfo> items =
          new ArrayList<>(
              List.of(new CatalogInfo().id(VISIBLE_ID), new CatalogInfo().id(HIDDEN_ID)));
      ServiceRequestContext.current()
          .attr(UnityAccessDecorator.RESULT_FILTER_ATTR)
          .filter(SecurableType.CATALOG, items);
      return HttpResponse.ofJson(items);
    }
  }
}
