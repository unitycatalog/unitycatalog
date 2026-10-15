package io.unitycatalog.server;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.prometheus.PrometheusExpositionService;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.observability.MetricsRegistries;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.TestUtils;
import java.net.ServerSocket;
import java.net.URI;
import java.util.List;
import java.util.Map;
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

    try (MetricsRegistries.PrometheusMetrics metrics = MetricsRegistries.createPrometheus();
        Server server =
            new ArmeriaServerBuilder(apiPort, "/api/", "/control/", properties)
                .observabilityPort(observabilityPort)
                .observabilityService(
                    "/metrics",
                    PrometheusExpositionService.of(metrics.registry().getPrometheusRegistry()))
                .annotate("unfiltered", new UnfilteredResponseService())
                .withSecurityDecorators(
                    new UnityAccessDecorator(new AllowingAuthorizer(), repositories),
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
  }

  public static class UnfilteredResponseService implements UnityCatalogRestService {
    @Get("")
    @AuthorizeExpression("true")
    @ResponseAuthorizeFilter
    public HttpResponse unfilteredResponse() {
      return HttpResponse.ofJson(Map.of("sensitive_data", "must-not-leak"));
    }
  }
}
