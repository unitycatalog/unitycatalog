package io.unitycatalog.server;

import static io.unitycatalog.server.utils.TestUtils.sendRawGet;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.util.BlockingTaskExecutor;
import com.linecorp.armeria.common.util.SafeCloseable;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerPortBindException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.prometheus.PrometheusExpositionService;
import io.unitycatalog.server.audit.CapturingAuditSink;
import io.unitycatalog.server.auth.AllowingAuthorizer;
import io.unitycatalog.server.auth.annotation.AuthorizeExpression;
import io.unitycatalog.server.auth.annotation.ResponseAuthorizeFilter;
import io.unitycatalog.server.auth.decorator.UnityAccessDecorator;
import io.unitycatalog.server.base.BaseServerTest;
import io.unitycatalog.server.exception.BaseException;
import io.unitycatalog.server.exception.ErrorCode;
import io.unitycatalog.server.logging.RequestContextDataProvider;
import io.unitycatalog.server.observability.MetricsRegistries;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.junit.jupiter.api.Test;

public class UnityCatalogServerTest extends BaseServerTest {

  @Test
  public void opaCannotRunWithAuthorizationDisabled() {
    ServerProperties properties = new ServerProperties(serverProperties);
    properties.set(Property.AUTHORIZATION_PROVIDER, "opa");
    properties.set(Property.OPA_DECISION_URL, "http://localhost:8181/v1/data/uc/authz");
    assertThatThrownBy(
            () ->
                UnityCatalogServer.builder()
                    .port(0)
                    .serverProperties(properties)
                    .hibernateConfigurator(hibernateConfigurator)
                    .build())
        .isInstanceOf(BaseException.class)
        .hasMessage(
            "server.authorization=enable is required when server.authorization.provider=opa")
        .extracting(error -> ((BaseException) error).getErrorCode())
        .isEqualTo(ErrorCode.INVALID_ARGUMENT);
  }

  @Test
  public void opaRequiresAUrlAndReleasesNativeAuthorizerOnClose() {
    ServerProperties properties = new ServerProperties(serverProperties);
    properties.set(Property.AUTHORIZATION_ENABLED, "enable");
    properties.set(Property.AUTHORIZATION_PROVIDER, "opa");
    assertThatThrownBy(
            () ->
                UnityCatalogServer.builder()
                    .port(0)
                    .serverProperties(properties)
                    .hibernateConfigurator(hibernateConfigurator)
                    .build())
        .isInstanceOf(BaseException.class)
        .hasMessageContaining("server.authorization.opa.decision-url is required");

    properties.set(Property.OPA_DECISION_URL, "http://localhost:8181/v1/data/uc/authz");
    int connections =
        hibernateConfigurator.getDataSource().getHikariPoolMXBean().getActiveConnections();
    UnityCatalogServer server =
        UnityCatalogServer.builder()
            .port(0)
            .serverProperties(properties)
            .hibernateConfigurator(hibernateConfigurator)
            .build();
    try {
      assertThat(hibernateConfigurator.getDataSource().getHikariPoolMXBean().getActiveConnections())
          .isEqualTo(connections + 1);
    } finally {
      server.close();
    }
    assertThat(hibernateConfigurator.getDataSource().getHikariPoolMXBean().getActiveConnections())
        .isEqualTo(connections);
    assertThat(hibernateConfigurator.getSessionFactory().isOpen()).isTrue();
  }

  @Test
  public void startFailsWhenThePortIsTaken() throws Exception {
    // The server setUp() built writes certs.json and token.txt to configurationFolder, next to the
    // signing keys copied there.
    String keyId = Files.readString(configurationFolder.resolve("key_id.txt"));
    assertThat(Files.readString(configurationFolder.resolve("certs.json"))).contains(keyId);
    assertThat(configurationFolder.resolve("token.txt")).isNotEmptyFile();
    assertThatThrownBy(() -> UnityCatalogServer.builder().configurationFolder(null))
        .isInstanceOf(NullPointerException.class);

    try (ServerSocket occupied = new ServerSocket(0)) {
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(occupied.getLocalPort())
              .serverProperties(new ServerProperties(serverProperties))
              .hibernateConfigurator(hibernateConfigurator)
              .configurationFolder(configurationFolder)
              .build();
      // BaseServerTest retries only a ServerPortBindException, so the cause is part of the
      // contract, as is a close() that releases what the failed start left behind.
      assertThatThrownBy(server::start)
          .isInstanceOf(CompletionException.class)
          .hasCauseInstanceOf(ServerPortBindException.class);
      assertThatCode(server::close).doesNotThrowAnyException();
    }
  }

  @Test
  public void failedConstructionClosesTheAuditSink() {
    Properties audited = new Properties();
    audited.putAll(serverProperties);
    audited.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    CapturingAuditSink.reset();
    // The decorator runs inside the Repositories constructor, after the sink is created.
    assertThatThrownBy(
            () ->
                UnityCatalogServer.builder()
                    .port(0)
                    .serverProperties(new ServerProperties(audited))
                    .hibernateConfigurator(hibernateConfigurator)
                    .configurationFolder(configurationFolder)
                    .fileOperationsDecorator(
                        fileOperations -> {
                          throw new IllegalStateException("decorator failed");
                        })
                    .build())
        .hasMessage("decorator failed");
    assertThat(CapturingAuditSink.CLOSE_COUNT.get()).isEqualTo(1);
  }

  @Test
  public void closeClosesTheAuditSinkOnce() {
    Properties audited = new Properties();
    audited.putAll(serverProperties);
    audited.setProperty(Property.AUDIT_SINK.getKey(), CapturingAuditSink.class.getName());
    CapturingAuditSink.reset();
    UnityCatalogServer server =
        UnityCatalogServer.builder()
            .port(0)
            .serverProperties(new ServerProperties(audited))
            .hibernateConfigurator(hibernateConfigurator)
            .configurationFolder(configurationFolder)
            .build();
    server.start();
    server.close();
    server.close();
    assertThat(CapturingAuditSink.CLOSE_COUNT.get()).isEqualTo(1);
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
  public void logLinesCarryTheRequestId() {
    // Log4j finds the provider through ServiceLoader, the same lookup as here.
    assertThat(ServiceLoader.load(ContextDataProvider.class))
        .anyMatch(RequestContextDataProvider.class::isInstance);
    RequestContextDataProvider provider = new RequestContextDataProvider();
    ServiceRequestContext ctx = ServiceRequestContext.of(HttpRequest.of(HttpMethod.GET, "/"));
    try (SafeCloseable ignored = ctx.push()) {
      assertThat(provider.supplyContextData())
          .containsExactly(Map.entry("request_id", ctx.id().text()));
    }
    assertThat(provider.supplyContextData()).isEmpty();
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

    UnfilteredResponseService service = new UnfilteredResponseService();
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
                  .annotate("unfiltered", service)
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

        for (String query : List.of("", "?include_browse=false")) {
          var denied = sendRawGet(serverConfig, "/api/unfiltered/denied" + query);
          assertThat(denied.statusCode()).isEqualTo(403);
          TestUtils.assertHttpApiException(denied, ErrorCode.PERMISSION_DENIED, "Access denied.");
        }
        assertThat(service.deniedCalls.get()).isZero();
      }
    } finally {
      blockingTaskExecutor.shutdown();
    }
  }

  public static class UnfilteredResponseService implements UnityCatalogRestService {
    private final AtomicInteger deniedCalls = new AtomicInteger();

    @Get("/denied")
    @AuthorizeExpression("false")
    @ResponseAuthorizeFilter(isList = false, browse = "true")
    public HttpResponse deniedResponse(@Param("include_browse") Optional<Boolean> includeBrowse) {
      deniedCalls.incrementAndGet();
      return HttpResponse.ofJson(Map.of("sensitive_data", "must-not-leak"));
    }

    @Get("")
    @AuthorizeExpression("true")
    @ResponseAuthorizeFilter(isList = false)
    public HttpResponse unfilteredResponse() {
      return HttpResponse.ofJson(Map.of("sensitive_data", "must-not-leak"));
    }
  }
}
