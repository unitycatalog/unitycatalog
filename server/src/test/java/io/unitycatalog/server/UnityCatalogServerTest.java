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
import io.unitycatalog.server.migrate.SchemaMigrator;
import io.unitycatalog.server.observability.MetricsRegistries;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.service.UnityCatalogRestService;
import io.unitycatalog.server.utils.ServerProperties;
import io.unitycatalog.server.utils.ServerProperties.Property;
import io.unitycatalog.server.utils.TestUtils;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.core.util.ContextDataProvider;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
  public void opaRequiresAUrlAndReleasesNativeAuthorizerOnClose() throws Exception {
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
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection()) {
      connection.createStatement().execute("CREATE TABLE callers_table (id INT)");
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
    }
    try {
      assertThat(hibernateConfigurator.getDataSource().getHikariPoolMXBean().getActiveConnections())
          .isEqualTo(connections + 1);
    } finally {
      server.close();
    }
    // A database the caller injected stays the caller's, with its data.
    try (Connection connection = hibernateConfigurator.getDataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT id FROM callers_table");
      statement.execute("DROP TABLE callers_table");
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
    }
    assertThat(hibernateConfigurator.getDataSource().getHikariPoolMXBean().getActiveConnections())
        .isEqualTo(connections);
    assertThat(hibernateConfigurator.getSessionFactory().isOpen()).isTrue();
  }

  /**
   * A server that builds its own database configuration refuses an H2 file that does not exist, and
   * does not create it.
   */
  @Test
  public void startRefusesAMissingH2FileWithoutCreatingIt(@TempDir Path dir) throws Exception {
    Path hibernateFile = dir.resolve("hibernate.properties");
    Files.writeString(
        hibernateFile,
        "hibernate.connection.driver_class=org.h2.Driver\n"
            + "hibernate.connection.url=jdbc:h2:file:"
            + dir.resolve("typo")
            + "\n");

    assertThatThrownBy(() -> ownedServer(new Properties(), hibernateFile))
        .hasMessageContaining("H2 database file that hibernate.connection.url names is not found");

    try (var files = Files.list(dir)) {
      assertThat(files).containsExactly(hibernateFile);
    }
  }

  /**
   * An in-memory test database lasts as long as its server, whether server.env=test or a missing
   * hibernate.properties chose it. A server's database is its own: closing one server leaves
   * another that is running with its tables.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void closingAServerDropsItsOwnInMemoryTestDatabaseOnly(
      boolean serverEnvTest, @TempDir Path dir) throws Exception {
    Properties properties = new Properties();
    Path hibernateFile = dir.resolve("hibernate.properties");
    if (serverEnvTest) {
      properties.setProperty(Property.SERVER_ENV.getKey(), "test");
    }

    UnityCatalogServer first = ownedServer(properties, hibernateFile);
    String firstUrl = databaseUrl(first);
    try (UnityCatalogServer second = ownedServer(properties, hibernateFile)) {
      assertThat(databaseUrl(second)).isNotEqualTo(firstUrl);
      try (Connection connection = DriverManager.getConnection(firstUrl, "", "")) {
        connection.createStatement().execute("CREATE TABLE first_servers_data (id INT)");
      }
      first.close();

      assertThat(tableCount(firstUrl, "FIRST_SERVERS_DATA")).isZero();
      assertThat(tableCount(databaseUrl(second), "UC_CATALOGS")).isOne();
    }
  }

  /** A database that the configuration names is never dropped: another server may use it. */
  @Test
  public void closingAServerLeavesAConfiguredInMemoryDatabaseAlone(@TempDir Path dir)
      throws Exception {
    String url = "jdbc:h2:mem:configured;DB_CLOSE_DELAY=-1";
    Path hibernateFile = dir.resolve("hibernate.properties");
    Files.writeString(
        hibernateFile,
        "hibernate.connection.driver_class=org.h2.Driver\nhibernate.connection.url=" + url + "\n");

    ownedServer(new Properties(), hibernateFile).close();

    assertThat(tableCount(url, "UC_CATALOGS")).isOne();
  }

  private static String databaseUrl(UnityCatalogServer server) {
    return server
        .hibernateConfigurator()
        .getHibernateProperties()
        .getProperty("hibernate.connection.url");
  }

  private static int tableCount(String url, String table) throws Exception {
    try (Connection connection = DriverManager.getConnection(url, "", "");
        ResultSet tables =
            connection
                .createStatement()
                .executeQuery(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '"
                        + table
                        + "'")) {
      tables.next();
      return tables.getInt(1);
    }
  }

  /**
   * Whether the database is in memory is decided from the connection the pool opens. A data source
   * class's file URL wins over a leftover in-memory {@code hibernate.connection.url}, and closing
   * the server, after a start or a refused start, leaves that file database alone.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void closingAServerLeavesAFileDatabaseAloneWhenAnotherUrlSettingIsInMemory(
      boolean schemaSetUp, @TempDir Path dir) throws Exception {
    Path hibernateFile = dir.resolve("hibernate.properties");
    String fileUrl = "jdbc:h2:file:" + dir.resolve("db");
    Files.writeString(
        hibernateFile,
        "hibernate.hikari.dataSourceClassName=org.h2.jdbcx.JdbcDataSource\n"
            + "hibernate.hikari.dataSource.url="
            + fileUrl
            + "\n"
            + "hibernate.connection.url=jdbc:h2:mem:leftover;DB_CLOSE_DELAY=-1\n");
    try (Connection connection = DriverManager.getConnection(fileUrl, "", "");
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE kept_data (id INT)");
      statement.execute("INSERT INTO kept_data VALUES (1)");
    }
    if (schemaSetUp) {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL(fileUrl);
      new SchemaMigrator(dataSource).upgradeSchema();
    }

    if (schemaSetUp) {
      ownedServer(new Properties(), hibernateFile).close();
    } else {
      assertThatThrownBy(() -> ownedServer(new Properties(), hibernateFile))
          .hasMessageContaining("no Unity Catalog schema");
    }

    try (Connection connection = DriverManager.getConnection(fileUrl, "", "");
        ResultSet rows =
            connection.createStatement().executeQuery("SELECT COUNT(*) FROM kept_data")) {
      rows.next();
      assertThat(rows.getInt(1)).isEqualTo(1);
    }
  }

  /**
   * A server that builds its own database configuration from {@code hibernateFile}. {@code
   * TestServer} injects the configurator; this builds one as the launcher does.
   */
  private UnityCatalogServer ownedServer(Properties properties, Path hibernateFile) {
    return UnityCatalogServer.builder()
        .port(0)
        .serverProperties(new ServerProperties(properties))
        .configurationFolder(configurationFolder)
        .hibernatePropertiesFile(hibernateFile)
        .build();
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
