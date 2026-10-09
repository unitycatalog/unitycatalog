package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiClientBuilder;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.hadoop.internal.id.IcebergPlanCredId;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class IcebergPlanCredentialFetcherTest {
  private static final long EXPIRATION = 4_102_444_800_000L;

  private HttpServer server;

  @AfterEach
  void stopServer() {
    if (server != null) {
      server.stop(0);
    }
  }

  @Test
  void fetchesAwsAzureAndGcsCredentialsWithPlanAndBearerToken() throws Exception {
    List<String> queries = new ArrayList<>();
    List<String> authorizationHeaders = new ArrayList<>();
    AtomicReference<String> token = new AtomicReference<>("token-1");
    String body =
        "{\"storage-credentials\":["
            + "{\"prefix\":\"s3://bucket/table\",\"config\":{"
            + "\"s3.access-key-id\":\"ak\",\"s3.secret-access-key\":\"sk\","
            + "\"s3.session-token\":\"st\",\"s3.session-token-expires-at-ms\":\""
            + EXPIRATION
            + "\"}},"
            + "{\"prefix\":\"abfss://container@acct.dfs.core.windows.net/table\",\"config\":{"
            + "\"adls.sas-token.acct\":\"sas\","
            + "\"adls.sas-token-expires-at-ms.acct\":\""
            + EXPIRATION
            + "\"}},"
            + "{\"prefix\":\"gs://bucket/table\",\"config\":{"
            + "\"gcs.oauth2.token\":\"oauth\",\"gcs.oauth2.token-expires-at\":\""
            + EXPIRATION
            + "\"}}]}";
    String endpoint =
        startServer(
            exchange -> {
              queries.add(exchange.getRequestURI().getRawQuery());
              authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
              respond(exchange, 200, body);
            });

    GenericCredentialFetcher fetcher = fetcher(endpoint, "plan id/1", tokenProvider(token));
    List<GenericCredential> credentials = fetcher.createCredentials();

    assertThat(credentials).hasSize(3);
    assertThat(credentials.get(0))
        .isEqualTo(new AwsCredential("ak", "sk", "st", EXPIRATION, "s3://bucket/table"));
    assertThat(credentials.get(1))
        .isEqualTo(
            new AzureCredential(
                "sas", EXPIRATION, "abfss://container@acct.dfs.core.windows.net/table"));
    assertThat(credentials.get(2))
        .isEqualTo(new GcsCredential("oauth", EXPIRATION, "gs://bucket/table"));
    assertThat(queries).containsExactly("planId=plan%20id%2F1");
    assertThat(authorizationHeaders).containsExactly("Bearer token-1");

    token.set("token-2");
    fetcher.createCredentials();
    assertThat(authorizationHeaders).containsExactly("Bearer token-1", "Bearer token-2");
  }

  @Test
  void preservesExistingEndpointQueryAndCredentialOrder() throws Exception {
    String body =
        "{\"storage-credentials\":["
            + awsCredential("s3://bucket/table", Long.toString(EXPIRATION))
            + ","
            + awsCredential("s3://bucket/table/partition", Long.toString(EXPIRATION))
            + "]}";
    AtomicReference<String> query = new AtomicReference<>();
    String endpoint =
        startServer(
                exchange -> {
                  query.set(exchange.getRequestURI().getRawQuery());
                  respond(exchange, 200, body);
                })
            + "?warehouse=w";

    List<GenericCredential> credentials =
        fetcher(endpoint, "plan-1", tokenProvider(new AtomicReference<>("tok")))
            .createCredentials();

    assertThat(credentials)
        .extracting(GenericCredential::prefix)
        .containsExactly("s3://bucket/table", "s3://bucket/table/partition");
    assertThat(query.get()).isEqualTo("warehouse=w&planId=plan-1");
  }

  @Test
  void propagatesHttpErrorsWithoutExposingResponseAsCredentials() throws Exception {
    String endpoint = startServer(exchange -> respond(exchange, 403, "{\"error\":\"denied\"}"));

    assertThatThrownBy(
            () ->
                fetcher(endpoint, "plan-1", tokenProvider(new AtomicReference<>("tok")))
                    .createCredentials())
        .isInstanceOf(ApiException.class)
        .satisfies(error -> assertThat(((ApiException) error).getCode()).isEqualTo(403));
  }

  @Test
  void rejectsMissingCredentialsAndMalformedExpiration() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    String endpoint =
        startServer(
            exchange -> {
              if (requests.getAndIncrement() == 0) {
                respond(exchange, 200, "{\"storage-credentials\":[]}");
              } else {
                respond(
                    exchange,
                    200,
                    "{\"storage-credentials\":["
                        + awsCredential("s3://bucket/table", "not-a-number")
                        + "]}");
              }
            });
    GenericCredentialFetcher fetcher =
        fetcher(endpoint, "plan-1", tokenProvider(new AtomicReference<>("tok")));

    assertThatThrownBy(fetcher::createCredentials)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("no storage credentials");
    assertThatThrownBy(fetcher::createCredentials)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("epoch milliseconds");
  }

  private GenericCredentialFetcher fetcher(
      String endpoint, String planId, TokenProvider tokenProvider) {
    ApiClient apiClient =
        ApiClientBuilder.create().uri(serverBaseUri()).tokenProvider(tokenProvider).build();
    return GenericCredentialFetcher.forIcebergPlan(
        new IcebergPlanCredId(EMPTY_CRED_CONTEXT_ID, endpoint, planId), apiClient);
  }

  private String startServer(ExchangeHandler handler) throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/iceberg/v1/ns/table/credentials", handler::handle);
    server.start();
    return serverBaseUri() + "/iceberg/v1/ns/table/credentials";
  }

  private String serverBaseUri() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private static TokenProvider tokenProvider(AtomicReference<String> token) {
    return new TokenProvider() {
      @Override
      public void initialize(Map<String, String> configs) {}

      @Override
      public String accessToken() {
        return token.get();
      }

      @Override
      public Map<String, String> configs() {
        return Map.of("type", "test");
      }
    };
  }

  private static String awsCredential(String prefix, String expiration) {
    return "{\"prefix\":\""
        + prefix
        + "\",\"config\":{\"s3.access-key-id\":\"ak\","
        + "\"s3.secret-access-key\":\"sk\",\"s3.session-token\":\"st\","
        + "\"s3.session-token-expires-at-ms\":\""
        + expiration
        + "\"}}";
  }

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  @FunctionalInterface
  private interface ExchangeHandler {
    void handle(HttpExchange exchange) throws IOException;
  }
}
