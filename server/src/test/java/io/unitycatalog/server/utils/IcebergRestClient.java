package io.unitycatalog.server.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.server.base.ServerConfig;
import io.unitycatalog.server.service.iceberg.IcebergObjectMapper;
import java.net.http.HttpResponse;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequestParser;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;

/**
 * Minimal Iceberg REST catalog client for tests.
 *
 * <p>The UC REST and Delta APIs have generated SDK clients; the Iceberg REST catalog does not, so
 * tests drive it over raw HTTP. This builds on {@link TestUtils#sendRaw} (the shared raw-HTTP
 * sender used across the test suite) for transport -- base path, bearer auth from the {@link
 * ServerConfig}, request execution -- and adds Iceberg path building and {@link
 * IcebergObjectMapper} (de)serialization. Construct one instance per authenticated principal, from
 * that principal's {@link ServerConfig}.
 *
 * <p>Each method returns the native Iceberg response type (or {@code void}/{@code boolean}) and, on
 * a non-2xx response, throws the SDK's {@link ApiException} -- the same exception the UC/Delta
 * clients throw -- carrying the HTTP status ({@link ApiException#getCode()}) and the Iceberg error
 * body. CRUD tests read typed results directly; access-control tests assert on the status.
 */
public class IcebergRestClient {

  private static final ObjectMapper MAPPER = IcebergObjectMapper.mapper();
  private static final String BASE_PATH = "/api/2.1/unity-catalog/iceberg";

  private final ServerConfig config;

  public IcebergRestClient(ServerConfig config) {
    this.config = config;
  }

  // --- endpoints ------------------------------------------------------------------------------

  public ConfigResponse config(String warehouse) throws ApiException {
    return parse(get("/v1/config?warehouse=" + warehouse), ConfigResponse.class);
  }

  public ListNamespacesResponse listNamespaces(String catalog) throws ApiException {
    return parse(get(namespacesPath(catalog)), ListNamespacesResponse.class);
  }

  public GetNamespaceResponse loadNamespace(String catalog, String namespace) throws ApiException {
    return parse(get(namespacePath(catalog, namespace)), GetNamespaceResponse.class);
  }

  public CreateNamespaceResponse createNamespace(String catalog, String namespace)
      throws ApiException {
    return createNamespace(
        catalog, CreateNamespaceRequest.builder().withNamespace(Namespace.of(namespace)).build());
  }

  public CreateNamespaceResponse createNamespace(String catalog, CreateNamespaceRequest request)
      throws ApiException {
    return parse(post(namespacesPath(catalog), toJson(request)), CreateNamespaceResponse.class);
  }

  public ListTablesResponse listTables(String catalog, String namespace) throws ApiException {
    return parse(get(tablesPath(catalog, namespace)), ListTablesResponse.class);
  }

  public LoadTableResponse loadTable(String catalog, String namespace, String table)
      throws ApiException {
    return parse(get(tablePath(catalog, namespace, table)), LoadTableResponse.class);
  }

  public boolean tableExists(String catalog, String namespace, String table) throws ApiException {
    HttpResponse<String> response = head(tablePath(catalog, namespace, table));
    int code = response.statusCode();
    // The Iceberg REST spec returns 204 on existence; the UC server currently returns 200. Accept
    // any 2xx so this is robust to either. 404 means "does not exist"; anything else is an error.
    if (code >= 200 && code < 300) {
      return true;
    }
    if (code == 404) {
      return false;
    }
    throw new ApiException(code, "Iceberg REST request failed", null, response.body());
  }

  public LoadTableResponse createTable(String catalog, String namespace, CreateTableRequest request)
      throws ApiException {
    return parse(post(tablesPath(catalog, namespace), toJson(request)), LoadTableResponse.class);
  }

  public LoadTableResponse updateTable(
      String catalog, String namespace, String table, UpdateTableRequest request)
      throws ApiException {
    return parse(
        post(tablePath(catalog, namespace, table), toJson(request)), LoadTableResponse.class);
  }

  public void dropTable(String catalog, String namespace, String table) throws ApiException {
    checkSuccess(delete(tablePath(catalog, namespace, table)));
  }

  public void reportMetrics(
      String catalog, String namespace, String table, ReportMetricsRequest report)
      throws ApiException {
    checkSuccess(
        post(
            tablePath(catalog, namespace, table) + "/metrics",
            ReportMetricsRequestParser.toJson(report)));
  }

  // --- plumbing -------------------------------------------------------------------------------

  @SneakyThrows
  private static <T> T parse(HttpResponse<String> response, Class<T> type) throws ApiException {
    checkSuccess(response);
    return MAPPER.readValue(response.body(), type);
  }

  private static void checkSuccess(HttpResponse<String> response) throws ApiException {
    int code = response.statusCode();
    if (code < 200 || code >= 300) {
      throw new ApiException(code, "Iceberg REST request failed", null, response.body());
    }
  }

  @SneakyThrows
  private static String toJson(Object body) {
    return MAPPER.writeValueAsString(body);
  }

  @SneakyThrows
  private HttpResponse<String> get(String path) {
    return TestUtils.sendRaw(config, "GET", BASE_PATH + path, Optional.empty());
  }

  @SneakyThrows
  private HttpResponse<String> head(String path) {
    return TestUtils.sendRaw(config, "HEAD", BASE_PATH + path, Optional.empty());
  }

  @SneakyThrows
  private HttpResponse<String> delete(String path) {
    return TestUtils.sendRaw(config, "DELETE", BASE_PATH + path, Optional.empty());
  }

  @SneakyThrows
  private HttpResponse<String> post(String path, String jsonBody) {
    return TestUtils.sendRaw(config, "POST", BASE_PATH + path, Optional.of(jsonBody));
  }

  private static String namespacesPath(String catalog) {
    return "/v1/catalogs/" + catalog + "/namespaces";
  }

  private static String namespacePath(String catalog, String namespace) {
    return namespacesPath(catalog) + "/" + namespace;
  }

  private static String tablesPath(String catalog, String namespace) {
    return namespacePath(catalog, namespace) + "/tables";
  }

  private static String tablePath(String catalog, String namespace, String table) {
    return tablesPath(catalog, namespace) + "/" + table;
  }
}
